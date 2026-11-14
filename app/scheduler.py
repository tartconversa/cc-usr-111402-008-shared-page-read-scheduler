"""扫描层调度器：区间合并、在途读取共享、配额与取消隔离。

设计概览
========

查询（request）沿用 baseline 的逻辑读取契约：``read(name, version, start, length)``
的半开字节区间，可选 ``sha256``。调度器不改对象版本与字节范围接口，所有物理数据
仍通过传入的 reader 取得。

内部两级结构：

* **Chunk**：一个查询按 ``page_size`` 切成的有界子区间，是调度与公平性的最小单位。
  chunk 尺寸不超过单查询在途配额，因此任何 chunk 单独看都是可接纳的，不存在天生
  无法调度的工作。
* **Segment（段）**：同一不可变对象版本 ``(name, version)`` 上若干 chunk 的区间并集，
  对应一次物理 ``reader.read``。不同版本、不同对象的区间永不合并。段间允许存在空洞
  （多读的字节），空洞字节计入物理字节与缓冲预算。

合并窗口是**字节窗口**（``max_merge_gap`` / ``max_segment_size``），不是时间窗口：
合并只在提交、读取完成、取消等事件触发的 ``_pump`` 中发生，模块内没有任何固定等待
或 sleep，不靠“等一会儿碰碰运气”合并。

三类配额
--------

1. ``max_inflight``：同时进行的物理读取段数。
2. ``buffer_budget_bytes``：全局**在途物理缓冲**上限；已交付但未被查询取走的页字节
   另计为持有量（仍计入 ``buffered_bytes_now`` 与峰值统计），但不阻塞后续段派发——
   慢消费者由此受到单查询占用约束（下条）的背压，而不会把自身工作锁死。
3. ``max_query_bytes``：单个查询的占用上限（在途字节 + 已交付但未取走的持有字节），
   限制单查询读放大与慢消费者占用；巨型查询被切分为有界 chunk，其后续 chunk 被自身配额
   挡住时不阻塞其他查询（队首避让）。

合并时即检查：同一段内任一查询的 chunk 字节份额不得超过其单查询配额，因此段永远不会
因合并而变成“谁都无法接纳”的死段。

调度次序：段按最早入队序号 FIFO 扫描，当前不可接纳的段跳过、继续考察后续段（工作保持）。
每次完成事件都会重新 pump，被跳过的段只等待占用资源的在途读完成，因此小查询有可说明的
进展保证：它的每个段不超过页大小，不会被一个超大范围长期堵在队首。

记账规则（公开）
----------------

* 逻辑字节：查询请求区间长度之和。
* 物理字节（全局）：每次物理读取实际请求的字节数，含合并空洞与重试；同一段只计一次。
* 逐查询物理字节：该查询挂载过的每个段的**整段物理字节**（含它受益的空洞），因此各查询
  归因之和可能大于全局唯一物理字节，差额即共享节省。
* 交付切片是独立 ``bytes`` 复制，不存在 memoryview 别名：同一字节的每次真实复制都计入
  全局持有统计与该查询占用峰值，直到查询取走（``result``/``iter_pages``）或取消/失败。
  全局账户分两个口径：``_inflight_bytes``（在途物理段，受预算约束）与 ``_held_bytes``
  （已交付未取走的页）；``buffered_bytes_now`` 与峰值为两者之和。
* 段缓冲在全部切片复制完成后立即离开在途账户；全部等待者离开后，孤儿段读完即释放，
  不永久占额。

失败与重试
----------

短读、底层异常使段失败：仍有重试次数的 chunk 以**原区间、原对象版本**重新入队，新段的
身份是新的，绝不可能把旧版本数据拼入新版本；重试耗尽的查询收到原始异常。sha256 在查询
的完整原始区间上增量校验，失败只使该查询失败，不牵连同段等待者。
"""
import bisect
import hashlib
import threading
import queue
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from time import monotonic


class QueryCancelled(Exception):
    """查询被自身取消；共享同一段的其他查询不会收到此异常。"""


@dataclass
class Chunk:
    qid: int
    name: str
    version: str
    start: int
    end: int
    seq: int                 # 在所属查询内的序号
    enqueue_seq: int         # 全局公平序号（重试保留原值）
    attempts: int = 0

    @property
    def length(self):
        return self.end - self.start

    @property
    def key(self):
        return (self.name, self.version)


@dataclass
class Segment:
    sid: int
    name: str
    version: str
    start: int
    end: int
    waiters: list = field(default_factory=list)   # Chunk 列表
    enqueue_seq: int = 0
    running: bool = False

    @property
    def length(self):
        return self.end - self.start

    @property
    def key(self):
        return (self.name, self.version)


@dataclass
class QueryStats:
    logical_bytes: int = 0
    physical_bytes: int = 0          # 归因物理字节（挂载段的整段字节，含空洞）
    peak_inflight_bytes: int = 0     # 在途字节峰值（受 max_query_bytes 约束）
    peak_occupancy_bytes: int = 0    # 在途 + 持有 总占用峰值
    segments_joined: int = 0
    read_attempts: int = 0


@dataclass
class SchedulerStats:
    reads_issued: int = 0
    physical_bytes: int = 0          # 全局唯一物理读取字节（含重试、空洞）
    hole_bytes: int = 0              # 物理读取中不被任何等待区间覆盖的字节
    logical_bytes: int = 0
    shared_saved_bytes: int = 0      # 各查询归因物理之和 - 全局物理
    buffered_bytes_now: int = 0
    peak_buffered_bytes: int = 0
    inflight_now: int = 0
    peak_inflight: int = 0


class QueryHandle:
    """单个查询的句柄。线程安全，可被查询自身线程取消/取结果。"""

    def __init__(self, scheduler, qid, request):
        self._scheduler = scheduler
        self.qid = qid
        self.query = request.get('query')
        self.request = request
        self._cond = threading.Condition()
        self._pages = deque()          # 已交付、待取走的 bytes（按序）
        self._terminal = False
        self._exc = None
        self._cancelled = False
        self.stats = QueryStats(logical_bytes=request['length'])

    # --- 仅由协调者线程调用 ---
    def _push_page(self, data):
        with self._cond:
            self._pages.append(data)
            self._cond.notify_all()

    def _set_terminal(self, exc=None, cancelled=False):
        with self._cond:
            self._terminal = True
            self._exc = exc
            self._cancelled = cancelled
            self._cond.notify_all()

    def _clear_pages(self):
        with self._cond:
            self._pages.clear()
            self._cond.notify_all()

    def _touch_peak(self, occupancy):
        if occupancy > self.stats.peak_occupancy_bytes:
            self.stats.peak_occupancy_bytes = occupancy

    # --- 调用方接口 ---
    def done(self):
        """查询已进入终结态（成功/失败/取消）；已交付页仍可被取走。"""
        with self._cond:
            return self._terminal

    def delivered(self):
        """终结态的成功查询且全部页已交付（可能尚未取走）。"""
        with self._cond:
            return self._terminal and self._exc is None and not self._cancelled

    def cancelled(self):
        with self._cond:
            return self._cancelled

    def cancel(self):
        """请求取消本查询。幂等；只影响本查询的等待与引用。"""
        self._scheduler._post(('cancel', self.qid))

    def iter_pages(self, timeout=None):
        """按序产出查询页；取走一页即释放它占用的全局缓冲。"""
        deadline = None if timeout is None else monotonic() + timeout
        while True:
            with self._cond:
                while not self._pages and not self._terminal:
                    remaining = None if deadline is None else deadline - monotonic()
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError('等待查询页超时')
                    self._cond.wait(remaining)
                if self._pages:
                    data = self._pages.popleft()
                else:
                    if self._exc is not None:
                        raise self._exc
                    return
            self._scheduler._post(('consumed', self.qid, len(data)))
            yield data

    def result(self, timeout=None):
        """返回与原始请求区间精确对应的完整字节；失败时抛出原始异常。"""
        return b''.join(self.iter_pages(timeout=timeout))


class ScanScheduler:
    """扫描层合并/共享调度器。

    参数均为正整数（字节）或正的并发数：

    :param reader: 具备 ``read(name, version, start, length) -> bytes`` 的对象读取端，
                   如 baseline.ObjectReader / GateReader。
    :param max_inflight: 同时在途物理读取上限。
    :param buffer_budget_bytes: 全局物理缓冲字节上限（在途段 + 未取走交付物）。
    :param max_query_bytes: 单查询在途字节上限；须 >= page_size。
    :param page_size: 查询切分 chunk 的最大字节。
    :param max_merge_gap: 同版本相邻区间允许跨越多大的空洞仍合并为一次读取。
    :param max_segment_size: 单个物理读取段最大字节；须 <= buffer_budget_bytes。
    :param max_attempts: 每个 chunk 的物理读取尝试总次数（>=1）。
    """

    def __init__(self, reader, *, max_inflight=4, buffer_budget_bytes=1 << 24,
                 max_query_bytes=1 << 22, page_size=1 << 20,
                 max_merge_gap=1 << 18, max_segment_size=1 << 23,
                 max_attempts=2):
        if not all(isinstance(v, int) and v > 0 for v in (
                max_inflight, buffer_budget_bytes, max_query_bytes,
                page_size, max_segment_size, max_attempts)):
            raise ValueError('调度参数必须为正整数')
        if not isinstance(max_merge_gap, int) or max_merge_gap < 0:
            raise ValueError('max_merge_gap 必须为非负整数')
        if page_size > max_query_bytes:
            raise ValueError('page_size 不能超过 max_query_bytes，否则单块无法被接纳')
        if page_size > max_segment_size:
            raise ValueError('page_size 不能超过 max_segment_size，否则单块无法成段')
        if page_size > buffer_budget_bytes:
            raise ValueError('page_size 不能超过 buffer_budget_bytes，否则单块无法被调度')
        if max_segment_size > buffer_budget_bytes:
            raise ValueError('max_segment_size 不能超过 buffer_budget_bytes，否则段永远无法调度')
        self._reader = reader
        self.max_inflight = max_inflight
        self.buffer_budget = buffer_budget_bytes
        self.max_query_bytes = max_query_bytes
        self.page_size = page_size
        self.max_merge_gap = max_merge_gap
        self.max_segment_size = max_segment_size
        self.max_attempts = max_attempts

        self._events = queue.Queue()
        self._pool = ThreadPoolExecutor(max_workers=max_inflight,
                                        thread_name_prefix='scan-seg')
        self._coord = threading.Thread(target=self._coordinate,
                                       name='scan-coordinator', daemon=True)
        self._seq = 0
        self._sid = 0
        self._qid = 0
        self._queries = {}          # qid -> _QueryState
        self._handles = {}
        self._queued = []           # 未挂载段的 Chunk（按 enqueue_seq 有序）
        self._pending = {}          # key -> list[Segment]，按 start 有序，未派发
        self._running = {}          # sid -> Segment
        self._inflight = 0
        self._inflight_bytes = 0    # 在途物理段字节（受 buffer_budget 约束）
        self._held_bytes = 0        # 已交付、未被查询取走的页字节（独立口径）
        self._peak_buffered = 0     # max(在途+持有)
        self._peak_inflight = 0
        self._reads = 0
        self._physical_bytes = 0
        self._hole_bytes = 0
        self._logical_bytes = 0
        self._closed = False
        self._fatal = None
        self._coord.start()

    # ---------- 对外 API ----------

    def submit(self, request):
        """提交一个 baseline 形态的请求，立即返回 QueryHandle。"""
        if self._closed:
            raise RuntimeError('调度器已关闭')
        for field_name in ('object', 'version', 'start', 'length'):
            if field_name not in request:
                raise ValueError(f'请求缺少字段 {field_name}')
        start, length = request['start'], request['length']
        if type(start) is not int or type(length) is not int or start < 0 or length < 0:
            raise ValueError('读取范围必须为非负整数')
        qid = self._qid
        self._qid += 1
        handle = QueryHandle(self, qid, request)
        self._logical_bytes += length
        self._post(('submit', qid, request, handle))
        return handle

    def run(self, requests):
        """便捷入口：提交全部请求并收集结果，返回与入参对齐的 [{query, bytes}]。"""
        handles = [self.submit(r) for r in requests]
        return [{'query': r.get('query'), 'bytes': h.result()}
                for r, h in zip(requests, handles)]

    def close(self, drain_timeout=30.0):
        """停止接收新查询；在途读取排空后释放线程。幂等。

        ``drain_timeout`` 限定等待在途物理读完成的时长；超时后协调者不再阻塞
        关闭流程，线程池随之回收（调用方应确保读取端最终会返回或自行放弃）。
        """
        if self._closed:
            return
        self._closed = True
        self._post(('stop', drain_timeout))
        self._coord.join(timeout=drain_timeout + 5)
        self._pool.shutdown(wait=True)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def stats(self):
        attributed = sum(h.stats.physical_bytes for h in self._handles.values())
        buffered_now = self._inflight_bytes + self._held_bytes
        return SchedulerStats(
            reads_issued=self._reads,
            physical_bytes=self._physical_bytes,
            hole_bytes=self._hole_bytes,
            logical_bytes=self._logical_bytes,
            shared_saved_bytes=attributed - self._physical_bytes,
            buffered_bytes_now=buffered_now,
            peak_buffered_bytes=self._peak_buffered,
            inflight_now=self._inflight,
            peak_inflight=self._peak_inflight,
        )

    def _touch_buffer_peak(self):
        total = self._inflight_bytes + self._held_bytes
        if total > self._peak_buffered:
            self._peak_buffered = total

    def _post(self, event):
        self._events.put(event)

    # ---------- 协调者（单线程拥有全部调度状态） ----------

    def _coordinate(self):
        while True:
            # 阻塞取第一个事件；stop 单独成批，保证此前提交/完成事件都已处理
            first = self._events.get()
            if first[0] == 'stop':
                self._shutdown(first[1] if len(first) > 1 else 30.0)
                return
            # 排空当前已到达的其余事件：同一积压批次内的提交先合并再派发，
            # 实现无定时器、无固定等待的合并。
            batch = [first]
            pending_stop = None
            while True:
                try:
                    ev = self._events.get_nowait()
                except queue.Empty:
                    break
                if ev[0] == 'stop':
                    pending_stop = ev
                    break
                batch.append(ev)
            try:
                for event in batch:
                    kind = event[0]
                    if kind == 'submit':
                        self._on_submit(event)
                    elif kind == 'cancel':
                        self._on_cancel(event)
                    elif kind == 'consumed':
                        self._on_consumed(event)
                    elif kind == 'finished':
                        self._on_finished(event)
                self._pump()
            except BaseException as exc:
                # 协调者不能静默死亡而把等待者永久挂起
                self._fatal = exc
                for state in list(self._queries.values()):
                    self._fail_query(state, exc)
                self._queued = []
                self._pending.clear()
            if pending_stop is not None:
                self._shutdown(pending_stop[1] if len(pending_stop) > 1 else 30.0)
                return

    def _shutdown(self, drain_timeout):
        # 未派发工作失败；在途读取有界排空，其等待者按正常路径交付
        inflight_qids = {c.qid for seg in self._running.values()
                         for c in seg.waiters}
        for qid, state in list(self._queries.items()):
            if not state.terminal and qid not in inflight_qids:
                self._fail_query(state, RuntimeError('调度器已关闭'))
        self._queued = []
        self._pending.clear()
        deadline = time.monotonic() + drain_timeout
        while self._running and time.monotonic() < deadline:
            try:
                ev = self._events.get(timeout=0.1)
            except queue.Empty:
                continue
            if ev[0] == 'finished':
                self._on_finished(ev)
            elif ev[0] == 'cancel':
                self._on_cancel(ev)
            elif ev[0] == 'consumed':
                self._on_consumed(ev)
        # 超时仍在途的读取：其等待者失败，避免句柄永久挂起
        for state in list(self._queries.values()):
            if not state.terminal:
                self._fail_query(state, RuntimeError('调度器关闭时读取仍在途'))
            elif state.cancelled or state.handle._exc is not None:
                self._release_held(state)
        # 成功查询已交付的页留给调用方取走，不强制回收

    def _on_submit(self, event):
        _, qid, request, handle = event
        name, version = request['object'], request['version']
        start, length = request['start'], request['length']
        state = _QueryState(qid, name, version, start, length,
                            request.get('sha256'), handle)
        self._queries[qid] = state
        self._handles[qid] = handle
        if length == 0:
            if request.get('sha256') is not None and \
                    hashlib.sha256(b'').hexdigest() != request['sha256']:
                self._fail_query(state, ValueError('页校验失败'))
                return
            handle._push_page(b'')
            self._complete_query(state)
            return
        end = start + length
        for i in range(start, end, self.page_size):
            chunk_end = min(i + self.page_size, end)
            self._seq += 1
            state.total_chunks += 1
            self._queued.append(Chunk(qid, name, version, i, chunk_end,
                                      state.total_chunks - 1, self._seq))

    def _on_cancel(self, event):
        qid = event[1]
        state = self._queries.get(qid)
        if state is None or state.terminal:
            return
        # 1. 尚未挂载的 chunk 由 pump 统一过滤（见 _pump），这里摘除待派发段引用
        self._detach_from_pending(qid)
        # 2. 在途段：只摘本查询等待关系；物理读不可中断，全局占用保留到读完
        for seg in self._running.values():
            seg.waiters = [c for c in seg.waiters if c.qid != qid]
        state.inflight_bytes = 0
        # 3. 释放本查询持有的全部已交付字节（其他查询不受影响）
        self._release_held(state)
        state.cancelled = True
        state.terminal = True
        state.handle._set_terminal(QueryCancelled('查询已取消'), cancelled=True)

    def _on_consumed(self, event):
        _, qid, nbytes = event
        state = self._queries.get(qid)
        # 取消/失败可能已整体释放持有量；迟到的 consumed 事件不得把账记成负数
        if state is None or state.held_bytes < nbytes or self._held_bytes < nbytes:
            return
        state.held_bytes -= nbytes
        self._held_bytes -= nbytes
        state.handle._touch_peak(state.inflight_bytes + state.held_bytes)
        self._touch_buffer_peak()

    def _on_finished(self, event):
        _, sid, data, error = event
        seg = self._running.pop(sid, None)
        if seg is None:
            return
        self._inflight -= 1
        self._inflight_bytes -= seg.length     # 段缓冲离开在途账户
        if not seg.waiters:
            self._touch_buffer_peak()
            return                            # 孤儿段：结果丢弃，额度已全部释放
        if error is not None:
            self._segment_failed(seg, error)
            return
        if not isinstance(data, (bytes, bytearray)) or len(data) != seg.length:
            self._segment_failed(seg, ValueError('对象短读'))
            return
        # 精确切片交付给每个仍在等待的 chunk
        for chunk in seg.waiters:
            state = self._queries[chunk.qid]
            if state.terminal:
                continue
            offset = chunk.start - seg.start
            page = bytes(data[offset:offset + chunk.length])
            state.inflight_bytes -= chunk.length
            # 页交给句柄后不再阻塞后续调度；持有量只用于全局统计与占用峰值
            state.held_bytes += chunk.length
            self._held_bytes += chunk.length
            state.pages[chunk.seq] = page
            if state.hasher is not None:
                state.hasher.update(page)
            state.handle._touch_peak(state.inflight_bytes + state.held_bytes)
            self._touch_buffer_peak()
            self._push_ready_pages(state)

    def _segment_failed(self, seg, error):
        survivors = []
        for chunk in seg.waiters:
            state = self._queries[chunk.qid]
            if state.terminal:
                continue
            state.inflight_bytes -= chunk.length
            chunk.attempts += 1
            if chunk.attempts >= self.max_attempts:
                self._fail_query(state, error)
            else:
                survivors.append(chunk)
        # 重试以原 (name, version, start, length) 重新入队，保留公平序号
        self._queued.extend(survivors)
        self._queued.sort(key=lambda c: c.enqueue_seq)

    # ---------- pump：挂载合并 + 配额派发（事件驱动，无等待） ----------

    def _pump(self):
        # 已终结（取消/失败）查询的残余 chunk 不再产生任何读取
        self._queued = [c for c in self._queued
                        if not self._queries[c.qid].terminal]
        # 阶段一：待调度 chunk 挂载到同版本的待派发段，或新建段
        for chunk in self._queued:
            self._attach(chunk)
        self._queued = []
        # 阶段二：按公平序号扫描；不可接纳则跳过（队首避让，工作保持）
        ordered = sorted(
            (seg for segs in self._pending.values() for seg in segs),
            key=lambda s: s.enqueue_seq)
        for seg in ordered:
            if seg.running or seg not in self._pending.get(seg.key, ()):
                continue
            if self._admissible(seg):
                self._dispatch(seg)

    def _attach(self, chunk):
        """把 chunk 挂到同版本待派发段：只与左邻/右邻合并一次，不递归吸收。

        不变量：每个 chunk 在任意时刻至多属于一个段；段内 waiters 互不重复。
        """
        segs = self._pending.setdefault(chunk.key, [])
        starts = [s.start for s in segs]
        idx = bisect.bisect_right(starts, chunk.start)
        left = segs[idx - 1] if idx else None
        right = segs[idx] if idx < len(segs) else None
        candidate = self._choose_merge(chunk, left, right)
        if candidate is not None:
            candidate.start = min(candidate.start, chunk.start)
            candidate.end = max(candidate.end, chunk.end)
            # 防御性去重：同一 chunk 绝不重复挂载
            if not any(c is chunk for c in candidate.waiters):
                candidate.waiters.append(chunk)
            candidate.enqueue_seq = min(candidate.enqueue_seq, chunk.enqueue_seq)
            segs.remove(candidate)
            bisect.insort(segs, candidate, key=lambda s: s.start)
            return
        seg = Segment(self._sid, chunk.name, chunk.version,
                      chunk.start, chunk.end, [chunk], chunk.enqueue_seq)
        self._sid += 1
        bisect.insort(segs, seg, key=lambda s: s.start)

    def _choose_merge(self, chunk, left, right):
        """在左右邻居中选出可合并者；两者都可时优先左邻，保持区间有序增长。"""
        for candidate in (left, right):
            if candidate is None or candidate.running:
                continue
            gap = max(0, candidate.start - chunk.end,
                      chunk.start - candidate.end)
            if gap > self.max_merge_gap:
                continue
            new_size = (max(candidate.end, chunk.end)
                        - min(candidate.start, chunk.start))
            if new_size > self.max_segment_size:
                continue
            if not self._merge_admissible(candidate, [chunk], new_size):
                continue
            return candidate
        return None

    def _merge_admissible(self, seg, extra_waiters, new_size):
        """合并后若立即派发是否仍满足在途预算与全部相关查询的在途配额。"""
        if self._inflight_bytes + new_size > self.buffer_budget:
            return False
        totals = {}
        for c in seg.waiters:
            totals[c.qid] = totals.get(c.qid, 0) + c.length
        for c in extra_waiters:
            totals[c.qid] = totals.get(c.qid, 0) + c.length
        for qid, nbytes in totals.items():
            state = self._queries[qid]
            # 准入只看在途；已交付持有页不再阻塞派发（避免慢消费者自死锁）
            if state.inflight_bytes + nbytes > self.max_query_bytes:
                return False
        return True

    def _admissible(self, seg):
        if self._inflight >= self.max_inflight:
            return False
        # 全局预算只约束在途物理段；已交付持有页计入统计但不阻塞后续段派发。
        if self._inflight_bytes + seg.length > self.buffer_budget:
            return False
        per_query = {}
        for chunk in seg.waiters:
            per_query[chunk.qid] = per_query.get(chunk.qid, 0) + chunk.length
        for qid, nbytes in per_query.items():
            state = self._queries[qid]
            if state.inflight_bytes + nbytes > self.max_query_bytes:
                return False
        return True

    def _dispatch(self, seg):
        seg.running = True
        self._pending[seg.key].remove(seg)
        if not self._pending[seg.key]:
            del self._pending[seg.key]
        self._running[seg.sid] = seg
        self._inflight += 1
        self._peak_inflight = max(self._peak_inflight, self._inflight)
        self._inflight_bytes += seg.length
        self._touch_buffer_peak()
        covered = self._covered_length(seg)
        self._reads += 1
        self._physical_bytes += seg.length
        self._hole_bytes += seg.length - covered
        for chunk in seg.waiters:
            state = self._queries[chunk.qid]
            state.inflight_bytes += chunk.length
            if state.inflight_bytes > state.handle.stats.peak_inflight_bytes:
                state.handle.stats.peak_inflight_bytes = state.inflight_bytes
            state.handle.stats.physical_bytes += seg.length
            state.handle.stats.segments_joined += 1
            state.handle.stats.read_attempts += 1
            state.handle._touch_peak(state.inflight_bytes + state.held_bytes)
        self._pool.submit(self._run_segment, seg)

    @staticmethod
    def _covered_length(seg):
        """段内全部等待区间的并集长度（同段区间同版本，按字节合并计算）。"""
        intervals = sorted((c.start, c.end) for c in seg.waiters)
        covered = 0
        cur_start = cur_end = None
        for lo, hi in intervals:
            if cur_start is None:
                cur_start, cur_end = lo, hi
            elif lo <= cur_end:
                cur_end = max(cur_end, hi)
            else:
                covered += cur_end - cur_start
                cur_start, cur_end = lo, hi
        if cur_start is not None:
            covered += cur_end - cur_start
        return covered

    def _run_segment(self, seg):
        try:
            data = self._reader.read(seg.name, seg.version,
                                     seg.start, seg.length)
            error = None
        except Exception as exc:                      # 传播给受影响等待者
            data, error = None, exc
        self._events.put(('finished', seg.sid, data, error))

    # ---------- 查询完成/失败辅助 ----------

    def _push_ready_pages(self, state):
        """按 chunk 序号连续交付，并做增量 sha256 校验。"""
        handle = state.handle
        while state.next_seq in state.pages:
            page = state.pages.pop(state.next_seq)
            state.next_seq += 1
            if state.sha256 is not None and state.next_seq == state.total_chunks:
                if state.hasher.hexdigest() != state.sha256:
                    self._fail_query(state, ValueError('页校验失败'))
                    return
            handle._push_page(page)
            if state.next_seq == state.total_chunks:
                self._complete_query(state)
                return

    def _complete_query(self, state):
        if state.terminal:
            return
        state.terminal = True
        state.handle._set_terminal()

    def _fail_query(self, state, error):
        if state.terminal:
            return
        self._detach_from_pending(state.qid)
        self._release_held(state)
        state.inflight_bytes = 0
        state.terminal = True
        state.handle._set_terminal(error)

    def _detach_from_pending(self, qid):
        for key, segs in list(self._pending.items()):
            kept = []
            for seg in segs:
                seg.waiters = [c for c in seg.waiters if c.qid != qid]
                if seg.waiters:
                    kept.append(seg)
            if kept:
                self._pending[key] = kept
            else:
                del self._pending[key]

    def _release_held(self, state):
        if state.held_bytes:
            self._held_bytes -= state.held_bytes
            state.held_bytes = 0
        state.pages.clear()
        state.handle._clear_pages()
        self._touch_buffer_peak()


class _QueryState:
    def __init__(self, qid, name, version, start, length, sha256, handle):
        self.qid = qid
        self.name = name
        self.version = version
        self.start = start
        self.length = length
        self.sha256 = sha256
        self.handle = handle
        self.hasher = hashlib.sha256() if sha256 is not None else None
        self.total_chunks = 0
        self.next_seq = 0
        self.pages = {}
        self.inflight_bytes = 0
        self.held_bytes = 0
        self.terminal = False
        self.cancelled = False
