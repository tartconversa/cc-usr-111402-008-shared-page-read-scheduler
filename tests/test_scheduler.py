"""扫描层调度器测试。

可控读取端全部基于事件放行（Event），不依赖固定 sleep 制造合并窗口：
合并由调度器在提交/完成/取消事件上即时完成，测试只等待可观察状态到达。
"""
import hashlib
import threading
import time
import unittest
from collections import deque

from app.baseline import ObjectReader, GateReader
from app.scheduler import ScanScheduler, QueryCancelled


OBJECTS = [
    {'name': 'carbon', 'version': 'v1', 'text': 'abcdefghij'},          # 10 字节
    {'name': 'carbon', 'version': 'v2', 'text': 'ABCDEFGHIJ'},
    {'name': 'lake', 'version': 'v1', 'text': '0123456789'},
    {'name': 'o1', 'version': 'v1', 'text': 'abcdefgh'},
    {'name': 'o2', 'version': 'v1', 'text': 'abcdefgh'},
    {'name': 'o3', 'version': 'v1', 'text': 'abcdefgh'},
    {'name': 'o4', 'version': 'v1', 'text': 'abcdefgh'},
]
OBJECT_BYTES = {(o['name'], o['version']): o['text'].encode('utf-8')
                for o in OBJECTS}


def wait_until(condition, timeout=5.0, message='条件未达成'):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.005)
    raise AssertionError(message)


class ControlledReader:
    """记录调用、逐次放行、可注入异常或短读的受控读取端。"""

    def __init__(self, objects=OBJECTS):
        self._objects = {(o['name'], o['version']): o['text'].encode('utf-8')
                         for o in objects}
        self.calls = []
        self._lock = threading.Lock()
        self._gates = deque()          # (release_event, call_record)
        self._arrived = threading.Condition(self._lock)
        self._fail_once = {}           # (name, version, start, length) -> Exception
        self._fail_always = {}         # 同上：每次都失败
        self._short_once = set()       # 同上键的集合：返回短读
        self.closed = False

    def _gate_for(self, key):
        gate = threading.Event()
        with self._lock:
            call = {'key': key, 'released': False}
            self.calls.append(call)
            self._gates.append((gate, call))
            self._arrived.notify_all()
        return gate, call

    def wait_for_pending(self, count=1, timeout=5.0):
        """等待门控上出现 count 个在途读取。"""
        deadline = time.monotonic() + timeout
        with self._lock:
            while len(self._gates) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._arrived.wait(timeout=remaining):
                    raise AssertionError(
                        f'在途读取数未达到 {count}（当前 {len(self._gates)}）')

    def read(self, name, version, start, length):
        data = self._objects[(name, version)]
        if type(start) is not int or type(length) is not int \
                or min(start, length) < 0 or start + length > len(data):
            raise ValueError('读取范围越界')
        gate, call = self._gate_for((name, version, start, length))
        if not gate.wait(timeout=10):
            raise TimeoutError('测试读取门未放行')
        rule_key = (name, version, start, length)
        if rule_key in self._fail_always:
            raise self._fail_always[rule_key]
        if rule_key in self._fail_once:
            raise self._fail_once.pop(rule_key)
        if rule_key in self._short_once:
            self._short_once.discard(rule_key)
            return data[start:start + max(length - 1, 0)]
        return data[start:start + length]

    def release_one(self):
        with self._lock:
            if not self._gates:
                raise AssertionError('没有在途读取可放行')
            gate, call = self._gates.popleft()
            call['released'] = True
        gate.set()

    def release_all(self):
        with self._lock:
            gates = list(self._gates)
            self._gates.clear()
        for gate, call in gates:
            call['released'] = True
            gate.set()

    def fail_once(self, name, version, start, length, exc):
        self._fail_once[(name, version, start, length)] = exc

    def fail_always(self, name, version, start, length, exc):
        self._fail_always[(name, version, start, length)] = exc

    def short_once(self, name, version, start, length):
        self._short_once.add((name, version, start, length))

    def pending_count(self):
        with self._lock:
            return len(self._gates)

    def drain_until_calls(self, expected, timeout=5.0):
        """逐批放行在途读取，直到累计调用数达到 expected，再排空残余在途门。"""
        deadline = time.monotonic() + timeout
        self.wait_for_pending(timeout=timeout)
        while len(self.calls) < expected:
            if time.monotonic() > deadline:
                raise AssertionError(
                    f'读取调用数 {len(self.calls)} 未达到 {expected}')
            if self.pending_count():
                self.release_one()
            else:
                time.sleep(0.005)
        self.release_all()

    def call_ranges(self):
        with self._lock:
            return [c['key'] for c in self.calls]


def req(query, name='carbon', version='v1', start=0, length=10, sha256=None):
    r = {'query': query, 'object': name, 'version': version,
         'start': start, 'length': length}
    if sha256 is not None:
        r['sha256'] = sha256
    return r


class CorrectnessTests(unittest.TestCase):
    def test_results_match_baseline_and_exact_ranges(self):
        reader = ObjectReader(OBJECTS)
        requests = [req('q1', start=1, length=3), req('q2', start=3, length=2)]
        with ScanScheduler(reader, page_size=4, max_merge_gap=4) as sched:
            out = sched.run(requests)
        self.assertEqual([o['bytes'] for o in out], [b'bcd', b'de'])
        # 与原始范围精确对应，逐字节核对（不同页大小都应如此）
        with ScanScheduler(ObjectReader(OBJECTS), page_size=3) as s2:
            for r in requests:
                got = s2.run([r])[0]['bytes']
                expect = OBJECT_BYTES[('carbon', 'v1')][r['start']:r['start'] + r['length']]
                self.assertEqual(got, expect)

    def test_multi_chunk_reassembles_in_order(self):
        reader = ObjectReader(OBJECTS)
        with ScanScheduler(reader, page_size=3, max_merge_gap=0) as sched:
            handle = sched.submit(req('big', start=0, length=10))
            self.assertEqual(handle.result(timeout=5),
                             OBJECT_BYTES[('carbon', 'v1')])

    def test_zero_length_read_issues_no_io(self):
        reader = ControlledReader()
        with ScanScheduler(reader, page_size=4) as sched:
            handle = sched.submit(req('empty', start=2, length=0))
            self.assertEqual(handle.result(timeout=5), b'')
            self.assertEqual(reader.call_ranges(), [])
            self.assertEqual(handle.stats.logical_bytes, 0)

    def test_merging_overlapping_queries_single_read_with_hole_accounting(self):
        reader = ControlledReader()
        # 两区间 [0,4) 与 [6,10)：空洞 [4,6) 共 2 字节，允许合并窗口 >= 2
        with ScanScheduler(reader, page_size=16, max_merge_gap=4) as sched:
            h1 = sched.submit(req('a', start=0, length=4))
            h2 = sched.submit(req('b', start=6, length=4))
            wait_until(lambda: reader.pending_count() == 1)
            self.assertEqual(reader.call_ranges(),
                             [('carbon', 'v1', 0, 10)])
            reader.release_all()
            self.assertEqual(h1.result(timeout=5), b'abcd')
            self.assertEqual(h2.result(timeout=5), b'ghij')
            stats = sched.stats()
            self.assertEqual(stats.reads_issued, 1)
            self.assertEqual(stats.physical_bytes, 10)
            self.assertEqual(stats.hole_bytes, 2)
            self.assertEqual(h1.stats.physical_bytes, 10)
            self.assertEqual(h2.stats.physical_bytes, 10)
            self.assertEqual(stats.shared_saved_bytes, 10)

    def test_hole_accounting_for_three_disjoint_intervals(self):
        reader = ControlledReader()
        # 三个区间 [0,2)、[4,6)、[8,10) 合并为一次读取：物理 10，覆盖 6，空洞 4
        with ScanScheduler(reader, page_size=16, max_merge_gap=4) as sched:
            handles = [
                sched.submit(req('a', start=0, length=2)),
                sched.submit(req('b', start=4, length=2)),
                sched.submit(req('c', start=8, length=2)),
            ]
            wait_until(lambda: reader.pending_count() == 1)
            self.assertEqual(reader.call_ranges(),
                             [('carbon', 'v1', 0, 10)])
            reader.release_all()
            self.assertEqual([h.result(timeout=5) for h in handles],
                             [b'ab', b'ef', b'ij'])
            stats = sched.stats()
            self.assertEqual(stats.hole_bytes, 4)
            self.assertEqual(stats.physical_bytes - stats.hole_bytes, 6)

    def test_gap_beyond_window_not_merged(self):
        reader = ControlledReader()
        with ScanScheduler(reader, page_size=16, max_merge_gap=1) as sched:
            h1 = sched.submit(req('a', start=0, length=2))
            h2 = sched.submit(req('b', start=8, length=2))
            wait_until(lambda: reader.pending_count() == 2)
            ranges = reader.call_ranges()
            self.assertEqual(sorted(ranges),
                             [('carbon', 'v1', 0, 2), ('carbon', 'v1', 8, 2)])
            reader.release_all()
            self.assertEqual(h1.result(timeout=5), b'ab')
            self.assertEqual(h2.result(timeout=5), b'ij')
            self.assertEqual(sched.stats().reads_issued, 2)

    def test_versions_and_objects_never_share(self):
        reader = ControlledReader()
        with ScanScheduler(reader, page_size=16, max_merge_gap=16) as sched:
            h1 = sched.submit(req('a', 'carbon', 'v1', 0, 10))
            h2 = sched.submit(req('b', 'carbon', 'v2', 0, 10))
            h3 = sched.submit(req('c', 'lake', 'v1', 0, 10))
            wait_until(lambda: reader.pending_count() == 3)
            keys = sorted((c[0], c[1]) for c in reader.call_ranges())
            self.assertEqual(keys,
                             [('carbon', 'v1'), ('carbon', 'v2'), ('lake', 'v1')])
            reader.release_all()
            self.assertEqual(h1.result(timeout=5), b'abcdefghij')
            self.assertEqual(h2.result(timeout=5), b'ABCDEFGHIJ')
            self.assertEqual(h3.result(timeout=5), b'0123456789')

    def test_sha256_verified_on_full_range(self):
        data = OBJECT_BYTES[('carbon', 'v1')]
        good = hashlib.sha256(data).hexdigest()
        with ScanScheduler(ObjectReader(OBJECTS), page_size=3) as sched:
            ok = sched.submit(req('ok', start=0, length=10, sha256=good))
            bad = sched.submit(req('bad', start=0, length=10, sha256='0' * 64))
            self.assertEqual(ok.result(timeout=5), data)
            with self.assertRaisesRegex(ValueError, '校验'):
                bad.result(timeout=5)
            self.assertFalse(ok.cancelled())


class CancellationTests(unittest.TestCase):
    def test_cancel_inflight_does_not_cancel_peer(self):
        reader = ControlledReader()
        with ScanScheduler(reader, page_size=16, max_merge_gap=8) as sched:
            h1 = sched.submit(req('a', start=0, length=4))
            h2 = sched.submit(req('b', start=2, length=4))
            wait_until(lambda: reader.pending_count() == 1)   # 合并为一段
            h1.cancel()
            with self.assertRaises(QueryCancelled):
                h1.result(timeout=5)
            # 读取仍在途，h2 不被连带取消
            self.assertFalse(h2.done())
            self.assertEqual(sched.stats().inflight_now, 1)
            reader.release_all()
            self.assertEqual(h2.result(timeout=5), b'cdef')
            wait_until(lambda: sched.stats().inflight_now == 0)
            # consumed 事件是异步入账的，等其处理完后全局缓冲应归零
            wait_until(lambda: sched.stats().buffered_bytes_now == 0)

    def test_cancel_pending_removes_segment_and_releases_quota(self):
        reader = ControlledReader()
        # 单在途槽：q1 占住，q2 在排队；取消 q2 不产生读取
        with ScanScheduler(reader, max_inflight=1, page_size=16) as sched:
            h1 = sched.submit(req('a', start=0, length=2))
            wait_until(lambda: reader.pending_count() == 1)
            h2 = sched.submit(req('b', start=8, length=2))
            h2.cancel()
            with self.assertRaises(QueryCancelled):
                h2.result(timeout=5)
            self.assertEqual(reader.call_ranges(),
                             [('carbon', 'v1', 0, 2)])
            reader.release_all()
            self.assertEqual(h1.result(timeout=5), b'ab')
            self.assertEqual(sched.stats().inflight_now, 0)

    def test_cancel_idempotent_and_double_cancel_safe(self):
        reader = ControlledReader()
        with ScanScheduler(reader) as sched:
            h = sched.submit(req('a', start=0, length=3))
            h.cancel()
            h.cancel()
            with self.assertRaises(QueryCancelled):
                h.result(timeout=5)
            reader.release_all()

    def test_orphan_segment_releases_budget_after_all_waiters_leave(self):
        reader = ControlledReader()
        with ScanScheduler(reader, page_size=16) as sched:
            h1 = sched.submit(req('a', start=0, length=3))
            h2 = sched.submit(req('b', start=4, length=3))
            wait_until(lambda: reader.pending_count() == 1)
            buffered_during = sched.stats().buffered_bytes_now
            self.assertEqual(buffered_during, 7)       # 合并段 [0,7)
            h1.cancel()
            h2.cancel()
            with self.assertRaises(QueryCancelled):
                h1.result(timeout=5)
            with self.assertRaises(QueryCancelled):
                h2.result(timeout=5)
            reader.release_all()
            wait_until(lambda: sched.stats().inflight_now == 0)
            self.assertEqual(sched.stats().buffered_bytes_now, 0)   # 无永久占用

    def test_cancel_race_with_consumed_event_keeps_accounts_nonnegative(self):
        reader = ControlledReader()
        # 三段中只放行第一段：取走第一页（触发 consumed）后取消，
        # 后两段尚未满足；验证取消与迟到 consumed 不会把缓冲记成负数。
        with ScanScheduler(reader, page_size=4, max_segment_size=4,
                           max_merge_gap=0, max_inflight=3) as sched:
            h = sched.submit(req('a', start=0, length=10))
            reader.wait_for_pending(3)               # 三段同时在途
            reader.release_one()                     # 只放行第一段
            wait_until(lambda: len(h._pages) == 1)
            it = h.iter_pages(timeout=5)
            first = next(it)                         # 取走第一页，触发 consumed
            self.assertEqual(first, b'abcd')
            h.cancel()                               # 其余等待立即解除
            with self.assertRaises(QueryCancelled):
                list(it)
            reader.release_all()                     # 排空残余在途读
            wait_until(lambda: sched.stats().inflight_now == 0)
            self.assertGreaterEqual(sched.stats().buffered_bytes_now, 0)


class FailureAndRetryTests(unittest.TestCase):
    def test_reader_exception_propagates_to_all_waiters(self):
        reader = ControlledReader()
        with ScanScheduler(reader, page_size=16, max_attempts=1) as sched:
            h1 = sched.submit(req('a', start=0, length=3))
            h2 = sched.submit(req('b', start=2, length=3))
            wait_until(lambda: reader.pending_count() == 1)
            reader.fail_once('carbon', 'v1', 0, 5, OSError('读取失败'))
            reader.release_all()
            for h in (h1, h2):
                with self.assertRaisesRegex(OSError, '读取失败'):
                    h.result(timeout=5)
            self.assertEqual(sched.stats().inflight_now, 0)
            self.assertEqual(sched.stats().buffered_bytes_now, 0)

    def test_short_read_propagates(self):
        reader = ControlledReader()
        with ScanScheduler(reader, page_size=16, max_attempts=1) as sched:
            h = sched.submit(req('a', start=0, length=5))
            wait_until(lambda: reader.pending_count() == 1)
            reader.short_once('carbon', 'v1', 0, 5)
            reader.release_all()
            with self.assertRaisesRegex(ValueError, '短读'):
                h.result(timeout=5)

    def test_retry_keeps_original_version_and_range(self):
        reader = ControlledReader()
        # 第一次对 v1 [0,6) 失败；重试必须仍是 v1、同区间，且不得改读 v2
        reader.fail_once('carbon', 'v1', 0, 6, OSError('瞬时故障'))
        with ScanScheduler(reader, page_size=6, max_merge_gap=2,
                           max_attempts=2) as sched:
            h = sched.submit(req('a', 'carbon', 'v1', 0, length=6))
            wait_until(lambda: reader.pending_count() == 1)
            reader.release_all()                        # 首次尝试失败
            wait_until(lambda: reader.pending_count() == 1)
            reader.release_all()                        # 重试成功
            self.assertEqual(h.result(timeout=10), b'abcdef')
            ranges = reader.call_ranges()
            self.assertEqual(ranges,
                             [('carbon', 'v1', 0, 6), ('carbon', 'v1', 0, 6)])
            self.assertTrue(all(v == 'v1' for _, v, _, _ in ranges))
            self.assertEqual(h.stats.read_attempts, 2)

    def test_retry_exhaustion_returns_original_exception(self):
        reader = ControlledReader()
        reader.fail_always('carbon', 'v1', 0, 4, OSError('持久故障'))
        with ScanScheduler(reader, page_size=4, max_attempts=2) as sched:
            h = sched.submit(req('a', start=0, length=4))
            wait_until(lambda: reader.pending_count() == 1)
            reader.release_all()
            wait_until(lambda: reader.pending_count() == 1)
            reader.release_all()
            with self.assertRaisesRegex(OSError, '持久故障'):
                h.result(timeout=5)
            self.assertEqual(sched.stats().inflight_now, 0)
            self.assertEqual(sched.stats().buffered_bytes_now, 0)

    def test_checksum_failure_isolates_co_joined_query(self):
        reader = ControlledReader()
        with ScanScheduler(reader, page_size=16, max_merge_gap=8) as sched:
            good = sched.submit(req('good', start=0, length=3))
            bad = sched.submit(req('bad', start=2, length=3,
                                   sha256='0' * 64))
            wait_until(lambda: reader.pending_count() == 1)  # 同段
            reader.release_all()
            self.assertEqual(good.result(timeout=5), b'abc')
            with self.assertRaisesRegex(ValueError, '校验'):
                bad.result(timeout=5)
            self.assertEqual(sched.stats().reads_issued, 1)

    def test_gate_reader_error_path_compatible(self):
        # 与现有 GateReader 事件设施兼容
        gate = GateReader(OBJECTS)
        with ScanScheduler(gate, page_size=4) as sched:
            h = sched.submit(req('a', start=0, length=4))
            self.assertTrue(gate.started.wait(timeout=5))
            gate.error = OSError('门控失败')
            gate.release.set()
            with self.assertRaisesRegex(OSError, '门控失败'):
                h.result(timeout=10)


class QuotaAndFairnessTests(unittest.TestCase):
    def test_inflight_cap_never_exceeded(self):
        reader = ControlledReader()
        # 四个互不可合并的独立对象；在途槽仅 2
        with ScanScheduler(reader, max_inflight=2, page_size=2,
                           max_segment_size=2, max_merge_gap=0) as sched:
            handles = [sched.submit(req(f'q{i}', f'o{i + 1}', 'v1', 0, length=8))
                       for i in range(4)]
            wait_until(lambda: reader.pending_count() == 2)
            self.assertEqual(sched.stats().inflight_now, 2)
            total = 0
            while total < 16:                         # 4 对象 × 4 段
                self.assertLessEqual(sched.stats().inflight_now, 2)
                wait_until(lambda: reader.pending_count() >= 1)
                reader.release_one()
                total += 1
            reader.release_all()
            for h in handles:
                self.assertEqual(h.result(timeout=5), b'abcdefgh')
            self.assertLessEqual(sched.stats().peak_inflight, 2)

    def test_per_query_cap_bounds_occupancy(self):
        reader = ControlledReader()
        cap = 4
        with ScanScheduler(reader, page_size=2, max_segment_size=2,
                           max_query_bytes=cap, max_merge_gap=0) as sched:
            h = sched.submit(req('big', start=0, length=10))
            reader.drain_until_calls(5)               # 5 个 2 字节段
            self.assertEqual(h.result(timeout=5),
                             OBJECT_BYTES[('carbon', 'v1')])
            # 在途峰值不超过单查询在途配额；总占用峰值另含已交付持有的页
            self.assertLessEqual(h.stats.peak_inflight_bytes, cap)
            self.assertEqual(h.stats.peak_occupancy_bytes, 10)

    def test_huge_query_does_not_block_small_query_at_head(self):
        reader = ControlledReader()
        # 巨型查询先入队占满在途槽；小查询只有一段，泵按公平序号扫描且跳过
        # 不可接纳段（队首避让），因此小查询在槽位腾出来的第一个泵周期即被派发，
        # 不必等巨型查询的全部剩余段完成。
        with ScanScheduler(reader, max_inflight=2, page_size=2,
                           max_segment_size=2, max_merge_gap=0) as sched:
            big = sched.submit(req('big', start=0, length=10))
            wait_until(lambda: reader.pending_count() == 2)
            small = sched.submit(req('small', start=8, length=2))
            # 持续放行；小查询的段必须在巨查询完成之前进入在途
            deadline = time.monotonic() + 5
            small_dispatched = False
            while time.monotonic() < deadline and not big.done():
                if reader.pending_count():
                    if any(c[2] == 8 and c[3] == 2 for c in reader.call_ranges()):
                        small_dispatched = True
                    reader.release_one()
                else:
                    time.sleep(0.005)
            reader.release_all()
            self.assertTrue(small_dispatched, '小查询被巨型范围长期堵在队首')
            self.assertEqual(small.result(timeout=5), b'ij')
            self.assertEqual(big.result(timeout=5),
                             OBJECT_BYTES[('carbon', 'v1')])

    def test_buffer_budget_backpressures_slow_consumer(self):
        reader = ControlledReader()
        # 在途预算只够一个 2 字节段；消费者不取页时，在途物理缓冲不得超预算。
        with ScanScheduler(reader, max_inflight=4, page_size=2,
                           max_segment_size=2,
                           buffer_budget_bytes=2, max_merge_gap=0) as sched:
            h = sched.submit(req('slow', start=0, length=8))
            reader.wait_for_pending(1)
            reader.release_one()                         # 只放行第一段
            # 消费者停滞 2 秒：在途物理缓冲绝不超预算；门控至多留一个在途段
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                self.assertLessEqual(reader.pending_count(), 1)
                self.assertLessEqual(
                    sched.stats().buffered_bytes_now,
                    sched.buffer_budget + h.request['length'])
                time.sleep(0.01)
            reader.drain_until_calls(4)                  # 恢复后全部段完成
            pages = list(h.iter_pages(timeout=5))
            self.assertEqual(b''.join(pages), b'abcdefgh')
            self.assertLessEqual(sched.stats().peak_buffered_bytes, 8)

    def test_held_pages_count_until_consumed(self):
        reader = ControlledReader()
        with ScanScheduler(reader, page_size=2, max_segment_size=2,
                           max_merge_gap=0,
                           buffer_budget_bytes=2) as sched:
            h = sched.submit(req('a', start=0, length=6))
            reader.drain_until_calls(3)               # 三段全部放行完成
            wait_until(lambda: h.delivered())        # 页已全部交付但未取走
            # 结果已交付但未取走：持有字节计入全局缓冲统计
            self.assertEqual(sched.stats().buffered_bytes_now, 6)
            self.assertEqual(h.stats.peak_occupancy_bytes, 6)
            data = h.result(timeout=5)
            self.assertEqual(data, b'abcdef')
            wait_until(lambda: sched.stats().buffered_bytes_now == 0)

    def test_reconciliation_after_all_queries_finish(self):
        reader = ControlledReader()
        with ScanScheduler(reader, page_size=4, max_merge_gap=4) as sched:
            handles = [
                sched.submit(req('a', start=0, length=5)),
                sched.submit(req('b', start=3, length=5)),
                sched.submit(req('c', 'lake', 'v1', start=1, length=4)),
            ]
            # 三段（carbon 上两个重叠查询合并为一段 [0,8)，lake 一段 [1,5)）
            wait_until(lambda: reader.pending_count() == 2)
            ranges = sorted(reader.call_ranges())
            self.assertEqual(ranges,
                             [('carbon', 'v1', 0, 8), ('lake', 'v1', 1, 4)])
            reader.release_all()
            results = [h.result(timeout=5) for h in handles]
        self.assertEqual(results, [b'abcde', b'defgh', b'1234'])
        stats = sched.stats()
        self.assertEqual(stats.inflight_now, 0)
        self.assertEqual(stats.buffered_bytes_now, 0)
        self.assertEqual(stats.logical_bytes, 14)
        self.assertEqual(stats.reads_issued, 2)
        self.assertEqual(stats.physical_bytes,
                         sum(c[3] for c in reader.call_ranges()))
        for h in handles:
            self.assertEqual(h.stats.logical_bytes, h.request['length'])


class LifecycleTests(unittest.TestCase):
    def test_close_fails_pending_and_drains_inflight(self):
        reader = ControlledReader()
        # 单在途槽 + 不相接区间：h1 在途时 h2 只能排队；在 h1 完成事件被
        # 处理前关闭，使 h2 作为未派发工作失败，h1 的已交付结果不受影响。
        sched = ScanScheduler(reader, max_inflight=1, page_size=16,
                              max_merge_gap=0)
        h1 = sched.submit(req('a', start=0, length=2))
        h2 = sched.submit(req('b', start=4, length=2))
        reader.wait_for_pending(1)                     # h1 在途；h2 排队
        self.assertEqual(reader.pending_count(), 1)
        reader.release_one()                           # 放行 h1
        wait_until(lambda: h1.delivered())
        sched.close()
        self.assertEqual(h1.result(timeout=5), b'ab')  # 已交付结果保留
        # h2 要么未派发而失败，要么已在途后被关闭终结；二者都以 RuntimeError 体现
        with self.assertRaisesRegex(RuntimeError, '调度器'):
            h2.result(timeout=5)
        with self.assertRaisesRegex(RuntimeError, '调度器'):
            sched.submit(req('x'))
        reader.release_all()
        wait_until(lambda: reader.pending_count() == 0)

    def test_invalid_constructor_and_request_arguments(self):
        with self.assertRaisesRegex(ValueError, '正整数'):
            ScanScheduler(ObjectReader(OBJECTS), max_inflight=0)
        with self.assertRaisesRegex(ValueError, 'page_size'):
            ScanScheduler(ObjectReader(OBJECTS), page_size=8, max_query_bytes=4)
        with self.assertRaisesRegex(ValueError, 'max_segment_size'):
            ScanScheduler(ObjectReader(OBJECTS),
                          max_segment_size=10, buffer_budget_bytes=8)
        sched = ScanScheduler(ObjectReader(OBJECTS))
        sched.close()
        with self.assertRaisesRegex(ValueError, '非负整数'):
            sched2 = ScanScheduler(ObjectReader(OBJECTS))
            try:
                sched2.submit(req('bad', start=-1, length=2))
            finally:
                sched2.close()


class ConcurrencyStressTests(unittest.TestCase):
    def test_many_concurrent_queries_results_and_invariants(self):
        # 20 个查询交错提交、各自取消一部分；全程无固定合并等待。
        reader = ControlledReader()
        requests = [req(f'q{i}', start=i % 5, length=4) for i in range(20)]
        cancelled = set()
        with ScanScheduler(reader, max_inflight=3, page_size=2,
                           max_segment_size=4, max_merge_gap=2,
                           max_query_bytes=8, buffer_budget_bytes=8) as sched:
            handles = [sched.submit(r) for r in requests]
            # 后台持续放行读取门，直到全部查询终结
            def release_forever():
                while not all(h.done() for h in handles):
                    if reader.pending_count():
                        reader.release_one()
                    else:
                        time.sleep(0.005)
                reader.release_all()
            releaser = threading.Thread(target=release_forever, daemon=True)
            releaser.start()
            # 交错取消偶数查询
            for i, h in enumerate(handles):
                if i % 4 == 0:
                    h.cancel()
                    cancelled.add(i)
            for i, (r, h) in enumerate(zip(requests, handles)):
                if i in cancelled:
                    with self.assertRaises(QueryCancelled):
                        h.result(timeout=10)
                    continue
                got = h.result(timeout=10)
                expect = OBJECT_BYTES[('carbon', 'v1')][
                    r['start']:r['start'] + r['length']]
                self.assertEqual(got, expect)
            releaser.join(timeout=5)
            stats = sched.stats()
            # 全局账目不超过配额
            self.assertLessEqual(stats.peak_inflight, 3)
            self.assertEqual(stats.inflight_now, 0)
            self.assertEqual(stats.buffered_bytes_now, 0)

    def test_shared_savings_end_to_end(self):
        # 5 个高度重叠的同版本查询，合并窗口足够大时物理读取远少于逻辑总量
        reader = ControlledReader()
        requests = [req(f'q{i}', start=i, length=6) for i in range(5)]  # 都落在 10 字节内
        with ScanScheduler(reader, page_size=16, max_merge_gap=16) as sched:
            handles = [sched.submit(r) for r in requests]
            reader.wait_for_pending(1)
            self.assertEqual(reader.pending_count(), 1)       # 合并为单段
            reader.release_all()
            for r, h in zip(requests, handles):
                expect = OBJECT_BYTES[('carbon', 'v1')][
                    r['start']:r['start'] + r['length']]
                self.assertEqual(h.result(timeout=5), expect)
            stats = sched.stats()
            self.assertEqual(stats.reads_issued, 1)
            self.assertEqual(stats.physical_bytes, 10)        # 整段一次读
            self.assertEqual(stats.logical_bytes, 30)
            # 各查询归因物理之和（5*10）超过全局物理（10），差额即可查证的共享收益
            self.assertEqual(stats.shared_saved_bytes, 40)

    def test_retry_does_not_mix_versions_under_failure(self):
        # 段失败重试期间对象出现新版本：重试必须钉住原版本，不得拼接新版本数据
        reader = ControlledReader()
        reader.fail_always('carbon', 'v1', 0, 4, OSError('v1 故障'))
        with ScanScheduler(reader, page_size=4, max_attempts=3) as sched:
            h = sched.submit(req('a', 'carbon', 'v1', 0, length=4))
            for _ in range(3):                          # 三次尝试各有一个门
                reader.wait_for_pending(1)
                reader.release_one()
            with self.assertRaisesRegex(OSError, 'v1 故障'):
                h.result(timeout=10)
            # 全部尝试都访问 v1，没有一次改读 v2
            self.assertTrue(all(v == 'v1' for _, v, _, _ in reader.call_ranges()))
            self.assertEqual(len(reader.call_ranges()), 3)


if __name__ == '__main__':
    unittest.main()
