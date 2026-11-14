# 资料与接口

## 内容身份

入口app.service.inspect(data)不修改调用方资料，返回input_digest及result。
input_digest是对完整输入JSON按键排序、UTF-8、无空白序列化的SHA-256。数组顺序保留；禁止NaN和无穷，磁盘JSON重复字段拒绝。
app.storage.freeze返回独立副本。输入快照和对象版本由业务字段明确声明，内容摘要不会替调用方创造版本。

## 现有业务约定

objects按name/version提供UTF-8文本作为本地对象字节替身。requests含query、object、version、start、length，按字节半开范围；越界和短读拒绝。baseline.read_one逐请求读取并核对可选sha256。ObjectReader统计调用范围，GateReader使用显式threading.Event控制放行或抛错，不使用sleep；调度层可替换为异步接口但应兼容逻辑读取契约。预算中物理缓冲与返回页引用的重叠如何记账须公开，不能把同一字节的多次实际复制忽略。

## 受限谓词

app.semantics.evaluate(row, expression)返回True/False/None，None为unknown。
eq/lt/le/gt/ge含field和value，缺字段按null；比较双方必须同类型，null比较结果unknown。
is_null只有field；and/or含args数组，not含arg，按三值逻辑计算。空and为true，空or为false。
where只接受结果严格为true的行。expression为null表示不限制。

## 现有执行链

app.service.inspect → storage.freeze/digest → baseline.execute。sample.json描述的是现有可执行输入，服务输出附资料摘要。
新增能力可以增加入口和计划结构，保留现有调用及输出语义；扩展格式应单独声明版本。

## 扫描层共享调度器（app.scheduler，新增）

ScanScheduler 在不改对象版本与字节范围接口的前提下，为多个查询合并区间并共享在途读取。

* 身份隔离：只在同一不可变对象版本键 `(object, version)` 内合并；不同对象或版本永不共享。
  物理读取仍调用 `reader.read(name, version, start, length)`，重试钉住原键，不跨版本拼接。
* 两级结构：查询按 `page_size` 切成有界 chunk；同版本 chunk 按字节合并窗口
  （`max_merge_gap`、`max_segment_size`）合并为 segment，一个 segment 对应一次物理读取。
  合并在提交/完成/取消事件触发的泵中完成，模块内没有定时等待或 sleep。
* 精确对应：每个查询取回的字节按其原始 `[start, start+length)` 半开区间从段缓冲精确切片，
  切片为独立 bytes 复制；合并多读的空洞计入物理字节。
* 配额：`max_inflight` 限在途物理段数；`buffer_budget_bytes` 限在途物理缓冲；
  `max_query_bytes` 限单查询在途字节。段按公平序号 FIFO 扫描，不可接纳则跳过（队首避让），
  小查询的段不超过页大小，具有可说明的进展保证。
* 取消隔离：取消只摘除本查询在待派发段与在途段上的等待关系并释放其持有页；同段其他查询
  不受影响。物理读不可中断，孤儿段读完即释放，不留下永久占用额度。
* 记账（公开可查证）：
  - 逐查询 `QueryStats`：`logical_bytes`（请求区间）、`physical_bytes`（挂载段整段字节，
    含空洞）、`peak_inflight_bytes`、`peak_occupancy_bytes`（在途+持有）、`segments_joined`、
    `read_attempts`。
  - 全局 `SchedulerStats`：`physical_bytes`（唯一物理读取，含空洞与重试）、`hole_bytes`、
    `logical_bytes`、`shared_saved_bytes = 各查询归因物理之和 − 全局物理`、
    `buffered_bytes_now`（在途物理+未取走持有页）、`peak_buffered_bytes`、在途计数与峰值。
  - 同一字节的每次真实复制都计入持有量，直到查询取走（result/iter_pages）、取消或失败；
    不存在 memoryview 别名。
* 失败传播：短读、底层异常传播给段上全部仍在等待的查询；sha256 在查询完整原始区间上
  增量校验，失败只终结该查询。协调者线程的未预期异常会终结全部等待者而非静默挂起。
