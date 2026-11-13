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
