# 申报门户不确定提交·自动核对

面向合作资料接收人员的 Python 采集组件。提供草稿、单次提交与审批号回执查询，普通录入可通过本地门户完成。
输入样本都是本地合成数据，example 域名只表示来源身份，不需联网访问。

## 可靠批量自动操作

在单次填表能力之上，`batch` 命令提供可恢复的批量状态机。门户不承诺通用幂等请求头，
安全保证全部来自流程本身，而不是本地标记或请求头：

- **业务身份** = 审批单号 + 内容摘要（SHA-256），同单号不同内容拒绝覆盖。
- **提交意图先落库再调用**：提交后断线或响应丢失时停在 `SUBMIT_UNCERTAIN`，
  恢复时只做业务查询，绝不直接再次提交。
- **查询核对三分支**：单号与摘要都一致 → `VERIFIED`；同单号不同摘要 → `NEEDS_MANUAL`
  人工介入；查不到（门户声明非终态）或查询本身不确定 → 继续保持待核对。
- **可解释停止**：登录过期整批停止等待人工重新登录；人工确认、验证码、页面结构变化
  只停止相关条目并记录原因，不替人确认、不处理验证码。
- **本地协调**：每个批次一把非阻塞文件锁（flock），两名操作员同时启动时第二个被拒绝，
  进程崩溃时内核自动释放锁。
- **任意步骤可恢复**：草稿、提交意图、远端回执、本地状态和每次迁移都在 SQLite 事务内
  落盘并写入审计 journal；重新打开同一数据库即从最后一个已提交迁移继续。
- **日志不泄密**：结构化 JSON 日志只记业务事实，过滤器兜底擦除 cookie/token/
  authorization 等会话值；journal 不存任何会话字段。

```bash
# 一条命令走完正常批次（全部核实时退出码 0，有待核对/人工项时退出码 3）
python3 -m collector batch --input fixtures/portal.json --db var/batch.sqlite3 --state var/portal-state.json

# 只核对不确定条目（只查询、不发起任何新提交）
python3 -m collector batch --recheck --db var/batch.sqlite3 --state var/portal-state.json

# 人工重新登录后恢复（自动流程不会自行刷新登录态）
python3 -m collector batch --recheck --renew-session --db var/batch.sqlite3 --state var/portal-state.json

# 查看批次状态与审计日志
python3 -m collector status --db var/batch.sqlite3

# 端到端演示：断线待核对 → 查询不确定 → 登录过期停止 → 重新登录后核实
python3 -m collector batch-demo
```

退出码：`0` 全部核实；`3` 存在待核对或人工项；`4` 批次已在另一进程运行。
批次标识默认由输入中全部（单号、摘要）推导，同一份输入重复运行是幂等的。

## 安装与运行

使用 Python 3.11，依赖仅为标准库，SQLite 随解释器提供，无需安装第三方包。
从项目根目录执行；演示启动真实的回环 HTTP 来源、处理响应、事务入库后读取实际结果。
每次演示使用独立临时目录，结束后清理；通常两秒内结束，不回放历史输出。

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
python3 -m collector batch-demo
```

常规导入保留在本地数据库中，可重复运行；默认路径已忽略，不会进入版本库。

```bash
python3 -m collector import --input fixtures/sample.json --db var/records.sqlite3
python3 -m collector show --db var/records.sqlite3
python3 -m collector serve --db var/records.sqlite3 --port 8080
```

查询服务监听回环地址，`GET /records` 返回数据库中的记录；按 Ctrl-C 停止。

## 组件与数据

`collector/domain.py` 负责来源格式和领域校验；`storage.py` 保留原始对象、导入凭据和当前记录。
单次导入在同一事务内提交，异常时整体回滚；相同来源和原文只登记一次凭据。
当前记录以领域标识为键，同键再次导入替换当前观察值，不代表保留了完整修订历史。
`transport.py` 提供请求客户端和本地响应服务；服务按路径依次消费响应脚本，并记录实际请求。
`api.py` 提供只读查询入口。`fixtures/sample.json` 是当前导入器支持的输入。
其他附件描述同领域的合成页面、协议或事件，可以由调用方构造本地来源。
当前响应服务是顺序脚本工具，不自动推断认证、文件或表单业务；领域对象提供对应的本地交互。

批量层模块：`batch_types.py`（状态与异常）、`batch_store.py`（SQLite 状态机与 journal）、
`batch.py`（不确定提交协议引擎）、`synthetic.py`（脚本化/文件化合成门户）、
`locking.py`（flock 批次锁）、`logging_setup.py`（脱敏结构化日志）。
合成门户状态（草稿、回执、会话、剩余事件）持久化在 `--state` 指定的 JSON 文件中，
使“提交响应丢失后另一进程仍能查到真实回执”的恢复过程可端到端复现。
`fixtures/portal.json` 的 `query_semantics.not_found_is_final=false` 表示查无此单不是终态，
此时自动流程不会重新提交，只会保持待核对。

## 可复现运行

宿主机或容器均不需要网络。镜像准备好以后可以关闭外部网络再运行上述命令。
仓库内不含真实账号、新闻正文或人员资料；不要将生产凭据写入样本或日志。
