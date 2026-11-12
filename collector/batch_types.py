"""批量自动操作共享的类型定义。

状态机与异常独立成模块，避免引擎与合成门户之间循环导入。
本模块不依赖任何远端约定：门户不承诺幂等请求头，因此所有安全保证
都来自“先持久化意图、再以业务查询核对远端事实”的流程，而不是请求头。
"""


class State:
    """单条录入的本地状态。

    PENDING             已登记，尚未保存草稿
    DRAFTED             远端草稿已保存并核对摘要
    SUBMIT_UNCERTAIN    提交意图已持久化，提交调用已发出，但受理结果未知
    VERIFIED            已通过提交响应或业务查询核实远端回执
    NEEDS_MANUAL        查询到相同单号但摘要不同，必须人工介入
    BLOCKED_MANUAL      遇到人工确认/验证码/页面结构变化，自动流程停止
    """

    PENDING = 'PENDING'
    DRAFTED = 'DRAFTED'
    SUBMIT_UNCERTAIN = 'SUBMIT_UNCERTAIN'
    VERIFIED = 'VERIFIED'
    NEEDS_MANUAL = 'NEEDS_MANUAL'
    BLOCKED_MANUAL = 'BLOCKED_MANUAL'


TERMINAL_STATES = frozenset({State.VERIFIED, State.NEEDS_MANUAL, State.BLOCKED_MANUAL})


class StopReason:
    SESSION_EXPIRED = 'SESSION_EXPIRED'
    HUMAN_CONFIRMATION = 'HUMAN_CONFIRMATION'
    CAPTCHA = 'CAPTCHA'
    STRUCTURE_CHANGED = 'STRUCTURE_CHANGED'


class BlockReason:
    HUMAN_CONFIRMATION = 'HUMAN_CONFIRMATION'
    CAPTCHA = 'CAPTCHA'
    STRUCTURE_CHANGED = 'STRUCTURE_CHANGED'
    REMOTE_DIGEST_MISMATCH = 'REMOTE_DIGEST_MISMATCH'


class BatchError(Exception):
    """批量流程的预期内错误基类。"""


class BatchAlreadyRunning(BatchError):
    """同一批次已有另一个操作员持锁运行。"""


class SessionExpired(PermissionError):
    """登录态过期，需要人工重新登录后再恢复。"""


class HumanConfirmationRequired(BatchError):
    """表单要求人工确认，自动流程不得代确认。"""


class CaptchaRequired(BatchError):
    """出现验证码，自动流程一律不处理。"""


class PortalStructureChanged(BatchError):
    """页面结构或返回报文结构变化，无法安全继续。"""


# 视为“结果不确定”的瞬时网络异常：绝不能据此判断远端失败或成功。
TRANSIENT_ERRORS = (ConnectionError, TimeoutError, OSError)

# 需要人工处理、自动流程必须停止的异常类型。
MANUAL_ERRORS = (HumanConfirmationRequired, CaptchaRequired, PortalStructureChanged)
