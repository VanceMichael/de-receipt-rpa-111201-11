"""本地合成门户。

只在本机运行，保存草稿、接收提交、按审批单号查询回执，并按
fixtures/portal.json 的事件脚本注入故障。提供两种形态：

* ``ScriptedPortal``：进程内、内存态，供单元测试使用；
* ``FileBackedPortal``：状态（草稿、回执、会话、事件游标）落在
  JSON 文件并带文件锁，供 CLI 跨进程使用——这样“提交响应丢失”
  后在另一个进程里仍能查询到真实回执，复现“远端已办、本地未知”
  如何被核实，而不是重办。

事件按队列顺序、且只在队首匹配当前操作时消费，因此全局的
``expire_session`` 会等待轮到它的那一次操作才发生。
"""

import fcntl
import json
from contextlib import contextmanager
from pathlib import Path

from .batch_types import (
    CaptchaRequired,
    HumanConfirmationRequired,
    PortalStructureChanged,
    SessionExpired,
)
from .domain import content_digest


def _empty_state():
    return {
        'drafts': {},
        'receipts': [],
        'session_valid': True,
        'events': None,
        'calls': 0,
    }


class ScriptedPortal:
    """脚本化合成门户；语义与 domain.Portal 对齐，事件驱动故障注入。"""

    def __init__(self, config=None, state=None):
        self.config = config or {}
        self.state = state if state is not None else _empty_state()
        # 事件队列属于门户状态：已消费的事件必须在进程间保持已消费，
        # 因此放入 state；首次使用时从 fixture 初始化。
        if self.state.get('events') is None:
            self.state['events'] = list(self.config.get('events', []))
        else:
            self.state['events'] = [dict(event) for event in self.state['events']]

    @property
    def events(self):
        return self.state['events']

    @events.setter
    def events(self, value):
        self.state['events'] = value

    # 查询语义：fixture 明确 not_found_is_final=false，查不到不代表没办过。
    @property
    def not_found_is_final(self):
        return bool(self.config.get('query_semantics', {}).get('not_found_is_final', False))

    def _next_event(self, action, approval=None):
        """返回并消费当前应触发的事件，否则返回 None。

        三类事件：
        * 全局事件（不带 approval，例如 expire_session）：位于队首时对
          下一次任意操作生效，模拟“操作时发现登录态已失效”；
        * 拦截类事件（human_confirmation/captcha_required/structure_changed）：
          模拟表单页面本身的要求，在该单号的任意前置操作上即生效，不必
          等到提交那一刻；
        * 其他单号事件：在队列中按 action + approval 扫描匹配。
        """
        if not self.events:
            return None
        head = self.events[0]
        if 'approval' not in head:
            del self.events[0]
            return head
        intercept = {
            'human_confirmation',
            'captcha_required',
            'structure_changed',
        }
        # 事件动作名与门户操作的别名映射。
        aliases = {'query': {'query', 'query_unavailable'}}
        candidates = aliases.get(action, {action})
        for index, event in enumerate(self.events):
            if event.get('approval') != approval:
                continue
            if event['action'] in candidates or event['action'] in intercept:
                del self.events[index]
                return event
        return None

    def _require_session(self):
        if not self.state['session_valid']:
            raise SessionExpired('会话已过期，请重新登录')

    def _apply_event(self, event):
        action = event['action']
        if action == 'expire_session':
            self.state['session_valid'] = False
            raise SessionExpired('会话已过期，请重新登录')
        if action == 'human_confirmation':
            raise HumanConfirmationRequired('表单要求人工确认，自动流程停止')
        if action == 'captcha_required':
            raise CaptchaRequired('页面要求验证码，自动流程不处理验证码')
        if action == 'structure_changed':
            raise PortalStructureChanged('页面结构已变化，无法安全继续')
        if action == 'query_unavailable':
            # 查询通道本身不可用：与“查无此单”严格区分，保持待核对。
            raise ConnectionError('业务查询暂不可用，查询结果不确定')

    def _pre_op(self, action, approval):
        self.state['calls'] += 1
        self._require_session()
        event = self._next_event(action, approval)
        if event is not None:
            self._apply_event(event)

    def draft(self, approval, content):
        self._pre_op('draft', approval)
        digest = content_digest(content)
        self.state['drafts'][approval] = dict(content)
        return dict(approval=approval, digest=digest)

    def submit(self, approval):
        self._pre_op('submit', approval)
        if approval not in self.state['drafts']:
            raise PortalStructureChanged(f'门户中找不到 {approval} 的草稿')
        content = self.state['drafts'][approval]
        receipt = dict(
            number=f'R{len(self.state["receipts"]) + 1}',
            approval=approval,
            digest=content_digest(content),
        )
        self.state['receipts'].append(receipt)
        # 回执已经落进门户状态，仅响应丢失：远端事实已成立。
        event = self._next_event('submit_then_drop_response', approval)
        if event is not None:
            raise ConnectionError('提交响应丢失，提交结果未知')
        return dict(receipt)

    def query(self, approval):
        self._pre_op('query', approval)
        return [
            dict(receipt)
            for receipt in self.state['receipts']
            if receipt['approval'] == approval
        ]

    def renew_session(self):
        """人工重新登录后调用；自动流程不会自行刷新登录态。"""
        self.state['session_valid'] = True
        return True


class FileBackedPortal(ScriptedPortal):
    """带文件锁和 JSON 持久化的合成门户，可跨进程共享远端状态。"""

    def __init__(self, state_path, config=None):
        super().__init__(config=config)
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_handle = open(self.state_path.with_suffix('.lock'), 'a+')
        fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            if self.state_path.exists():
                # 已存在的门户状态以磁盘为准（含已消费后的剩余事件队列），
                # fixture 只用于首次初始化，不会在重开时重放历史事件。
                self.state = json.loads(self.state_path.read_text(encoding='utf-8'))
            else:
                # 构造即落盘，保证多个进程同时启动时看到同一份初始状态。
                self._persist_locked()
        finally:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)

    def close(self):
        self._lock_handle.close()

    @contextmanager
    def _locked(self):
        fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            # 拿锁后重新读取磁盘上的最新状态，避免覆盖其他进程的写入。
            self.state = json.loads(self.state_path.read_text(encoding='utf-8'))
            try:
                yield
            finally:
                # 即使操作抛异常（响应丢失、查询不可用、会话过期），
                # 事件消费和已经成立的远端事实（回执）也必须落盘。
                self.state_path.write_text(
                    json.dumps(self.state, ensure_ascii=False, sort_keys=True, indent=2),
                    encoding='utf-8',
                )
        finally:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)

    def _persist_locked(self):
        self.state_path.write_text(
            json.dumps(self.state, ensure_ascii=False, sort_keys=True, indent=2),
            encoding='utf-8',
        )

    def draft(self, approval, content):
        with self._locked():
            return super().draft(approval, content)

    def submit(self, approval):
        with self._locked():
            return super().submit(approval)

    def query(self, approval):
        with self._locked():
            return super().query(approval)

    def renew_session(self):
        with self._locked():
            return super().renew_session()
