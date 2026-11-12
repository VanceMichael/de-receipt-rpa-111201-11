"""可靠批量自动操作的核心引擎。

协议要点（门户不承诺幂等请求头，安全保证全部来自流程本身）：

1. 业务身份 = 审批单号 + 内容摘要（SHA-256）。
2. 提交前先把 ``SUBMIT_UNCERTAIN`` 意图落库；提交调用发出后若断线、
   超时或响应丢失，本地停在 ``SUBMIT_UNCERTAIN``，恢复时只做业务查询，
   绝不直接再次提交。
3. 查询到相同单号且摘要一致 → ``VERIFIED``；相同单号但摘要不同 →
   ``NEEDS_MANUAL``；查不到（门户声明 not_found 非终态）或查询本身
   不确定 → 继续保持待核对。
4. 会话过期、人工确认、验证码、结构变化 → 停在可解释的步骤，不替人
   确认、不处理验证码。
"""

import hashlib
import json
import logging
from pathlib import Path

from .batch_types import (
    MANUAL_ERRORS,
    TRANSIENT_ERRORS,
    BlockReason,
    SessionExpired,
    State,
    StopReason,
    TERMINAL_STATES,
)
from .domain import content_digest

logger = logging.getLogger('collector.batch')


def load_applications(input_path):
    payload = json.loads(Path(input_path).read_bytes())
    applications = payload.get('applications')
    if not isinstance(applications, list) or not applications:
        raise ValueError('输入文件缺少非空 applications 列表')
    normalized = []
    seen = set()
    for row in applications:
        if not isinstance(row, dict):
            raise ValueError('每条录入必须是对象')
        approval = row.get('approval')
        content = row.get('content')
        if not isinstance(approval, str) or not approval.strip():
            raise ValueError('审批单号必须是非空字符串')
        if not isinstance(content, dict) or not content:
            raise ValueError(f'{approval} 的内容必须是非空对象')
        digest = content_digest(content)
        identity = (approval, digest)
        if identity in seen:
            raise ValueError(f'批次内存在重复业务身份：{approval}（相同单号与摘要）')
        seen.add(identity)
        normalized.append(dict(approval=approval, content=content, digest=digest))
    return normalized


def default_batch_id(applications):
    canonical = json.dumps(
        [(row['approval'], row['digest']) for row in applications],
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def prepare_batch(store, batch_id, portal_state_path, applications):
    """登记批次与全部条目；重复运行同一份输入是幂等的。"""
    store.ensure_batch(batch_id, portal_state_path)
    registered = 0
    for row in applications:
        if store.register_entry(batch_id, row['approval'], row['content'], row['digest']):
            registered += 1
    return registered


class BatchEngine:
    def __init__(self, store, batch_id):
        self.store = store
        self.batch_id = batch_id

    def run(self, portal, *, recheck_only=False):
        """推进批次内所有条目，返回结构化报告。

        recheck_only=True 时只处理 ``SUBMIT_UNCERTAIN`` 条目（只查询、
        不提交任何新申请），适用于操作员显式发起的核对。
        """
        entries = self.store.list_entries(self.batch_id)
        report = {
            'batch_id': self.batch_id,
            'recheck_only': recheck_only,
            'processed': 0,
            'stopped': False,
            'stop_reason': None,
            'states': {},
            'entries': [],
        }
        logger.info('批次开始处理，共 %d 条，recheck_only=%s', len(entries), recheck_only)
        for entry in entries:
            state = entry['state']
            if state in TERMINAL_STATES:
                self._note(report, entry, state, '已处于终态，跳过')
                continue
            if recheck_only and state != State.SUBMIT_UNCERTAIN:
                self._note(report, entry, state, '核对模式跳过非待核对条目')
                continue
            # 同一条目在一轮内持续推进（草稿→提交→核实），直到终态、
            # 待核对（结果未知，必须等下一次查询）或批次级停止。
            while True:
                state = entry['state']
                if state in TERMINAL_STATES:
                    break
                if recheck_only:
                    # 核对模式只做业务查询，不保存草稿、不发起提交。
                    if state != State.SUBMIT_UNCERTAIN:
                        break
                report['processed'] += 1
                try:
                    if state == State.PENDING:
                        self._save_draft(portal, entry)
                    elif state == State.DRAFTED:
                        self._submit_once(portal, entry)
                    elif state == State.SUBMIT_UNCERTAIN:
                        self._verify_by_query(portal, entry)
                except SessionExpired as error:
                    # 登录过期：不自动重登，不自动继续，停在可解释的步骤。
                    report['stopped'] = True
                    report['stop_reason'] = StopReason.SESSION_EXPIRED
                    logger.warning('登录态过期，批次停止等待人工重新登录：%s', type(error).__name__)
                    entry = self.store.get_entry(self.batch_id, entry['approval'])
                    self._note(report, entry, entry['state'], StopReason.SESSION_EXPIRED)
                    break
                entry = self.store.get_entry(self.batch_id, entry['approval'])
                if state == State.SUBMIT_UNCERTAIN:
                    # 查询后无论核实、冲突还是仍不确定，本轮都只查询一次。
                    break
            if report['stopped']:
                break
            refreshed = self.store.get_entry(self.batch_id, entry['approval'])
            report['states'][refreshed['state']] = report['states'].get(refreshed['state'], 0) + 1
            self._note(report, refreshed, refreshed['state'], refreshed.get('reason'))
        self._finalize_counts(report)
        report['all_verified'] = all(
            item['state'] == State.VERIFIED for item in report['entries']
        ) and not report['stopped']
        logger.info(
            '批次处理结束，processed=%d stopped=%s all_verified=%s',
            report['processed'],
            report['stopped'],
            report['all_verified'],
        )
        return report

    # ---------- 各状态的处理 ----------

    def _save_draft(self, portal, entry):
        approval = entry['approval']
        try:
            echo = portal.draft(approval, entry['content'])
        except SessionExpired:
            # 必须先于 OSError 分支（PermissionError 也是 OSError 子类），
            # 让批次级停止逻辑处理，而不是误判为瞬时网络问题。
            raise
        except TRANSIENT_ERRORS as error:
            logger.warning('草稿保存网络异常，保持 %s：%s', State.PENDING, type(error).__name__)
            return
        except MANUAL_ERRORS as error:
            self._block_manual(entry, error)
            return
        if not isinstance(echo, dict) or echo.get('digest') != entry['content_digest']:
            logger.warning('草稿回执摘要与本地不一致，转人工：%s', approval)
            self.store.transition(
                self.batch_id, approval, 'draft_digest_mismatch', State.BLOCKED_MANUAL,
                reason=BlockReason.STRUCTURE_CHANGED,
            )
            return
        self.store.transition(
            self.batch_id, approval, 'draft_saved', State.DRAFTED,
            detail=echo.get('digest'),
        )
        logger.info('草稿已保存：%s', approval)

    def _submit_once(self, portal, entry):
        approval = entry['approval']
        # 关键顺序：先持久化提交意图，再发出提交调用。
        # 若进程在这两步之间死亡，恢复后只会查询，不会重提。
        self.store.transition(
            self.batch_id, approval, 'submit_intent_persisted', State.SUBMIT_UNCERTAIN,
            increment_attempt=True,
        )
        logger.info('提交意图已落库，开始单次提交：%s', approval)
        try:
            receipt = portal.submit(approval)
        except SessionExpired:
            raise
        except TRANSIENT_ERRORS as error:
            # 响应丢失不代表远端失败：可能已受理，保持待核对，禁止重提。
            logger.warning(
                '提交结果不确定（%s），保持待核对，恢复时仅查询：%s',
                type(error).__name__, approval,
            )
            return
        except MANUAL_ERRORS as error:
            self._block_manual(entry, error)
            return
        if not isinstance(receipt, dict) or 'number' not in receipt:
            logger.warning('提交响应结构异常，转人工：%s', approval)
            self.store.transition(
                self.batch_id, approval, 'submit_response_malformed', State.BLOCKED_MANUAL,
                reason=BlockReason.STRUCTURE_CHANGED,
            )
            return
        if receipt.get('digest') != entry['content_digest']:
            # 相同单号、不同内容：不得覆盖，交人工。
            self.store.transition(
                self.batch_id, approval, 'receipt_digest_conflict', State.NEEDS_MANUAL,
                receipt_number=receipt.get('number'),
                receipt_digest=receipt.get('digest'),
                reason=BlockReason.REMOTE_DIGEST_MISMATCH,
            )
            logger.warning('提交回执摘要与本地不一致，需人工介入：%s', approval)
            return
        self.store.transition(
            self.batch_id, approval, 'receipt_verified_direct', State.VERIFIED,
            receipt_number=receipt['number'], receipt_digest=receipt['digest'],
        )
        logger.info('提交成功并核实回执：%s -> %s', approval, receipt['number'])

    def _verify_by_query(self, portal, entry):
        approval = entry['approval']
        try:
            receipts = portal.query(approval)
        except SessionExpired:
            raise
        except TRANSIENT_ERRORS as error:
            # 查询本身不确定：继续保持待核对，等待下一次核对。
            logger.warning('业务查询不确定（%s），保持待核对：%s', type(error).__name__, approval)
            return
        except MANUAL_ERRORS as error:
            self._block_manual(entry, error)
            return
        if not receipts:
            if getattr(portal, 'not_found_is_final', False):
                # 门户明确承诺“查无即终态”，才允许退回草稿阶段重新走一遍；
                # 草稿保存是幂等的，不会产生第二条申请。
                self.store.transition(
                    self.batch_id, approval, 'query_not_found_final_reopen', State.DRAFTED,
                )
                logger.warning('门户确认查无此单，退回草稿阶段：%s', approval)
            else:
                logger.warning('查询为空且非终态，继续保持待核对：%s', approval)
            return
        matched = next(
            (receipt for receipt in receipts if receipt.get('digest') == entry['content_digest']),
            None,
        )
        if matched is None:
            # 相同单号、不同内容：远端事实与本地意图冲突，人工介入。
            first = receipts[0]
            self.store.transition(
                self.batch_id, approval, 'query_digest_conflict', State.NEEDS_MANUAL,
                receipt_number=first.get('number'),
                receipt_digest=first.get('digest'),
                reason=BlockReason.REMOTE_DIGEST_MISMATCH,
            )
            logger.warning('查询到相同单号但摘要不同，需人工介入：%s', approval)
            return
        self.store.transition(
            self.batch_id, approval, 'receipt_verified_by_query', State.VERIFIED,
            receipt_number=matched.get('number'), receipt_digest=matched.get('digest'),
        )
        logger.info(
            '经业务查询核实远端已办，本地待核对转为已核实：%s -> %s',
            approval, matched.get('number'),
        )

    # ---------- 公共处理 ----------

    def _block_manual(self, entry, error):
        reason = {
            'HumanConfirmationRequired': BlockReason.HUMAN_CONFIRMATION,
            'CaptchaRequired': BlockReason.CAPTCHA,
            'PortalStructureChanged': BlockReason.STRUCTURE_CHANGED,
        }.get(type(error).__name__, BlockReason.STRUCTURE_CHANGED)
        self.store.transition(
            self.batch_id, entry['approval'], 'manual_stop', State.BLOCKED_MANUAL,
            reason=reason, detail=type(error).__name__,
        )
        logger.warning('遇到需人工处理的情形（%s），停止该条目：%s', reason, entry['approval'])

    def _note(self, report, entry, state, reason):
        report['entries'].append(
            {
                'approval': entry['approval'],
                'state': state,
                'reason': reason,
                'receipt_number': entry.get('receipt_number'),
            }
        )

    def _finalize_counts(self, report):
        counts = {}
        for item in report['entries']:
            counts[item['state']] = counts.get(item['state'], 0) + 1
        report['states'] = counts
