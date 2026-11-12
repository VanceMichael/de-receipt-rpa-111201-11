"""可靠批量自动操作的单元测试。

覆盖：不确定提交协议、业务查询核对、摘要冲突转人工、查询不确定保持、
会话过期/验证码/结构变化停止、崩溃边界恢复、幂等重跑、双人锁、
日志脱敏与文件门户的跨进程持久化。
"""

import io
import json
import logging
import tempfile
import threading
import unittest
from pathlib import Path

from collector.batch import BatchEngine, default_batch_id, load_applications, prepare_batch
from collector.batch_store import BatchStore
from collector.batch_types import (
    BatchAlreadyRunning,
    BlockReason,
    CaptchaRequired,
    PortalStructureChanged,
    SessionExpired,
    State,
)
from collector.domain import content_digest
from collector.locking import BatchLock
from collector.logging_setup import configure_logging, redact
from collector.synthetic import FileBackedPortal, ScriptedPortal

FIXTURE = json.loads(Path('fixtures/portal.json').read_text())


def make_applications():
    rows = [
        dict(approval='A100', content={'equipment': '成像仪', 'count': 1}),
        dict(approval='A101', content={'equipment': '水听器', 'count': 2}),
    ]
    for row in rows:
        row['digest'] = content_digest(row['content'])
    return rows


class PortalHarness:
    """构造临时数据库、批次与引擎的测试夹具。"""

    def __init__(self, config=None, portal_class=ScriptedPortal):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.db_path = root / 'batch.sqlite3'
        self.state_path = root / 'portal-state.json'
        self.config = config if config is not None else {'events': []}
        self.portal_class = portal_class
        self.applications = make_applications()
        self.batch_id = default_batch_id(self.applications)
        self.store = BatchStore(self.db_path)
        prepare_batch(self.store, self.batch_id, self.state_path, self.applications)
        self.engine = BatchEngine(self.store, self.batch_id)

    def portal(self):
        if self.portal_class is FileBackedPortal:
            return FileBackedPortal(self.state_path, self.config)
        return ScriptedPortal(self.config)

    def entry(self, approval):
        return self.store.get_entry(self.batch_id, approval)

    def close(self):
        self.store.close()
        self.temporary.cleanup()


class HappyPathTests(unittest.TestCase):
    def test_full_batch_verifies_with_receipts(self):
        harness = PortalHarness()
        try:
            report = harness.engine.run(harness.portal())
            self.assertFalse(report['stopped'])
            self.assertTrue(report['all_verified'])
            self.assertEqual(harness.entry('A100')['state'], State.VERIFIED)
            self.assertEqual(harness.entry('A101')['state'], State.VERIFIED)
            self.assertIsNotNone(harness.entry('A100')['receipt_number'])
        finally:
            harness.close()

    def test_rerun_completed_batch_is_idempotent(self):
        harness = PortalHarness()
        try:
            harness.engine.run(harness.portal())
            portal = harness.portal()
            calls_before = portal.state['calls']
            report = harness.engine.run(portal)
            self.assertEqual(portal.state['calls'], calls_before)
            self.assertTrue(report['all_verified'])
            self.assertEqual(harness.entry('A100')['attempts'], 1)
        finally:
            harness.close()


class UncertainSubmitTests(unittest.TestCase):
    def test_dropped_response_then_query_verifies_without_resubmit(self):
        config = {'events': [
            {'approval': 'A100', 'action': 'submit_then_drop_response'},
            {'approval': 'A100', 'action': 'query_unavailable'},
        ]}
        harness = PortalHarness(config)
        try:
            portal = harness.portal()
            report = harness.engine.run(portal)
            # 第一遍：A100 提交响应丢失、随后查询不可用，停在待核对；
            # A101 正常核实。
            self.assertEqual(harness.entry('A100')['state'], State.SUBMIT_UNCERTAIN)
            self.assertEqual(harness.entry('A101')['state'], State.VERIFIED)
            self.assertEqual(len(portal.state['receipts']), 2)
            self.assertFalse(report['all_verified'])

            # 第二遍：只查询不重提，查询恢复后核实远端回执。
            report = harness.engine.run(portal, recheck_only=True)
            self.assertEqual(harness.entry('A100')['state'], State.VERIFIED)
            self.assertEqual(harness.entry('A100')['receipt_number'], 'R1')
            self.assertEqual(harness.entry('A100')['attempts'], 1)
            journal_events = [item['event'] for item in harness.store.journal(harness.batch_id, 'A100')]
            self.assertEqual(journal_events.count('submit_intent_persisted'), 1)
            self.assertIn('receipt_verified_by_query', journal_events)
        finally:
            harness.close()

    def test_uncertain_entry_never_calls_submit_again(self):
        config = {'events': [
            {'approval': 'A100', 'action': 'submit_then_drop_response'},
            {'approval': 'A100', 'action': 'query_unavailable'},
            {'approval': 'A100', 'action': 'query_unavailable'},
            {'approval': 'A100', 'action': 'query_unavailable'},
        ]}
        harness = PortalHarness(config)
        try:
            portal = harness.portal()
            harness.engine.run(portal)
            submits_before = len(portal.state['receipts'])
            for _ in range(3):
                harness.engine.run(portal, recheck_only=True)
            # 无论核对多少遍，门户侧提交次数不增加。
            self.assertEqual(len(portal.state['receipts']), submits_before)
        finally:
            harness.close()

    def test_query_unavailable_keeps_uncertain(self):
        config = {'events': [
            {'approval': 'A100', 'action': 'submit_then_drop_response'},
            {'approval': 'A100', 'action': 'query_unavailable'},
            {'approval': 'A100', 'action': 'query_unavailable'},
        ]}
        harness = PortalHarness(config)
        try:
            portal = harness.portal()
            harness.engine.run(portal)
            harness.engine.run(portal, recheck_only=True)
            self.assertEqual(harness.entry('A100')['state'], State.SUBMIT_UNCERTAIN)
            # 查询恢复后即可核实
            harness.engine.run(portal, recheck_only=True)
            self.assertEqual(harness.entry('A100')['state'], State.VERIFIED)
        finally:
            harness.close()

    def test_empty_query_non_final_keeps_uncertain(self):
        config = {'events': [
            {'approval': 'A100', 'action': 'submit_then_drop_response'},
        ]}
        harness = PortalHarness(config)
        try:
            portal = ScriptedPortal(config)
            # 门户中没有任何回执（模拟远端尚未可见），且 not_found 非终态
            portal.state['receipts'] = []
            portal.state['drafts'] = {}
            # 手动把条目置为待核对
            harness.store.transition(
                harness.batch_id, 'A100', 'test_seed', State.SUBMIT_UNCERTAIN)
            report = harness.engine.run(portal, recheck_only=True)
            self.assertEqual(harness.entry('A100')['state'], State.SUBMIT_UNCERTAIN)
            self.assertFalse(report['all_verified'])
        finally:
            harness.close()

    def test_empty_query_final_reopens_draft_without_duplicate(self):
        config = {'events': [], 'query_semantics': {'not_found_is_final': True}}
        harness = PortalHarness(config)
        try:
            portal = ScriptedPortal(config)
            # 先正常保存草稿（门户提交要求草稿存在），再构造待核对状态
            portal.draft('A100', harness.entry('A100')['content'])
            portal.state['receipts'] = []
            harness.store.transition(
                harness.batch_id, 'A100', 'test_seed', State.SUBMIT_UNCERTAIN,
                increment_attempt=True)
            harness.engine.run(portal, recheck_only=True)
            self.assertEqual(harness.entry('A100')['state'], State.DRAFTED)
            # 再正常跑一遍：门户确认此前没有任何受理记录，只产生一次提交
            harness.engine.run(portal)
            self.assertEqual(harness.entry('A100')['state'], State.VERIFIED)
            a100_receipts = [r for r in portal.state['receipts'] if r['approval'] == 'A100']
            self.assertEqual(len(a100_receipts), 1)
            # 第一次是测试种子意图，门户确认查无后重新发起的是第二次意图
            self.assertEqual(harness.entry('A100')['attempts'], 2)
        finally:
            harness.close()


class ConflictTests(unittest.TestCase):
    def test_same_approval_different_digest_goes_manual(self):
        harness = PortalHarness({'events': []})
        try:
            portal = ScriptedPortal({'events': []})
            # 远端已有相同单号、但内容摘要不同的回执
            portal.state['receipts'].append(
                dict(number='R9', approval='A100', digest=content_digest({'different': True})))
            harness.store.transition(
                harness.batch_id, 'A100', 'test_seed', State.SUBMIT_UNCERTAIN)
            harness.engine.run(portal, recheck_only=True)
            entry = harness.entry('A100')
            self.assertEqual(entry['state'], State.NEEDS_MANUAL)
            self.assertEqual(entry['reason'], BlockReason.REMOTE_DIGEST_MISMATCH)
            self.assertEqual(entry['receipt_number'], 'R9')
        finally:
            harness.close()

    def test_direct_submit_conflict_goes_manual(self):
        harness = PortalHarness({'events': []})
        try:
            class ConflictingPortal(ScriptedPortal):
                def submit(self, approval):
                    return dict(number='R9', approval=approval, digest='not-the-local-digest')

            harness.engine.run(ConflictingPortal({'events': []}))
            entry = harness.entry('A100')
            self.assertEqual(entry['state'], State.NEEDS_MANUAL)
            self.assertEqual(entry['reason'], BlockReason.REMOTE_DIGEST_MISMATCH)
        finally:
            harness.close()

    def test_duplicate_identity_in_input_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'input.json'
            path.write_text(json.dumps({'applications': [
                {'approval': 'A1', 'content': {'x': 1}},
                {'approval': 'A1', 'content': {'x': 1}},
            ]}))
            with self.assertRaises(ValueError):
                load_applications(path)

    def test_same_approval_changed_content_rejected_on_reregister(self):
        harness = PortalHarness({'events': []})
        try:
            with self.assertRaises(ValueError):
                harness.store.register_entry(
                    harness.batch_id, 'A100', {'equipment': '完全不同的设备'},
                    content_digest({'equipment': '完全不同的设备'}))
        finally:
            harness.close()


class ManualStopTests(unittest.TestCase):
    def test_human_confirmation_blocks_entry(self):
        config = {'events': [{'approval': 'A100', 'action': 'human_confirmation'}]}
        harness = PortalHarness(config)
        try:
            harness.engine.run(harness.portal())
            entry = harness.entry('A100')
            self.assertEqual(entry['state'], State.BLOCKED_MANUAL)
            self.assertEqual(entry['reason'], BlockReason.HUMAN_CONFIRMATION)
            # 后续条目仍可继续处理
            self.assertEqual(harness.entry('A101')['state'], State.VERIFIED)
        finally:
            harness.close()

    def test_captcha_blocks_entry(self):
        config = {'events': [{'approval': 'A101', 'action': 'captcha_required'}]}
        harness = PortalHarness(config)
        try:
            harness.engine.run(harness.portal())
            self.assertEqual(harness.entry('A101')['reason'], BlockReason.CAPTCHA)
            self.assertEqual(harness.entry('A101')['state'], State.BLOCKED_MANUAL)
        finally:
            harness.close()

    def test_structure_change_blocks_entry(self):
        config = {'events': [{'approval': 'A100', 'action': 'structure_changed'}]}
        harness = PortalHarness(config)
        try:
            harness.engine.run(harness.portal())
            self.assertEqual(harness.entry('A100')['reason'], BlockReason.STRUCTURE_CHANGED)
        finally:
            harness.close()

    def test_session_expiry_stops_whole_batch(self):
        config = {'events': [{'action': 'expire_session'}]}
        harness = PortalHarness(config)
        try:
            report = harness.engine.run(harness.portal())
            self.assertTrue(report['stopped'])
            # 条目停在原始状态，不被误标失败
            self.assertEqual(harness.entry('A100')['state'], State.PENDING)
        finally:
            harness.close()

    def test_renew_session_allows_resume(self):
        config = {'events': [{'action': 'expire_session'}]}
        harness = PortalHarness(config)
        try:
            portal = harness.portal()
            report = harness.engine.run(portal)
            self.assertTrue(report['stopped'])
            portal.renew_session()
            report = harness.engine.run(portal)
            self.assertFalse(report['stopped'])
            self.assertTrue(report['all_verified'])
        finally:
            harness.close()


class RecoveryTests(unittest.TestCase):
    def test_reopen_store_resumes_from_last_transition(self):
        config = {'events': [
            {'approval': 'A100', 'action': 'submit_then_drop_response'},
            {'approval': 'A100', 'action': 'query_unavailable'},
        ]}
        harness = PortalHarness(config)
        try:
            portal = harness.portal()
            harness.engine.run(portal)
            self.assertEqual(harness.entry('A100')['state'], State.SUBMIT_UNCERTAIN)
            harness.store.close()

            # 模拟新进程重新打开数据库与门户
            store2 = BatchStore(harness.db_path)
            try:
                engine2 = BatchEngine(store2, harness.batch_id)
                report = engine2.run(portal, recheck_only=True)
                entry = store2.get_entry(harness.batch_id, 'A100')
                self.assertEqual(entry['state'], State.VERIFIED)
                self.assertEqual(entry['attempts'], 1)
                self.assertFalse(report['stopped'])
            finally:
                store2.close()
        finally:
            harness.temporary.cleanup()

    def test_crash_between_intent_and_submit_does_not_duplicate(self):
        harness = PortalHarness({'events': []})
        try:
            portal = harness.portal()
            # 模拟进程在“意图落库后、提交调用前”被杀死
            harness.store.transition(
                harness.batch_id, 'A100', 'submit_intent_persisted',
                State.SUBMIT_UNCERTAIN, increment_attempt=True)
            # 恢复：只查询。门户里没有回执，非终态，保持待核对而不是重提
            harness.engine.run(portal, recheck_only=True)
            self.assertEqual(harness.entry('A100')['state'], State.SUBMIT_UNCERTAIN)
            self.assertEqual(len(portal.state['receipts']), 0)
            self.assertEqual(harness.entry('A100')['attempts'], 1)
        finally:
            harness.close()


class LockTests(unittest.TestCase):
    def test_second_operator_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / 'batch.lock'
            first = BatchLock(lock_path).acquire()
            try:
                with self.assertRaises(BatchAlreadyRunning):
                    BatchLock(lock_path).acquire()
            finally:
                first.release()

    def test_lock_releases_after_processing(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / 'batch.lock'
            with BatchLock(lock_path):
                pass
            # 锁释放后可再次获取
            with BatchLock(lock_path):
                pass

    def test_concurrent_operators_only_one_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / 'coop.lock'
            barrier = threading.Barrier(2)
            outcomes = []
            outcome_lock = threading.Lock()

            def worker():
                barrier.wait()
                try:
                    with BatchLock(lock_path):
                        # 持锁代表该操作员获得本批次的唯一执行权
                        outcomes.append('ran')
                except BatchAlreadyRunning:
                    with outcome_lock:
                        outcomes.append('rejected')

            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
            self.assertEqual(sorted(outcomes), ['ran', 'rejected'])


class FileBackedPortalTests(unittest.TestCase):
    def test_state_survives_process_reopen(self):
        config = {'events': [
            {'approval': 'A100', 'action': 'submit_then_drop_response'},
        ]}
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / 'state.json'
            first = FileBackedPortal(state_path, config)
            first.draft('A100', {'equipment': '成像仪', 'count': 1})
            with self.assertRaises(ConnectionError):
                first.submit('A100')
            first.close()

            second = FileBackedPortal(state_path, config)
            try:
                # 事件已消费，不会重放；回执作为远端事实仍然可查
                receipts = second.query('A100')
                self.assertEqual(len(receipts), 1)
                self.assertEqual(receipts[0]['number'], 'R1')
                self.assertEqual(second.events, [])
            finally:
                second.close()

    def test_exception_path_persists_event_consumption(self):
        config = {'events': [{'approval': 'A100', 'action': 'query_unavailable'}]}
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / 'state.json'
            portal = FileBackedPortal(state_path, config)
            portal.draft('A100', {'x': 1})
            with self.assertRaises(ConnectionError):
                portal.query('A100')
            portal.close()
            on_disk = json.loads(state_path.read_text(encoding='utf-8'))
            self.assertEqual(on_disk['events'], [])


class LoggingTests(unittest.TestCase):
    def test_redacts_session_values(self):
        samples = [
            'Set-Cookie: SESSIONID=abc123secret; Path=/',
            'Authorization: Bearer eyJhbGciOi.JzdWIiOiIx',
            'token=top-secret-value',
            'session_id = 9f8e7d6c',
        ]
        for sample in samples:
            cleaned = redact(sample)
            self.assertNotIn('abc123secret', cleaned)
            self.assertNotIn('top-secret-value', cleaned)
            self.assertNotIn('9f8e7d6c', cleaned)
            self.assertIn('REDACTED', cleaned)

    def test_journal_contains_no_session_values(self):
        harness = PortalHarness({'events': []})
        try:
            harness.engine.run(harness.portal())
            raw = json.dumps(harness.store.journal(harness.batch_id), ensure_ascii=False)
            for forbidden in ('cookie', 'Cookie', 'token', 'Token', 'authorization', 'Bearer'):
                self.assertNotIn(forbidden, raw)
        finally:
            harness.close()

    def test_log_output_is_valid_json_and_redacted(self):
        stream = io.StringIO()
        logger = configure_logging('collector.test_log')
        logger.handlers[0].stream = stream
        logger.info('Set-Cookie: SESSIONID=leakme123')
        line = stream.getvalue().strip()
        payload = json.loads(line)
        self.assertNotIn('leakme123', payload['message'])
        self.assertIn('REDACTED', payload['message'])


class FixtureScenarioTests(unittest.TestCase):
    """按 fixtures/portal.json 的完整叙事走一遍。"""

    def test_fixture_four_pass_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db_path = root / 'batch.sqlite3'
            state_path = root / 'portal-state.json'
            applications = load_applications('fixtures/portal.json')
            batch_id = default_batch_id(applications)
            store = BatchStore(db_path)
            try:
                prepare_batch(store, batch_id, state_path, applications)
                engine = BatchEngine(store, batch_id)

                portal = FileBackedPortal(state_path, FIXTURE)
                engine.run(portal)
                portal.close()
                self.assertEqual(store.get_entry(batch_id, 'A100')['state'], State.SUBMIT_UNCERTAIN)
                self.assertEqual(store.get_entry(batch_id, 'A101')['state'], State.BLOCKED_MANUAL)

                portal = FileBackedPortal(state_path, FIXTURE)
                engine.run(portal, recheck_only=True)
                portal.close()
                self.assertEqual(store.get_entry(batch_id, 'A100')['state'], State.SUBMIT_UNCERTAIN)

                portal = FileBackedPortal(state_path, FIXTURE)
                report = engine.run(portal, recheck_only=True)
                portal.close()
                self.assertTrue(report['stopped'])

                portal = FileBackedPortal(state_path, FIXTURE)
                portal.renew_session()
                engine.run(portal, recheck_only=True)
                portal.close()
                entry = store.get_entry(batch_id, 'A100')
                self.assertEqual(entry['state'], State.VERIFIED)
                self.assertEqual(entry['attempts'], 1)
                self.assertEqual(entry['receipt_number'], 'R1')
            finally:
                store.close()


if __name__ == '__main__':
    unittest.main()
