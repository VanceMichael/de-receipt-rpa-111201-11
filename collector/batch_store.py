"""批量流程的持久化状态与审计日志。

草稿保存、提交意图、远端回执、本地状态以及每一次状态迁移都写入
SQLite。任何相邻步骤之间进程被杀死，重新打开同一个数据库即可从
最后一次已提交的迁移继续。

journal 只记录业务事实（单号、状态、原因、回执号），不记录任何
会话值（cookie、token、请求头）。
"""

import json
import sqlite3
import time
from contextlib import contextmanager

from .batch_types import State


def _now():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


class BatchStore:
    def __init__(self, path):
        self.path = str(path)
        # check_same_thread=False：配合 WAL 与 BEGIN IMMEDIATE 事务，
        # 允许协调锁测试中不同线程各自打开连接；写入仍由事务串行化。
        self.connection = sqlite3.connect(
            self.path, timeout=10, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS batches(
                batch_id TEXT PRIMARY KEY,
                portal_state_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS entries(
                batch_id TEXT NOT NULL REFERENCES batches(batch_id),
                approval TEXT NOT NULL,
                content_digest TEXT NOT NULL,
                content TEXT NOT NULL,
                state TEXT NOT NULL,
                receipt_number TEXT,
                receipt_digest TEXT,
                reason TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(batch_id, approval));
            CREATE TABLE IF NOT EXISTS journal(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL,
                approval TEXT NOT NULL,
                event TEXT NOT NULL,
                from_state TEXT,
                to_state TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL);
            """
        )

    def close(self):
        self.connection.close()

    @contextmanager
    def transaction(self):
        self.connection.execute('BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    # ---------- 批次 ----------

    def ensure_batch(self, batch_id, portal_state_path):
        """登记批次；同一 batch_id 重复登记是幂等的，不覆盖原门户路径。"""
        with self.transaction():
            existing = self.connection.execute(
                'SELECT batch_id FROM batches WHERE batch_id=?', (batch_id,)
            ).fetchone()
            if existing is None:
                stamp = _now()
                self.connection.execute(
                    'INSERT INTO batches VALUES (?,?,?,?)',
                    (batch_id, str(portal_state_path), stamp, stamp),
                )

    def list_batches(self):
        rows = self.connection.execute(
            'SELECT batch_id, portal_state_path, created_at, updated_at FROM batches ORDER BY created_at'
        ).fetchall()
        return [dict(row) for row in rows]

    # ---------- 条目 ----------

    def register_entry(self, batch_id, approval, content, digest):
        """把一条录入纳入批次；已存在（同单号同摘要）时不做任何改动。

        返回 True 表示新登记，False 表示批次中已有完全相同的业务身份。
        """
        encoded = json.dumps(content, ensure_ascii=False, sort_keys=True, allow_nan=False)
        with self.transaction():
            row = self.connection.execute(
                'SELECT content_digest, state FROM entries WHERE batch_id=? AND approval=?',
                (batch_id, approval),
            ).fetchone()
            if row is not None:
                if row['content_digest'] != digest:
                    raise ValueError(
                        f'审批单号 {approval} 在同一批次中对应不同内容，拒绝覆盖'
                    )
                return False
            stamp = _now()
            self.connection.execute(
                'INSERT INTO entries(batch_id,approval,content_digest,content,state,attempts,updated_at)'
                ' VALUES (?,?,?,?,?,0,?)',
                (batch_id, approval, digest, encoded, State.PENDING, stamp),
            )
            self._append_journal_locked(
                batch_id, approval, 'entry_registered', None, State.PENDING, None
            )
            self._touch_batch_locked(batch_id)
            return True

    def get_entry(self, batch_id, approval):
        row = self.connection.execute(
            'SELECT * FROM entries WHERE batch_id=? AND approval=?',
            (batch_id, approval),
        ).fetchone()
        if row is None:
            return None
        data = dict(row)
        data['content'] = json.loads(data['content'])
        return data

    def list_entries(self, batch_id):
        rows = self.connection.execute(
            'SELECT * FROM entries WHERE batch_id=? ORDER BY approval', (batch_id,)
        ).fetchall()
        result = []
        for row in rows:
            data = dict(row)
            data['content'] = json.loads(data['content'])
            result.append(data)
        return result

    def transition(self, batch_id, approval, event, to_state, *, detail=None,
                   receipt_number=None, receipt_digest=None, reason=None,
                   increment_attempt=False):
        """在单个事务内完成状态迁移、回执记录与审计追加。

        调用方必须在迁移前已经拿到业务事实（例如已保存草稿或已发出
        提交意图）；本方法只负责让本地状态与事实对齐并落盘。
        """
        with self.transaction():
            row = self.connection.execute(
                'SELECT state FROM entries WHERE batch_id=? AND approval=?',
                (batch_id, approval),
            ).fetchone()
            if row is None:
                raise KeyError(f'批次中不存在条目：{approval}')
            from_state = row['state']
            stamp = _now()
            self.connection.execute(
                'UPDATE entries SET state=?, receipt_number=COALESCE(?, receipt_number),'
                ' receipt_digest=COALESCE(?, receipt_digest),'
                ' reason=COALESCE(?, reason),'
                ' attempts=attempts+?,'
                ' updated_at=? WHERE batch_id=? AND approval=?',
                (
                    to_state,
                    receipt_number,
                    receipt_digest,
                    reason,
                    1 if increment_attempt else 0,
                    stamp,
                    batch_id,
                    approval,
                ),
            )
            self._append_journal_locked(batch_id, approval, event, from_state, to_state, detail)
            self._touch_batch_locked(batch_id)
            return from_state

    def journal(self, batch_id, approval=None):
        if approval is None:
            rows = self.connection.execute(
                'SELECT event,from_state,to_state,detail,created_at FROM journal'
                ' WHERE batch_id=? ORDER BY id',
                (batch_id,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                'SELECT event,from_state,to_state,detail,created_at FROM journal'
                ' WHERE batch_id=? AND approval=? ORDER BY id',
                (batch_id, approval),
            ).fetchall()
        return [dict(row) for row in rows]

    # ---------- 内部 ----------

    def _append_journal_locked(self, batch_id, approval, event, from_state, to_state, detail):
        self.connection.execute(
            'INSERT INTO journal(batch_id,approval,event,from_state,to_state,detail,created_at)'
            ' VALUES (?,?,?,?,?,?,?)',
            (batch_id, approval, event, from_state, to_state, detail, _now()),
        )

    def _touch_batch_locked(self, batch_id):
        self.connection.execute(
            'UPDATE batches SET updated_at=? WHERE batch_id=?', (_now(), batch_id)
        )
