"""本地协调：防止两名操作员同时启动同一批次。

使用非阻塞排他文件锁（flock）。锁随操作系统内核的文件描述句柄存在，
进程退出或崩溃时内核自动释放，不会留下脏标记；这是本地协调手段，
不构成任何远端承诺。
"""

import fcntl
from pathlib import Path

from .batch_types import BatchAlreadyRunning


class BatchLock:
    def __init__(self, lock_path):
        self.path = Path(lock_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = None

    def acquire(self):
        self.handle = open(self.path, 'a+')
        try:
            # LOCK_NB：拿不到锁立即报错，而不是排队等待后造成重复点击。
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self.handle.close()
            self.handle = None
            raise BatchAlreadyRunning(
                f'批次已在另一进程中运行，锁文件：{self.path}'
            ) from error
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write('locked\n')
        self.handle.flush()
        return self

    def release(self):
        if self.handle is not None:
            try:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.handle.close()
                self.handle = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc, traceback):
        self.release()
        return False
