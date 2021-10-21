import os
import filelock
from gladier.config import GladierSecretsConfig


class LockedConfig(GladierSecretsConfig):

    lock = filelock.FileLock(os.path.expanduser('~/.gladier-secrets.cfg.lock'), timeout=1)

    def save(self):
        with self.lock:
            super().save()
