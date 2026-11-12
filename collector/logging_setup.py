"""日志配置：结构化记录业务步骤，且绝不泄露会话值。

引擎只记录审批单号、状态、回执号、异常类型名等业务信息，不记录
cookie/token/请求头。RedactionFilter 再兜底过滤任何形如
``Set-Cookie: ...``、``Authorization: ...`` 的内容。
"""

import json
import logging
import re

_SECRET_PATTERNS = [
    re.compile(r'(?i)(set-cookie\s*[:=]\s*)[^\s;,]+'),
    re.compile(r'(?i)(authorization\s*[:=]\s*)\S+'),
    re.compile(r'(?i)((?:session[_-]?id|sid|token|cookie)\s*[:=]\s*)\S+'),
    re.compile(r'(?i)(bearer\s+)[A-Za-z0-9._\-]+'),
]
_REDACTION = r'\1***REDACTED***'


def redact(message):
    cleaned = str(message)
    for pattern in _SECRET_PATTERNS:
        cleaned = pattern.sub(_REDACTION, cleaned)
    return cleaned


class RedactionFilter(logging.Filter):
    def filter(self, record):
        record.msg = redact(record.getMessage())
        record.args = ()
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            'ts': time_str(record.created),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        if record.exc_info:
            payload['error_type'] = record.exc_info[0].__name__
        return json.dumps(payload, ensure_ascii=False)


def time_str(timestamp):
    import time

    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(timestamp))


def configure_logging(logger_name='collector.batch', level=logging.INFO):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.addFilter(RedactionFilter())
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger
