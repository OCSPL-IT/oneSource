# main/log_filters.py
import uuid
from threading import local

_state = local()

def set_ctx(**kwargs):
    _state.ctx = kwargs

def get_ctx():
    return getattr(_state, "ctx", {})

class RequestContextFilter:
    """Injects request context (req_id, user, ip, method, path, status) into log records."""
    def filter(self, record):
        ctx = get_ctx()
        record.req_id = ctx.get("req_id", "-")
        record.ip     = ctx.get("ip", "-")
        record.method = ctx.get("method", "-")
        record.path   = ctx.get("path", "-")
        record.user   = ctx.get("user", "-")
        record.status = ctx.get("status", "-")
        return True

class DropBrokenPipeFilter:
    """Suppress 'Broken pipe' lines from django.server access logs."""
    def filter(self, record):
        return "Broken pipe from" not in record.getMessage()

# Helper to generate a request id if you want (not required for filters)
def new_req_id():
    return uuid.uuid4().hex[:12]
