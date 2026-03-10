from django.utils import timezone
from django.utils.deprecation import MiddlewareMixin
from django.db import transaction
from django.utils.dateparse import parse_datetime
from .models import UserStatus


class LastSeenMiddleware(MiddlewareMixin):
    """
    Update UserStatus.last_seen for authenticated users.
    - Uses 'default' DB explicitly.
    - Throttles to once per 60s via session.
    """
    UPDATE_EVERY_SECONDS = 60
    DB_ALIAS = 'default'

    def process_request(self, request):
        user = getattr(request, "user", None)
        if not user or not user.is_authenticated:
            return

        now = timezone.now()
        last_touch = request.session.get("_last_seen_ts")
        if last_touch:
            dt = parse_datetime(last_touch)
            if dt and (now - dt).total_seconds() < self.UPDATE_EVERY_SECONDS:
                return

        try:
            with transaction.atomic(using=self.DB_ALIAS):
                obj, _ = (UserStatus.objects.using(self.DB_ALIAS)
                          .select_for_update()
                          .get_or_create(user_id=user.id))
                if (now - obj.last_seen).total_seconds() > self.UPDATE_EVERY_SECONDS:
                    obj.last_seen = now
                    obj.save(using=self.DB_ALIAS, update_fields=["last_seen"])
            request.session["_last_seen_ts"] = now.isoformat()
        except Exception as e:
            # log if you want, but don't break the request
            pass


# main/middleware.py
import uuid
from threading import local

_state = local()

def get_ctx():
    return getattr(_state, "ctx", {})

class RequestLogContextMiddleware:
    """Attach request context for log records."""
    def __init__(self, get_response):
        self.get_response = get_response

        # Small helper: drop noisy broken-pipe messages (set on class to reuse)
        self.BROKEN_PIPE = "- Broken pipe from"

    def __call__(self, request):
        _state.ctx = {
            "req_id": uuid.uuid4().hex[:12],
            "ip": (request.META.get("HTTP_X_FORWARDED_FOR", "").split(",")[0].strip()
                   or request.META.get("REMOTE_ADDR", "")) or "-",
            "method": request.method,
            "path": request.path,
            "user": (request.user.username if getattr(request, "user", None) and request.user.is_authenticated else "anon"),
            "status": "-",
        }
        try:
            response = self.get_response(request)
            _state.ctx["status"] = getattr(response, "status_code", "-")
            return response
        finally:
            _state.ctx = {}

class RequestContextFilter:
    """Injects request context into each log record."""
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
    """Suppress 'Broken pipe' info lines from django.server."""
    def filter(self, record):
        msg = str(record.getMessage())
        return "Broken pipe from" not in msg


# main/middleware.py
from django.utils.deprecation import MiddlewareMixin
from .log_filters import set_ctx, new_req_id

class RequestLogContextMiddleware(MiddlewareMixin):
    """
    Attaches request context to threadlocal so log filters can read it.
    Safe to import during request time (not used by LOGGING config).
    """
    def process_request(self, request):
        ip = (request.META.get("HTTP_X_FORWARDED_FOR", "").split(",")[0].strip()
              or request.META.get("REMOTE_ADDR", "")) or "-"
        user = getattr(request, "user", None)
        set_ctx(
            req_id=new_req_id(),
            ip=ip,
            method=request.method,
            path=request.path,
            user=(user.username if user and user.is_authenticated else "anon"),
            status="-",
        )

    def process_response(self, request, response):
        # Update status for access log lines
        set_ctx(status=getattr(response, "status_code", "-"))
        return response



########################################################################################################

## AUdit Log Related code



# main/middleware.py
import threading

_thread_locals = threading.local()


def get_current_request():
    return getattr(_thread_locals, "request", None)


def get_current_user():
    request = get_current_request()
    if request is not None:
        return getattr(request, "user", None)
    return None


class CurrentUserMiddleware:
    """
    Store the current request & user in thread-local storage.

    Add this to MIDDLEWARE so signals can know who triggered the change.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        _thread_locals.request = request
        response = self.get_response(request)
        # optional: clear after response
        _thread_locals.request = None
        return response


