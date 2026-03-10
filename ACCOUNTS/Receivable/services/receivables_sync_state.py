# ACCOUNTS/services/receivables_sync_state.py
from django.core.cache import cache

TTL_SECONDS = 60 * 60  # 1 hour

def _key(run_id: str) -> str:
    return f"recv_sync:{run_id}"

def set_state(run_id: str, **kwargs):
    key = _key(run_id)
    state = cache.get(key) or {}
    state.update(kwargs)
    cache.set(key, state, TTL_SECONDS)
    return state

def get_state(run_id: str):
    return cache.get(_key(run_id)) or {
        "status": "unknown",
        "percent": 0,
        "step": "",
        "message": "No sync info available.",
    }