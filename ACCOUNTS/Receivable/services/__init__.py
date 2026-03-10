# ACCOUNTS/services/__init__.py
"""
Service package exports.

Goal:
- Keep imports STABLE (avoid circular import failures)
- Still allow: from ACCOUNTS.services import <public funcs>
- Export underscore date parsers only if you really need them in views
"""

# --- Always-safe, lightweight exports first (no circular dependencies) ---
from .receivables_parsers import _parse_ui_date, _parse_sql_display_date


# --- Optional / heavier exports (guarded to prevent circular import crash) ---
# NOTE:
# If any of these modules import ACCOUNTS.services again (directly/indirectly),
# Python may partially initialize the package. Guarded imports prevent hard crashes.
try:
    from .receivables_dashboard import build_receivable_dashboard_context
except Exception:
    build_receivable_dashboard_context = None

try:
    from .receivables_targets import (
        get_receivable_entries_for_period,
        get_open_bills_for_period,
        get_open_bills_for_party,
        build_paid_lookup_for_period,
        get_received_rows_for_period,
    )
except Exception:
    get_receivable_entries_for_period = None
    get_open_bills_for_period = None
    get_open_bills_for_party = None
    build_paid_lookup_for_period = None
    get_received_rows_for_period = None


__all__ = [
    # parsers (used by views / services)
    "_parse_ui_date",
    "_parse_sql_display_date",

    # dashboard
    "build_receivable_dashboard_context",

    # targets
    "get_receivable_entries_for_period",
    "get_open_bills_for_period",
    "get_open_bills_for_party",
    "build_paid_lookup_for_period",
    "get_received_rows_for_period",
]
