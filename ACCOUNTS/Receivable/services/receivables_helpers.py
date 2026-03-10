# ACCOUNTS/services/receivables_helpers.py

"""
Compatibility shim.

Some parts of the project import helper functions from:
    ACCOUNTS.services.receivables_helpers

We actually keep the implementations in:
    ACCOUNTS.services.receivables_targets

This module re-exports the same functions to avoid changing older imports.
"""

from .receivables_targets import (
    get_receivable_entries_for_period,
    get_open_bills_for_party,
    get_open_bills_for_period,
    build_paid_lookup_for_period,
    get_received_rows_for_period,
)
