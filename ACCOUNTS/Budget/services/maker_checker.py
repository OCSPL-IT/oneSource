# services/maker_checker.py
import logging
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from django.utils import timezone

from ..models import MakerCheckerState, MCStatus

logger = logging.getLogger(__name__)

# keep logs readable if someone pastes large remarks
_LOG_REMARKS_MAX = 200


def _trim(s: str, n: int = _LOG_REMARKS_MAX) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."


def _user_tag(user) -> str:
    if not user:
        return "-"
    uid = getattr(user, "pk", None)
    uname = getattr(user, "get_username", None)
    uname = uname() if callable(uname) else getattr(user, "username", None)
    if uname:
        return f"{uname}({uid})"
    return f"{uid}"


def _obj_tag(obj) -> str:
    cls = obj.__class__.__name__
    pk = getattr(obj, "pk", None)
    return f"{cls}({pk})"


def _mc_tag(st) -> str:
    if not st:
        return "MC(None)"
    return f"MC({st.pk})"


def _log_transition(action: str, obj, scope: str, st, old_status: str, new_status: str, user=None, remarks: str = ""):
    logger.info(
        "MC %s %s scope=%s %s %s -> %s by=%s remarks=%r",
        action,
        _obj_tag(obj),
        (scope or ""),
        _mc_tag(st),
        old_status,
        new_status,
        _user_tag(user),
        _trim(remarks),
    )


def _log_noop(action: str, obj, scope: str, st, reason: str, user=None):
    # Use DEBUG so production logs don’t get noisy; bump to INFO if you want.
    logger.debug(
        "MC %s NO-OP %s scope=%s %s status=%s by=%s reason=%s",
        action,
        _obj_tag(obj),
        (scope or ""),
        _mc_tag(st),
        getattr(st, "status", None),
        _user_tag(user),
        reason,
    )


# ----------------------------
# schema helpers
# ----------------------------
def _model_field_names(model_cls) -> set[str]:
    return {
        f.name
        for f in model_cls._meta.get_fields()
        if getattr(f, "concrete", False)
    }


_MC_FIELDS = _model_field_names(MakerCheckerState)


def _has_field(name: str) -> bool:
    return name in _MC_FIELDS


def _set_if_field(obj, field: str, value) -> bool:
    if _has_field(field):
        setattr(obj, field, value)
        return True
    return False


def _status_codes() -> set[str]:
    return {c[0] for c in MCStatus.choices}


def _has_checked_status() -> bool:
    # Backward compatible even if CHECKED isn't in older DB/code
    return "CHECKED" in _status_codes()


def _checked_code() -> str:
    # Always return the literal "CHECKED" so it matches DB value
    return "CHECKED"


def _safe_update_fields(*names: str) -> list[str]:
    # Only include fields that exist in MakerCheckerState
    out = []
    for n in names:
        if _has_field(n):
            out.append(n)
    return out


# ----------------------------
# core getter
# ----------------------------
def mc_get(obj, scope: str):
    """
    IMPORTANT:
    - If obj is not saved (pk=None), do NOT create MC state (object_id cannot be NULL).
    - Return None in that case; views must treat None as unlocked/new.
    """
    pk = getattr(obj, "pk", None)
    if not pk:
        logger.debug("MC get skipped (unsaved obj) %s scope=%s", _obj_tag(obj), (scope or ""))
        return None

    ct = ContentType.objects.get_for_model(obj.__class__)
    st, created = MakerCheckerState.objects.get_or_create(
        content_type=ct,
        object_id=pk,
        scope=(scope or ""),
        defaults={"status": MCStatus.DRAFT},
    )
    if created:
        logger.info(
            "MC created %s scope=%s %s status=%s",
            _obj_tag(obj),
            (scope or ""),
            _mc_tag(st),
            getattr(st, "status", None),
        )
    return st


# ----------------------------
# actions
# ----------------------------
@transaction.atomic
def mc_submit(obj, scope: str, user):
    """
    Maker action:
      DRAFT/REJECTED -> SUBMITTED
    """
    st = mc_get(obj, scope)
    if st is None:
        _log_noop("submit", obj, scope, st, "object_not_saved", user=user)
        return None

    if st.status not in (MCStatus.DRAFT, MCStatus.REJECTED):
        _log_noop("submit", obj, scope, st, "invalid_current_status", user=user)
        return st

    old = st.status
    st.status = MCStatus.SUBMITTED
    _set_if_field(st, "submitted_by", user)
    _set_if_field(st, "submitted_at", timezone.now())

    # reset downstream
    _set_if_field(st, "checked_by", None)
    _set_if_field(st, "checked_at", None)
    _set_if_field(st, "checker_remarks", "")

    _set_if_field(st, "approved_by", None)
    _set_if_field(st, "approved_at", None)
    _set_if_field(st, "approver_remarks", "")

    st.save(update_fields=_safe_update_fields(
        "status",
        "submitted_by", "submitted_at",
        "checked_by", "checked_at", "checker_remarks",
        "approved_by", "approved_at", "approver_remarks",
        "updated_at",
    ))

    _log_transition("submit", obj, scope, st, old, st.status, user=user)
    return st


@transaction.atomic
def mc_check(obj, scope: str, user, remarks: str = ""):
    """
    Checker action:

    If CHECKED exists:
      SUBMITTED -> CHECKED

    If CHECKED does NOT exist:
      no-op (keeps older workflow unchanged)
    """
    st = mc_get(obj, scope)
    if st is None:
        _log_noop("check", obj, scope, st, "object_not_saved", user=user)
        return None

    if not _has_checked_status():
        _log_noop("check", obj, scope, st, "checked_status_not_supported", user=user)
        return st

    if st.status != MCStatus.SUBMITTED:
        _log_noop("check", obj, scope, st, "invalid_current_status", user=user)
        return st

    old = st.status
    st.status = _checked_code()
    _set_if_field(st, "checked_by", user)
    _set_if_field(st, "checked_at", timezone.now())
    _set_if_field(st, "checker_remarks", _trim(remarks))

    st.save(update_fields=_safe_update_fields(
        "status", "checked_by", "checked_at", "checker_remarks", "updated_at"
    ))

    _log_transition("check", obj, scope, st, old, st.status, user=user, remarks=remarks)
    return st


@transaction.atomic
def mc_approve(obj, scope: str, user, remarks: str = ""):
    """
    Approver action:

    If CHECKED exists:
      CHECKED -> APPROVED

    Else (old flow):
      SUBMITTED -> APPROVED
    """
    st = mc_get(obj, scope)
    if st is None:
        _log_noop("approve", obj, scope, st, "object_not_saved", user=user)
        return None

    if _has_checked_status():
        if st.status != _checked_code():
            _log_noop("approve", obj, scope, st, "invalid_current_status_requires_checked", user=user)
            return st
    else:
        if st.status != MCStatus.SUBMITTED:
            _log_noop("approve", obj, scope, st, "invalid_current_status_requires_submitted", user=user)
            return st

    old = st.status
    st.status = MCStatus.APPROVED

    # If you have dedicated approver fields, use them; else reuse checked_*
    if _has_field("approved_by"):
        _set_if_field(st, "approved_by", user)
        _set_if_field(st, "approved_at", timezone.now())
        _set_if_field(st, "approver_remarks", _trim(remarks))
        fields = _safe_update_fields("status", "approved_by", "approved_at", "approver_remarks", "updated_at")
    else:
        _set_if_field(st, "checked_by", user)
        _set_if_field(st, "checked_at", timezone.now())
        _set_if_field(st, "checker_remarks", _trim(remarks))
        fields = _safe_update_fields("status", "checked_by", "checked_at", "checker_remarks", "updated_at")

    st.save(update_fields=fields)

    _log_transition("approve", obj, scope, st, old, st.status, user=user, remarks=remarks)
    return st


@transaction.atomic
def mc_reject(obj, scope: str, user, remarks: str):
    """
    Reject / Disapprove:
      SUBMITTED/CHECKED/APPROVED -> REJECTED

    Stores actor & remarks (backward compatible).
    """
    st = mc_get(obj, scope)
    if st is None:
        _log_noop("reject", obj, scope, st, "object_not_saved", user=user)
        return None

    allowed = {MCStatus.SUBMITTED, MCStatus.APPROVED, MCStatus.REJECTED, MCStatus.DRAFT}
    if _has_checked_status():
        allowed.add(_checked_code())

    if st.status not in allowed:
        _log_noop("reject", obj, scope, st, "invalid_current_status", user=user)
        return st

    old = st.status
    st.status = MCStatus.REJECTED

    # keep old audit fields
    _set_if_field(st, "checked_by", user)
    _set_if_field(st, "checked_at", timezone.now())
    _set_if_field(st, "checker_remarks", _trim(remarks))

    # optional future fields (if you add later)
    _set_if_field(st, "rejected_by", user)
    _set_if_field(st, "rejected_at", timezone.now())
    _set_if_field(st, "rejection_remarks", _trim(remarks))

    st.save(update_fields=_safe_update_fields(
        "status",
        "checked_by", "checked_at", "checker_remarks",
        "rejected_by", "rejected_at", "rejection_remarks",
        "updated_at",
    ))

    _log_transition("reject", obj, scope, st, old, st.status, user=user, remarks=remarks)
    return st
