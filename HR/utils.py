# HR/utils.py (or wherever your utils live)
from django.contrib.auth.models import Group
from django.core.mail import send_mail
from django.urls import reverse
from django.conf import settings
from django.utils.html import escape


def is_hr(user):
    return user.is_authenticated and user.groups.filter(name="Emp_joining_HR").exists()

def is_it_user(user):
    return user.is_authenticated and user.groups.filter(name="IT_User").exists()

def is_it_approver(user):
    return user.is_authenticated and user.groups.filter(name="IT_Approver").exists()


# ---------- EMAIL HELPERS ----------

def _get_group_emails(group_name: str):
    """
    Return list of email addresses of all active users in the given group.
    """
    try:
        group = Group.objects.get(name=group_name)
    except Group.DoesNotExist:
        return []

    return list(
        group.user_set.filter(is_active=True)
        .exclude(email__isnull=True)
        .exclude(email__exact="")
        .values_list("email", flat=True)
    )


def send_employee_joining_to_it_users(obj, request):
    """
    Send notification email to all users in IT_User group
    when HR submits a new EmployeeJoining form.
    """
    recipients = _get_group_emails("IT_User")
    if not recipients:
        return 0

    subject = f"[Employee Joining] IT provisioning required for {obj.employee_name} ({obj.employee_id})"

    it_url = request.build_absolute_uri(
        reverse("employee_joining_it_update", args=[obj.pk])
    )

    # Plain text (fallback)
    message = f"""Dear IT Team,

A new employee joining form has been submitted by HR and requires IT / infra provisioning.

Employee : {obj.employee_name} ({obj.employee_id})
Company  : {obj.company}
Location : {obj.location}
Dept     : {obj.department or '-'}
Designation : {obj.designation or '-'}
DOJ      : {obj.date_of_joining:%d-%b-%Y}
Status   : {obj.status or '-'}

Please open the IT processing screen:
{it_url}

This is an automated email from oneSource. Do Not Replay.
"""

    # HTML (bold + highlight for Location and Status)
    html_message = f"""
    <p>Dear IT Team,</p>

    <p>A new employee joining form has been submitted by HR and requires IT / infra provisioning.</p>

    <p>
      Employee : {escape(obj.employee_name)} ({escape(str(obj.employee_id))})<br>
      Company  : {escape(str(obj.company))}<br>
      <strong style="background:#fff3cd;padding:2px 6px;border-radius:4px;">
        Location : {escape(str(obj.location))}
      </strong><br>
      Dept     : {escape(str(obj.department or '-'))}<br>
      Designation : {escape(str(obj.designation or '-'))}<br>
      DOJ      : {escape(obj.date_of_joining.strftime('%d-%b-%Y'))}<br>
      <strong style="background:#d1e7dd;padding:2px 6px;border-radius:4px;">
        Status : {escape(str(obj.status or '-'))}
      </strong>
    </p>

    <p>Please open the IT processing screen:<br>
      <a href="{escape(it_url)}">{escape(it_url)}</a>
    </p>

    <p>This is an automated email from oneSource. Do Not Replay .</p>
    """

    return send_mail(
        subject=subject,
        message=message,  # text fallback
        from_email=settings.DEFAULT_FROM_EMAIL,
        recipient_list=recipients,
        fail_silently=False,
        html_message=html_message,  # ✅ HTML with bold/highlight
    )


def send_employee_joining_to_approvers(obj, request):
    recipients = _get_group_emails("IT_Approver")
    if not recipients:
        return 0

    subject = f"[Employee Joining] Approval required for {obj.employee_name} ({obj.employee_id})"

    approve_url = request.build_absolute_uri(
        reverse("employee_joining_approve", args=[obj.pk])
    )

    # Plain text (fallback)
    message = f"""Dear Approver,

IT has completed infra provisioning for the following employee and the record is pending your approval.

Employee : {obj.employee_name} ({obj.employee_id})
Company  : {obj.company}
Location : {obj.location}
Dept     : {obj.department or '-'}
Designation : {obj.designation or '-'}
Status   : {obj.status or '-'}

Please review and approve / reject here:
{approve_url}

This is an automated email from oneSource. Do Not Reply.
"""

    # HTML (bold + highlight Location and Status)
    html_message = f"""
    <p>Dear Approver,</p>

    <p>IT has completed infra provisioning for the following employee and the record is pending your approval.</p>

    <p>
      Employee : {escape(obj.employee_name)} ({escape(str(obj.employee_id))})<br>
      Company  : {escape(str(obj.company))}<br>
      <strong style="background:#fff3cd;padding:2px 6px;border-radius:4px;">
        Location : {escape(str(obj.location))}
      </strong><br>
      Dept     : {escape(str(obj.department or '-'))}<br>
      Designation : {escape(str(obj.designation or '-'))}<br>
      <strong style="background:#d1e7dd;padding:2px 6px;border-radius:4px;">
        Status : {escape(str(obj.status or '-'))}
      </strong>
    </p>

    <p>Please review and approve / reject here:<br>
      <a href="{escape(approve_url)}">{escape(approve_url)}</a>
    </p>

    <p>This is an automated email from oneSource. Do Not Reply.</p>
    """

    return send_mail(
        subject=subject,
        message=message,  # text fallback
        from_email=settings.DEFAULT_FROM_EMAIL,
        recipient_list=recipients,
        fail_silently=False,
        html_message=html_message,  # ✅ HTML with bold/highlight
    )
    
    
    
def send_employee_joining_status_to_hr_and_it(obj, request):
    recipients = set()
    for gname in ["Emp_joining_HR", "IT_User"]:
        recipients.update(_get_group_emails(gname))
    if not recipients:
        return 0

    detail_url = request.build_absolute_uri(
        reverse("employee_joining_detail", args=[obj.pk])
    )

    status_display = obj.get_status_display()  # from STATUS_CHOICES
    subject = f"[Employee Joining] {status_display} for {obj.employee_name} ({obj.employee_id})"

    # Plain text (fallback)
    message = f"""Dear Team,

Employee joining request has been {status_display.lower()}.

Employee : {obj.employee_name} ({obj.employee_id})
Company  : {obj.company}
Location : {obj.location}
Dept     : {obj.department or '-'}
Designation : {obj.designation or '-'}

Status   : {status_display}
Approval Remark: {obj.approval_remark or '-'}

You can view the full record here:
{detail_url}

This is an automated email from oneSource Do Not Replay.
"""

    # HTML (bold + highlight Location and Status)
    html_message = f"""
    <p>Dear Team,</p>

    <p>Employee joining request has been <strong>{escape(status_display.lower())}</strong>.</p>

    <p>
      Employee : {escape(obj.employee_name)} ({escape(str(obj.employee_id))})<br>
      Company  : {escape(str(obj.company))}<br>
      <strong style="background:#fff3cd;padding:2px 6px;border-radius:4px;">
        Location : {escape(str(obj.location))}
      </strong><br>
      Dept     : {escape(str(obj.department or '-'))}<br>
      Designation : {escape(str(obj.designation or '-'))}<br><br>

      <strong style="background:#d1e7dd;padding:2px 6px;border-radius:4px;">
        Status : {escape(status_display)}
      </strong><br>
      Approval Remark: {escape(str(obj.approval_remark or '-'))}
    </p>

    <p>You can view the full record here:<br>
      <a href="{escape(detail_url)}">{escape(detail_url)}</a>
    </p>

    <p>This is an automated email from oneSource Do Not Replay .</p>
    """

    return send_mail(
        subject=subject,
        message=message,  # text fallback
        from_email=settings.DEFAULT_FROM_EMAIL,
        recipient_list=list(recipients),
        fail_silently=False,
        html_message=html_message,  # ✅ HTML with bold/highlight
    )