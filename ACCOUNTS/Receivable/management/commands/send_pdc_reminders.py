from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from django.db import transaction

from ACCOUNTS.Receivable.models import Receivable
from ACCOUNTS.Receivable.models import Party
from ACCOUNTS.Receivable.models import OutgoingEmailAccount
from ACCOUNTS.Receivable.models import PDCReminderLog

from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string

def _is_pdc(receivable, today):
    remarks_l = (receivable.remarks or "").lower()
    if "pdc" in remarks_l:
        return True
    if receivable.cheque_date and receivable.cheque_date > today:
        return True
    return False

class Command(BaseCommand):
    help = "Send PDC reminder emails 7 days before cheque_date."

    def handle(self, *args, **options):
        today = timezone.localdate()
        target_day = today + timedelta(days=7)  # cheque_date is 7 days ahead of today

        qs = (
            Receivable.objects
            .filter(cheque_date=target_day)
            .exclude(status="Received")  # adapt if your statuses differ
            .order_by("customer_name", "invoice_number", "id")
        )

        sent = 0
        skipped = 0
        failed = 0

        for rv in qs:
            if not _is_pdc(rv, today):
                skipped += 1
                continue

            # Idempotency check
            if PDCReminderLog.objects.filter(receivable=rv, reminder_date=today).exists():
                skipped += 1
                continue

            party = Party.objects.filter(party_code=rv.customer_code, is_active=True).first()
            if not party:
                self._log_fail(rv, today, "Party master not found for customer_code.")
                failed += 1
                continue

            emails = list(
                party.contacts.filter(is_active=True, receive_pdc_reminder=True)
                .exclude(email="")
                .values_list("email", flat=True)
            )
            emails = sorted({e.strip().lower() for e in emails if e})
            if not emails:
                self._log_fail(rv, today, "No party contact emails marked for PDC reminder.")
                failed += 1
                continue

            # Sender account (company_group is available in targets; for receivable use your logic)
            # If you cannot map company_group per receivable, keep a default "ALL".
            company_group = "ALL"
            acct = OutgoingEmailAccount.objects.filter(company_group=company_group, is_active=True).order_by("id").first()
            if not acct:
                self._log_fail(rv, today, f"No OutgoingEmailAccount found for company_group={company_group}.")
                failed += 1
                continue

            subject = f"PDC Reminder: Cheque Due on {rv.cheque_date.strftime('%d-%m-%Y')} | {rv.customer_name}"

            # Email body (use templates)
            ctx = {
                "party": party,
                "rv": rv,
                "today": today,
            }
            html_body = render_to_string("accounts/emails/pdc_reminder.html", ctx)
            text_body = render_to_string("accounts/emails/pdc_reminder.txt", ctx)

            try:
                with transaction.atomic():
                    msg = EmailMultiAlternatives(
                        subject=subject,
                        body=text_body,
                        from_email=f"{acct.from_name} <{acct.from_email}>",
                        to=emails,
                    )
                    msg.attach_alternative(html_body, "text/html")
                    msg.send(fail_silently=False)

                    PDCReminderLog.objects.create(
                        receivable=rv,
                        reminder_date=today,
                        sent_to=", ".join(emails),
                        subject=subject,
                        status="Sent",
                    )
                sent += 1

            except Exception as ex:
                self._log_fail(rv, today, str(ex))
                failed += 1

        self.stdout.write(self.style.SUCCESS(f"PDC Reminders done. Sent={sent}, Skipped={skipped}, Failed={failed}"))

    def _log_fail(self, rv, today, err):
        PDCReminderLog.objects.get_or_create(
            receivable=rv,
            reminder_date=today,
            defaults={"status": "Failed", "error": err},
        )
