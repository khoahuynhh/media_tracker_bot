import os
import smtplib
from email.message import EmailMessage


def _smtp_configured() -> bool:
    return bool(os.getenv("SMTP_HOST") and os.getenv("SMTP_USER") and os.getenv("SMTP_PASS"))


def send_email(to_email: str, subject: str, body: str) -> bool:
    """Send an email using SMTP env config.

    Required env: SMTP_HOST, SMTP_PORT (optional, default 587), SMTP_USER, SMTP_PASS, SMTP_FROM (optional)
    """
    if not _smtp_configured():
        # Not configured; pretend success so UX can proceed in dev
        print(f"[DEV] Email to {to_email}: {subject}\n{body}")
        return True

    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    use_tls = os.getenv("SMTP_USE_TLS", "1") == "1"
    from_email = os.getenv("SMTP_FROM") or user

    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port, timeout=10) as smtp:
            if use_tls:
                smtp.starttls()
            smtp.login(user, password)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False

