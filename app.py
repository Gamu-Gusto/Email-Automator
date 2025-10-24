import streamlit as st
from PIL import Image
import os

# Modern UI imports
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_extras.colored_header import colored_header
from streamlit_extras.card import card
from streamlit_option_menu import option_menu
import plotly.express as px
import plotly.graph_objects as go

# Set page config must be the first Streamlit command
st.set_page_config(
    page_title="UIF TERS Compliance Audit",
    page_icon="✉️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern styling
def load_css():
    with open("static/styles.css") as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Load modern CSS
load_css()

# Logo will be added to sidebar

import os
import sqlite3
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.utils import make_msgid, formatdate
import time
import imaplib
import traceback

DATABASE_FILE = 'compliance_emails.db'
SMTP_SERVER = "mail.dithetoaccountants.co.za"
IMAP_SERVER = "mail.dithetoaccountants.co.za"
SMTP_PORT = 587
IMAP_PORT = 993
SENDER_EMAIL = "gamu@dithetoaccountants.co.za"

def init_db():
    with sqlite3.connect(DATABASE_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                UIF_REFERENCE TEXT PRIMARY KEY,
                TRADE_NAME TEXT,
                EMAIL_ADDRESS TEXT,
                PHONE TEXT,
                emails_sent INTEGER DEFAULT 0,
                last_sent TEXT,
                completed INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS email_templates (
                template_key TEXT PRIMARY KEY,
                subject TEXT,
                body TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS company_emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                UIF_REFERENCE TEXT,
                EMAIL TEXT,
                UNIQUE(UIF_REFERENCE, EMAIL)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS email_error_logs (
                error_id INTEGER PRIMARY KEY AUTOINCREMENT,
                UIF_REFERENCE TEXT,
                recipient TEXT,
                stage TEXT,
                timestamp TEXT,
                error_type TEXT,
                error_message TEXT,
                traceback TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS email_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                UIF_REFERENCE TEXT,
                timestamp TEXT,
                date TEXT,
                subject TEXT,
                status TEXT,
                FOREIGN KEY (UIF_REFERENCE) REFERENCES companies(UIF_REFERENCE)
            )
        ''')
        # Migration: ensure completed column exists
        cursor.execute("PRAGMA table_info(companies)")
        cols = [row[1] for row in cursor.fetchall()]
        if 'completed' not in cols:
            cursor.execute("ALTER TABLE companies ADD COLUMN completed INTEGER DEFAULT 0")
        if 'PHONE' not in cols:
            cursor.execute("ALTER TABLE companies ADD COLUMN PHONE TEXT")
        # Seed default email templates if missing
        try:
            defaults = {
                'initial': {
                    'subject': 'Initial Request for Documentation – UIF TERS Compliance Audit',
                    'body': (
                        """
                        <!DOCTYPE html>
                        <html>
                          <head>
                            <meta http-equiv="content-type" content="text/html; charset=UTF-8">
                          </head>
                          <body>
                            <p>Good day,</p>
                            <p>I trust this email finds you well.</p>
                            <p>My name is Gamu, and I am an Auditor at Ditheto Accountants. We are conducting a compliance review on behalf of the Unemployment Insurance Fund (UIF), regarding the COVID-19 TERS funds disbursed to <strong>{TRADE_NAME}</strong> with UIF Ref Number: <strong>{UIF_REFERENCE}</strong>.</p>
                            <p>The following documents are required to complete the audit:</p>
                            <ul>
                              <li>Bank statements showing receipt of the TERS funds</li>
                              <li>Proof of payments made to employees</li>
                              <li>Payroll records or payslips for January to December 2020</li>
                              <li>2021 IRP5 certificates</li>
                              <li>EMP501s/201s for the 2020/2021 financial year</li>
                            </ul>
                            <p>We fully understand that administrative delays can occur. However, these documents are essential to finalising the audit in line with UIF compliance procedures. Without them, the file remains incomplete and may be subject to further administrative steps.</p>
                            <p>Kindly forward the requested documents to <strong><a href="mailto:gamu@dithetoaccountants.co.za">gamu@dithetoaccountants.co.za</a></strong> at your earliest convenience.</p>
                            <p>Your cooperation in resolving this matter is greatly appreciated.</p>
                            <p>Kind regards,<br>
                              <strong>Gamu Dambanjera</strong></p>
                            <div class="moz-signature"> <img src='cid:signature'></div>
                          </body>
                        </html>
                        """
                    )
                },
                'followup': {
                    'subject': 'Follow-up: Urgent Action Required – UIF TERS Compliance Audit',
                    'body': (
                        """
                        <!DOCTYPE html>
                        <html>
                          <head>
                            <meta http-equiv="content-type" content="text/html; charset=UTF-8">
                          </head>
                          <body>
                            <p>Dear {TRADE_NAME},</p>
                            <p>This is a reminder that the requested documentation for the COVID-19 TERS audit is still outstanding. This is an urgent matter that requires your immediate attention.</p>
                            <p>Failure to provide the necessary documents may result in non-compliance with UIF regulations, which could lead to further administrative actions.</p>
                            <p>Please submit the required documents to <strong><a href="mailto:gamu@dithetoaccountants.co.za">gamu@dithetoaccountants.co.za</a></strong> without delay.</p>
                            <p>Kind regards,<br>
                              <strong>Gamu Dambanjera</strong></p>
                            <div class="moz-signature"> <img src='cid:signature'></div>
                          </body>
                        </html>
                        """
                    )
                },
                'final': {
                    'subject': 'Final Notice: Non-Compliance with UIF TERS Audit Requirements',
                    'body': (
                        """
                        <!DOCTYPE html>
                        <html>
                          <head>
                            <meta http-equiv="content-type" content="text/html; charset=UTF-8">
                          </head>
                          <body>
                            <p>Good day,</p>
                            <p>This serves as a final notice regarding your non-compliance with the UIF TERS audit requirements and the Memorandum of Agreement (MOA) signed during your TERS application.</p>
                            <p>Despite multiple attempts to obtain the required documentation, including:</p>
                            <ol>
                              <li>Initial audit notification</li>
                              <li>Detailed information request</li>
                              <li>Follow-up communication</li>
                            </ol>
                            <p>You have failed to cooperate with the audit process, which constitutes a breach of the MOA, specifically:</p>
                            <ul>
                              <li><strong>Section 17.2:</strong> Obligation to make all accounting records accessible to UIF-authorized persons</li>
                              <li><strong>Section 18:</strong> Requirement to maintain a proper audit trail of funds received and benefits paid to employees</li>
                            </ul>
                            <h3>NOTICE OF ESCALATION</h3>
                            <p>Be advised that failure to respond to this final notice within 24 hours will result in:</p>
                            <ol>
                              <li>Immediate referral of this matter to the Directorate for Priority Crime Investigation (HAWKS) or Special Investigation Unit (SIU) for investigation of potential fraud</li>
                              <li>Mandatory repayment of all TERS funds within 10 working days of referral</li>
                              <li>Potential criminal charges for non-compliance and fraud</li>
                            </ol>
                            <h3>REQUIRED IMMEDIATE ACTION</h3>
                            <p>To prevent escalation to law enforcement, provide ALL previously requested documentation within 24 hours of this notice to everyone copied on this email.</p>
                            <p>Please note that this is your final opportunity to comply with the audit requirements before legal action is initiated.</p>
                            <p>Kind regards,<br>
                              <strong>Gamu Dambanjera</strong></p>
                            <div class="moz-signature"> <img src='cid:signature'></div>
                          </body>
                        </html>
                        """
                    )
                }
            }
            for k, v in defaults.items():
                cursor.execute(
                    "INSERT OR IGNORE INTO email_templates (template_key, subject, body) VALUES (?, ?, ?)",
                    (k, v['subject'], v['body'])
                )
        except Exception:
            pass
        # Add test company
        cursor.execute('''
            INSERT OR IGNORE INTO companies (UIF_REFERENCE, TRADE_NAME, EMAIL_ADDRESS) VALUES (?, ?, ?)
        ''', ('TEST123456', 'Test Company', 'nicknungu@gmail.com'))
        conn.commit()

def log_send_error(stage, uif_ref, recipient, exc: Exception):
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO email_error_logs (UIF_REFERENCE, recipient, stage, timestamp, error_type, error_message, traceback) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    uif_ref or '',
                    recipient or '',
                    stage,
                    datetime.now().isoformat(),
                    type(exc).__name__,
                    str(exc),
                    traceback.format_exc()
                )
            )
            conn.commit()
    except Exception as e:
        st.error(f"Failed to write error log: {e}")

def get_companies_data():
    with sqlite3.connect(DATABASE_FILE) as conn:
        df = pd.read_sql_query("SELECT * FROM companies", conn)
    return df

def get_company_emails(uif_ref):
    """Return a deduplicated list of all recipient emails for a company.
    Includes the primary `companies.EMAIL_ADDRESS` and any in `company_emails`.
    """
    emails = []
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            # Primary email from companies table
            cursor.execute("SELECT EMAIL_ADDRESS FROM companies WHERE UIF_REFERENCE = ?", (uif_ref,))
            row = cursor.fetchone()
            if row and row[0]:
                emails.append(row[0].strip())
            # Additional emails
            cursor.execute("SELECT EMAIL FROM company_emails WHERE UIF_REFERENCE = ?", (uif_ref,))
            emails.extend([r[0].strip() for r in cursor.fetchall() if r and r[0]])
    except Exception:
        pass
    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for e in emails:
        if e and e not in seen:
            seen.add(e)
            deduped.append(e)
    return deduped

def add_additional_email(uif_ref, email):
    """Add an additional email for a company (no-op if duplicate)."""
    if not uif_ref or not email:
        return False, "Missing UIF reference or email"
    email = email.strip()
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            # Do not add if matches primary (case-insensitive)
            cursor.execute("SELECT EMAIL_ADDRESS FROM companies WHERE UIF_REFERENCE = ?", (uif_ref,))
            row = cursor.fetchone()
            if row and row[0] and row[0].strip().lower() == email.lower():
                return False, "Email matches the primary address"
            cursor.execute(
                "INSERT OR IGNORE INTO company_emails (UIF_REFERENCE, EMAIL) VALUES (?, ?)",
                (uif_ref, email)
            )
            conn.commit()
        return True, "Email added"
    except Exception as e:
        return False, str(e)

def remove_additional_email(uif_ref, email):
    """Remove an additional email for a company."""
    if not uif_ref or not email:
        return False, "Missing UIF reference or email"
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM company_emails WHERE UIF_REFERENCE = ? AND EMAIL = ?",
                (uif_ref, email)
            )
            conn.commit()
        return True, "Email removed"
    except Exception as e:
        return False, str(e)

def _parse_email_candidates(raw_text: str):
    """Parse a raw string of emails separated by commas, semicolons, or whitespace into unique emails."""
    if not raw_text:
        return []
    separators = [',', ';', '\n', '\t']
    text = raw_text.strip().replace('\r', ' ')
    for sep in separators:
        text = text.replace(sep, ' ')
    parts = [p.strip() for p in text.split(' ') if p and p.strip()]
    # Deduplicate preserving order; basic sanity filter
    seen = set()
    result = []
    for p in parts:
        candidate = p.strip()
        if candidate and candidate.lower() not in seen:
            seen.add(candidate.lower())
            result.append(candidate)
    return result

def add_additional_emails_bulk(uif_ref: str, raw_text: str):
    """Add multiple additional emails from a pasted list. Returns summary dict."""
    emails = _parse_email_candidates(raw_text)
    added = []
    skipped_duplicates = []
    skipped_invalid = []
    for e in emails:
        # Basic validation
        lower = e.lower()
        if '@' not in lower or '.' not in lower.split('@')[-1]:
            skipped_invalid.append(e)
            continue
        ok, msg = add_additional_email(uif_ref, e)
        if ok:
            added.append(e)
        else:
            # If duplicate or matches primary, treat as duplicate skip
            skipped_duplicates.append(e)
    return {
        'added': added,
        'skipped_duplicates': skipped_duplicates,
        'skipped_invalid': skipped_invalid
    }

def remove_additional_emails_bulk(uif_ref: str, emails_to_remove: list):
    """Remove multiple additional emails. Returns summary dict."""
    removed = []
    failed = []
    for email in emails_to_remove:
        ok, msg = remove_additional_email(uif_ref, email)
        if ok:
            removed.append(email)
        else:
            failed.append({'email': email, 'error': msg})
    return {
        'removed': removed,
        'failed': failed
    }

def validate_email_list(emails: list):
    """Validate a list of emails and return validation results."""
    valid = []
    invalid = []
    for email in emails:
        email = email.strip()
        if '@' in email and '.' in email.split('@')[-1] and len(email) > 5:
            valid.append(email)
        else:
            invalid.append(email)
    return valid, invalid

def update_final_email_template():
    """Update the final email template in the database with the new content."""
    new_final_template = {
        'subject': 'Final Notice: Non-Compliance with UIF TERS Audit Requirements',
        'body': '''<!DOCTYPE html>
<html>
  <head>
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
  </head>
  <body>
    <p>Good day,</p>
    <p>This serves as a final notice regarding your non-compliance with the UIF TERS audit requirements and the Memorandum of Agreement (MOA) signed during your TERS application.</p>
    <p>Despite multiple attempts to obtain the required documentation, including:</p>
    <ol>
      <li>Initial audit notification</li>
      <li>Detailed information request</li>
      <li>Follow-up communication</li>
    </ol>
    <p>You have failed to cooperate with the audit process, which constitutes a breach of the MOA, specifically:</p>
    <ul>
      <li><strong>Section 17.2:</strong> Obligation to make all accounting records accessible to UIF-authorized persons</li>
      <li><strong>Section 18:</strong> Requirement to maintain a proper audit trail of funds received and benefits paid to employees</li>
    </ul>
    <h3>NOTICE OF ESCALATION</h3>
    <p>Be advised that failure to respond to this final notice within 24 hours will result in:</p>
    <ol>
      <li>Immediate referral of this matter to the Directorate for Priority Crime Investigation (HAWKS) or Special Investigation Unit (SIU) for investigation of potential fraud</li>
      <li>Mandatory repayment of all TERS funds within 10 working days of referral</li>
      <li>Potential criminal charges for non-compliance and fraud</li>
    </ol>
    <h3>REQUIRED IMMEDIATE ACTION</h3>
    <p>To prevent escalation to law enforcement, provide ALL previously requested documentation within 24 hours of this notice to everyone copied on this email.</p>
    <p>Please note that this is your final opportunity to comply with the audit requirements before legal action is initiated.</p>
    <p>Kind regards,<br>
      <strong>Gamu Dambanjera</strong></p>
    <div class="moz-signature"> <img src='cid:signature'></div>
  </body>
</html>'''
    }
    
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE email_templates SET subject = ?, body = ? WHERE template_key = 'final'",
                (new_final_template['subject'], new_final_template['body'])
            )
            conn.commit()
        return True, "Final email template updated successfully"
    except Exception as e:
        return False, f"Failed to update template: {str(e)}"

def get_bounced_companies():
    """Get all companies with bounced emails along with their details."""
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            # Get companies with bounced emails
            bounced_query = """
                SELECT DISTINCT c.UIF_REFERENCE, c.TRADE_NAME, c.EMAIL_ADDRESS, c.PHONE,
                       COUNT(el.log_id) as bounce_count,
                       MAX(el.timestamp) as last_bounce_date,
                       GROUP_CONCAT(DISTINCT el.subject) as bounced_subjects
                FROM companies c
                INNER JOIN email_logs el ON c.UIF_REFERENCE = el.UIF_REFERENCE
                WHERE el.status = 'Bounced'
                GROUP BY c.UIF_REFERENCE, c.TRADE_NAME, c.EMAIL_ADDRESS, c.PHONE
                ORDER BY last_bounce_date DESC
            """
            bounced_df = pd.read_sql_query(bounced_query, conn)
            return bounced_df
    except Exception as e:
        st.error(f"Error retrieving bounced companies: {e}")
        return pd.DataFrame()

def get_failed_companies():
    """Get all companies with failed emails along with their details."""
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            # Get companies with failed emails
            failed_query = """
                SELECT DISTINCT c.UIF_REFERENCE, c.TRADE_NAME, c.EMAIL_ADDRESS, c.PHONE,
                       COUNT(el.log_id) as failure_count,
                       MAX(el.timestamp) as last_failure_date,
                       GROUP_CONCAT(DISTINCT el.subject) as failed_subjects
                FROM companies c
                INNER JOIN email_logs el ON c.UIF_REFERENCE = el.UIF_REFERENCE
                WHERE el.status = 'Failed'
                GROUP BY c.UIF_REFERENCE, c.TRADE_NAME, c.EMAIL_ADDRESS, c.PHONE
                ORDER BY last_failure_date DESC
            """
            failed_df = pd.read_sql_query(failed_query, conn)
            return failed_df
    except Exception as e:
        st.error(f"Error retrieving failed companies: {e}")
        return pd.DataFrame()

def get_unreachable_companies():
    """Get comprehensive list of all unreachable companies (bounced + failed)."""
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            # Get companies with any email delivery issues
            unreachable_query = """
                SELECT DISTINCT c.UIF_REFERENCE, c.TRADE_NAME, c.EMAIL_ADDRESS, c.PHONE,
                       COUNT(CASE WHEN el.status = 'Bounced' THEN 1 END) as bounce_count,
                       COUNT(CASE WHEN el.status = 'Failed' THEN 1 END) as failure_count,
                       COUNT(el.log_id) as total_issues,
                       MAX(el.timestamp) as last_issue_date,
                       GROUP_CONCAT(DISTINCT CASE WHEN el.status = 'Bounced' THEN el.subject END) as bounced_subjects,
                       GROUP_CONCAT(DISTINCT CASE WHEN el.status = 'Failed' THEN el.subject END) as failed_subjects
                FROM companies c
                INNER JOIN email_logs el ON c.UIF_REFERENCE = el.UIF_REFERENCE
                WHERE el.status IN ('Bounced', 'Failed')
                GROUP BY c.UIF_REFERENCE, c.TRADE_NAME, c.EMAIL_ADDRESS, c.PHONE
                ORDER BY last_issue_date DESC, total_issues DESC
            """
            unreachable_df = pd.read_sql_query(unreachable_query, conn)
            return unreachable_df
    except Exception as e:
        st.error(f"Error retrieving unreachable companies: {e}")
        return pd.DataFrame()

def export_unreachable_companies():
    """Export unreachable companies to CSV format."""
    try:
        df = get_unreachable_companies()
        if not df.empty:
            csv_data = df.to_csv(index=False).encode('utf-8')
            return csv_data
        else:
            return None
    except Exception as e:
        st.error(f"Error exporting unreachable companies: {e}")
        return None

def log_email(uif_reference, subject, status):
    """Log email activity to the database"""
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            date = datetime.now().strftime('%Y-%m-%d')
            cursor.execute(
                "INSERT INTO email_logs (UIF_REFERENCE, timestamp, date, subject, status) VALUES (?, ?, ?, ?, ?)",
                (uif_reference, timestamp, date, subject, status)
            )
            conn.commit()
    except Exception as e:
        st.warning(f"Failed to log email activity: {e}")

def get_email_logs(uif_ref=None):
    with sqlite3.connect(DATABASE_FILE) as conn:
        if uif_ref:
            df = pd.read_sql_query(f"SELECT * FROM email_logs WHERE UIF_REFERENCE = '{uif_ref}'", conn)
        else:
            df = pd.read_sql_query("SELECT * FROM email_logs", conn)
    return df

def get_error_logs(uif_ref=None):
    with sqlite3.connect(DATABASE_FILE) as conn:
        try:
            if uif_ref:
                df = pd.read_sql_query(
                    f"SELECT * FROM email_error_logs WHERE UIF_REFERENCE = '{uif_ref}' ORDER BY error_id DESC",
                    conn
                )
            else:
                df = pd.read_sql_query(
                    "SELECT * FROM email_error_logs ORDER BY error_id DESC",
                    conn
                )
        except Exception:
            # Table might not exist yet
            df = pd.DataFrame()
    return df

def upsert_company(uif_ref, trade_name, email_address, phone=None):
    """Insert or update a company record."""
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO companies (UIF_REFERENCE, TRADE_NAME, EMAIL_ADDRESS, PHONE) VALUES (?, ?, ?, ?)",
                (uif_ref, trade_name, email_address, phone)
            )
            conn.commit()
        return True, "Company upserted successfully"
    except Exception as e:
        return False, str(e)

def append_signature(body):
    signature_image_path = "Email Signature/asignature1.png"
    signature = f"<br><br><img src='cid:signature'><br>"
    return body + signature

def _list_mailbox_names(mail: imaplib.IMAP4_SSL):
    names = []
    try:
        typ, mailboxes = mail.list()
        if typ == 'OK' and mailboxes:
            for mb in mailboxes:
                decoded = mb.decode(errors='ignore') if isinstance(mb, bytes) else str(mb)
                # Mailbox name is typically the last quoted string; fallback to last token
                if ' "' in decoded:
                    name = decoded.split(' "')[-1].rstrip('"')
                else:
                    name = decoded.split()[-1].strip('"')
                if name:
                    names.append(name)
    except Exception:
        return names
    return names

def _find_sent_folder(mail: imaplib.IMAP4_SSL) -> str:
    """Try to detect and select an existing Sent folder; return a usable name if selectable."""
    try:
        names = _list_mailbox_names(mail)
        # Rank candidates: special-use names first, then common names
        preferred = []
        for nm in names:
            lower = nm.lower()
            if 'sent' in lower:
                preferred.append(nm)
        # Add common static candidates as a fallback order
        preferred.extend([
            'Sent', 'Sent Items', 'Sent Mail', 'INBOX.Sent'
        ])
        # Deduplicate preserving order
        seen = set()
        ordered = [x for x in preferred if not (x in seen or seen.add(x))]
        for cand in ordered:
            typ, _ = mail.select(cand, readonly=True)
            if typ == 'OK':
                try:
                    mail.close()
                except Exception:
                    pass
                return cand
    except Exception:
        pass
    return ''

def append_email_to_sent_folder(msg, smtp_password):
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        mail.login(SENDER_EMAIL, smtp_password)
        sent_folder = _find_sent_folder(mail)
        # If we couldn't select an existing one, create a simple 'Sent' folder
        if not sent_folder:
            simple_name = 'Sent'
            try:
                mail.create(simple_name)
            except Exception:
                # If create fails, we cannot proceed safely
                raise
            mail.select(simple_name)
            target_mailbox = simple_name
        else:
            mail.select(sent_folder)
            target_mailbox = sent_folder
        mail.append(target_mailbox, '(\\Seen)', imaplib.Time2Internaldate(time.localtime()), msg.as_bytes())
        mail.logout()
    except Exception as e:
        st.error(f"Failed to append email to Sent folder: {e}")
        try:
            recipient = msg.get('To', '')
            uif_ref = None
        except Exception:
            recipient = ''
            uif_ref = None
        log_send_error('IMAP Append to Sent', uif_ref, recipient, e)

def send_email(recipient_email, subject, body, smtp_password, dry_run=False, uif_reference=None, emails_sent=0):
    # Support one or many recipients; normalize to list for sending, keep header readable
    if isinstance(recipient_email, (list, tuple, set)):
        recipient_list = [e for e in list(recipient_email) if e]
        recipient_header = ", ".join(recipient_list)
    else:
        recipient_list = [recipient_email] if recipient_email else []
        recipient_header = recipient_email

    if dry_run:
        st.info(f"DRY RUN: Would send email to {recipient_header} with subject: {subject}")
        st.code(body)
        return True

    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = recipient_header
    # Do not BCC self; we'll append the message to Sent via IMAP
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    if not msg.get('Message-ID'):
        msg['Message-ID'] = make_msgid()
    msg.attach(MIMEText(append_signature(body), "html"))
    signature_image_path = "Email Signature/asignature1.png"
    try:
        with open(signature_image_path, 'rb') as img:
            mime_img = MIMEImage(img.read(), _subtype="png")
            mime_img.add_header('Content-ID', '<signature>')
            msg.attach(mime_img)
    except Exception as e:
        st.warning(f"Signature image not attached: {e}")

    # Attach the PDF
    pdf_path = "Ditheto Accountants - Appointment Letter and Audit Notification.pdf"
    try:
        with open(pdf_path, 'rb') as pdf_file:
            pdf_attachment = MIMEApplication(pdf_file.read(), _subtype="pdf")
            pdf_attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(pdf_path))
            msg.attach(pdf_attachment)
    except Exception as e:
        st.warning(f"PDF not attached: {e}")

    # Attach the Letter of demand PDF for 3rd email onwards (emails_sent >= 2)
    if emails_sent >= 2:
        demand_letter_path = "Letter of demand _UIF TERS Audit_250729_150955.pdf"
        try:
            with open(demand_letter_path, 'rb') as demand_file:
                demand_attachment = MIMEApplication(demand_file.read(), _subtype="pdf")
                demand_attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(demand_letter_path))
                msg.attach(demand_attachment)
                st.info(f"Letter of demand attached for email #{emails_sent + 1}")
        except Exception as e:
            st.warning(f"Letter of demand PDF not attached: {e}")

    # Attach company-specific summaries using UIF reference from multiple folders (Phase 4 and Phase 3)
    if uif_reference and uif_reference != "TEST-UIF-REF":
        summary_folders = [
            "PHASE 4 - Employer Claims Summaries",
            "Phase 3 - Employer Claims Summaries",
        ]
        # Clean the UIF reference to match the filename format
        clean_uif = uif_reference.replace('/', '_').replace('\\', '_')
        attached_any = False
        attached_paths = set()
        for folder in summary_folders:
            try:
                if not os.path.isdir(folder):
                    continue
                summary_files = [f for f in os.listdir(folder) if clean_uif in f and f.endswith('.xlsx')]
                for summary_filename in summary_files:
                    summary_path = os.path.join(folder, summary_filename)
                    if summary_path in attached_paths:
                        continue
                    try:
                        with open(summary_path, 'rb') as summary_file:
                            summary_attachment = MIMEApplication(summary_file.read(), _subtype="xlsx")
                            summary_attachment.add_header(
                                'Content-Disposition',
                                'attachment',
                                filename=summary_filename
                            )
                            msg.attach(summary_attachment)
                            attached_paths.add(summary_path)
                            attached_any = True
                    except Exception as e:
                        st.error(f"Error attaching summary file '{summary_filename}' for {uif_reference}: {e}")
            except Exception as e:
                st.warning(f"Could not scan folder '{folder}' for summaries: {e}")
        if not attached_any:
            st.warning(f"No summary files found for UIF reference: {uif_reference}")
    else:
        st.info("Skipping summary attachment for test email")

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, smtp_password)
            # Use sendmail with explicit recipient list for multi-recipient reliability
            server.sendmail(SENDER_EMAIL, recipient_list, msg.as_string())
        append_email_to_sent_folder(msg, smtp_password)
        return True
    except smtplib.SMTPRecipientsRefused as e:
        st.error(f"Email to {recipient_email} bounced: {e}")
        log_email(uif_reference, subject, "Bounced")
        log_send_error('SMTP Send (RecipientsRefused)', uif_reference, recipient_email, e)
        return False
    except Exception as e:
        st.error(f"Error sending email to {recipient_email}: {e}")
        log_send_error('SMTP Send', uif_reference, recipient_email, e)
        return False

def send_test_email(smtp_password, dry_run=False):
    # Use the first company as an example for the test email
    uif_reference = "TEST-UIF-REF"
    trade_name = "Test Company"
    emails_sent = 0  # Use initial email template
    
    subject, body = get_email_template(uif_reference, trade_name, emails_sent)
    body = append_signature(body)
    
    return send_email("nicknungu@gmail.com", subject, body, smtp_password, dry_run, uif_reference, emails_sent)

def test_smtp_connection(smtp_password):
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, smtp_password)
        return True, "SMTP connection successful!"
    except Exception as e:
        return False, f"SMTP connection failed: {e}"

def get_email_template(uif_reference, trade_name, emails_sent):
    # Determine template key
    if emails_sent == 0:
        template_key = 'initial'
    elif 1 <= emails_sent < 10:
        template_key = 'followup'
    else:
        template_key = 'final'

    subject = None
    body = None
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            df_tpl = pd.read_sql_query(
                f"SELECT subject, body FROM email_templates WHERE template_key = '{template_key}'",
                conn
            )
            if not df_tpl.empty:
                subject = df_tpl.iloc[0]['subject'] or ''
                body = df_tpl.iloc[0]['body'] or ''
    except Exception:
        subject = None
        body = None

    # Fallback to built-in defaults if DB missing
    if not subject or not body:
        if template_key == 'initial':
            subject = "Initial Request for Documentation – UIF TERS Compliance Audit"
            body = (
                """
                <!DOCTYPE html>
                <html>
                  <head>
                    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
                  </head>
                  <body>
                    <p>Good day,</p>
                    <p>I trust this email finds you well.</p>
                    <p>My name is Gamu, and I am an Auditor at Ditheto Accountants. We are conducting a compliance review on behalf of the Unemployment Insurance Fund (UIF), regarding the COVID-19 TERS funds disbursed to <strong>{TRADE_NAME}</strong> with UIF Ref Number: <strong>{UIF_REFERENCE}</strong>.</p>
                    <p>The following documents are required to complete the audit:</p>
                    <ul>
                      <li>Bank statements showing receipt of the TERS funds</li>
                      <li>Proof of payments made to employees</li>
                      <li>Payroll records or payslips for January to December 2020</li>
                      <li>2021 IRP5 certificates</li>
                      <li>EMP501s/201s for the 2020/2021 financial year</li>
                    </ul>
                    <p>We fully understand that administrative delays can occur. However, these documents are essential to finalising the audit in line with UIF compliance procedures. Without them, the file remains incomplete and may be subject to further administrative steps.</p>
                    <p>Kindly forward the requested documents to <strong><a href="mailto:gamu@dithetoaccountants.co.za">gamu@dithetoaccountants.co.za</a></strong> at your earliest convenience.</p>
                    <p>Your cooperation in resolving this matter is greatly appreciated.</p>
                    <p>Kind regards,<br>
                      <strong>Gamu Dambanjera</strong></p>
                    <div class="moz-signature"> <img src='cid:signature'></div>
                  </body>
                </html>
                """
            )
        elif template_key == 'followup':
            subject = "Follow-up: Urgent Action Required – UIF TERS Compliance Audit"
            body = (
                """
                <!DOCTYPE html>
                <html>
                  <head>
                    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
                  </head>
                  <body>
                    <p>Dear {TRADE_NAME},</p>
                    <p>This is a reminder that the requested documentation for the COVID-19 TERS audit is still outstanding. This is an urgent matter that requires your immediate attention.</p>
                    <p>Failure to provide the necessary documents may result in non-compliance with UIF regulations, which could lead to further administrative actions.</p>
                    <p>Please submit the required documents to <strong><a href="mailto:gamu@dithetoaccountants.co.za">gamu@dithetoaccountants.co.za</a></strong> without delay.</p>
                    <p>Kind regards,<br>
                      <strong>Gamu Dambanjera</strong></p>
                    <div class="moz-signature"> <img src='cid:signature'></div>
                  </body>
                </html>
                """
            )
        else:
            subject = "Final Notice: Non-Compliance with UIF TERS Audit Requirements"
            body = (
                """
                <!DOCTYPE html>
                <html>
                  <head>
                    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
                  </head>
                  <body>
                    <p>Good day,</p>
                    <p>This serves as a final notice regarding your non-compliance with the UIF TERS audit requirements and the Memorandum of Agreement (MOA) signed during your TERS application.</p>
                    <p>Despite multiple attempts to obtain the required documentation, including:</p>
                    <ol>
                      <li>Initial audit notification</li>
                      <li>Detailed information request</li>
                      <li>Follow-up communication</li>
                    </ol>
                    <p>You have failed to cooperate with the audit process, which constitutes a breach of the MOA, specifically:</p>
                    <ul>
                      <li><strong>Section 17.2:</strong> Obligation to make all accounting records accessible to UIF-authorized persons</li>
                      <li><strong>Section 18:</strong> Requirement to maintain a proper audit trail of funds received and benefits paid to employees</li>
                    </ul>
                    <h3>NOTICE OF ESCALATION</h3>
                    <p>Be advised that failure to respond to this final notice within 24 hours will result in:</p>
                    <ol>
                      <li>Immediate referral of this matter to the Directorate for Priority Crime Investigation (HAWKS) or Special Investigation Unit (SIU) for investigation of potential fraud</li>
                      <li>Mandatory repayment of all TERS funds within 10 working days of referral</li>
                      <li>Potential criminal charges for non-compliance and fraud</li>
                    </ol>
                    <h3>REQUIRED IMMEDIATE ACTION</h3>
                    <p>To prevent escalation to law enforcement, provide ALL previously requested documentation within 24 hours of this notice to everyone copied on this email.</p>
                    <p>Please note that this is your final opportunity to comply with the audit requirements before legal action is initiated.</p>
                    <p>Kind regards,<br>
                      <strong>Gamu Dambanjera</strong></p>
                    <div class="moz-signature"> <img src='cid:signature'></div>
                  </body>
                </html>
                """
            )

    # Replace placeholders
    placeholder_values = {
        'UIF_REFERENCE': uif_reference,
        'TRADE_NAME': trade_name,
    }
    def replace_placeholders(text):
        try:
            for k, v in placeholder_values.items():
                text = text.replace('{' + k + '}', str(v))
            return text
        except Exception:
            return text

    subject_final = replace_placeholders(subject)
    body_final = replace_placeholders(body)
    return subject_final, body_final

def get_daily_email_count(date=None):
    with sqlite3.connect(DATABASE_FILE) as conn:
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM email_logs WHERE date = ? AND status = 'Sent'", (date,))
        return cursor.fetchone()[0]

def get_email_stats():
    with sqlite3.connect(DATABASE_FILE) as conn:
        cursor = conn.cursor()
        # Get today's count
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("SELECT COUNT(*) FROM email_logs WHERE date = ? AND status = 'Sent'", (today,))
        today_count = cursor.fetchone()[0]
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM email_logs WHERE status = 'Sent'")
        total_count = cursor.fetchone()[0]
        
        # Get daily breakdown for last 7 days
        cursor.execute("""
            SELECT date, COUNT(*) as count 
            FROM email_logs 
            WHERE status = 'Sent' AND date >= date('now', '-7 days')
            GROUP BY date 
            ORDER BY date DESC
        """)
        daily_breakdown = cursor.fetchall()
        
        return {
            'today_count': today_count,
            'total_count': total_count,
            'daily_breakdown': daily_breakdown
        }

def send_follow_up_emails(selected_companies_df, smtp_password, dry_run, email_type="Auto (Based on current count)"):
    progress_text = st.empty()
    progress_bar = st.progress(0)
    total_companies = len(selected_companies_df)
    first_iter_index = None
    last_iter_index = None
    try:
        indices_list = list(selected_companies_df.index)
        first_iter_index = indices_list[0] if indices_list else None
        last_iter_index = indices_list[-1] if indices_list else None
    except Exception:
        first_iter_index = None
        last_iter_index = None

    for i, row in selected_companies_df.iterrows():
        uif_reference = row["UIF_REFERENCE"]
        trade_name = row["TRADE_NAME"]
        email_address = row["EMAIL_ADDRESS"]
        current_emails_sent = row["emails_sent"]
        is_completed = False
        if "completed" in row:
            try:
                is_completed = int(row["completed"]) == 1
            except Exception:
                is_completed = False
        
        # Determine which email to send based on email_type selection
        if email_type == "Auto (Based on current count)":
            email_count_to_use = current_emails_sent
        elif email_type == "Initial Email (Override count)":
            email_count_to_use = 0
        else:
            # Extract follow-up number from selection
            follow_up_num = int(email_type.split("#")[1].split()[0]) if "#" in email_type else 0
            email_count_to_use = follow_up_num

        if is_completed:
            status_message = f"🟢 Skipped {trade_name} (UIF Ref: {uif_reference}): Marked as completed."
            st.info(status_message)
            log_email(uif_reference, "N/A", "Skipped - Completed")
        else:
            # Show warning if this is beyond the 10th email
            if current_emails_sent >= 10:
                warning_message = f"⚠️ WARNING: {trade_name} (UIF Ref: {uif_reference}) has already received {current_emails_sent} emails. Proceeding with email #{current_emails_sent + 1}..."
                st.warning(warning_message)
            
            # Proceed with email sending for all non-completed companies
            subject, body = get_email_template(uif_reference, trade_name, email_count_to_use)
            if subject and body:
                recipients = get_company_emails(uif_reference)
                if not recipients:
                    recipients = [email_address] if pd.notna(email_address) and email_address else []
                attempted_send = False
                if recipients:
                    if send_email(recipients, subject, body, smtp_password, dry_run, uif_reference, email_count_to_use):
                        attempted_send = True
                        email_type_display = f" ({email_type})" if email_type != "Auto (Based on current count)" else ""
                        st.success(f"✅ Email sent to {trade_name} (UIF Ref: {uif_reference}){email_type_display} — {len(recipients)} recipient(s)")
                        if not dry_run:
                            log_email(uif_reference, subject, "Sent")
                    else:
                        attempted_send = True
                        st.error(f"❌ Failed to send email to {trade_name} (UIF Ref: {uif_reference})")
                        log_email(uif_reference, subject, "Failed")
            else:
                status_message = f"❌ Skipped {trade_name} (UIF Ref: {uif_reference}): No email template found for this count."
                st.warning(status_message)
                log_email(uif_reference, "N/A", "Skipped - No template")

        progress_bar.progress(min(1.0, max(0.0, (i + 1) / total_companies)))
        progress_text.text(f"Processing company {i + 1} of {total_companies}")

        # Enforce 15-second interval between sends to avoid provider flags
        # First email is sent immediately, delay applies from 2nd email onwards
        if not dry_run:
            try:
                if (
                    'attempted_send' in locals()
                    and attempted_send
                    and (first_iter_index is None or i != first_iter_index)
                ):
                    countdown_placeholder = st.empty()
                    for remaining in range(15, 0, -1):
                        countdown_placeholder.info(f"Waiting {remaining} seconds before next email...")
                        time.sleep(1)
                    countdown_placeholder.empty()
            except Exception:
                # Fallback to a single sleep if UI countdown fails
                time.sleep(15)

    progress_text.empty()
    progress_bar.empty()
    st.success("Email sending process completed!")

# Initialize database
init_db()

# Modern UI Main Section
st.markdown('''
<div style="background: linear-gradient(135deg, #2563eb 0%, #1e40af 100%); 
            color: #ffffff; 
            border-radius: 11px; 
            padding: 1.5rem; 
            text-align: center; 
            margin-bottom: 1.5rem;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);">
    <h1 style="color: #ffffff !important; 
               font-size: 2rem; 
               font-weight: 700; 
               margin: 0 0 0.5rem 0;">📧 UIF TERS Compliance Email Automation</h1>
    <p style="color: #ffffff !important; 
              font-size: 1.1rem; 
              margin: 0; 
              opacity: 0.95;">Streamline your compliance audit process with automated follow-up emails</p>
</div>
''', unsafe_allow_html=True)

# Modern Sidebar with enhanced styling
st.markdown("""
<style>
div[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #f8fafc 0%, #ffffff 100%);
}
</style>
""", unsafe_allow_html=True)

# Add logo to sidebar with modern styling
try:
    logo = Image.open("GD logo.png")
    st.sidebar.markdown('<div class="sidebar-header">', unsafe_allow_html=True)
    col1, col2, col3 = st.sidebar.columns([1, 2, 1])
    with col2:
        st.image(logo, width=120)
    st.sidebar.markdown('</div>', unsafe_allow_html=True)
except Exception as e:
    st.sidebar.markdown('<div class="sidebar-header"><h3>Ditheto Accountants</h3></div>', unsafe_allow_html=True)
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'dashboard'

# Modern Navigation with option menu
st.sidebar.markdown("---")
selected_page = option_menu(
    menu_title="Navigation",
    options=[
        "🏠 Dashboard",
        "🏢 Company Management",
        "✉️ Send Emails",
        "📊 Email Analytics",
        "✅ Completed Companies",
        "📝 Email Templates",
        "🚫 Unreachable Companies"
    ],
    icons=[
        "house",
        "building",
        "envelope",
        "bar-chart",
        "check-circle",
        "file-text",
        "exclamation-triangle"
    ],
    menu_icon="cast",
    default_index=0,
    styles={
        "container": {"padding": "0!important", "background-color": "transparent"},
        "icon": {"color": "#667eea", "font-size": "18px"},
        "nav-link": {
            "font-size": "14px",
            "text-align": "left",
            "margin": "0px",
            "--hover-color": "#667eea",
            "padding": "10px 15px",
            "border-radius": "8px"
        },
        "nav-link-selected": {"background-color": "#667eea", "color": "white"},
    }
)

# Map selected page to session state
page_mapping = {
    "🏠 Dashboard": "dashboard",
    "🏢 Company Management": "company_management",
    "✉️ Send Emails": "send_emails",
    "📊 Email Analytics": "email_logs",
    "✅ Completed Companies": "completed_companies",
    "📝 Email Templates": "email_templates",
    "🚫 Unreachable Companies": "unreachable_companies"
}

st.session_state.current_page = page_mapping[selected_page]

# Modern Sidebar Configuration Section
st.sidebar.markdown("---")
st.sidebar.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
st.sidebar.markdown("### 🔧 Configuration")

smtp_password = st.sidebar.text_input("SMTP Password", type="password", help="Password for gamu@dithetoaccountants.co.za")

# Test SMTP Connection with modern styling
col1, col2 = st.sidebar.columns(2)
with col1:
    if st.sidebar.button("🔗 Test Connection", use_container_width=True):
        if smtp_password:
            success, message = test_smtp_connection(smtp_password)
            if success:
                st.sidebar.success("✅ Connected")
            else:
                st.sidebar.error("❌ Failed")
        else:
            st.sidebar.error("Enter password")

with col2:
    if st.sidebar.button("📧 Test Email", use_container_width=True):
        if smtp_password:
            if send_test_email(smtp_password, dry_run=True):
                st.sidebar.success("✅ Test sent")
            else:
                st.sidebar.error("❌ Failed")
        else:
            st.sidebar.error("Enter password")

# Dry Run Mode Toggle with modern styling
dry_run = st.sidebar.checkbox("🧪 Dry Run Mode", help="Preview emails without sending")
st.sidebar.markdown('</div>', unsafe_allow_html=True)

# Modern Dashboard Function
def show_modern_dashboard():
    st.markdown('<div class="modern-card">', unsafe_allow_html=True)
    colored_header(
        label="Welcome to UIF Compliance Hub",
        description="Manage your compliance audit process efficiently",
        color_name="violet-70"
    )

    # Key Metrics Row
    st.markdown("### 📈 Key Performance Indicators")

    stats = get_email_stats()
    companies_df = get_companies_data()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{stats['today_count']}</div>
            <div class="metric-label">Today's Emails</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{stats['total_count']}</div>
            <div class="metric-label">Total Sent</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        completed_count = len(companies_df[companies_df['completed'] == 1]) if 'completed' in companies_df.columns else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{completed_count}</div>
            <div class="metric-label">Completed</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        total_companies = len(companies_df)
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{total_companies}</div>
            <div class="metric-label">Total Companies</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

    # Modern Navigation Cards
    st.markdown("### 🚀 Quick Actions")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown('<div class="nav-card" onclick="document.getElementById(\'nav-company\').click()">', unsafe_allow_html=True)
        st.markdown('<div class="nav-icon">🏢</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-title">Company Management</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-description">Add, edit, and manage company information</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        if st.button("Go to Company Management", key="nav-company", help="Navigate to company management"):
            st.session_state.current_page = 'company_management'
            st.rerun()

    with col2:
        st.markdown('<div class="nav-card" onclick="document.getElementById(\'nav-email\').click()">', unsafe_allow_html=True)
        st.markdown('<div class="nav-icon">✉️</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-title">Send Emails</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-description">Send automated follow-up emails</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        if st.button("Go to Send Emails", key="nav-email", help="Navigate to email sending"):
            st.session_state.current_page = 'send_emails'
            st.rerun()

    with col3:
        st.markdown('<div class="nav-card" onclick="document.getElementById(\'nav-analytics\').click()">', unsafe_allow_html=True)
        st.markdown('<div class="nav-icon">📊</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-title">Email Analytics</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-description">View detailed logs and statistics</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        if st.button("Go to Analytics", key="nav-analytics", help="Navigate to analytics"):
            st.session_state.current_page = 'email_logs'
            st.rerun()

    # Second row of navigation cards
    col4, col5, col6 = st.columns(3)

    with col4:
        st.markdown('<div class="nav-card" onclick="document.getElementById(\'nav-completed\').click()">', unsafe_allow_html=True)
        st.markdown('<div class="nav-icon">✅</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-title">Completed Companies</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-description">Track completed audit processes</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        if st.button("Go to Completed", key="nav-completed", help="Navigate to completed companies"):
            st.session_state.current_page = 'completed_companies'
            st.rerun()

    with col5:
        st.markdown('<div class="nav-card" onclick="document.getElementById(\'nav-templates\').click()">', unsafe_allow_html=True)
        st.markdown('<div class="nav-icon">📝</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-title">Email Templates</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-description">Customize email templates</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        if st.button("Go to Templates", key="nav-templates", help="Navigate to templates"):
            st.session_state.current_page = 'email_templates'
            st.rerun()

    with col6:
        st.markdown('<div class="nav-card" onclick="document.getElementById(\'nav-unreachable\').click()">', unsafe_allow_html=True)
        st.markdown('<div class="nav-icon">🚫</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-title">Unreachable Companies</div>', unsafe_allow_html=True)
        st.markdown('<div class="nav-description">Monitor delivery issues</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        if st.button("Go to Unreachable", key="nav-unreachable", help="Navigate to unreachable companies"):
            st.session_state.current_page = 'unreachable_companies'
            st.rerun()

    # Activity Chart
    if stats['daily_breakdown']:
        st.markdown("### 📊 Recent Activity")
        st.markdown('<div class="modern-card">', unsafe_allow_html=True)

        breakdown_df = pd.DataFrame(stats['daily_breakdown'], columns=['Date', 'Emails Sent'])
        breakdown_df['Date'] = pd.to_datetime(breakdown_df['Date'])

        fig = px.bar(
            breakdown_df,
            x='Date',
            y='Emails Sent',
            title='Daily Email Activity (Last 7 Days)',
            color='Emails Sent',
            color_continuous_scale='Blues'
        )
        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

# Navigation logic
if st.session_state.current_page == 'dashboard':
    show_modern_dashboard()
elif st.session_state.current_page == 'company_management':
    st.markdown('<div class="modern-card">', unsafe_allow_html=True)
    colored_header(
        label="Company Management",
        description="Manage company information and contact details",
        color_name="blue-70"
    )

    # Company Management content
    st.markdown("### 🔍 Company Search & Update")

    # Get all companies for searchable dropdown (type-ahead)
    conn = sqlite3.connect('compliance_emails.db')
    all_companies = pd.read_sql_query("SELECT UIF_REFERENCE, TRADE_NAME, EMAIL_ADDRESS, PHONE FROM companies", conn)
    conn.close()

    # Company selection (outside forms so it can be reused by both sections)
    if not all_companies.empty:
        display_options = all_companies.apply(
            lambda x: f"{x['UIF_REFERENCE']} - {x['TRADE_NAME']} - {x['EMAIL_ADDRESS']} - {x.get('PHONE', '')}",
            axis=1
        )
        selected_company = st.selectbox(
            "Select company (type to search by UIF Ref or Trade Name)",
            options=display_options.tolist(),
            key="company_selector"
        )
        uif_ref = selected_company.split(" - ")[0]
        # Expect format: UIF - TRADE - EMAIL - PHONE
        parts = selected_company.split(" - ")
        current_email = parts[2] if len(parts) >= 3 else ""
        current_phone = parts[3] if len(parts) >= 4 else ""

        # Modern form styling for company update
        st.markdown('<div class="modern-form">', unsafe_allow_html=True)
        st.markdown("#### 📝 Update Company Information")

        with st.form("update_company_form"):
            col1, col2 = st.columns(2)

            with col1:
                new_email = st.text_input("Email Address", value=current_email, help="Primary email for communications")
                new_phone = st.text_input("Phone Number", value=current_phone, help="Contact phone number")

            with col2:
                conn = sqlite3.connect('compliance_emails.db')
                current_completed = pd.read_sql_query(
                    f"SELECT completed FROM companies WHERE UIF_REFERENCE = '{uif_ref}'",
                    conn
                )
                conn.close()
                is_completed = bool(int(current_completed.iloc[0]['completed'])) if not current_completed.empty else False

                completed_toggle = st.checkbox(
                    "Mark as Completed",
                    value=is_completed,
                    help="Mark this company as completed (no further follow-ups)"
                )

            st.markdown("---")
            col1, col2, col3 = st.columns([1, 1, 1])

            with col1:
                submitted = st.form_submit_button("💾 Save Changes", type="primary", use_container_width=True)

            with col2:
                preview = st.form_submit_button("👁️ Preview Emails", use_container_width=True)

            with col3:
                reset = st.form_submit_button("🔄 Reset", use_container_width=True)

        if submitted:
            # Save logic here
            if new_email and new_email != current_email:
                try:
                    conn = sqlite3.connect('compliance_emails.db')
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE companies SET EMAIL_ADDRESS = ? WHERE UIF_REFERENCE = ?",
                        (new_email, uif_ref)
                    )
                    conn.commit()
                    conn.close()
                    st.success(f"✅ Successfully updated email for {uif_ref} to {new_email}")
                    log_email(uif_ref, "Email Address Updated", f"Changed from {current_email} to {new_email}")
                except Exception as e:
                    st.error(f"❌ Error updating email: {str(e)}")

            if new_phone != current_phone:
                try:
                    conn = sqlite3.connect('compliance_emails.db')
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE companies SET PHONE = ? WHERE UIF_REFERENCE = ?",
                        (new_phone, uif_ref)
                    )
                    conn.commit()
                    conn.close()
                    st.success(f"✅ Successfully updated phone for {uif_ref} to {new_phone}")
                except Exception as e:
                    st.error(f"❌ Error updating phone: {str(e)}")

            # Update completion status
            try:
                new_completed_value = 1 if completed_toggle else 0
                old_completed_value = 1 if is_completed else 0
                if new_completed_value != old_completed_value:
                    conn = sqlite3.connect('compliance_emails.db')
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE companies SET completed = ? WHERE UIF_REFERENCE = ?",
                        (new_completed_value, uif_ref)
                    )
                    conn.commit()
                    conn.close()
                    if new_completed_value == 1:
                        st.success("✅ Company marked as completed")
                        log_email(uif_ref, "Marked Completed", "Completed")
                    else:
                        st.warning("⚠️ Company marked as incomplete")
                        log_email(uif_ref, "Marked Incomplete", "Incomplete")
            except Exception as e:
                st.error(f"❌ Error saving completion status: {str(e)}")

        if reset:
            st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

        # Email Summary Section
        st.markdown("### 📊 Email Summary")

        addl_emails = get_company_emails(uif_ref)
        recipient_preview = []
        if current_email:
            recipient_preview.append(current_email)
        for e in addl_emails:
            if e and e not in recipient_preview:
                recipient_preview.append(e)
        display_addl = [e for e in addl_emails if e != current_email]

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{"1" if current_email else "0"}</div>
                <div class="metric-label">Primary Email</div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{len(display_addl)}</div>
                <div class="metric-label">Additional Emails</div>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{len(recipient_preview)}</div>
                <div class="metric-label">Total Recipients</div>
            </div>
            """, unsafe_allow_html=True)

        # Recipient preview for clarity
        if recipient_preview:
            st.markdown("**📧 Will send to:**")
            st.code(", ".join(recipient_preview))
        else:
            st.warning("⚠️ No email addresses configured for this company")

    else:
        st.info("📝 No companies found. Upload a CSV file to get started.")

    st.markdown('</div>', unsafe_allow_html=True)

    # Bulk Email Management Section
    st.markdown("### 🔧 Bulk Email Management")

    if display_addl:
        # Initialize session state for email selection if not exists
        if f'email_selections_{uif_ref}' not in st.session_state:
            st.session_state[f'email_selections_{uif_ref}'] = [False] * len(display_addl)

        # Update session state if email list changed
        if len(st.session_state[f'email_selections_{uif_ref}']) != len(display_addl):
            st.session_state[f'email_selections_{uif_ref}'] = [False] * len(display_addl)

        # Create a dataframe for bulk selection
        emails_df = pd.DataFrame({
            'Email': display_addl,
            'Select': st.session_state[f'email_selections_{uif_ref}']
        })

        col1, col2 = st.columns([3, 1])
        with col1:
            st.write("**Select emails to delete:**")
        with col2:
            select_all_clicked = st.button("Select All", key=f"select_all_{uif_ref}")
            deselect_all_clicked = st.button("Deselect All", key=f"deselect_all_{uif_ref}")

            # Handle select all/deselect all
            if select_all_clicked:
                st.session_state[f'email_selections_{uif_ref}'] = [True] * len(display_addl)
                st.rerun()
            if deselect_all_clicked:
                st.session_state[f'email_selections_{uif_ref}'] = [False] * len(display_addl)
                st.rerun()

        edited_emails_df = st.data_editor(
            emails_df,
            column_config={
                "Select": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select emails to delete",
                    default=False,
                ),
                "Email": st.column_config.TextColumn(
                    "Email Address",
                    help="Additional email address",
                    disabled=True
                )
            },
            disabled=["Email"],
            hide_index=True,
            use_container_width=True,
            key=f"bulk_email_selector_{uif_ref}"
        )

        # Update session state with current selections
        st.session_state[f'email_selections_{uif_ref}'] = edited_emails_df['Select'].tolist()

        selected_emails = edited_emails_df[edited_emails_df['Select'] == True]['Email'].tolist()

        if selected_emails:
            st.warning(f"⚠️ Selected {len(selected_emails)} email(s) for deletion:")
            for email in selected_emails:
                st.write(f"• {email}")

            # Confirmation checkbox
            confirm_delete = st.checkbox(
                "I confirm I want to delete these emails",
                key=f"confirm_delete_{uif_ref}"
            )

            if confirm_delete:
                if st.button("🗑️ Delete Selected Emails", key=f"bulk_delete_{uif_ref}", type="primary"):
                    # Debug information
                    st.info(f"Attempting to delete emails for UIF: {uif_ref}")
                    st.info(f"Selected emails: {selected_emails}")

                    result = remove_additional_emails_bulk(uif_ref, selected_emails)

                    st.info(f"Deletion result: {result}")

                    if result['removed']:
                        st.success(f"✅ Successfully deleted {len(result['removed'])} email(s): {', '.join(result['removed'])}")

                    if result['failed']:
                        st.error(f"❌ Failed to delete {len(result['failed'])} email(s):")
                        for failure in result['failed']:
                            st.error(f"• {failure['email']}: {failure['error']}")

                    if result['removed']:
                        # Clear session state for this company
                        if f'email_selections_{uif_ref}' in st.session_state:
                            del st.session_state[f'email_selections_{uif_ref}']
                        st.rerun()
            else:
                st.info("Please confirm deletion to proceed")
        else:
            st.info("No emails selected for deletion")

        # Debug section (can be removed later)
        with st.expander("🔍 Debug Information"):
            st.write(f"Current additional emails: {display_addl}")
            st.write(f"Session state selections: {st.session_state.get(f'email_selections_{uif_ref}', 'Not set')}")
            st.write(f"UIF Reference: {uif_ref}")

            # Test database connection
            if st.button("Test Database Connection", key=f"test_db_{uif_ref}"):
                try:
                    with sqlite3.connect(DATABASE_FILE) as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT EMAIL FROM company_emails WHERE UIF_REFERENCE = ?", (uif_ref,))
                        db_emails = [row[0] for row in cursor.fetchall()]
                        st.success(f"Database connection successful. Found {len(db_emails)} emails: {db_emails}")
                except Exception as e:
                    st.error(f"Database error: {e}")

    # Individual Email Management Form
    st.markdown("### ✏️ Individual Email Management")
    with st.form("manage_additional_emails_form"):
        new_addl_email = st.text_input("Add another email address", value="")
        remove_choice = st.selectbox(
            "Remove an existing email (optional)",
            options=["None"] + display_addl,
            index=0
        )
        submitted = st.form_submit_button("Update Additional Emails")
        if submitted:
            any_change = False
            if new_addl_email:
                ok, msg = add_additional_email(uif_ref, new_addl_email)
                if ok:
                    st.success(f"Added additional email: {new_addl_email}")
                    any_change = True
                else:
                    st.error(f"Failed to add email: {msg}")
            if remove_choice and remove_choice != "None":
                ok, msg = remove_additional_email(uif_ref, remove_choice)
                if ok:
                    st.success(f"Removed email: {remove_choice}")
                    any_change = True
                else:
                    st.error(f"Failed to remove email: {msg}")
            if any_change:
                st.rerun()

    # Bulk Add Section
    st.markdown("### 📥 Bulk Add Emails")
    with st.form("bulk_add_emails_form"):
        bulk_text = st.text_area(
            "Paste multiple emails (comma/semicolon/space separated)",
            placeholder="email1@example.com, email2@example.com; email3@example.com"
        )
        submitted_bulk = st.form_submit_button("📥 Add Multiple Emails")
        if submitted_bulk and bulk_text.strip():
            summary = add_additional_emails_bulk(uif_ref, bulk_text)
            if summary['added']:
                st.success(f"✅ Added {len(summary['added'])} email(s): {', '.join(summary['added'])}")
            if summary['skipped_duplicates']:
                st.info(f"ℹ️ Skipped duplicates/primary matches: {', '.join(summary['skipped_duplicates'])}")
            if summary['skipped_invalid']:
                st.warning(f"⚠️ Skipped invalid: {', '.join(summary['skipped_invalid'])}")
            if summary['added'] or summary['skipped_duplicates'] or summary['skipped_invalid']:
                st.rerun()
        elif submitted_bulk and not bulk_text.strip():
            st.error("Please enter some emails to add")

    # Bulk Replace Section (if there are existing additional emails)
    if display_addl:
        st.markdown("### 🔄 Bulk Replace All Additional Emails")
        with st.form("bulk_replace_emails_form"):
            st.warning("⚠️ This will replace ALL existing additional emails with the new ones!")
            st.info(f"Current additional emails: {len(display_addl)}")
            replace_text = st.text_area(
                "New emails (comma/semicolon/space separated)",
                placeholder="newemail1@example.com, newemail2@example.com; newemail3@example.com"
            )
            submitted_replace = st.form_submit_button("🔄 Replace All Additional Emails", type="primary")
            if submitted_replace and replace_text.strip():
                # First remove all existing additional emails
                result_remove = remove_additional_emails_bulk(uif_ref, display_addl)
                removed_count = len(result_remove['removed'])

                # Then add new emails
                summary = add_additional_emails_bulk(uif_ref, replace_text)
                added_count = len(summary['added'])

                st.success(f"✅ Replaced {removed_count} emails with {added_count} new emails")
                if summary['skipped_invalid']:
                    st.warning(f"⚠️ Skipped invalid: {', '.join(summary['skipped_invalid'])}")
                st.rerun()
            elif submitted_replace and not replace_text.strip():
                st.error("Please enter some emails to replace with")
    else:
        st.info("No companies found matching your search criteria.")
    
    st.markdown("---")

    
    # Display current companies
    st.subheader("Current Companies in Database")
    companies_df = get_companies_data()
    
    if not companies_df.empty:
        # Search/Filter (prefix matching for cleaner predictive behavior)
        search_term = st.text_input("🔍 Search companies (UIF Reference or Trade Name)")
        
        if search_term:
            st_term = search_term.strip().upper()
            filtered_df = companies_df[
                companies_df['UIF_REFERENCE'].str.upper().str.startswith(st_term, na=False) |
                companies_df['TRADE_NAME'].str.upper().str.startswith(st_term, na=False) |
                companies_df.get('PHONE', pd.Series(['']*len(companies_df))).astype(str).str.upper().str.startswith(st_term, na=False)
            ]
        else:
            filtered_df = companies_df
        
        # Show completion status visually
        if 'completed' in filtered_df.columns:
            filtered_df_display = filtered_df.copy()
            filtered_df_display['completed'] = filtered_df_display['completed'].map({0: '❌', 1: '✅'})
            st.dataframe(filtered_df_display, use_container_width=True)
        else:
            st.dataframe(filtered_df, use_container_width=True)
        st.info(f"Showing {len(filtered_df)} of {len(companies_df)} companies")
    else:
        st.info("No companies in database. Upload a CSV file to get started.")

    st.markdown("---")
    st.caption("Upload an Excel file containing new companies. Required columns: UIF_REFERENCE, TRADE_NAME, EMAIL_ADDRESS. Optional: PHONE")
    uploaded_excel = st.file_uploader("Upload Excel file", type=["xlsx", "xls"], key="phase_additions_uploader")
    sheet_name = st.text_input("Sheet name (leave blank for first sheet)", value="")
    if st.button("Import Companies"):
        if uploaded_excel is None:
            st.error("Please upload an Excel file first")
        else:
            try:
                # Read Excel
                xls = pd.ExcelFile(uploaded_excel)
                target_sheet = sheet_name if sheet_name.strip() else xls.sheet_names[0]
                df_new = pd.read_excel(xls, sheet_name=target_sheet)
                # Normalize columns: trim and rename likely variants
                df_new.columns = df_new.columns.astype(str).str.strip()
                rename_map = {
                    "﻿UIF Reference": "UIF_REFERENCE",
                    "UIF Reference": "UIF_REFERENCE",
                    "UIF_REFERENCE": "UIF_REFERENCE",
                    "TRADE NAMES": "TRADE_NAME",
                    "TRADE NAME": "TRADE_NAME",
                    "TRADE_NAME": "TRADE_NAME",
                    "EMAIL_ADDRESS": "EMAIL_ADDRESS",
                    "EMAIL": "EMAIL_ADDRESS",
                    "Email": "EMAIL_ADDRESS",
                    "PHONE": "PHONE",
                    "Phone": "PHONE",
                    "CONTACT NUMBER": "PHONE",
                }
                df_new.rename(columns={k: v for k, v in rename_map.items() if k in df_new.columns}, inplace=True)
                required_columns = ["UIF_REFERENCE", "TRADE_NAME", "EMAIL_ADDRESS"]
                if not all(col in df_new.columns for col in required_columns):
                    missing = [c for c in required_columns if c not in df_new.columns]
                    st.error(f"Missing required columns: {', '.join(missing)}")
                else:
                    added = 0
                    for _, row in df_new.iterrows():
                        uif = str(row.get("UIF_REFERENCE", "")).strip()
                        name = str(row.get("TRADE_NAME", "")).strip()
                        email = str(row.get("EMAIL_ADDRESS", "")).strip()
                        phone = str(row.get("PHONE", "")).strip() if "PHONE" in df_new.columns else None
                        if uif and name:
                            upsert_company(uif, name, email, phone)
                            added += 1
                    st.success(f"Imported or updated {added} companie(s) from sheet '{target_sheet}'")
                    st.rerun()
            except Exception as e:
                st.error(f"Failed to import: {e}")

elif st.session_state.current_page == 'send_emails':
    st.markdown('<div class="modern-card">', unsafe_allow_html=True)
    colored_header(
        label="Send Emails",
        description="Send automated follow-up emails to selected companies",
        color_name="blue-70"
    )

    st.markdown("### ✉️ Email Campaign Manager")

    companies_df = get_companies_data()

    if not companies_df.empty:
        # Enhanced company selection section
        st.markdown('<div class="modern-form">', unsafe_allow_html=True)
        st.markdown("#### 🎯 Select Target Companies")

        # Quick search & select one company (type-ahead)
        filtered_df_email = companies_df

        # Prepare selection dataframe and always exclude completed companies
        selection_df = filtered_df_email.copy()
        if 'completed' in selection_df.columns:
            selection_df = selection_df[selection_df['completed'] != 1]

        # Quick search functionality
        st.markdown("**🔍 Quick Company Search**")
        quick_options = selection_df.apply(
            lambda r: f"{r['UIF_REFERENCE']} - {r['TRADE_NAME']} - {r['EMAIL_ADDRESS']}", axis=1
        ).tolist()

        if quick_options:
            quick_choice = st.selectbox(
                "Search and select a company",
                ["None"] + quick_options,
                help="Type to search for a specific company"
            )
            if quick_choice and quick_choice != "None":
                quick_selected = quick_choice.split(" - ")[0]
            else:
                quick_selected = None
        else:
            st.info("No active companies available for email campaigns.")
            quick_selected = None

        # Multi-select interface
        st.markdown("**📋 Multi-Select Companies**")
        st.info("💡 Select multiple companies using the checkboxes below (Max 30 companies)")

        # Add selection column and pre-select quick choice if present
        selection_df = selection_df.copy()
        if 'Select' not in selection_df.columns:
            selection_df.insert(0, 'Select', False)
        if quick_selected:
            try:
                selection_df.loc[selection_df['UIF_REFERENCE'] == quick_selected, 'Select'] = True
            except Exception:
                pass

        # Modern data editor for selection
        st.markdown('<div class="modern-table">', unsafe_allow_html=True)
        edited_df = st.data_editor(
            selection_df,
            column_config={
                "Select": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select companies to send emails to",
                    default=False,
                ),
                "UIF_REFERENCE": st.column_config.TextColumn(
                    "UIF Reference",
                    help="Company UIF reference number"
                ),
                "TRADE_NAME": st.column_config.TextColumn(
                    "Company Name",
                    help="Company trade name"
                ),
                "EMAIL_ADDRESS": st.column_config.TextColumn(
                    "Email",
                    help="Primary contact email"
                ),
                "emails_sent": st.column_config.NumberColumn(
                    "Emails Sent",
                    help="Number of emails already sent to this company"
                )
            },
            disabled=["UIF_REFERENCE", "TRADE_NAME", "EMAIL_ADDRESS", "emails_sent", "last_sent"],
            hide_index=True,
            use_container_width=True,
            key="email_selection_editor"
        )
        st.markdown('</div>', unsafe_allow_html=True)

        # Get selected companies
        selected_companies = edited_df[edited_df['Select'] == True]

        # Display selection summary
        if len(selected_companies) > 0:
            st.markdown("### 📊 Selection Summary")

            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{len(selected_companies)}</div>
                    <div class="metric-label">Selected</div>
                </div>
                """, unsafe_allow_html=True)

            with col2:
                max_emails = selected_companies['emails_sent'].max() if len(selected_companies) > 0 else 0
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{max_emails}</div>
                    <div class="metric-label">Max Emails Sent</div>
                </div>
                """, unsafe_allow_html=True)

            with col3:
                total_recipients = 0
                for _, row in selected_companies.iterrows():
                    emails = get_company_emails(row['UIF_REFERENCE'])
                    total_recipients += len(emails)
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{total_recipients}</div>
                    <div class="metric-label">Total Recipients</div>
                </div>
                """, unsafe_allow_html=True)

            # Validation
            if len(selected_companies) > 30:
                st.error(f"⚠️ You can select maximum 30 companies. Currently selected: {len(selected_companies)}")
            else:
                st.success(f"✅ Ready to send emails to {len(selected_companies)} companies")

                # Show selected companies details
                with st.expander("📋 Selected Companies Details"):
                    for _, row in selected_companies.iterrows():
                        emails_sent = row['emails_sent']
                        if 'completed' in row and row['completed'] == 1:
                            status = "🟢 Completed"
                        else:
                            if emails_sent < 10:
                                status = "✅ Ready"
                            else:
                                status = f"⚠️ Warning: {emails_sent} emails sent"

                        recipients = get_company_emails(row['UIF_REFERENCE'])
                        st.write(f"• **{row['TRADE_NAME']}** (UIF: {row['UIF_REFERENCE']}) - Status: {status} - Recipients: {len(recipients)}")

                # Email Type Selection
                st.markdown("### 📧 Campaign Configuration")

                col1, col2 = st.columns(2)

                with col1:
                    email_type = st.selectbox(
                        "Email Type:",
                        options=[
                            "Auto (Based on current count)",
                            "Initial Email (Override count)",
                            "Follow-up #1", "Follow-up #2", "Follow-up #3",
                            "Follow-up #4", "Follow-up #5", "Follow-up #6",
                            "Follow-up #7", "Follow-up #8", "Follow-up #9 (Final)"
                        ],
                        help="Choose the type of email to send"
                    )

                with col2:
                    st.markdown("**Campaign Options:**")
                    dry_run_display = "🧪 Dry Run (Preview only)" if dry_run else "📤 Live Send"
                    st.info(f"Mode: {dry_run_display}")

                # Email Preview Section
                st.markdown("### 👁️ Email Preview")

                if len(selected_companies) > 0:
                    preview_company = st.selectbox(
                        "Preview email for:",
                        options=selected_companies.index,
                        format_func=lambda x: f"{selected_companies.loc[x, 'TRADE_NAME']} ({selected_companies.loc[x, 'UIF_REFERENCE']})"
                    )

                    if preview_company is not None:
                        preview_row = selected_companies.loc[preview_company]

                        # Determine email count for preview based on selection
                        if email_type == "Auto (Based on current count)":
                            preview_email_count = preview_row['emails_sent']
                        elif email_type == "Initial Email (Override count)":
                            preview_email_count = 0
                        else:
                            follow_up_num = int(email_type.split("#")[1].split()[0]) if "#" in email_type else 0
                            preview_email_count = follow_up_num

                        preview_subject, preview_body = get_email_template(
                            preview_row['UIF_REFERENCE'],
                            preview_row['TRADE_NAME'],
                            preview_email_count
                        )

                        col1, col2 = st.columns(2)

                        with col1:
                            st.markdown("**📧 Email Details:**")
                            st.write(f"**Type:** {email_type}")
                            st.write(f"**Subject:**")
                            st.code(preview_subject)

                        with col2:
                            st.markdown("**📊 Campaign Stats:**")
                            st.write(f"**Companies:** {len(selected_companies)}")
                            st.write(f"**Recipients:** {total_recipients}")
                            st.write(f"**Mode:** {dry_run_display}")

                        st.markdown("**📝 Email Content:**")
                        st.text_area("", preview_body, height=300, disabled=True)

                        # Send emails button
                        st.markdown("### 🚀 Execute Campaign")

                        col1, col2, col3 = st.columns([1, 1, 2])

                        with col1:
                            if st.button("📧 Send Emails", type="primary", use_container_width=True):
                                if not smtp_password and not dry_run:
                                    st.error("❌ Please enter SMTP password in the sidebar")
                                else:
                                    st.info("🚀 Starting email campaign...")
                                    send_follow_up_emails(selected_companies, smtp_password, dry_run, email_type)
                                    st.rerun()

                        with col2:
                            if st.button("🔄 Clear Selection", use_container_width=True):
                                st.rerun()

                        with col3:
                            st.info("💡 **Tip:** Use Dry Run mode to preview emails before sending")
        else:
            st.info("💡 Select companies using the checkboxes above to proceed with email campaigns")

        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.info("📝 No companies in database. Please upload a CSV file first in the Company Management section.")

    st.markdown('</div>', unsafe_allow_html=True)

elif st.session_state.current_page == 'email_logs':
    st.markdown('<div class="modern-card">', unsafe_allow_html=True)
    colored_header(
        label="Email Analytics",
        description="Comprehensive insights into email delivery and performance",
        color_name="violet-70"
    )

    st.markdown("### 📈 Email Performance Dashboard")

    # Get companies data for completed count
    companies_df = get_companies_data()

    # Email Statistics
    stats = get_email_stats()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{stats['today_count']}</div>
            <div class="metric-label">📅 Today's Emails</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{stats['total_count']}</div>
            <div class="metric-label">📊 Total Sent</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        completed_count = len(companies_df[companies_df['completed'] == 1]) if 'completed' in companies_df.columns else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{completed_count}</div>
            <div class="metric-label">✅ Completed</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        total_companies = len(companies_df)
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{total_companies}</div>
            <div class="metric-label">🏢 Total Companies</div>
        </div>
        """, unsafe_allow_html=True)

    # Enhanced Daily Breakdown Chart
    if stats['daily_breakdown']:
        st.markdown("### 📊 Email Activity Trends")

        breakdown_df = pd.DataFrame(stats['daily_breakdown'], columns=['Date', 'Emails Sent'])
        breakdown_df['Date'] = pd.to_datetime(breakdown_df['Date'])

        # Create enhanced chart with plotly
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=breakdown_df['Date'],
            y=breakdown_df['Emails Sent'],
            name='Emails Sent',
            marker_color='rgba(102, 126, 234, 0.8)',
            marker_line_color='rgba(102, 126, 234, 1)',
            marker_line_width=1
        ))

        fig.update_layout(
            title='Daily Email Activity (Last 7 Days)',
            xaxis_title='Date',
            yaxis_title='Number of Emails',
            height=300,
            margin=dict(l=20, r=20, t=40, b=40),
            showlegend=False,
            hovermode='x unified'
        )

        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### 📋 Detailed Email Logs")

    # Get email logs
    logs_df = get_email_logs()

    if not logs_df.empty:
        # Enhanced filter section
        col1, col2 = st.columns(2)

        with col1:
            uif_filter = st.selectbox(
                "Filter by UIF Reference",
                ["All"] + list(logs_df['UIF_REFERENCE'].unique()),
                help="Filter logs by specific company"
            )

        with col2:
            status_filter = st.selectbox(
                "Filter by Status",
                ["All", "Sent", "Failed", "Bounced", "Skipped"],
                help="Filter logs by delivery status"
            )

        # Apply filters
        filtered_logs = logs_df
        if uif_filter != "All":
            filtered_logs = filtered_logs[filtered_logs['UIF_REFERENCE'] == uif_filter]
        if status_filter != "All":
            filtered_logs = filtered_logs[filtered_logs['status'] == status_filter]

        # Status summary
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_emails = len(filtered_logs)
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{total_emails}</div>
                <div class="metric-label">Total Logs</div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            sent_count = len(filtered_logs[filtered_logs['status'] == 'Sent'])
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{sent_count}</div>
                <div class="metric-label">✅ Sent</div>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            failed_count = len(filtered_logs[filtered_logs['status'] == 'Failed'])
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{failed_count}</div>
                <div class="metric-label">❌ Failed</div>
            </div>
            """, unsafe_allow_html=True)

        with col4:
            success_rate = (sent_count / total_emails * 100) if total_emails > 0 else 0
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{success_rate:.1f}%</div>
                <div class="metric-label">📈 Success Rate</div>
            </div>
            """, unsafe_allow_html=True)

        # Display logs with modern styling
        st.markdown('<div class="modern-table">', unsafe_allow_html=True)
        st.dataframe(
            filtered_logs,
            use_container_width=True,
            column_config={
                "status": st.column_config.SelectboxColumn(
                    "Status",
                    help="Email delivery status",
                    options=["Sent", "Failed", "Bounced", "Skipped"],
                    required=True
                ),
                "timestamp": st.column_config.DatetimeColumn(
                    "Timestamp",
                    help="When the email was processed"
                ),
                "date": st.column_config.DateColumn(
                    "Date",
                    help="Date of email activity"
                )
            }
        )
        st.markdown('</div>', unsafe_allow_html=True)

        # Export functionality
        st.markdown("### 💾 Export Options")
        col1, col2 = st.columns(2)

        with col1:
            csv_data = filtered_logs.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Filtered Logs (CSV)",
                data=csv_data,
                file_name=f"email_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )

        with col2:
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.rerun()

    else:
        st.info("📭 No email logs found. Send some emails first!")

    # Error logs section
    st.markdown("### 🚨 Error Analysis")

    error_logs_df = get_error_logs()
    if not error_logs_df.empty:
        st.markdown('<div class="modern-form">', unsafe_allow_html=True)

        # Error filter options
        cols = st.columns(2)
        with cols[0]:
            uif_filter_err = st.selectbox(
                "Filter by UIF Reference (errors)",
                ["All"] + sorted(list(set(error_logs_df['UIF_REFERENCE'].dropna().astype(str)))),
                key="error_uif_filter"
            )
        with cols[1]:
            stage_filter = st.selectbox(
                "Filter by Error Stage",
                ["All"] + sorted(list(set(error_logs_df['stage'].dropna().astype(str)))),
                key="error_stage_filter"
            )

        filtered_errors = error_logs_df
        if uif_filter_err != "All":
            filtered_errors = filtered_errors[filtered_errors['UIF_REFERENCE'] == uif_filter_err]
        if stage_filter != "All":
            filtered_errors = filtered_errors[filtered_errors['stage'] == stage_filter]

        st.markdown('<div class="modern-table">', unsafe_allow_html=True)
        st.dataframe(filtered_errors, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Error download
        csv_data = filtered_errors.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Error Logs (CSV)",
            data=csv_data,
            file_name=f"email_error_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.success("🎉 No errors detected! All emails are being processed successfully.")

    st.markdown('</div>', unsafe_allow_html=True)

elif st.session_state.current_page == 'completed_companies':
    st.markdown('<div class="modern-card">', unsafe_allow_html=True)
    colored_header(
        label="Completed Companies",
        description="Track companies that have completed the audit process",
        color_name="green-70"
    )

    st.markdown("### ✅ Audit Completion Tracker")

    companies_df_completed = get_companies_data()
    if not companies_df_completed.empty and 'completed' in companies_df_completed.columns:
        completed_df = companies_df_completed[companies_df_completed['completed'] == 1].copy()

        # Summary metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{len(completed_df)}</div>
                <div class="metric-label">✅ Completed</div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            total_companies = len(companies_df_completed)
            completion_rate = (len(completed_df) / total_companies * 100) if total_companies > 0 else 0
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{completion_rate:.1f}%</div>
                <div class="metric-label">📈 Completion Rate</div>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            remaining = total_companies - len(completed_df)
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{remaining}</div>
                <div class="metric-label">⏳ In Progress</div>
            </div>
            """, unsafe_allow_html=True)

        if not completed_df.empty:
            # Enhanced search and filter
            st.markdown("### 🔍 Search & Filter")

            search_completed = st.text_input(
                "Search completed companies (UIF Reference or Trade Name)",
                key="completed_search",
                help="Type to search by UIF reference or company name"
            )

            if search_completed:
                search_term = search_completed.strip().upper()
                completed_filtered = completed_df[
                    completed_df['UIF_REFERENCE'].str.upper().str.startswith(search_term, na=False) |
                    completed_df['TRADE_NAME'].str.upper().str.startswith(search_term, na=False)
                ]
            else:
                completed_filtered = completed_df

            # Modern data display
            st.markdown("### 📋 Completed Companies List")
            st.markdown('<div class="modern-table">', unsafe_allow_html=True)

            # Enhanced dataframe with completion status
            completed_display = completed_filtered.copy()
            completed_display['Status'] = '✅ Completed'
            completed_display['Progress'] = '100%'

            # Add completion date if available
            if 'last_sent' in completed_display.columns:
                completed_display['Completed Date'] = pd.to_datetime(completed_display['last_sent']).dt.strftime('%Y-%m-%d')
            else:
                completed_display['Completed Date'] = 'N/A'

            # Reorder columns for better display
            display_cols = ['Status', 'Progress', 'UIF_REFERENCE', 'TRADE_NAME', 'EMAIL_ADDRESS', 'PHONE', 'Completed Date']
            available_cols = [col for col in display_cols if col in completed_display.columns]
            completed_display = completed_display[available_cols]

            st.dataframe(
                completed_display,
                use_container_width=True,
                column_config={
                    "Status": st.column_config.TextColumn(
                        "Status",
                        help="Audit completion status",
                        width="small"
                    ),
                    "Progress": st.column_config.ProgressColumn(
                        "Progress",
                        help="Audit completion progress",
                        min_value=0,
                        max_value=100,
                        format="%.0f%%"
                    ),
                    "UIF_REFERENCE": st.column_config.TextColumn(
                        "UIF Reference",
                        help="Unique UIF reference number"
                    ),
                    "TRADE_NAME": st.column_config.TextColumn(
                        "Company Name",
                        help="Company trade name"
                    ),
                    "EMAIL_ADDRESS": st.column_config.TextColumn(
                        "Email",
                        help="Primary contact email"
                    ),
                    "PHONE": st.column_config.TextColumn(
                        "Phone",
                        help="Contact phone number"
                    ),
                    "Completed Date": st.column_config.DateColumn(
                        "Completed",
                        help="Date when audit was completed"
                    )
                }
            )
            st.markdown('</div>', unsafe_allow_html=True)

            st.success(f"🎉 Showing {len(completed_filtered)} of {len(completed_df)} completed companies")

            # Export functionality
            st.markdown("### 💾 Export Options")
            col1, col2 = st.columns(2)

            with col1:
                csv_data = completed_filtered.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Completed Companies (CSV)",
                    data=csv_data,
                    file_name=f"completed_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )

            with col2:
                if st.button("🔄 Refresh List", use_container_width=True):
                    st.rerun()

        else:
            st.info("📝 No companies are marked as completed yet.")

            # Show progress towards completion
            if len(companies_df_completed) > 0:
                st.markdown("### 📊 Progress Overview")
                col1, col2 = st.columns(2)

                with col1:
                    st.info(f"🏢 **Total Companies:** {len(companies_df_completed)}")

                with col2:
                    st.warning(f"⏳ **Still in Progress:** {len(companies_df_completed)}")

                # Progress bar
                st.markdown("**Overall Completion Progress:**")
                progress = 0  # Since none are completed
                st.progress(progress)
                st.caption(f"{progress*100:.1f}% of companies have completed the audit process")
    else:
        st.info("📝 No companies found or completion status not available.")

    st.markdown('</div>', unsafe_allow_html=True)

elif st.session_state.current_page == 'email_templates':
    st.markdown('<div class="modern-card">', unsafe_allow_html=True)
    colored_header(
        label="Email Templates",
        description="Customize email templates for different stages of the audit process",
        color_name="orange-70"
    )

    st.markdown("### 📝 Template Management")

    # Update Final Template Section
    st.markdown('<div class="modern-form">', unsafe_allow_html=True)
    st.markdown("#### 🔄 Update Final Email Template")

    st.info("💡 The final email template has been updated with enhanced content. Click below to apply the changes to the database.")

    if st.button("🔄 Update Final Email Template", type="primary", use_container_width=True):
        success, message = update_final_email_template()
        if success:
            st.success(f"✅ {message}")
        else:
            st.error(f"❌ {message}")

    st.markdown('</div>', unsafe_allow_html=True)

    # Template Editor
    st.markdown("### ✏️ Template Editor")

    def _load_template_row(key: str):
        try:
            with sqlite3.connect(DATABASE_FILE) as conn:
                df = pd.read_sql_query(
                    f"SELECT subject, body FROM email_templates WHERE template_key = '{key}'",
                    conn
                )
                if not df.empty:
                    return df.iloc[0]['subject'] or '', df.iloc[0]['body'] or ''
        except Exception:
            pass
        return '', ''

    def _save_template_row(key: str, subject: str, body: str):
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO email_templates (template_key, subject, body) VALUES (?, ?, ?) ON CONFLICT(template_key) DO UPDATE SET subject=excluded.subject, body=excluded.body",
                (key, subject, body)
            )
            conn.commit()

    template_keys = [
        ("initial", "📧 Initial Email"),
        ("followup", "📨 Follow-up Email"),
        ("final", "⚠️ Final Notice Email"),
    ]

    # Template selection with modern styling
    st.markdown('<div class="modern-form">', unsafe_allow_html=True)

    selected_tpl_label = st.selectbox(
        "Choose a template to edit",
        [label for _, label in template_keys],
        help="Select which email template you want to modify"
    )
    selected_tpl_key = [k for k, lbl in template_keys if lbl == selected_tpl_label][0]

    subj_current, body_current = _load_template_row(selected_tpl_key)

    with st.form(f"tpl_form_{selected_tpl_key}"):
        st.markdown(f"**Editing: {selected_tpl_label}**")

        new_subject = st.text_input(
            "Email Subject",
            value=subj_current,
            help="Subject line for the email"
        )

        new_body = st.text_area(
            "Email Body (HTML)",
            value=body_current,
            height=400,
            help="HTML content of the email. Use {UIF_REFERENCE} and {TRADE_NAME} as placeholders."
        )

        col1, col2 = st.columns(2)

        with col1:
            save_clicked = st.form_submit_button("💾 Save Template", type="primary", use_container_width=True)

        with col2:
            preview_clicked = st.form_submit_button("👁️ Preview Template", use_container_width=True)

    if save_clicked:
        try:
            _save_template_row(selected_tpl_key, new_subject, new_body)
            st.success(f"✅ Template '{selected_tpl_label}' saved successfully!")
        except Exception as e:
            st.error(f"❌ Failed to save template: {e}")

    if preview_clicked:
        # Preview section
        st.markdown("### 👁️ Template Preview")

        companies_df_preview = get_companies_data()
        if not companies_df_preview.empty:
            row = companies_df_preview.iloc[0]
            sample_uif = row.get('UIF_REFERENCE', 'SAMPLE-UIF')
            sample_trade = row.get('TRADE_NAME', 'Sample Company')
        else:
            sample_uif = 'SAMPLE-UIF'
            sample_trade = 'Sample Company'

        try:
            subject_preview = new_subject.replace('{UIF_REFERENCE}', str(sample_uif)).replace('{TRADE_NAME}', str(sample_trade))
            body_preview = new_body.replace('{UIF_REFERENCE}', str(sample_uif)).replace('{TRADE_NAME}', str(sample_trade))

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**📧 Subject:**")
                st.code(subject_preview, language=None)

            with col2:
                st.markdown("**📄 Template Type:**")
                st.info(f"{selected_tpl_label}")

            st.markdown("**📝 Email Body (Rendered):**")
            st.markdown(body_preview, unsafe_allow_html=True)

        except Exception as e:
            st.error(f"❌ Failed to render preview: {e}")

    st.markdown('</div>', unsafe_allow_html=True)

    # Template Information
    st.markdown("### ℹ️ Template Information")
    st.info("""
    **Available Placeholders:**
    - `{UIF_REFERENCE}` - The company's UIF reference number
    - `{TRADE_NAME}` - The company's trade name

    **Template Types:**
    - **Initial Email** - First contact requesting documentation
    - **Follow-up Email** - Reminder for outstanding documents
    - **Final Notice** - Last warning before escalation
    """)

    st.markdown('</div>', unsafe_allow_html=True)

elif st.session_state.current_page == 'unreachable_companies':
    st.markdown('<div class="modern-card">', unsafe_allow_html=True)
    colored_header(
        label="Unreachable Companies",
        description="Monitor and track companies with email delivery issues",
        color_name="red-70"
    )

    st.markdown("### 🚫 Email Delivery Issues Tracker")

    # Enhanced Filter options
    st.markdown('<div class="modern-form">', unsafe_allow_html=True)
    st.markdown("#### 🔍 Filter & Sort Options")

    col1, col2, col3 = st.columns(3)

    with col1:
        filter_type = st.selectbox(
            "Issue Type:",
            ["All Issues", "Bounced Emails Only", "Failed Emails Only", "Both Bounced & Failed"],
            help="Filter by type of delivery issue"
        )

    with col2:
        sort_by = st.selectbox(
            "Sort by:",
            ["Last Issue Date", "Total Issues", "Company Name", "UIF Reference"],
            help="Sort results by selected criteria"
        )

    with col3:
        show_count = st.selectbox(
            "Show:",
            ["All", "Top 10", "Top 25", "Top 50"],
            help="Limit number of results displayed"
        )

    st.markdown('</div>', unsafe_allow_html=True)

    # Get unreachable companies data
    if filter_type == "All Issues":
        unreachable_df = get_unreachable_companies()
    elif filter_type == "Bounced Emails Only":
        unreachable_df = get_bounced_companies()
        if not unreachable_df.empty:
            unreachable_df = unreachable_df.rename(columns={
                'bounce_count': 'total_issues',
                'last_bounce_date': 'last_issue_date',
                'bounced_subjects': 'failed_subjects'
            })
            unreachable_df['failure_count'] = 0
            unreachable_df['failed_subjects'] = ''
    elif filter_type == "Failed Emails Only":
        unreachable_df = get_failed_companies()
        if not unreachable_df.empty:
            unreachable_df = unreachable_df.rename(columns={
                'failure_count': 'total_issues',
                'last_failure_date': 'last_issue_date',
                'failed_subjects': 'bounced_subjects'
            })
            unreachable_df['bounce_count'] = 0
            unreachable_df['bounced_subjects'] = ''
    else:  # Both Bounced & Failed
        unreachable_df = get_unreachable_companies()

    if not unreachable_df.empty:
        # Apply sorting
        if sort_by == "Last Issue Date":
            unreachable_df = unreachable_df.sort_values('last_issue_date', ascending=False)
        elif sort_by == "Total Issues":
            unreachable_df = unreachable_df.sort_values('total_issues', ascending=False)
        elif sort_by == "Company Name":
            unreachable_df = unreachable_df.sort_values('TRADE_NAME', ascending=True)
        elif sort_by == "UIF Reference":
            unreachable_df = unreachable_df.sort_values('UIF_REFERENCE', ascending=True)

        # Apply count limit
        if show_count != "All":
            count_limit = int(show_count.split()[1])
            unreachable_df = unreachable_df.head(count_limit)

        # Summary metrics
        st.markdown("### 📊 Issue Summary")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{len(unreachable_df)}</div>
                <div class="metric-label">🚫 Unreachable</div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            total_bounces = unreachable_df['bounce_count'].sum() if 'bounce_count' in unreachable_df.columns else 0
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{total_bounces}</div>
                <div class="metric-label">📤 Bounces</div>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            total_failures = unreachable_df['failure_count'].sum() if 'failure_count' in unreachable_df.columns else 0
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{total_failures}</div>
                <div class="metric-label">❌ Failures</div>
            </div>
            """, unsafe_allow_html=True)

        with col4:
            total_issues = unreachable_df['total_issues'].sum()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{total_issues}</div>
                <div class="metric-label">📋 Total Issues</div>
            </div>
            """, unsafe_allow_html=True)

        # Search functionality
        st.markdown("### 🔍 Search Companies")
        search_term = st.text_input(
            "Search by UIF Reference, Trade Name, or Email",
            help="Type to search for specific companies"
        )

        if search_term:
            search_mask = (
                unreachable_df['UIF_REFERENCE'].str.contains(search_term, case=False, na=False) |
                unreachable_df['TRADE_NAME'].str.contains(search_term, case=False, na=False) |
                unreachable_df['EMAIL_ADDRESS'].str.contains(search_term, case=False, na=False)
            )
            unreachable_df = unreachable_df[search_mask]

        # Display the data
        st.markdown("### 📋 Companies with Delivery Issues")
        st.markdown('<div class="modern-table">', unsafe_allow_html=True)

        # Format the dataframe for better display
        display_df = unreachable_df.copy()

        # Format dates
        if 'last_issue_date' in display_df.columns:
            display_df['last_issue_date'] = pd.to_datetime(display_df['last_issue_date']).dt.strftime('%Y-%m-%d %H:%M')

        # Truncate long text fields
        if 'bounced_subjects' in display_df.columns:
            display_df['bounced_subjects'] = display_df['bounced_subjects'].fillna('')
            mask = display_df['bounced_subjects'].str.len() > 50
            display_df.loc[mask, 'bounced_subjects'] = display_df.loc[mask, 'bounced_subjects'].str[:50] + '...'

        if 'failed_subjects' in display_df.columns:
            display_df['failed_subjects'] = display_df['failed_subjects'].fillna('')
            mask = display_df['failed_subjects'].str.len() > 50
            display_df.loc[mask, 'failed_subjects'] = display_df.loc[mask, 'failed_subjects'].str[:50] + '...'

        st.dataframe(
            display_df,
            use_container_width=True,
            column_config={
                "UIF_REFERENCE": st.column_config.TextColumn("UIF Reference", help="Company UIF reference"),
                "TRADE_NAME": st.column_config.TextColumn("Company Name", help="Company trade name"),
                "EMAIL_ADDRESS": st.column_config.TextColumn("Email", help="Primary email address"),
                "total_issues": st.column_config.NumberColumn("Total Issues", help="Total number of delivery problems"),
                "bounce_count": st.column_config.NumberColumn("Bounces", help="Number of bounced emails"),
                "failure_count": st.column_config.NumberColumn("Failures", help="Number of failed emails"),
                "last_issue_date": st.column_config.DatetimeColumn("Last Issue", help="Date of most recent issue")
            }
        )
        st.markdown('</div>', unsafe_allow_html=True)

        # Export functionality
        st.markdown("### 💾 Export & Actions")
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("📥 Export to CSV", use_container_width=True):
                csv_data = export_unreachable_companies()
                if csv_data:
                    st.download_button(
                        label="Download CSV",
                        data=csv_data,
                        file_name=f"unreachable_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.error("No data to export")

        with col2:
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.rerun()

        with col3:
            if st.button("📊 View Statistics", use_container_width=True):
                # Show detailed statistics
                st.markdown("#### 📈 Detailed Statistics")
                st.info(f"**Filter Applied:** {filter_type}")
                st.info(f"**Sort Order:** {sort_by}")
                st.info(f"**Results Shown:** {len(unreachable_df)} companies")

    else:
        st.success("🎉 Excellent! No unreachable companies found!")

        # Show overall email delivery statistics
        st.markdown("### 📊 Overall Email Performance")

        try:
            with sqlite3.connect(DATABASE_FILE) as conn:
                stats_query = """
                    SELECT
                        COUNT(CASE WHEN status = 'Sent' THEN 1 END) as successful_sends,
                        COUNT(CASE WHEN status = 'Bounced' THEN 1 END) as bounces,
                        COUNT(CASE WHEN status = 'Failed' THEN 1 END) as failures,
                        COUNT(*) as total_attempts
                    FROM email_logs
                """
                stats_df = pd.read_sql_query(stats_query, conn)
                if not stats_df.empty:
                    stats = stats_df.iloc[0]
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value">{stats['successful_sends']}</div>
                            <div class="metric-label">✅ Successful</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with col2:
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value">{stats['bounces']}</div>
                            <div class="metric-label">📤 Bounced</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with col3:
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value">{stats['failures']}</div>
                            <div class="metric-label">❌ Failed</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with col4:
                        success_rate = (stats['successful_sends'] / stats['total_attempts'] * 100) if stats['total_attempts'] > 0 else 0
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value">{success_rate:.1f}%</div>
                            <div class="metric-label">📈 Success Rate</div>
                        </div>
                        """, unsafe_allow_html=True)

        except Exception as e:
            st.error(f"Error retrieving email statistics: {e}")

    st.markdown('</div>', unsafe_allow_html=True)

# Modern Footer
st.markdown("---")
st.markdown("""
<div style="background: linear-gradient(135deg, #2563eb 0%, #1e40af 100%); 
            color: #ffffff !important; 
            border-radius: 11px; 
            padding: 1.5rem; 
            text-align: center; 
            margin-top: 2rem;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);">
    <div style="display: flex; justify-content: center; align-items: center; margin-bottom: 1rem;">
        <span style="font-size: 1.25rem; font-weight: 600; color: #ffffff !important;">✉️ UIF Compliance Email Automation</span>
    </div>
    <div style="display: flex; justify-content: center; align-items: center; flex-wrap: wrap; gap: 2rem; color: #ffffff !important;">
        <span style="color: #ffffff !important;">Powered by Streamlit</span>
        <span style="color: #ffffff !important;">•</span>
        <span style="color: #ffffff !important;">Gamu Dambanjera</span>
    </div>
    <div style="margin-top: 1rem; font-size: 0.875rem; opacity: 0.95; color: #ffffff !important;">
        Streamline your compliance audit process with automated follow-up emails
    </div>
</div>
""", unsafe_allow_html=True)

# Automatically load Book1.csv if it exists
csv_path = '/home/ubuntu/upload/Book1.csv'
if os.path.exists(csv_path):
    try:
        df_preload = pd.read_csv(csv_path)
        # Clean up column names
        df_preload.columns = df_preload.columns.str.strip()
        df_preload.rename(columns={
            "﻿UIF Reference": "UIF_REFERENCE",
            "TRADE NAMES": "TRADE_NAME",
            "EMAIL_ADDRESS": "EMAIL_ADDRESS"
        }, inplace=True)

        required_columns = ["UIF_REFERENCE", "TRADE_NAME", "EMAIL_ADDRESS"]
        if all(col in df_preload.columns for col in required_columns):
            for _, row in df_preload.iterrows():
                upsert_company(row["UIF_REFERENCE"], row["TRADE_NAME"], row["EMAIL_ADDRESS"])
            st.sidebar.success("📊 Book1.csv data pre-loaded successfully!")
        else:
            st.sidebar.error(f"Pre-load failed: Book1.csv must contain columns: {', '.join(required_columns)}")
    except Exception as e:
        st.sidebar.error(f"Error pre-loading Book1.csv: {e}")

