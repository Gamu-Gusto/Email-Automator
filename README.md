# UIF Compliance Email Automation

A Python + Streamlit application that automates follow-up emails for compliance document requests.

## Features

- **Email Logic**: Uses SMTP server `smtp.dithetoaccountants.co.za` with sender `gamu@dithetoaccountants.co.za`
- **Email Limits**: Maximum 10 emails per company (1 initial + 9 follow-ups), then marked as "non-cooperative"
- **Progressive Templates**: Follow-up emails vary in subject lines and tone, becoming stronger with each reminder
- **Personalization**: Each email includes company's UIF Reference and Trade Name
- **CSV Data Source**: Upload CSV with `UIF_REFERENCE`, `TRADE_NAME`, `EMAIL_ADDRESS` columns
- **Batch Processing**: Select up to 30 companies per batch for email sending
- **SQLite Tracking**: Logs all email activity with timestamps and status
- **Streamlit UI**: User-friendly interface with search, filtering, and progress tracking

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
streamlit run app.py
```

## Usage

### 1. Company Management Tab
- Upload CSV file with company data
- View current companies in database
- Search and filter companies

### 2. Send Emails Tab
- Select companies using checkboxes (max 30)
- Configure SMTP password in sidebar
- Enable "Dry Run Mode" for testing
- Send follow-up emails with progress tracking

### 3. Email Logs Tab
- View all email logs with timestamps
- Filter by UIF Reference
- See summary statistics

## SMTP Configuration

- **Server**: smtp.dithetoaccountants.co.za
- **Sender**: gamu@dithetoaccountants.co.za
- **Password**: Enter in sidebar (required for sending emails)

## Email Templates

### Initial Email
- Subject: "UIF Compliance Review – Request for Documents (UIF Ref: [UIF Reference])"
- Professional tone requesting required documents

### Follow-up Emails (1-9)
- Progressively stronger tone
- Varying subject lines
- Final emails warn of non-cooperative status

## Database Schema

### Companies Table
- `UIF_REFERENCE` (Primary Key)
- `TRADE_NAME`
- `EMAIL_ADDRESS`
- `emails_sent` (Counter)
- `last_sent` (Timestamp)

### Email Logs Table
- `log_id` (Auto-increment)
- `UIF_REFERENCE` (Foreign Key)
- `timestamp`
- `subject`
- `status`

## CSV Format

Your CSV file must contain these columns:
```
UIF_REFERENCE,TRADE_NAME,EMAIL_ADDRESS
UIF001,ABC Trading Company,abc@example.com
UIF002,XYZ Manufacturing Ltd,xyz@example.com
```

## Testing

1. Enable "Dry Run Mode" to test without sending emails
2. Use "Test SMTP Connection" to verify credentials
3. Upload the provided `sample_companies.csv` for testing

## Security Notes

- SMTP password is not stored permanently
- Database is local SQLite file
- All email activity is logged for audit purposes

