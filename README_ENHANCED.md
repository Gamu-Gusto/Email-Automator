# UIF Compliance Email Automation - Enhanced Version

## 🚀 New Features Added

### 📧 Email Preview
- **Preview emails before sending**: Select any company from your batch to preview the exact email content that will be sent
- **Subject and body preview**: See both the subject line and full email body before sending
- **Accuracy verification**: Ensure all personalization (UIF Reference, Trade Name) is correct before sending

### 📊 Daily Email Tracking
- **Today's email count**: Track how many emails you've sent today
- **Total email statistics**: View your overall email sending statistics
- **Daily breakdown**: See email activity for the last 7 days with visual charts
- **Date tracking**: All emails are now tracked with precise date information

### 📨 Sent Folder Integration
- **Automatic BCC to sender**: All sent emails are automatically copied to your email account (gamu@dithetoaccountants.co.za)
- **Sent folder visibility**: You can now see all sent emails in your email client's sent folder
- **Email audit trail**: Complete record of all communications for compliance purposes

## 🎯 Core Features (Unchanged)

- ✅ SMTP integration with smtp.dithetoaccountants.co.za
- ✅ Email limit enforcement (max 10 emails per company)
- ✅ Progressive email templates with varying tone/subjects
- ✅ Personalized emails with UIF Reference and Trade Name
- ✅ Automatic CSV data loading from Book1.csv
- ✅ Batch selection (up to 30 companies)
- ✅ SQLite database for comprehensive email tracking
- ✅ Dry run mode for testing
- ✅ SMTP connection testing

## 📱 User Interface Enhancements

### Company Management Tab
- Pre-loaded data from Book1.csv (no manual upload needed)
- Search and filter functionality
- Complete company overview

### Send Emails Tab
- **NEW**: Email preview section with company selector
- **NEW**: Real-time email content preview
- Company selection with checkboxes (max 30)
- Progress tracking during email sending
- Dry run mode toggle

### Email Logs & Statistics Tab
- **NEW**: Daily email metrics dashboard
- **NEW**: Visual charts for email activity
- **NEW**: 7-day email breakdown
- Complete email history and logs
- Filter by UIF Reference

## 🚀 How to Run

1. **Extract the zip file** to your desired location
2. **Install dependencies**: `pip install -r requirements.txt`
3. **Run the app**: `streamlit run app.py`
4. **Access the interface** at http://localhost:8501

## 📝 Quick Start Guide

1. **Data is pre-loaded**: Book1.csv data is automatically loaded on startup
2. **Configure SMTP**: Enter your password in the sidebar
3. **Test connection**: Use "Test SMTP Connection" to verify credentials
4. **Preview emails**: Go to Send Emails tab, select companies, and preview emails
5. **Send emails**: Use "Dry Run Mode" for testing, then send real emails
6. **Track progress**: Monitor daily statistics in the Email Logs tab

## 🔧 Technical Improvements

- Enhanced SQLite schema with date tracking
- Improved error handling and user feedback
- Better column mapping for CSV data
- Automatic BCC functionality for sent folder integration
- Real-time statistics calculation
- Visual data representation with charts

## 📊 Email Statistics Features

- **Today's Count**: Number of emails sent today
- **Total Count**: Lifetime email count
- **Daily Breakdown**: Last 7 days activity with bar chart
- **Email Logs**: Complete history with timestamps and status

## 🎯 Email Preview Features

- **Company Selection**: Choose any company to preview their email
- **Subject Preview**: See the exact subject line that will be used
- **Body Preview**: Full email content with personalization
- **Template Progression**: Preview shows the correct template based on email count

The application now provides complete visibility into your email campaigns with enhanced tracking, preview capabilities, and automatic sent folder integration for better compliance and audit trails.

