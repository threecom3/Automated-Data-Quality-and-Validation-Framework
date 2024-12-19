import pandas as pd
import sqlalchemy
from datetime import datetime
import schedule
import time
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from retry import retry
import os

# Load database credentials from environment variables
DATABASE_URI = os.getenv("DATABASE_URI")

if not DATABASE_URI:
    raise EnvironmentError("Database URI is not set. Please configure the DATABASE_URI environment variable.")

engine = sqlalchemy.create_engine(DATABASE_URI)

# Email notification setup
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

if not all([SMTP_SERVER, EMAIL_USER, EMAIL_PASSWORD, EMAIL_RECEIVER]):
    raise EnvironmentError("Email credentials are not fully configured. Please set the required environment variables.")

# Define validation rules
def validate_data(df):
    issues = []

    # Check for missing values
    missing_values = df.isnull().sum()
    if missing_values.any():
        issues.append({"type": "Missing Values", "details": missing_values.to_dict()})

    # Check for duplicates
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        issues.append({"type": "Duplicates", "details": {"count": duplicates}})

    # Check for valid ranges (example: age column)
    if 'age' in df.columns:
        invalid_ages = df[(df['age'] < 0) | (df['age'] > 120)]
        if not invalid_ages.empty:
            issues.append({"type": "Invalid Range", "details": invalid_ages.to_dict(orient='records')})

    # Example: Add column type validation
    for column, dtype in df.dtypes.items():
        if dtype == 'object':
            if df[column].str.len().max() > 255:
                issues.append({"type": "String Length Exceeded", "details": {"column": column, "max_length": df[column].str.len().max()}})

    # Log and notify issues
    if issues:
        log_issues(issues)
        send_email_notification(issues)
    else:
        print("Data quality checks passed.")

# Log issues in JSON format or text format 
def log_issues(issues):
    with open("data_quality_log.json", "a") as log_file:
        log_entry = {"timestamp": datetime.now().isoformat(), "issues": issues}
        log_file.write(json.dumps(log_entry) + "\n")

# Send email notifications
def send_email_notification(issues):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = EMAIL_RECEIVER
        msg['Subject'] = "Data Quality Issues Detected"

        body = "Data quality issues were detected:\n\n" + json.dumps(issues, indent=4)
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)
            print("Email notification sent.")
    except Exception as e:
        print(f"Failed to send email notification: {e}")

# Retry decorator for database connection errors
@retry(tries=3, delay=5)
def fetch_data():
    query = "SELECT * FROM your_table_name"  # Replace with your actual query
    return pd.read_sql(query, engine)

# Fetch and validate data
def check_data_quality():
    try:
        df = fetch_data()
        validate_data(df)
    except Exception as e:
        print(f"Error while validating data: {e}")

# Schedule the script to run periodically (e.g., every 5 minutes)
schedule.every(5).minutes.do(check_data_quality)

# Start the scheduler
if __name__ == "__main__":
    print("Starting data quality check automation...")
    while True:
        schedule.run_pending()
        time.sleep(1)
