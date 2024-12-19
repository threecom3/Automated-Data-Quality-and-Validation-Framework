 
# Automated Data Quality and Validation Framework

This project automates data quality checks and validation processes, ensuring data integrity in a dynamic environment.

## Features
- **Automated Checks:**
  - Identifies missing values and duplicate records.
  - Validates data ranges (e.g., age between 0 and 120).
  - Ensures string fields do not exceed specified limits (e.g., max length of 255 characters).
- **Email Notifications:** Alerts stakeholders with detailed reports of data quality issues.
- **Retry Mechanism:** Handles database connection retries to ensure smooth execution.
- **Logging:** Records all detected issues in JSON format for auditing.

## Tech Stack
- **Languages & Tools:** Python, Pandas, SQLAlchemy
- **Database Integration:** Uses environment variable-based connection to SQL databases.
- **Email Notification:** Configured SMTP for real-time alerts.
- **Task Scheduling:** Periodic execution using `schedule`.

## Prerequisites
1. **Python**: Ensure Python 3.7 or later is installed.
2. **Dependencies**: Install required libraries:
   ```bash
   pip install -r requirements.txt
