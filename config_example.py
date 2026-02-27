"""
Configuration settings for SEC Form 4 Insider Transaction Scanner

Copy this file to config.py and fill in your credentials:
    cp config_example.py config.py
"""

# EMAIL SETTINGS
EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'sender_email': 'your_email@gmail.com',
    'sender_password': 'your_app_password_here',
    'recipient_email': 'your_email@gmail.com',
}

# SEC EDGAR SETTINGS
EDGAR_CONFIG = {
    'user_agent': 'YourName your_email@gmail.com',
    'request_delay': 0.15,
    'form4_rss_url': 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=only&count=100&output=atom',
    'edgar_base_url': 'https://www.sec.gov/Archives/edgar/data/',
}

# DETECTION THRESHOLDS
ALERT_THRESHOLDS = {
    'min_cluster_size': 2,
    'cluster_window_days': 14,
    'min_purchase_value': 10000,
    'transaction_types': ['P'],
}

# DATABASE SETTINGS
DATABASE_CONFIG = {
    'db_path': 'form4_insider_trades.db',
}

# OPERATIONAL SETTINGS
OPERATIONAL_CONFIG = {
    'lookback_days': 3,
    'max_filings_per_run': 500,
}
