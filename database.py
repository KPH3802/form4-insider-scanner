import sqlite3
from datetime import datetime, timedelta
from config import DATABASE_CONFIG

def get_connection():
    return sqlite3.connect(DATABASE_CONFIG['db_path'])

def initialize_database():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS form4_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            accession_number TEXT,
            filing_date DATE,
            accepted_datetime DATETIME,
            issuer_cik TEXT,
            issuer_name TEXT,
            issuer_ticker TEXT,
            insider_cik TEXT,
            insider_name TEXT,
            insider_title TEXT,
            is_director INTEGER,
            is_officer INTEGER,
            is_ten_percent_owner INTEGER,
            transaction_date DATE,
            transaction_code TEXT,
            shares_amount REAL,
            price_per_share REAL,
            total_value REAL,
            acquired_disposed TEXT,
            shares_owned_after REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_filings (
            accession_number TEXT PRIMARY KEY,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT,
            error_message TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sent_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT,
            issuer_ticker TEXT,
            alert_date DATE,
            details TEXT,
            sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(alert_type, issuer_ticker, alert_date)
        )
    ''')
    conn.commit()
    conn.close()
    print("Database initialized successfully.")

def insert_transaction(t):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO form4_transactions (
                accession_number, filing_date, accepted_datetime,
                issuer_cik, issuer_name, issuer_ticker,
                insider_cik, insider_name, insider_title,
                is_director, is_officer, is_ten_percent_owner,
                transaction_date, transaction_code, shares_amount,
                price_per_share, total_value, acquired_disposed, shares_owned_after
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (t.get('accession_number'), t.get('filing_date'), t.get('accepted_datetime'),
              t.get('issuer_cik'), t.get('issuer_name'), t.get('issuer_ticker'),
              t.get('insider_cik'), t.get('insider_name'), t.get('insider_title'),
              t.get('is_director',0), t.get('is_officer',0), t.get('is_ten_percent_owner',0),
              t.get('transaction_date'), t.get('transaction_code'), t.get('shares_amount'),
              t.get('price_per_share'), t.get('total_value'), t.get('acquired_disposed'),
              t.get('shares_owned_after')))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def mark_filing_processed(accession, status='success', error_message=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO processed_filings (accession_number, status, error_message, processed_at) VALUES (?,?,?,?)',
                   (accession, status, error_message, datetime.now()))
    conn.commit()
    conn.close()

def is_filing_processed(accession):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM processed_filings WHERE accession_number = ?', (accession,))
    result = cursor.fetchone() is not None
    conn.close()
    return result

def get_recent_purchases(ticker, days=14):
    conn = get_connection()
    cursor = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    cursor.execute('''
        SELECT insider_name, insider_title, transaction_date, shares_amount, price_per_share, total_value
        FROM form4_transactions
        WHERE issuer_ticker = ? AND transaction_code = 'P' AND acquired_disposed = 'A' AND transaction_date >= ?
        ORDER BY transaction_date DESC
    ''', (ticker, cutoff))
    results = [{'insider_name': r[0], 'insider_title': r[1], 'transaction_date': r[2],
                'shares_amount': r[3], 'price_per_share': r[4], 'total_value': r[5]} for r in cursor.fetchall()]
    conn.close()
    return results

def get_cluster_candidates(min_insiders=2, days=14, min_value=10000):
    conn = get_connection()
    cursor = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    cursor.execute('''
        SELECT issuer_ticker, issuer_name, COUNT(DISTINCT insider_cik) as unique_insiders,
               SUM(total_value) as total_purchased, MIN(transaction_date) as first_purchase,
               MAX(transaction_date) as last_purchase
        FROM form4_transactions
        WHERE transaction_code = 'P' AND acquired_disposed = 'A' AND transaction_date >= ?
              AND total_value >= ? AND issuer_ticker IS NOT NULL AND issuer_ticker != ''
        GROUP BY issuer_ticker
        HAVING COUNT(DISTINCT insider_cik) >= ?
        ORDER BY unique_insiders DESC, total_purchased DESC
    ''', (cutoff, min_value, min_insiders))
    results = [{'ticker': r[0], 'company_name': r[1], 'unique_insiders': r[2],
                'total_purchased': r[3], 'first_purchase': r[4], 'last_purchase': r[5]} for r in cursor.fetchall()]
    conn.close()
    return results


# ============================================================
#  INSIDER SELLING QUERIES (backtest-validated)
# ============================================================

def get_significant_sells(days=3, min_value=50000):
    """
    Find insider sells matching backtest-validated criteria.
    
    Backtest results (2020-2025, 432K trades):
      Officer+Director: -2.54% 5d alpha (STRONGEST)
      $250K-$5M: sweet spot (-1.49% to -1.52% 5d)
      LOW SI + sell: -1.64% 5d (worst — bad news not priced in)
    
    Returns all sells >= min_value in last N days, with role flags
    for tier classification by the analyzer.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    cursor.execute('''
        SELECT 
            issuer_ticker,
            issuer_name,
            insider_name,
            insider_title,
            is_officer,
            is_director,
            is_ten_percent_owner,
            transaction_date,
            shares_amount,
            price_per_share,
            total_value,
            accession_number
        FROM form4_transactions
        WHERE transaction_code = 'S' 
          AND acquired_disposed = 'D'
          AND transaction_date >= ?
          AND total_value >= ?
          AND issuer_ticker IS NOT NULL 
          AND issuer_ticker != ''
        ORDER BY total_value DESC
    ''', (cutoff, min_value))
    
    results = []
    for r in cursor.fetchall():
        results.append({
            'ticker': r[0],
            'company_name': r[1],
            'insider_name': r[2],
            'insider_title': r[3],
            'is_officer': r[4],
            'is_director': r[5],
            'is_ten_percent_owner': r[6],
            'transaction_date': r[7],
            'shares_amount': r[8],
            'price_per_share': r[9],
            'total_value': r[10],
            'accession_number': r[11],
        })
    
    conn.close()
    return results


def get_recent_sells_by_ticker(ticker, days=14):
    """Get all recent sells for a specific ticker (for context in alerts)."""
    conn = get_connection()
    cursor = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    cursor.execute('''
        SELECT insider_name, insider_title, is_officer, is_director,
               transaction_date, shares_amount, price_per_share, total_value
        FROM form4_transactions
        WHERE issuer_ticker = ? 
          AND transaction_code = 'S' 
          AND acquired_disposed = 'D' 
          AND transaction_date >= ?
        ORDER BY transaction_date DESC
    ''', (ticker, cutoff))
    
    results = [{'insider_name': r[0], 'insider_title': r[1], 'is_officer': r[2],
                'is_director': r[3], 'transaction_date': r[4], 'shares_amount': r[5],
                'price_per_share': r[6], 'total_value': r[7]} for r in cursor.fetchall()]
    conn.close()
    return results


def was_alert_sent(alert_type, ticker, alert_date):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM sent_alerts WHERE alert_type=? AND issuer_ticker=? AND alert_date=?',
                   (alert_type, ticker, alert_date))
    result = cursor.fetchone() is not None
    conn.close()
    return result

def record_alert_sent(alert_type, ticker, alert_date, details=''):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('INSERT INTO sent_alerts (alert_type, issuer_ticker, alert_date, details) VALUES (?,?,?,?)',
                       (alert_type, ticker, alert_date, details))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()

def get_database_stats():
    conn = get_connection()
    cursor = conn.cursor()
    stats = {}
    cursor.execute('SELECT COUNT(*) FROM form4_transactions')
    stats['total_transactions'] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM form4_transactions WHERE transaction_code = 'P'")
    stats['total_purchases'] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM form4_transactions WHERE transaction_code = 'S'")
    stats['total_sells'] = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(DISTINCT issuer_ticker) FROM form4_transactions WHERE issuer_ticker IS NOT NULL')
    stats['unique_companies'] = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(DISTINCT insider_cik) FROM form4_transactions')
    stats['unique_insiders'] = cursor.fetchone()[0]
    cursor.execute('SELECT MIN(transaction_date), MAX(transaction_date) FROM form4_transactions')
    dr = cursor.fetchone()
    stats['earliest_transaction'] = dr[0]
    stats['latest_transaction'] = dr[1]
    cursor.execute('SELECT COUNT(*) FROM processed_filings')
    stats['processed_filings'] = cursor.fetchone()[0]
    conn.close()
    return stats

if __name__ == '__main__':
    initialize_database()
    print(get_database_stats())
