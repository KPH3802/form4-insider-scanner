#!/usr/bin/env python3
"""
SEC Form 4 Insider Transaction Scanner - Main Script

This script orchestrates the entire workflow:
1. Fetch recent Form 4 filings from SEC EDGAR
2. Parse and store transaction data
3. Analyze for insider buying clusters
4. Check options volume contamination (Phase 4 cross-signal filter)
5. Analyze for insider sell signals (backtest-validated)
6. Send email alerts (or daily status if no clusters/sells)

Phase 4 Result: Options volume is a kill switch for insider signals.
  Insider + options spike (±20d): -11.83% alpha at 20d, 16.7% win rate
  Call-heavy + insider: -13.86% at 20d, 5.9% win rate
  Clean insider clusters: +0.99% at 20d (significant)

Usage:
    python main.py                # Run full scan
    python main.py --dry-run      # Test without sending emails
    python main.py --fetch-only   # Only fetch, no analysis
    python main.py --analyze-only # Only analyze existing data
    python main.py --status-only  # Just send status email
"""

import sys
from datetime import datetime, timezone

from config import EMAIL_CONFIG
from database import (
    initialize_database, 
    get_database_stats,
    insert_transaction,
    mark_filing_processed,
    is_filing_processed,
    get_cluster_candidates,
    get_recent_purchases,
    was_alert_sent,
    record_alert_sent
)
from edgar_fetcher import EdgarFetcher
from form4_parser import Form4Parser
from analyzer import ClusterAnalyzer, SellSignalAnalyzer
from email_reporter import EmailReporter

# Phase 4 cross-signal filter — graceful fallback if module/DB unavailable
try:
    from options_volume_check import check_options_contamination
    OPTIONS_CHECK_AVAILABLE = True
except ImportError:
    OPTIONS_CHECK_AVAILABLE = False
    print("NOTE: options_volume_check not available — contamination filter disabled")


def fetch_and_store_filings():
    """Fetch recent Form 4 filings and store transactions."""
    fetcher = EdgarFetcher()
    parser = Form4Parser()
    
    print("Fetching recent Form 4 filings from SEC EDGAR...")
    filings = fetcher.get_recent_form4_filings(count=100)
    print(f"Found {len(filings)} filings in feed")
    
    filings_processed = 0
    transactions_added = 0
    errors = 0
    
    for filing in filings:
        accession = filing.get('accession_number')
        if not accession:
            continue
            
        if is_filing_processed(accession):
            continue
        
        try:
            cik = filing.get('cik')
            if not cik:
                mark_filing_processed(accession, 'error', 'Missing CIK')
                errors += 1
                continue
            
            xml_content = fetcher.fetch_form4_xml(cik, accession)
            if not xml_content:
                mark_filing_processed(accession, 'error', 'Could not fetch XML')
                errors += 1
                continue
            
            filing_date = filing.get('updated', '')[:10] if filing.get('updated') else None
            transactions = parser.parse(xml_content, accession, filing_date)
            
            for txn in transactions:
                if insert_transaction(txn):
                    transactions_added += 1
            
            mark_filing_processed(accession, 'success')
            filings_processed += 1
            
        except Exception as e:
            print(f"  Error processing {accession}: {e}")
            mark_filing_processed(accession, 'error', str(e))
            errors += 1
    
    print(f"Processed {filings_processed} new filings, added {transactions_added} transactions, {errors} errors")
    return filings_processed, transactions_added, errors


# ============================================================
#  PHASE 4: OPTIONS VOLUME CONTAMINATION CHECK
# ============================================================

def check_cluster_contamination(clusters):
    """
    Check each insider buying cluster for concurrent options volume spikes.
    
    Phase 4 Backtest (2021-2026, ±20d window):
      - Insider + options spike: -11.83% alpha at 20d, 16.7% win rate
      - Clean insider clusters: +0.99% alpha at 20d
      - Call-heavy + insider: -13.86% at 20d, 5.9% win rate (nuclear)
      - Options spike first → insider buys: -13.15% at 20d, 8.0% win rate
    
    Attaches 'options_contamination' dict to each cluster for email rendering.
    """
    if not OPTIONS_CHECK_AVAILABLE:
        print("  Options contamination check: SKIPPED (module not available)")
        return

    print("Checking options volume contamination (Phase 4 filter)...")
    contaminated_count = 0
    clean_count = 0
    error_count = 0

    for cluster in clusters:
        ticker = cluster['ticker']
        # Use last purchase date as anchor for the ±20 trading day window
        event_date = cluster.get('last_purchase') or datetime.now().strftime('%Y-%m-%d')

        try:
            result = check_options_contamination(ticker, event_date)
            cluster['options_contamination'] = result

            if result['contaminated']:
                contaminated_count += 1
                print(f"  ⚠️  {ticker}: CONTAMINATED ({result['max_deviation']:.1f}x, "
                      f"{len(result['anomalies'])} event{'s' if len(result['anomalies']) != 1 else ''})")
            elif result['error']:
                error_count += 1
                print(f"  ⚪ {ticker}: check skipped ({result['error']})")
            else:
                clean_count += 1
                print(f"  ✅ {ticker}: clean")
        except Exception as e:
            error_count += 1
            cluster['options_contamination'] = {
                'contaminated': False, 'anomalies': [], 'max_deviation': 0,
                'signal_types': [], 'warning_html': '', 'warning_text': '',
                'error': str(e)
            }
            print(f"  ❌ {ticker}: ERROR - {e}")

    print(f"  Summary: {contaminated_count} contaminated, {clean_count} clean, "
          f"{error_count} errors/skipped")


def analyze_and_alert(dry_run=False):
    """Analyze for buying clusters, check contamination, and send alerts."""
    analyzer = ClusterAnalyzer()
    reporter = EmailReporter()
    
    print("Analyzing for insider buying clusters...")
    clusters = analyzer.find_clusters()
    print(f"Found {len(clusters)} potential clusters")
    
    today = datetime.now().strftime('%Y-%m-%d')
    new_clusters = []
    
    for cluster in clusters:
        ticker = cluster['ticker']
        
        if was_alert_sent('cluster', ticker, today):
            print(f"  Already alerted on {ticker} today, skipping")
            continue
        
        purchases = get_recent_purchases(ticker)
        cluster['purchases'] = purchases
        new_clusters.append(cluster)
    
    # Phase 4: Check options volume contamination on new clusters
    if new_clusters:
        check_cluster_contamination(new_clusters)
    
    if new_clusters:
        if dry_run:
            for cluster in new_clusters:
                contam = cluster.get('options_contamination', {})
                tag = " ⚠️ CONTAMINATED" if contam.get('contaminated') else ""
                print(f"  [DRY RUN] Would send BUY alert for "
                      f"{cluster['ticker']}{tag}: "
                      f"{cluster['unique_insiders']} insiders, "
                      f"${cluster['total_purchased']:,.0f}")
        else:
            if reporter.send_cluster_alert(new_clusters):
                for cluster in new_clusters:
                    record_alert_sent('cluster', cluster['ticker'], today,
                                      f"{cluster['unique_insiders']} insiders")
                print(f"  Sent 1 email with {len(new_clusters)} buying clusters")
    
    return clusters, len(new_clusters)


def analyze_sells_and_alert(dry_run=False):
    """
    Analyze for insider sell signals and send alerts.
    
    Backtest-validated criteria:
      S1: Officer+Director, $250K-$5M → -2.54% 5d alpha
      S2: Officer or Director, $250K-$5M → -0.50% to -0.86% 5d
    """
    sell_analyzer = SellSignalAnalyzer(lookback_days=3, min_value=50000)
    reporter = EmailReporter()
    
    print("Analyzing for insider sell signals...")
    
    # Find all significant sells (for logging)
    all_sells = sell_analyzer.find_sell_signals()
    s1_total = sum(1 for s in all_sells if s['tier_tag'] == 'S1')
    s2_total = sum(1 for s in all_sells if s['tier_tag'] == 'S2')
    watch_total = sum(1 for s in all_sells if s['tier_tag'] == 'SELL_WATCH')
    print(f"  Found {len(all_sells)} tickers with significant sells: "
          f"{s1_total} S1, {s2_total} S2, {watch_total} WATCH")
    
    # Get only new (un-alerted) S1 and S2 signals
    new_alerts = sell_analyzer.get_new_sell_alerts()
    
    if new_alerts:
        if dry_run:
            for alert in new_alerts:
                print(f"  [DRY RUN] Would send SELL alert for {alert['ticker']} "
                      f"({alert['tier_tag']}): {alert['num_sellers']} seller(s), "
                      f"${alert['total_value']:,.0f}")
        else:
            if reporter.send_sell_alert(new_alerts):
                sell_analyzer.mark_sell_alerts_sent(new_alerts)
                print(f"  Sent 1 email with {len(new_alerts)} sell signals")
    else:
        print("  No new sell signals to alert on")
    
    return all_sells, len(new_alerts)


def send_status_report(filings_processed, transactions_added, clusters_found,
                       sells_found=None, dry_run=False):
    """Send daily status email."""
    reporter = EmailReporter()
    stats = get_database_stats()
    
    status_data = {
        'filings_processed': filings_processed,
        'transactions_added': transactions_added,
        'clusters_found': len(clusters_found) if clusters_found else 0,
        'sells_found': len(sells_found) if sells_found else 0,
        'stats': stats,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    if dry_run:
        print(f"[DRY RUN] Would send daily status email")
        print(f"  Filings processed: {filings_processed}")
        print(f"  Transactions added: {transactions_added}")
        print(f"  Buying clusters found: {status_data['clusters_found']}")
        print(f"  Sell signals found: {status_data['sells_found']}")
        return True
    else:
        return reporter.send_status_report(stats)


def main():
    # Skip weekends - markets closed
    if datetime.now(timezone.utc).weekday() >= 5:  # 5=Saturday, 6=Sunday
        print("Weekend - skipping scan")
        return

    print("#" * 60)
    print("# FORM 4 INSIDER TRANSACTION SCANNER")
    print(f"# {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("#" * 60)
    
    # Parse arguments
    dry_run = '--dry-run' in sys.argv
    fetch_only = '--fetch-only' in sys.argv
    analyze_only = '--analyze-only' in sys.argv
    status_only = '--status-only' in sys.argv
    
    if dry_run:
        print("\n*** DRY RUN MODE - No emails will be sent ***\n")
    
    # Initialize database
    initialize_database()
    
    # Show current stats
    stats = get_database_stats()
    print(f"Current database stats:")
    print(f"  Transactions: {stats['total_transactions']}")
    print(f"  Purchases: {stats['total_purchases']}")
    print(f"  Sells: {stats.get('total_sells', 'N/A')}")
    print(f"  Companies: {stats['unique_companies']}")
    print(f"  Options contamination filter: "
          f"{'ENABLED' if OPTIONS_CHECK_AVAILABLE else 'DISABLED'}")
    
    filings_processed = 0
    transactions_added = 0
    clusters = []
    sells = []
    
    # Fetch new filings
    if not analyze_only and not status_only:
        print("\n" + "=" * 40)
        filings_processed, transactions_added, errors = fetch_and_store_filings()
    
    # Analyze for buying clusters (now includes Phase 4 contamination check)
    if not fetch_only and not status_only:
        print("\n" + "=" * 40)
        clusters, buy_alerts_sent = analyze_and_alert(dry_run=dry_run)
    
    # Analyze for sell signals
    if not fetch_only and not status_only:
        print("\n" + "=" * 40)
        sells, sell_alerts_sent = analyze_sells_and_alert(dry_run=dry_run)
    
    # Send daily status (always, unless fetch-only or analyze-only)
    if not fetch_only and not analyze_only:
        print("\n" + "=" * 40)
        print("Sending daily status email...")
        send_status_report(filings_processed, transactions_added, clusters,
                           sells, dry_run=dry_run)
    
    print("\n" + "=" * 40)
    print("Scan complete!")


if __name__ == '__main__':
    main()
