#!/usr/bin/env python3
"""
Options Volume Contamination Check
====================================
Queries the options scanner database to check if a ticker had
unusual options volume within ±20 trading days of an insider event.

Phase 4 Backtest Results (2021-2026):
  Insider clusters WITH concurrent options volume spike:
    5d:  -0.87% alpha (vs +1.32% clean)
    20d: -2.02% alpha (vs +0.99% clean)
    60d: -6.76% alpha (vs +0.47% clean)

  Call-heavy options + insider buying: -13.86% at 20d, 5.9% win rate
  C-suite + options spike: -9.13% at 60d (vs +1.96% clean)

  Conclusion: Options volume spikes KILL insider alpha.
  Use as negative filter on insider cluster alerts.

Usage:
    from options_volume_check import check_options_contamination
    result = check_options_contamination('AAPL', '2026-01-15')
"""

import sqlite3
import os
from datetime import datetime, timedelta

# Path to options scanner database (relative to form4_scanner/)
OPTIONS_DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', 'options_scanner', 'options_data.db'
)

# Coincidence window in calendar days (±20 trading days ≈ ±28 calendar days)
WINDOW_CALENDAR_DAYS = 28

# Minimum deviation multiple to flag as contamination
MIN_DEVIATION_MULTIPLE = 2.0


def check_options_contamination(ticker, event_date, window_days=WINDOW_CALENDAR_DAYS):
    """
    Check if a ticker had unusual options volume near an insider event.

    Args:
        ticker: Stock ticker symbol
        event_date: Date string 'YYYY-MM-DD' of the insider transaction
        window_days: Calendar days to look before/after (default 28 ≈ 20 trading days)

    Returns:
        dict with:
            'contaminated': bool - True if options spike found
            'anomalies': list of anomaly records found
            'max_deviation': float - highest deviation multiple found
            'signal_types': list - types of options signals found
            'warning_html': str - HTML snippet for email banner
            'warning_text': str - plain text warning
            'error': str or None
    """
    result = {
        'contaminated': False,
        'anomalies': [],
        'max_deviation': 0.0,
        'signal_types': [],
        'warning_html': '',
        'warning_text': '',
        'error': None,
    }

    if not os.path.exists(OPTIONS_DB_PATH):
        result['error'] = f'Options DB not found: {OPTIONS_DB_PATH}'
        return result

    try:
        # Parse event date
        if isinstance(event_date, str):
            evt = datetime.strptime(event_date, '%Y-%m-%d')
        else:
            evt = event_date

        date_min = (evt - timedelta(days=window_days)).strftime('%Y-%m-%d')
        date_max = (evt + timedelta(days=window_days)).strftime('%Y-%m-%d')

        conn = sqlite3.connect(OPTIONS_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check anomalies table for flagged events
        cursor.execute("""
            SELECT ticker, detected_date, volume_today,
                   avg_volume_1month, deviation_multiple,
                   percentage_above_avg, signal_type, notes,
                   oi_today, oi_change_pct
            FROM anomalies
            WHERE ticker = ?
              AND detected_date BETWEEN ? AND ?
            ORDER BY deviation_multiple DESC
        """, (ticker, date_min, date_max))

        anomalies = [dict(r) for r in cursor.fetchall()]

        # Also check daily_options_volume for spikes not caught by anomaly detector
        # (if the anomalies table is sparse, use raw volume data)
        cursor.execute("""
            SELECT trade_date, total_volume, total_call_volume,
                   total_put_volume, total_oi
            FROM daily_options_volume
            WHERE ticker = ?
              AND trade_date BETWEEN ? AND ?
            ORDER BY total_volume DESC
            LIMIT 5
        """, (ticker, date_min, date_max))

        raw_volume = [dict(r) for r in cursor.fetchall()]

        # Check if any raw volume days are significantly elevated
        # Get baseline average for this ticker
        cursor.execute("""
            SELECT AVG(total_volume) as avg_vol,
                   COUNT(*) as days
            FROM daily_options_volume
            WHERE ticker = ?
        """, (ticker,))
        baseline = cursor.fetchone()

        conn.close()

        # Evaluate anomalies
        if anomalies:
            result['contaminated'] = True
            result['anomalies'] = anomalies
            result['max_deviation'] = max(a['deviation_multiple'] or 0 for a in anomalies)
            result['signal_types'] = list(set(a['signal_type'] for a in anomalies if a['signal_type']))

        # Evaluate raw volume spikes (backup check)
        elif raw_volume and baseline and baseline['avg_vol'] and baseline['avg_vol'] > 0:
            avg_vol = baseline['avg_vol']
            for day in raw_volume:
                vol = day['total_volume'] or 0
                if vol > 0 and avg_vol > 0:
                    ratio = vol / avg_vol
                    if ratio >= 3.0:  # 3x average = significant spike
                        result['contaminated'] = True
                        result['max_deviation'] = max(result['max_deviation'], ratio)
                        result['anomalies'].append({
                            'ticker': ticker,
                            'detected_date': day['trade_date'],
                            'volume_today': vol,
                            'deviation_multiple': ratio,
                            'signal_type': 'raw_volume_spike',
                            'notes': f'{ratio:.1f}x average volume',
                        })

        # Build warning messages
        if result['contaminated']:
            result['warning_html'] = _build_warning_html(ticker, result)
            result['warning_text'] = _build_warning_text(ticker, result)

    except Exception as e:
        result['error'] = str(e)

    return result


def check_batch(tickers_and_dates):
    """
    Check multiple ticker/date pairs for options contamination.

    Args:
        tickers_and_dates: list of (ticker, date_string) tuples

    Returns:
        dict: ticker -> check result
    """
    results = {}
    for ticker, date_str in tickers_and_dates:
        results[ticker] = check_options_contamination(ticker, date_str)
    return results


def _build_warning_html(ticker, result):
    """Build HTML warning banner for contaminated insider signal."""
    max_dev = result['max_deviation']
    n_anomalies = len(result['anomalies'])
    sig_types = ', '.join(result['signal_types']) if result['signal_types'] else 'volume spike'

    # Severity color
    if max_dev >= 4.0:
        border = '#c53030'
        bg = '#fff5f5'
        icon = '🔴'
        severity = 'HIGH'
    elif max_dev >= 2.5:
        border = '#dd6b20'
        bg = '#fffaf0'
        icon = '🟠'
        severity = 'MODERATE'
    else:
        border = '#d69e2e'
        bg = '#fffff0'
        icon = '🟡'
        severity = 'LOW'

    dates_str = ', '.join(a['detected_date'] for a in result['anomalies'][:3])
    if n_anomalies > 3:
        dates_str += f' +{n_anomalies - 3} more'

    html = f"""
    <div style="background:{bg}; border-left:4px solid {border};
                padding:10px 14px; margin:8px 0; border-radius:0 6px 6px 0;
                font-size:12px;">
        <div style="font-weight:bold; color:{border}; margin-bottom:3px;">
            {icon} OPTIONS VOLUME CONTAMINATION — {severity}
        </div>
        <div style="color:#555;">
            Unusual options activity detected near this insider event
            ({max_dev:.1f}x deviation, {sig_types}).
            Dates: {dates_str}
        </div>
        <div style="color:#888; margin-top:4px; font-size:11px;">
            Phase 4 backtest: insider+options = -2% to -7% alpha degradation vs clean signal.
            Call-heavy overlap: -13.9% at 20d. Consider downgrading conviction.
        </div>
    </div>
    """
    return html


def _build_warning_text(ticker, result):
    """Build plain text warning for contaminated insider signal."""
    max_dev = result['max_deviation']
    n = len(result['anomalies'])
    dates = ', '.join(a['detected_date'] for a in result['anomalies'][:3])

    return (
        f"⚠️ OPTIONS CONTAMINATION: {ticker} had {n} unusual options volume "
        f"event{'s' if n > 1 else ''} ({max_dev:.1f}x deviation) on {dates}. "
        f"Backtest: insider+options = -2% to -7% alpha loss vs clean insider signal."
    )


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import sys
    ticker = sys.argv[1] if len(sys.argv) > 1 else 'AAPL'
    date = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime('%Y-%m-%d')

    print(f"Checking options contamination for {ticker} around {date}...")
    result = check_options_contamination(ticker, date)

    if result['error']:
        print(f"Error: {result['error']}")
    elif result['contaminated']:
        print(f"⚠️  CONTAMINATED: {len(result['anomalies'])} anomalies found")
        print(f"   Max deviation: {result['max_deviation']:.1f}x")
        print(f"   Signal types: {result['signal_types']}")
        for a in result['anomalies']:
            print(f"   {a['detected_date']}: {a.get('volume_today', 0):,} vol, "
                  f"{a['deviation_multiple']:.1f}x, {a.get('signal_type', 'N/A')}")
        print(f"\n{result['warning_text']}")
    else:
        print("✅ Clean — no options volume contamination detected")
