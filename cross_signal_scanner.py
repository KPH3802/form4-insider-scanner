#!/usr/bin/env python3
"""
Cross-Signal Scanner: Insider Buying × Short Interest × Options Volume
======================================================================
Runs after the Form 4 scanner. Checks recent insider purchases for
Tier2 quality (C-Suite + $500K+), then enriches with short interest
data and checks for options volume contamination.

Backtested edge (2020-2025):
  Tier2 + DTC>5 + SI Increasing >10%:
    5d:  +6.73% alpha, 69.7% WR (n=142, p<0.0001)
    10d: +8.12% alpha, 62.7% WR
    20d: +9.90% alpha, 52.8% WR
    40d: +15.46% alpha, 67.6% WR

  Tier2 + DTC>5 (without SI filter):
    5d:  +4.67% alpha, 70.2% WR (n=527, p<0.0001)

Phase 4 OPTIONS CONTAMINATION (2021-2026):
  Insider + options spike (±20d): -11.83% alpha at 20d, 16.7% WR
  Call-heavy + insider: -13.86% at 20d, 5.9% WR
  Clean insider clusters: +0.99% at 20d
  → Options volume is a KILL SWITCH for insider signals

Scheduled: 22:15 UTC daily (after Form 4 scanner at 22:00 UTC)
Command:  cd /home/KPH3802/form4_scanner && python3 cross_signal_scanner.py
"""

import sqlite3
import os
import sys
import smtplib
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# CONFIG - uses same email settings as the Form 4 scanner
# ---------------------------------------------------------------------------
try:
    from config import EMAIL_CONFIG, EDGAR_CONFIG
except ImportError:
    # Fallback if config structure differs
    EMAIL_CONFIG = None

# Phase 4 cross-signal filter
try:
    from options_volume_check import check_options_contamination
    OPTIONS_CHECK_AVAILABLE = True
except ImportError:
    OPTIONS_CHECK_AVAILABLE = False

# Database path (same as Form 4 scanner)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "form4_insider_trades.db")

# FRED economic database (for macro regime overlay)
FRED_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "fred_collector", "fred_economic.db")

# How many days back to scan for new insider purchases
LOOKBACK_DAYS = 3

# Tier2 criteria (from backtest)
CSUITE_KEYWORDS = ['CEO', 'CFO', 'COO', 'CTO', 'CIO', 'CMO',
                   'President', 'Chief']
MIN_PURCHASE_VALUE = 500_000  # $500K minimum

# Short interest thresholds (from backtest)
DTC_THRESHOLD = 5.0       # Days-to-cover minimum
SI_CHANGE_THRESHOLD = 10  # SI increase % for "increasing" tier
SI_SURGE_THRESHOLD = 25   # SI increase % for "surging" tier


# ---------------------------------------------------------------------------
# DATABASE: Query recent insider purchases
# ---------------------------------------------------------------------------
def get_recent_purchases(days_back=LOOKBACK_DAYS):
    """
    Pull recent open-market purchases from the Form 4 database.
    Returns list of dicts with ticker, insider info, and transaction details.
    """
    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}")
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime('%Y-%m-%d')

    # Get open market purchases (transaction_code = 'P')
    cursor.execute("""
        SELECT
            issuer_ticker AS ticker,
            insider_name,
            insider_title,
            transaction_date,
            shares_amount AS shares,
            price_per_share,
            COALESCE(total_value, shares_amount * price_per_share, 0) AS total_value
        FROM form4_transactions
        WHERE transaction_code = 'P'
          AND transaction_date >= ?
        ORDER BY transaction_date DESC
    """, (cutoff,))

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()

    print(f"Found {len(rows)} open-market purchases in last {days_back} days")
    return rows


# ---------------------------------------------------------------------------
# TIER2 FILTER
# ---------------------------------------------------------------------------
def is_csuite(title):
    """Check if insider title indicates C-Suite executive."""
    if not title:
        return False
    title_lower = title.lower()
    return any(kw.lower() in title_lower for kw in CSUITE_KEYWORDS)


def filter_tier2(purchases):
    """Filter for Tier2: C-Suite + $500K+ purchase value."""
    tier2 = []
    for p in purchases:
        if is_csuite(p['insider_title']) and p['total_value'] >= MIN_PURCHASE_VALUE:
            tier2.append(p)

    print(f"Tier2 signals (C-Suite + ${MIN_PURCHASE_VALUE:,}+): {len(tier2)}")
    return tier2


# ---------------------------------------------------------------------------
# SHORT INTEREST ENRICHMENT via yfinance
# ---------------------------------------------------------------------------
def fetch_short_interest(tickers):
    """
    Fetch short interest metrics for a list of tickers via yfinance.

    Returns dict: ticker -> {
        'days_to_cover': float,
        'shares_short': int,
        'shares_short_prior': int,
        'si_change_pct': float,
        'short_pct_float': float,
        'error': str or None
    }
    """
    try:
        import yfinance as yf
    except ImportError:
        print("ERROR: yfinance not installed. Run: pip install yfinance")
        return {}

    results = {}
    unique_tickers = list(set(tickers))
    print(f"Fetching short interest for {len(unique_tickers)} tickers...")

    for ticker in unique_tickers:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            shares_short = info.get('sharesShort', 0) or 0
            shares_short_prior = info.get('sharesShortPriorMonth', 0) or 0
            days_to_cover = info.get('shortRatio', 0) or 0
            short_pct_float = info.get('shortPercentOfFloat', 0) or 0

            # Calculate SI change %
            if shares_short_prior > 0:
                si_change_pct = ((shares_short - shares_short_prior)
                                 / shares_short_prior * 100)
            else:
                si_change_pct = 0.0

            results[ticker] = {
                'days_to_cover': float(days_to_cover),
                'shares_short': shares_short,
                'shares_short_prior': shares_short_prior,
                'si_change_pct': round(si_change_pct, 1),
                'short_pct_float': round(float(short_pct_float) * 100, 2),
                'error': None
            }
            print(f"  {ticker}: DTC={days_to_cover:.1f}, "
                  f"SI change={si_change_pct:+.1f}%, "
                  f"SI%float={short_pct_float*100:.1f}%")

        except Exception as e:
            results[ticker] = {
                'days_to_cover': 0,
                'shares_short': 0,
                'shares_short_prior': 0,
                'si_change_pct': 0,
                'short_pct_float': 0,
                'error': str(e)
            }
            print(f"  {ticker}: ERROR - {e}")

    return results


# ---------------------------------------------------------------------------
# FRED MACRO REGIME: VIX, Yield Curve, Credit Spreads
# ---------------------------------------------------------------------------
MACRO_SERIES = {
    'vix':    'VIXCLS',
    'yc':     'T10Y2Y',
    'credit': 'BAMLH0A0HYM2',
}


def get_macro_regime():
    """
    Read latest VIX, yield curve, and credit spread from fred_economic.db.
    """
    result = {
        'vix': None, 'yc': None, 'credit': None,
        'flags': [], 'label': 'UNKNOWN', 'error': None
    }

    if not os.path.exists(FRED_DB_PATH):
        result['error'] = f"FRED DB not found: {FRED_DB_PATH}"
        print(f"  MACRO: {result['error']}")
        return result

    try:
        conn = sqlite3.connect(FRED_DB_PATH)
        cursor = conn.cursor()

        for key, series_id in MACRO_SERIES.items():
            cursor.execute("""
                SELECT value FROM observations
                WHERE series_id = ?
                  AND value IS NOT NULL
                ORDER BY date DESC
                LIMIT 1
            """, (series_id,))
            row = cursor.fetchone()
            if row:
                result[key] = float(row[0])

        conn.close()

        flags = []
        if result['vix'] is not None and result['vix'] > 30:
            flags.append(f"VIX={result['vix']:.1f} (>30: alpha NOT significant)")
        if result['yc'] is not None and 0 <= result['yc'] < 0.5:
            flags.append(f"Yield curve={result['yc']:.2f} (flat 0-0.5: 20d alpha negative)")
        if result['credit'] is not None and result['credit'] >= 4:
            flags.append(f"Credit spread={result['credit']:.2f} (>=4: alpha halved)")

        result['flags'] = flags
        if len(flags) >= 2:
            result['label'] = 'UNFAVORABLE'
        elif len(flags) == 1:
            result['label'] = 'CAUTION'
        else:
            result['label'] = 'FAVORABLE'

        print(f"  MACRO REGIME: {result['label']}")
        print(f"    VIX:     {result['vix']:.1f}" if result['vix'] else "    VIX:     N/A")
        print(f"    Yield:   {result['yc']:.2f}" if result['yc'] is not None else "    Yield:   N/A")
        print(f"    Credit:  {result['credit']:.2f}" if result['credit'] is not None else "    Credit:  N/A")
        for f in flags:
            print(f"    ⚠️  {f}")

    except Exception as e:
        result['error'] = str(e)
        print(f"  MACRO: ERROR reading FRED DB - {e}")

    return result


# ---------------------------------------------------------------------------
# PHASE 4: OPTIONS VOLUME CONTAMINATION CHECK
# ---------------------------------------------------------------------------
def check_signals_contamination(signals):
    """
    Check each Tier2 signal for options volume contamination.
    
    Phase 4 Backtest (2021-2026):
      Insider + options spike (±20d): -11.83% alpha at 20d
      Call-heavy + insider: -13.86% at 20d, 5.9% WR
      Clean insider clusters: +0.99% at 20d
    
    Attaches 'options_contamination' dict to each signal.
    """
    if not OPTIONS_CHECK_AVAILABLE:
        print("  Options contamination check: SKIPPED (module not available)")
        return

    print("\nChecking options volume contamination (Phase 4)...")
    contaminated = 0
    clean = 0
    errors = 0

    for signal in signals:
        ticker = signal['ticker']
        event_date = signal.get('transaction_date') or datetime.now().strftime('%Y-%m-%d')

        try:
            result = check_options_contamination(ticker, event_date)
            signal['options_contamination'] = result

            if result['contaminated']:
                contaminated += 1
                print(f"  ⚠️  {ticker}: CONTAMINATED ({result['max_deviation']:.1f}x, "
                      f"{len(result['anomalies'])} events)")
            elif result['error']:
                errors += 1
            else:
                clean += 1
        except Exception as e:
            errors += 1
            signal['options_contamination'] = {
                'contaminated': False, 'anomalies': [], 'max_deviation': 0,
                'signal_types': [], 'warning_html': '', 'warning_text': '',
                'error': str(e)
            }

    print(f"  Summary: {contaminated} contaminated, {clean} clean, {errors} errors/skipped")


# ---------------------------------------------------------------------------
# SIGNAL CLASSIFICATION
# ---------------------------------------------------------------------------
def classify_signals(tier2_purchases, si_data, macro_regime=None):
    """
    Combine Tier2 insider data with SI data.
    Classify each signal by strength tier.
    Attach macro regime context to each signal.
    """
    signals = []

    if macro_regime is None:
        macro_regime = {'label': 'UNKNOWN', 'flags': [], 'vix': None,
                        'yc': None, 'credit': None, 'error': 'Not fetched'}

    for p in tier2_purchases:
        ticker = p['ticker']
        si = si_data.get(ticker, {})

        if si.get('error'):
            tier = 'UNVERIFIED'
            tier_num = 3
        else:
            dtc = si.get('days_to_cover', 0)
            si_chg = si.get('si_change_pct', 0)

            if dtc > DTC_THRESHOLD and si_chg > SI_SURGE_THRESHOLD:
                tier = '🔴 TIER 1 — HIGHEST CONVICTION'
                tier_num = 0
            elif dtc > DTC_THRESHOLD and si_chg > SI_CHANGE_THRESHOLD:
                tier = '🟠 TIER 2 — HIGH CONVICTION'
                tier_num = 1
            elif dtc > DTC_THRESHOLD:
                tier = '🟡 TIER 3 — ELEVATED SHORT INTEREST'
                tier_num = 2
            else:
                tier = '⚪ TIER 4 — TIER2 INSIDER BUY (no SI confirmation)'
                tier_num = 4

        signal = {
            **p,
            'days_to_cover': si.get('days_to_cover', 0),
            'si_change_pct': si.get('si_change_pct', 0),
            'short_pct_float': si.get('short_pct_float', 0),
            'shares_short': si.get('shares_short', 0),
            'shares_short_prior': si.get('shares_short_prior', 0),
            'si_error': si.get('error'),
            'tier': tier,
            'tier_num': tier_num,
            'macro_label': macro_regime['label'],
            'macro_flags': macro_regime['flags'],
            'options_contamination': {},  # Populated later by check_signals_contamination
        }
        signals.append(signal)

    signals.sort(key=lambda x: (x['tier_num'], -x['total_value']))
    return signals


# ---------------------------------------------------------------------------
# EMAIL ALERT
# ---------------------------------------------------------------------------
def build_email_html(signals, all_purchases_count, tier2_count, macro_regime=None):
    """Build HTML email for cross-signal alerts with macro regime and
    options contamination context."""

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    confirmed = [s for s in signals if s['tier_num'] <= 2]
    unconfirmed = [s for s in signals if s['tier_num'] >= 3]
    contaminated = [s for s in signals
                    if s.get('options_contamination', {}).get('contaminated')]

    if confirmed:
        subject = (f"🚨 CROSS-SIGNAL ALERT: {len(confirmed)} High-Conviction "
                   f"Insider Buy{'s' if len(confirmed)>1 else ''}")
        if contaminated:
            subject += f" ({len(contaminated)} ⚠️ options-contaminated)"
        priority = "high"
    elif signals:
        subject = (f"📊 Insider Buy Alert: {len(signals)} Tier2 Signal"
                   f"{'s' if len(signals)>1 else ''} (no SI confirmation)")
        priority = "normal"
    else:
        subject = f"📋 Cross-Signal Scanner: No Tier2 signals today"
        priority = "low"

    html = f"""
    <html><body style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto;">

    <h2 style="color: #1a1a2e; border-bottom: 3px solid #e94560; padding-bottom: 8px;">
        Cross-Signal Scanner Report
    </h2>
    <p style="color: #666; font-size: 12px;">{now} | Purchases scanned: {all_purchases_count} | Tier2 matches: {tier2_count} | Options check: {'ON' if OPTIONS_CHECK_AVAILABLE else 'OFF'}</p>
    """

    # --- Macro Regime Banner ---
    if macro_regime and not macro_regime.get('error'):
        macro_colors = {
            'FAVORABLE':   ('#2e7d32', '#e8f5e9', '🟢'),
            'CAUTION':     ('#f57f17', '#fff8e1', '🟡'),
            'UNFAVORABLE': ('#c62828', '#ffebee', '🔴'),
        }
        m_border, m_bg, m_icon = macro_colors.get(
            macro_regime['label'], ('#757575', '#f5f5f5', '⚪'))

        vix_str = f"{macro_regime['vix']:.1f}" if macro_regime['vix'] is not None else "N/A"
        yc_str = f"{macro_regime['yc']:.2f}" if macro_regime['yc'] is not None else "N/A"
        cr_str = f"{macro_regime['credit']:.2f}" if macro_regime['credit'] is not None else "N/A"

        html += f"""
        <div style="background: {m_bg}; border-left: 4px solid {m_border};
                    padding: 10px 15px; margin: 10px 0 15px 0; border-radius: 4px;">
            <div style="font-size: 14px; font-weight: bold; color: {m_border}; margin-bottom: 4px;">
                {m_icon} Macro Regime: {macro_regime['label']}
            </div>
            <table style="font-size: 12px; color: #444;">
                <tr>
                    <td style="padding: 1px 15px 1px 0;">VIX: <b>{vix_str}</b></td>
                    <td style="padding: 1px 15px 1px 0;">Yield Curve: <b>{yc_str}</b></td>
                    <td style="padding: 1px 0;">Credit Spread: <b>{cr_str}</b></td>
                </tr>
            </table>
        """

        if macro_regime['flags']:
            html += '<div style="font-size: 11px; color: #888; margin-top: 4px;">'
            for flag in macro_regime['flags']:
                html += f'⚠️ {flag}<br>'
            html += '</div>'

        html += """
            <div style="font-size: 10px; color: #aaa; margin-top: 4px;">
                Based on FRED macro backtest (2006-2026). Best alpha: normal/inverted curve + tight spreads + VIX &lt;30.
            </div>
        </div>
        """
    elif macro_regime and macro_regime.get('error'):
        html += f"""
        <div style="background: #f5f5f5; border-left: 4px solid #bbb;
                    padding: 8px 15px; margin: 10px 0 15px 0; border-radius: 4px;
                    font-size: 11px; color: #999;">
            ⚪ Macro regime unavailable: {macro_regime['error']}
        </div>
        """

    if not signals:
        html += """
        <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center;">
            <p style="color: #666; font-size: 16px;">No Tier2 insider purchases detected in the last {lookback} days.</p>
            <p style="color: #999; font-size: 12px;">Tier2 = C-Suite executive + $500K+ open market purchase</p>
        </div>
        """.format(lookback=LOOKBACK_DAYS)
    else:
        for s in signals:
            tier_colors = {
                0: ('#ff2d2d', '#fff5f5'),
                1: ('#ff8c00', '#fff8f0'),
                2: ('#ffd700', '#fffef0'),
                3: ('#888', '#f8f8f8'),
                4: ('#ccc', '#fafafa'),
            }
            border_color, bg_color = tier_colors.get(s['tier_num'], ('#ccc', '#fafafa'))
            contam = s.get('options_contamination', {})

            html += f"""
            <div style="background: {bg_color}; border-left: 4px solid {border_color};
                        padding: 15px; margin: 15px 0; border-radius: 4px;">
                <h3 style="margin: 0 0 5px 0; color: #1a1a2e;">
                    {s['ticker']} — {s['tier']}
                </h3>
                <table style="font-size: 13px; color: #333; width: 100%;">
                    <tr>
                        <td style="padding: 2px 10px 2px 0;"><b>Insider:</b></td>
                        <td>{s['insider_name']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 2px 10px 2px 0;"><b>Title:</b></td>
                        <td>{s['insider_title']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 2px 10px 2px 0;"><b>Date:</b></td>
                        <td>{s['transaction_date']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 2px 10px 2px 0;"><b>Purchase:</b></td>
                        <td>${s['total_value']:,.0f} ({s['shares']:,.0f} shares @ ${s['price_per_share']:.2f})</td>
                    </tr>
            """

            if s['si_error']:
                html += f"""
                    <tr>
                        <td colspan="2" style="color: #999; padding-top: 5px;">
                            ⚠️ SI data unavailable: {s['si_error']}
                        </td>
                    </tr>
                """
            else:
                html += f"""
                    <tr><td colspan="2" style="padding-top: 8px;"><b>Short Interest:</b></td></tr>
                    <tr>
                        <td style="padding: 2px 10px 2px 0;">Days to Cover:</td>
                        <td><b>{s['days_to_cover']:.1f}</b> {'✅' if s['days_to_cover'] > DTC_THRESHOLD else '—'}</td>
                    </tr>
                    <tr>
                        <td style="padding: 2px 10px 2px 0;">SI Change:</td>
                        <td><b>{s['si_change_pct']:+.1f}%</b>
                            {'🔥 SURGING' if s['si_change_pct'] > SI_SURGE_THRESHOLD
                             else '📈 INCREASING' if s['si_change_pct'] > SI_CHANGE_THRESHOLD
                             else '📉 DECREASING' if s['si_change_pct'] < -10
                             else '➡️ STABLE'}
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 2px 10px 2px 0;">Short % Float:</td>
                        <td>{s['short_pct_float']:.1f}%</td>
                    </tr>
                    <tr>
                        <td style="padding: 2px 10px 2px 0;">Shares Short:</td>
                        <td>{s['shares_short']:,} (prior: {s['shares_short_prior']:,})</td>
                    </tr>
                """

            html += """
                </table>
            """

            # ── Phase 4: Options Contamination Warning ──
            if contam.get('contaminated'):
                warning_html = contam.get('warning_html', '')
                if warning_html:
                    html += warning_html
                else:
                    max_dev = contam.get('max_deviation', 0)
                    n_anom = len(contam.get('anomalies', []))
                    html += f"""
                <div style="background:#fff5f5; border-left:4px solid #c53030;
                            padding:10px 14px; margin:8px 0; border-radius:0 6px 6px 0;
                            font-size:12px;">
                    <div style="font-weight:bold; color:#c53030; margin-bottom:3px;">
                        ⚠️ OPTIONS VOLUME CONTAMINATION
                    </div>
                    <div style="color:#555;">
                        {n_anom} unusual options event{'s' if n_anom != 1 else ''}
                        ({max_dev:.1f}x deviation) within ±20 trading days.
                    </div>
                    <div style="color:#888; margin-top:4px; font-size:11px;">
                        Phase 4: insider+options = -11.83% alpha vs +0.99% clean.
                        Do NOT use insider buying as confirmation — it amplifies negative signal.
                    </div>
                </div>
                    """

            html += """
            </div>
            """

    # Backtest reference
    html += """
    <div style="background: #f0f0f0; padding: 12px; margin-top: 20px; border-radius: 4px;
                font-size: 11px; color: #666;">
        <b>Backtested Performance (2020-2025):</b><br>
        🔴 Tier 1 (DTC>5 + SI Surging >25%): +11.55% avg 5d alpha, 75% WR (n=12)<br>
        🟠 Tier 2 (DTC>5 + SI Increasing >10%): +6.73% avg 5d alpha, 69.7% WR (n=142)<br>
        🟡 Tier 3 (DTC>5 only): +4.67% avg 5d alpha, 70.2% WR (n=527)<br>
        ⚪ Tier 4 (Tier2 insider buy, no SI): +3.66% avg 5d alpha, 65.7% WR (n=1803)<br>
        <br>
        <b>Phase 4 Options Filter (2021-2026):</b><br>
        ⚠️ Insider + options spike: -11.83% alpha at 20d, 16.7% WR<br>
        ⚠️ Call-heavy + insider: -13.86% at 20d, 5.9% WR (NUCLEAR)<br>
        ✅ Clean insider (no options): +0.99% at 20d<br>
        <i>Past performance does not guarantee future results.</i>
    </div>
    </body></html>
    """

    return subject, html, priority


def send_email(subject, html_body, priority="normal"):
    """Send HTML email using the Form 4 scanner's email config."""
    if EMAIL_CONFIG is None:
        print("ERROR: Could not import EMAIL_CONFIG from config.py")
        return False

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['recipient_email']

        if priority == "high":
            msg['X-Priority'] = '1'
            msg['Importance'] = 'high'

        msg.attach(MIMEText(html_body, 'html'))

        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'],
                          EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'],
                         EMAIL_CONFIG['sender_password'])
            server.send_message(msg)

        print(f"Email sent: {subject}")
        return True

    except Exception as e:
        print(f"ERROR sending email: {e}")
        traceback.print_exc()
        return False


# ---------------------------------------------------------------------------
# LOGGING: Save signals to a local log for tracking
# ---------------------------------------------------------------------------
def log_signals(signals):
    """Append today's signals to a CSV log file."""
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "cross_signal_log.csv")
    write_header = not os.path.exists(log_path)

    try:
        with open(log_path, 'a') as f:
            if write_header:
                f.write("scan_date,ticker,insider_name,insider_title,"
                        "transaction_date,total_value,days_to_cover,"
                        "si_change_pct,short_pct_float,tier,macro_regime,"
                        "options_contaminated,options_max_deviation\n")
            scan_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            for s in signals:
                macro = s.get('macro_label', 'UNKNOWN')
                contam = s.get('options_contamination', {})
                opt_flag = 'YES' if contam.get('contaminated') else 'NO'
                opt_dev = contam.get('max_deviation', 0)
                f.write(f"{scan_date},{s['ticker']},{s['insider_name']},"
                        f"{s['insider_title']},{s['transaction_date']},"
                        f"{s['total_value']:.0f},{s['days_to_cover']:.1f},"
                        f"{s['si_change_pct']:.1f},{s['short_pct_float']:.1f},"
                        f"\"{s['tier']}\",{macro},{opt_flag},{opt_dev:.1f}\n")
        print(f"Logged {len(signals)} signals to {log_path}")
    except Exception as e:
        print(f"Warning: Could not write log: {e}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():

    # Skip weekends - no new SEC filings
    if datetime.now(timezone.utc).weekday() >= 5:
        print("Weekend - skipping scan")
        return
        
    print("=" * 60)
    print("CROSS-SIGNAL SCANNER: Insider × Short Interest × Options Volume")
    print(f"Run time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Options contamination filter: {'ENABLED' if OPTIONS_CHECK_AVAILABLE else 'DISABLED'}")
    print("=" * 60)

    dry_run = '--dry-run' in sys.argv

    # Step 1: Get recent purchases from Form 4 database
    purchases = get_recent_purchases()
    if not purchases:
        print("No recent purchases found. Sending status email.")
        if not dry_run:
            subject, html, priority = build_email_html([], 0, 0)
            send_email(subject, html, priority)
        return

    # Step 2: Filter for Tier2 (C-Suite + $500K+)
    tier2 = filter_tier2(purchases)
    if not tier2:
        print("No Tier2 signals. Sending status email.")
        if not dry_run:
            subject, html, priority = build_email_html([], len(purchases), 0)
            send_email(subject, html, priority)
        return

    # Step 3: Fetch short interest data for Tier2 tickers
    tickers = [p['ticker'] for p in tier2]
    si_data = fetch_short_interest(tickers)

    # Step 3.5: Get macro regime from FRED data
    print("\nFetching macro regime...")
    macro_regime = get_macro_regime()

    # Step 4: Classify signals (with macro context)
    signals = classify_signals(tier2, si_data, macro_regime)

    # Step 4.5: Check options volume contamination (Phase 4)
    check_signals_contamination(signals)

    # Step 5: Display results
    print("\n" + "=" * 60)
    print(f"RESULTS  |  Macro: {macro_regime['label']}  |  "
          f"Options filter: {'ON' if OPTIONS_CHECK_AVAILABLE else 'OFF'}")
    print("=" * 60)
    for s in signals:
        contam = s.get('options_contamination', {})
        contam_tag = " ⚠️ OPT-CONTAMINATED" if contam.get('contaminated') else ""
        print(f"\n  {s['ticker']} — {s['tier']}{contam_tag}")
        print(f"    {s['insider_name']} ({s['insider_title']})")
        print(f"    ${s['total_value']:,.0f} on {s['transaction_date']}")
        if not s['si_error']:
            print(f"    DTC: {s['days_to_cover']:.1f} | "
                  f"SI Change: {s['si_change_pct']:+.1f}% | "
                  f"SI% Float: {s['short_pct_float']:.1f}%")
        if contam.get('contaminated'):
            print(f"    ⚠️  Options: {contam['max_deviation']:.1f}x deviation, "
                  f"{len(contam['anomalies'])} events")

    # Step 6: Log signals (now includes contamination columns)
    log_signals(signals)

    # Step 7: Send email
    if dry_run:
        print("\n[DRY RUN] Email not sent. Would have sent:")
        subject, html, _ = build_email_html(signals, len(purchases), len(tier2), macro_regime)
        print(f"  Subject: {subject}")
    else:
        subject, html, priority = build_email_html(signals, len(purchases), len(tier2), macro_regime)
        send_email(subject, html, priority)

    # Summary
    confirmed = sum(1 for s in signals if s['tier_num'] <= 2)
    contaminated_n = sum(1 for s in signals
                         if s.get('options_contamination', {}).get('contaminated'))
    print(f"\nDone. {len(signals)} Tier2 signals, {confirmed} with SI confirmation, "
          f"{contaminated_n} options-contaminated.")


if __name__ == '__main__':
    main()
