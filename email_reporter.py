"""
Email Reporter for Form 4 Scanner

Actionable format:
  Cluster buys — BUY action line, contamination warning at top of card
  Sells        — SHORT action line, tier as large header badge
  Both         — VIX banner, clean names, capped transaction lists
"""
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from config import EMAIL_CONFIG
try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False


# ── Helpers ──────────────────────────────────────────────────────

def _clean_name(name, ticker=None):
    """Strip CIK numbers, ticker duplicates, and state suffixes."""
    if not name:
        return 'Unknown'
    name = re.sub(r'\s*\(?\s*CIK[:\s#]*\d+\s*\)?\s*', '', name)
    if ticker:
        name = re.sub(rf'\s*\({re.escape(ticker)}\)\s*', '',
                       name, flags=re.IGNORECASE)
    name = re.sub(r'\s*/[A-Z]{2}/\s*$', '', name)
    return name.strip().rstrip('.') or 'Unknown'


def _next_trading_day(ref_date=None):
    """Next Mon-Fri after ref_date (datetime) or today."""
    dt = ref_date or datetime.now()
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    dt += timedelta(days=1)
    while dt.weekday() >= 5:
        dt += timedelta(days=1)
    return dt


def _add_trading_days_dt(ref_dt, n):
    """Add n trading days to a datetime object."""
    dt = ref_dt
    added = 0
    while added < n:
        dt += timedelta(days=1)
        if dt.weekday() < 5:
            added += 1
    return dt


def _fetch_vix():
    """Fetch current VIX from yfinance. Returns float or None."""
    if not _YF_AVAILABLE:
        return None
    try:
        ticker = yf.Ticker('^VIX')
        info = ticker.fast_info
        val = getattr(info, 'last_price', None)
        if val is None:
            hist = ticker.history(period='1d')
            if not hist.empty:
                val = float(hist['Close'].iloc[-1])
        return float(val) if val else None
    except Exception:
        return None


def _vix_banner(vix):
    """Return VIX warning HTML if VIX >= 25, else empty string."""
    if not isinstance(vix, (int, float)) or vix < 25:
        return ''
    return (f'<div style="background:#fff3cd;border-left:5px solid #ff9800;'
            f'padding:12px 15px;font-size:14px;font-weight:bold;'
            f'color:#856404">'
            f'VIX {vix:.1f} — Elevated volatility. '
            f'Consider half-sizing positions and wider stops.</div>')


def _fmt_value(val):
    """Format dollar value compactly."""
    if val >= 1_000_000:
        return f"${val / 1e6:.2f}M"
    return f"${val:,.0f}"


# ── EmailReporter Class ─────────────────────────────────────────

class EmailReporter:
    def __init__(self):
        self.config = EMAIL_CONFIG

    # ============================================================
    #  CLUSTER BUY ALERTS
    # ============================================================

    def send_cluster_alert(self, alerts, vix=None, dry_run=False):
        if vix is None:
            vix = _fetch_vix()
        if not alerts:
            return False

        tickers = [a.get('ticker', '?') for a in alerts]
        subject = f"Insider Buy Cluster: {', '.join(tickers[:5])}"
        if len(tickers) > 5:
            subject += f" +{len(tickers) - 5}"

        # Flag contaminated in subject
        n_contam = sum(1 for a in alerts
                       if a.get('options_contamination', {}).get(
                           'contaminated'))
        if n_contam:
            subject += f" ({n_contam} contaminated)"

        html_content = self._build_cluster_html(alerts, vix)
        text_content = self._build_cluster_text(alerts)

        if dry_run:
            print(f"DRY RUN - Subject: {subject}")
            print(text_content)
            return True
        return self._send_email(subject, html_content, text_content)

    # ============================================================
    #  SELL ALERTS
    # ============================================================

    def send_sell_alert(self, alerts, vix=None, dry_run=False):
        if vix is None:
            vix = _fetch_vix()
        if not alerts:
            return False

        # Subject: "Insider SELL: AAPL [Tier 1], MSFT [Tier 2]"
        parts = []
        for a in alerts[:5]:
            tag = a.get('tier_tag', '?')
            tier = 'Tier 1' if tag == 'S1' else 'Tier 2'
            parts.append(f"{a.get('ticker', '?')} [{tier}]")
        subject = f"Insider SELL: {', '.join(parts)}"
        if len(alerts) > 5:
            subject += f" +{len(alerts) - 5}"

        html_content = self._build_sell_html(alerts, vix)
        text_content = self._build_sell_text(alerts)

        if dry_run:
            print(f"DRY RUN - Subject: {subject}")
            print(text_content)
            return True
        return self._send_email(subject, html_content, text_content)

    # ============================================================
    #  STATUS REPORT (unchanged)
    # ============================================================

    def send_status_report(self, stats, message="", dry_run=False):
        subject = (f"Form 4 Scanner Status - "
                   f"{datetime.now().strftime('%Y-%m-%d')}")
        html_content = self._build_status_html(stats, message)
        text_content = self._build_status_text(stats, message)
        if dry_run:
            print(f"DRY RUN - Subject: {subject}")
            return True
        return self._send_email(subject, html_content, text_content)

    # ────────────────────────────────────────────────────────────
    #  CLUSTER HTML
    # ────────────────────────────────────────────────────────────

    def _build_cluster_html(self, alerts, vix=None):
        now = datetime.now()
        entry_dt = _next_trading_day(now)
        exit_dt = _add_trading_days_dt(entry_dt, 5)
        entry_str = entry_dt.strftime('%a %b %-d')
        exit_str = exit_dt.strftime('%a %b %-d')

        total_insiders = sum(a.get('unique_insiders', 0) for a in alerts)
        total_dollars = sum(a.get('total_purchased', 0) for a in alerts)

        html = (f'<html><body style="font-family:Arial,sans-serif;'
                f'max-width:700px;margin:auto;padding:0">'
                # Header
                f'<div style="background:linear-gradient(135deg,#276749,#38a169);'
                f'color:white;padding:20px;text-align:center">'
                f'<h1 style="margin:0;font-size:22px">'
                f'Insider Buying Clusters</h1>'
                f'<p style="margin:5px 0 0;font-size:16px;font-weight:bold">'
                f'{len(alerts)} stock{"s" if len(alerts) != 1 else ""} '
                f'| {total_insiders} insiders '
                f'| {_fmt_value(total_dollars)}</p>'
                f'<p style="margin:5px 0 0;font-size:12px;opacity:0.85">'
                f'{now.strftime("%B %d, %Y")}</p>'
                f'</div>')

        # VIX warning
        html += _vix_banner(vix)

        html += '<div style="padding:15px">'

        for i, alert in enumerate(alerts, 1):
            ticker = alert.get('ticker', 'N/A')
            company = _clean_name(alert.get('company_name', ''), ticker)
            unique_ins = alert.get('unique_insiders', 0)
            total_purch = alert.get('total_purchased', 0)
            first_dt = alert.get('first_purchase', 'N/A')
            last_dt = alert.get('last_purchase', 'N/A')
            transactions = alert.get('transactions', [])
            contam = alert.get('options_contamination', {})
            is_contaminated = contam.get('contaminated', False)

            # Card border color
            border_color = '#c53030' if is_contaminated else '#276749'

            html += (f'<div style="background:white;border:1px solid #e0e0e0;'
                     f'border-left:4px solid {border_color};border-radius:8px;'
                     f'margin-bottom:15px;overflow:hidden">')

            # ── Contamination warning at TOP ──
            if is_contaminated:
                warning_html = contam.get('warning_html', '')
                if warning_html:
                    html += warning_html
                else:
                    max_dev = contam.get('max_deviation', 0)
                    n_anom = len(contam.get('anomalies', []))
                    html += (
                        f'<div style="background:#fff5f5;padding:10px 15px;'
                        f'font-size:12px;border-bottom:1px solid #feb2b2">'
                        f'<span style="font-weight:bold;color:#c53030">'
                        f'OPTIONS CONTAMINATION</span> — '
                        f'{n_anom} unusual volume event'
                        f'{"s" if n_anom != 1 else ""} '
                        f'({max_dev:.1f}x deviation). '
                        f'<span style="color:#888">insider+options = '
                        f'-11.83% alpha at 20d vs +0.99% clean. '
                        f'Downgrade conviction.</span></div>')
            elif contam.get('error'):
                html += (
                    f'<div style="background:#f5f5f5;padding:6px 15px;'
                    f'font-size:11px;color:#999;'
                    f'border-bottom:1px solid #eee">'
                    f'Options check unavailable: {contam["error"]}</div>')

            # ── Card header ──
            html += (f'<div style="padding:15px">'
                     # Ticker + BUY badge
                     f'<div style="display:flex;justify-content:space-between;'
                     f'align-items:center">'
                     f'<div>'
                     f'<span style="font-size:22px;font-weight:bold">'
                     f'{ticker}</span>'
                     f'<span style="color:#666;font-size:13px;'
                     f'margin-left:8px">{company}</span>'
                     f'</div>'
                     f'<span style="background:#276749;color:white;'
                     f'padding:4px 10px;border-radius:4px;font-weight:bold;'
                     f'font-size:13px">BUY</span>'
                     f'</div>')

            # Stats row
            html += (f'<div style="color:#555;font-size:13px;margin-top:4px">'
                     f'{unique_ins} insider{"s" if unique_ins != 1 else ""}'
                     f' | {_fmt_value(total_purch)} purchased'
                     f' | {first_dt} to {last_dt}</div>')

            # ── Action line ──
            html += (f'<div style="background:#f0fff4;border-left:4px solid '
                     f'#276749;padding:8px 12px;margin-top:10px;'
                     f'font-size:13px;font-weight:600;color:#333">'
                     f'BUY at {entry_str} open &nbsp;|&nbsp; '
                     f'Hold 5 trading days &nbsp;|&nbsp; '
                     f'Exit {exit_str}'
                     f'</div>')

            # ── Transactions (capped at 5) ──
            if transactions:
                html += ('<table style="width:100%;border-collapse:collapse;'
                         'font-size:12px;margin-top:10px">'
                         '<tr style="color:#999;font-size:11px;'
                         'text-transform:uppercase">'
                         '<th style="text-align:left;padding:4px 0">'
                         'Name</th>'
                         '<th style="text-align:left;padding:4px 0">'
                         'Role</th>'
                         '<th style="text-align:left;padding:4px 0">'
                         'Date</th>'
                         '<th style="text-align:right;padding:4px 0">'
                         'Value</th></tr>')

                for txn in transactions[:5]:
                    name = txn.get('insider_name', 'Unknown')
                    role = txn.get('insider_title', '') or 'N/A'
                    if len(role) > 25:
                        role = role[:23] + '..'
                    date = txn.get('transaction_date', 'N/A')
                    val = txn.get('total_value', 0)

                    html += (f'<tr><td style="padding:4px 0">'
                             f'<b>{name}</b></td>'
                             f'<td style="padding:4px 0;color:#666">'
                             f'{role}</td>'
                             f'<td style="padding:4px 0;color:#888">'
                             f'{date}</td>'
                             f'<td style="padding:4px 0;text-align:right;'
                             f'font-weight:600;color:#276749">'
                             f'{_fmt_value(val)}</td></tr>')

                if len(transactions) > 5:
                    html += (f'<tr><td colspan="4" style="padding:4px 0;'
                             f'color:#999;font-size:11px">'
                             f'+ {len(transactions) - 5} more</td></tr>')
                html += '</table>'

            html += '</div></div>'  # close padding + card

        html += '</div>'  # close content padding

        # Footer
        html += ('<div style="padding:10px 15px;font-size:11px;color:#aaa;'
                 'border-top:1px solid #eee">'
                 'Backtest: +0.60% 5d alpha (clean, no options) | '
                 'Not financial advice.</div></body></html>')
        return html

    def _build_cluster_text(self, alerts):
        now = datetime.now()
        entry_dt = _next_trading_day(now)
        exit_dt = _add_trading_days_dt(entry_dt, 5)
        entry_str = entry_dt.strftime('%a %b %-d')
        exit_str = exit_dt.strftime('%a %b %-d')

        lines = [
            f"INSIDER BUY CLUSTERS — {len(alerts)} stocks",
            "=" * 50, "",
        ]

        for i, alert in enumerate(alerts, 1):
            ticker = alert.get('ticker', 'N/A')
            company = _clean_name(alert.get('company_name', ''), ticker)
            contam = alert.get('options_contamination', {})

            # Contamination warning first
            if contam.get('contaminated'):
                lines.append(
                    f"  !! OPTIONS CONTAMINATED — "
                    f"{len(contam.get('anomalies', []))} events, "
                    f"{contam.get('max_deviation', 0):.1f}x dev")

            lines.append(f"#{i}: {ticker} — {company}")
            lines.append(
                f"    {alert.get('unique_insiders', 0)} insiders | "
                f"{_fmt_value(alert.get('total_purchased', 0))} purchased | "
                f"{alert.get('first_purchase', '')} to "
                f"{alert.get('last_purchase', '')}")
            lines.append(
                f"    ACTION: BUY at {entry_str} open, "
                f"Hold 5 days, Exit {exit_str}")

            for txn in alert.get('transactions', [])[:5]:
                name = txn.get('insider_name', 'Unknown')
                role = txn.get('insider_title', '') or 'N/A'
                date = txn.get('transaction_date', 'N/A')
                val = txn.get('total_value', 0)
                lines.append(f"      {name} [{role}] {date} {_fmt_value(val)}")

            remaining = len(alert.get('transactions', [])) - 5
            if remaining > 0:
                lines.append(f"      + {remaining} more")
            lines.append("")

        return "\n".join(lines)

    # ────────────────────────────────────────────────────────────
    #  SELL HTML
    # ────────────────────────────────────────────────────────────

    def _build_sell_html(self, alerts, vix=None):
        now = datetime.now()
        entry_dt = _next_trading_day(now)
        exit_dt = _add_trading_days_dt(entry_dt, 5)
        entry_str = entry_dt.strftime('%a %b %-d')
        exit_str = exit_dt.strftime('%a %b %-d')

        s1_count = sum(1 for a in alerts if a['tier_tag'] == 'S1')
        s2_count = sum(1 for a in alerts if a['tier_tag'] == 'S2')

        html = (f'<html><body style="font-family:Arial,sans-serif;'
                f'max-width:700px;margin:auto;padding:0">'
                # Header
                f'<div style="background:linear-gradient(135deg,#9b2c2c,#c53030);'
                f'color:white;padding:20px;text-align:center">'
                f'<h1 style="margin:0;font-size:22px">'
                f'Insider SELL Signals</h1>'
                f'<p style="margin:5px 0 0;font-size:16px;font-weight:bold">'
                f'{len(alerts)} ticker{"s" if len(alerts) != 1 else ""}'
                f'{f" | {s1_count} Tier 1" if s1_count else ""}'
                f'{f" | {s2_count} Tier 2" if s2_count else ""}</p>'
                f'<p style="margin:5px 0 0;font-size:12px;opacity:0.85">'
                f'{now.strftime("%B %d, %Y")}</p>'
                f'</div>')

        # VIX warning
        html += _vix_banner(vix)

        html += '<div style="padding:15px">'

        for alert in alerts:
            ticker = alert.get('ticker', 'N/A')
            company = _clean_name(alert.get('company_name', ''), ticker)
            tier_tag = alert.get('tier_tag', 'S2')
            tier_info = alert.get('tier_info', {})
            sells = alert.get('sells', [])
            total_val = alert.get('total_value', 0)
            num_sellers = alert.get('num_sellers', 0)

            # Tier styling
            if tier_tag == 'S1':
                tier_bg = '#c53030'
                tier_label = 'TIER 1'
                tier_sub = tier_info.get('alpha', '-2.54% avg 5d')
            else:
                tier_bg = '#dd6b20'
                tier_label = 'TIER 2'
                tier_sub = tier_info.get('alpha', '-0.50% to -0.86% 5d')

            html += (f'<div style="background:white;border:1px solid #e0e0e0;'
                     f'border-radius:8px;margin-bottom:15px;overflow:hidden">')

            # ── Large tier badge header ──
            html += (f'<div style="background:{tier_bg};color:white;'
                     f'padding:15px 20px">'
                     f'<div style="font-size:26px;font-weight:bold">'
                     f'{tier_label}</div>'
                     f'<div style="font-size:20px;margin-top:4px">'
                     f'{ticker}'
                     f'<span style="font-size:14px;opacity:0.85;'
                     f'margin-left:8px">{company}</span></div>'
                     f'<div style="font-size:12px;opacity:0.8;'
                     f'margin-top:2px">{tier_sub}</div>'
                     f'</div>')

            # Card body
            html += '<div style="padding:15px">'

            # Stats row
            html += (f'<div style="color:#555;font-size:13px">'
                     f'{num_sellers} seller{"s" if num_sellers != 1 else ""}'
                     f' | {_fmt_value(total_val)} total sold'
                     f' | {len(sells)} transaction'
                     f'{"s" if len(sells) != 1 else ""}</div>')

            # ── Action line ──
            html += (f'<div style="background:#fff0f0;border-left:4px solid '
                     f'{tier_bg};padding:8px 12px;margin-top:10px;'
                     f'font-size:13px;font-weight:600;color:#333">'
                     f'SHORT at {entry_str} open &nbsp;|&nbsp; '
                     f'Hold 5 trading days &nbsp;|&nbsp; '
                     f'Exit {exit_str}'
                     f'</div>')

            # SI note
            html += ('<div style="font-size:11px;color:#888;margin-top:6px">'
                     'LOW short interest amplifies signal. '
                     'Check cross_signal_scanner for SI data.</div>')

            # ── Transactions (capped at 5) ──
            if sells:
                html += ('<table style="width:100%;border-collapse:collapse;'
                         'font-size:12px;margin-top:10px">'
                         '<tr style="color:#999;font-size:11px;'
                         'text-transform:uppercase">'
                         '<th style="text-align:left;padding:4px 0">'
                         'Name</th>'
                         '<th style="text-align:left;padding:4px 0">'
                         'Role</th>'
                         '<th style="text-align:left;padding:4px 0">'
                         'Date</th>'
                         '<th style="text-align:right;padding:4px 0">'
                         'Value</th></tr>')

                for sell in sells[:5]:
                    name = sell.get('insider_name', 'Unknown')
                    is_off = sell.get('is_officer', 0)
                    is_dir = sell.get('is_director', 0)
                    if is_off and is_dir:
                        role = 'Off+Dir'
                        role_color = '#c53030'
                    elif is_off:
                        role = 'Officer'
                        role_color = '#c05621'
                    elif is_dir:
                        role = 'Director'
                        role_color = '#6b46c1'
                    else:
                        role = 'Other'
                        role_color = '#666'

                    date = sell.get('transaction_date', 'N/A')
                    val = sell.get('total_value', 0)

                    html += (
                        f'<tr><td style="padding:4px 0">'
                        f'<b>{name}</b></td>'
                        f'<td style="padding:4px 0;color:{role_color};'
                        f'font-weight:600;font-size:11px">{role}</td>'
                        f'<td style="padding:4px 0;color:#888">{date}</td>'
                        f'<td style="padding:4px 0;text-align:right;'
                        f'font-weight:600;color:#c53030">'
                        f'{_fmt_value(val)}</td></tr>')

                if len(sells) > 5:
                    html += (f'<tr><td colspan="4" style="padding:4px 0;'
                             f'color:#999;font-size:11px">'
                             f'+ {len(sells) - 5} more</td></tr>')
                html += '</table>'

            html += '</div></div>'  # close body + card

        html += '</div>'  # close content

        # Footer
        html += ('<div style="padding:10px 15px;font-size:11px;color:#aaa;'
                 'border-top:1px solid #eee">'
                 'Backtest: Tier 1 -2.54% 5d alpha (Off+Dir $250K-$5M) | '
                 'Not financial advice.</div></body></html>')
        return html

    def _build_sell_text(self, alerts):
        now = datetime.now()
        entry_dt = _next_trading_day(now)
        exit_dt = _add_trading_days_dt(entry_dt, 5)
        entry_str = entry_dt.strftime('%a %b %-d')
        exit_str = exit_dt.strftime('%a %b %-d')

        lines = [
            f"INSIDER SELL SIGNALS — {len(alerts)} tickers",
            "=" * 50, "",
        ]

        for alert in alerts:
            ticker = alert.get('ticker', 'N/A')
            company = _clean_name(alert.get('company_name', ''), ticker)
            tier_tag = alert.get('tier_tag', '?')
            tier_info = alert.get('tier_info', {})
            total_val = alert.get('total_value', 0)
            num_sellers = alert.get('num_sellers', 0)
            sells = alert.get('sells', [])

            tier_label = 'TIER 1' if tier_tag == 'S1' else 'TIER 2'
            lines.append(f"[{tier_label}] {ticker} — {company}")
            lines.append(
                f"    {num_sellers} sellers | "
                f"{_fmt_value(total_val)} total sold")
            lines.append(
                f"    ACTION: SHORT at {entry_str} open, "
                f"Hold 5 days, Exit {exit_str}")

            for sell in sells[:5]:
                name = sell.get('insider_name', 'Unknown')
                is_off = sell.get('is_officer', 0)
                is_dir = sell.get('is_director', 0)
                role = ('Off+Dir' if (is_off and is_dir) else
                        'Officer' if is_off else
                        'Director' if is_dir else 'Other')
                date = sell.get('transaction_date', 'N/A')
                val = sell.get('total_value', 0)
                lines.append(
                    f"      {name} [{role}] {date} {_fmt_value(val)}")

            remaining = len(sells) - 5
            if remaining > 0:
                lines.append(f"      + {remaining} more")
            lines.append("")

        return "\n".join(lines)

    # ────────────────────────────────────────────────────────────
    #  STATUS REPORT (unchanged)
    # ────────────────────────────────────────────────────────────

    def _build_status_html(self, stats, message=""):
        now = datetime.now()
        total_txns = stats.get('total_transactions', 0)
        purchases = stats.get('total_purchases', 0)
        sells = stats.get('total_sells', 0)
        unique_tickers = stats.get('unique_companies', 0)

        html = f'''
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 700px; margin: 0 auto; padding: 0; }}
                .header {{ background: linear-gradient(135deg, #1a365d 0%, #2c5282 100%); color: white; padding: 25px; border-radius: 8px 8px 0 0; }}
                .header h1 {{ margin: 0; font-size: 24px; }}
                .header p {{ margin: 8px 0 0 0; opacity: 0.9; font-size: 14px; }}
                .content {{ padding: 20px; background: #f8f9fa; }}
                .summary-grid {{ display: flex; gap: 15px; margin-bottom: 20px; flex-wrap: wrap; }}
                .stat-box {{ background: white; border-radius: 8px; padding: 20px; text-align: center; flex: 1; min-width: 120px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
                .stat-box .number {{ font-size: 32px; font-weight: bold; color: #1a365d; }}
                .stat-box .label {{ font-size: 12px; color: #718096; text-transform: uppercase; margin-top: 5px; }}
                .stat-box.highlight .number {{ color: #38a169; }}
                .stat-box.sell .number {{ color: #c53030; }}
                .info-table {{ background: white; border-radius: 8px; padding: 15px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
                .info-table table {{ width: 100%; border-collapse: collapse; }}
                .info-table td {{ padding: 10px; border-bottom: 1px solid #e2e8f0; }}
                .info-table tr:last-child td {{ border-bottom: none; }}
                .info-table td:first-child {{ color: #718096; width: 40%; }}
                .info-table td:last-child {{ font-weight: 500; }}
                .message-box {{ background: #ebf8ff; border-left: 4px solid #3182ce; padding: 15px; border-radius: 0 8px 8px 0; margin-bottom: 20px; }}
                .footer {{ padding: 20px; text-align: center; font-size: 12px; color: #a0aec0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>&#128203; Form 4 Scanner Status</h1>
                <p>{now.strftime('%A, %B %d, %Y')}</p>
            </div>

            <div class="content">
                <div class="summary-grid">
                    <div class="stat-box">
                        <div class="number">{total_txns:,}</div>
                        <div class="label">Total Transactions</div>
                    </div>
                    <div class="stat-box highlight">
                        <div class="number">{purchases}</div>
                        <div class="label">Purchases</div>
                    </div>
                    <div class="stat-box sell">
                        <div class="number">{sells}</div>
                        <div class="label">Sells</div>
                    </div>
                    <div class="stat-box">
                        <div class="number">{unique_tickers}</div>
                        <div class="label">Unique Companies</div>
                    </div>
                </div>
        '''

        if message:
            html += f'''
                <div class="message-box">
                    {message}
                </div>
            '''

        html += f'''
                <div class="info-table">
                    <table>
                        <tr>
                            <td>Scanner Run Time</td>
                            <td>{now.strftime('%Y-%m-%d %H:%M:%S')} UTC</td>
                        </tr>
                    </table>
                </div>
            </div>

            <div class="footer">
                <p>Form 4 Insider Trading Scanner | Scheduled Task Running at 22:00 UTC</p>
            </div>
        </body>
        </html>
        '''
        return html

    def _build_status_text(self, stats, message=""):
        lines = [
            "FORM 4 SCANNER STATUS",
            "=" * 40,
            f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "",
            f"Total Transactions: {stats.get('total_transactions', 0):,}",
            f"Purchases: {stats.get('total_purchases', 0)}",
            f"Sells: {stats.get('total_sells', 0)}",
            f"Unique Companies: {stats.get('unique_companies', 0)}",
        ]
        if message:
            lines.extend(["", message])
        return "\n".join(lines)

    # ────────────────────────────────────────────────────────────
    #  SEND
    # ────────────────────────────────────────────────────────────

    def _send_email(self, subject, html_content, text_content):
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.config['sender_email']
            msg['To'] = self.config['recipient_email']
            msg.attach(MIMEText(text_content, 'plain'))
            msg.attach(MIMEText(html_content, 'html'))
            with smtplib.SMTP(self.config['smtp_server'],
                              self.config['smtp_port']) as server:
                server.starttls()
                server.login(self.config['sender_email'],
                             self.config['sender_password'])
                server.send_message(msg)
            print(f"Email sent to {self.config['recipient_email']}")
            return True
        except Exception as e:
            print(f"Failed to send email: {e}")
            return False
