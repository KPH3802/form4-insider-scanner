"""
Email Reporter for Form 4 Scanner

Phase 4 Update: Cluster alert emails now include options volume
contamination warnings when detected. Warning HTML is generated
by options_volume_check.py and attached to each alert dict.
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from config import EMAIL_CONFIG


class EmailReporter:
    def __init__(self):
        self.config = EMAIL_CONFIG
    
    def send_cluster_alert(self, alerts, dry_run=False):
        if not alerts:
            return False
        subject = f"Insider Buying Clusters: {len(alerts)} stocks detected"
        
        # Check if any alerts are contaminated — upgrade subject line
        contaminated = [a for a in alerts
                        if a.get('options_contamination', {}).get('contaminated')]
        if contaminated:
            subject = (f"Insider Buying Clusters: {len(alerts)} stocks "
                       f"({len(contaminated)} ⚠️ options-contaminated)")
        
        html_content = self._build_cluster_html(alerts)
        text_content = self._build_cluster_text(alerts)
        if dry_run:
            print(f"DRY RUN - Subject: {subject}")
            print(text_content)
            return True
        return self._send_email(subject, html_content, text_content)
    
    # ============================================================
    #  SELL ALERT EMAIL (backtest-validated)
    # ============================================================
    
    def send_sell_alert(self, alerts, dry_run=False):
        """
        Send email for insider sell signals.
        
        alerts: list of ticker-grouped sell signals from SellSignalAnalyzer.
        Each has tier_tag, tier_info, sells[], total_value, num_sellers.
        """
        if not alerts:
            return False
        
        # Count by tier
        s1_count = sum(1 for a in alerts if a['tier_tag'] == 'S1')
        s2_count = sum(1 for a in alerts if a['tier_tag'] == 'S2')
        
        tier_parts = []
        if s1_count:
            tier_parts.append(f"{s1_count} TIER 1")
        if s2_count:
            tier_parts.append(f"{s2_count} TIER 2")
        tier_str = ', '.join(tier_parts) if tier_parts else f"{len(alerts)} signals"
        
        subject = f"⚠️ Insider SELL Signals: {tier_str}"
        html_content = self._build_sell_html(alerts)
        text_content = self._build_sell_text(alerts)
        
        if dry_run:
            print(f"DRY RUN - Subject: {subject}")
            print(text_content)
            return True
        
        return self._send_email(subject, html_content, text_content)
    
    def _build_sell_html(self, alerts):
        """Build styled HTML for sell signal alerts."""
        now = datetime.now()
        
        total_value = sum(a.get('total_value', 0) for a in alerts)
        s1_count = sum(1 for a in alerts if a['tier_tag'] == 'S1')
        s2_count = sum(1 for a in alerts if a['tier_tag'] == 'S2')
        
        html = f'''
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 700px; margin: 0 auto; padding: 0; }}
                .header {{ background: linear-gradient(135deg, #9b2c2c 0%, #c53030 100%); color: white; padding: 25px; border-radius: 8px 8px 0 0; }}
                .header h1 {{ margin: 0; font-size: 24px; }}
                .header p {{ margin: 8px 0 0 0; opacity: 0.9; font-size: 14px; }}
                .content {{ padding: 20px; background: #f8f9fa; }}
                .summary-grid {{ display: flex; gap: 15px; margin-bottom: 20px; flex-wrap: wrap; }}
                .stat-box {{ background: white; border-radius: 8px; padding: 20px; text-align: center; flex: 1; min-width: 120px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
                .stat-box .number {{ font-size: 28px; font-weight: bold; color: #c53030; }}
                .stat-box .label {{ font-size: 11px; color: #718096; text-transform: uppercase; margin-top: 5px; }}
                .backtest-box {{ background: #fff5f5; border: 1px solid #feb2b2; border-radius: 8px; padding: 15px; margin-bottom: 20px; font-size: 13px; }}
                .backtest-box strong {{ color: #c53030; }}
                .sell-card {{ background: white; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden; }}
                .sell-header {{ padding: 15px 20px; color: white; }}
                .sell-header.s1 {{ background: #c53030; }}
                .sell-header.s2 {{ background: #dd6b20; }}
                .sell-header.watch {{ background: #718096; }}
                .sell-header .ticker {{ font-size: 24px; font-weight: bold; }}
                .sell-header .company {{ font-size: 14px; opacity: 0.9; margin-top: 3px; }}
                .sell-header .tier-badge {{ display: inline-block; background: rgba(255,255,255,0.2); padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: bold; margin-top: 5px; }}
                .sell-body {{ padding: 20px; }}
                .sell-stats {{ display: flex; gap: 20px; margin-bottom: 15px; padding-bottom: 15px; border-bottom: 1px solid #e2e8f0; flex-wrap: wrap; }}
                .sell-stat .value {{ font-size: 18px; font-weight: bold; color: #c53030; }}
                .sell-stat .label {{ font-size: 11px; color: #718096; text-transform: uppercase; }}
                .txn-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
                .txn-table th {{ text-align: left; padding: 8px; border-bottom: 2px solid #e2e8f0; color: #718096; font-size: 11px; text-transform: uppercase; }}
                .txn-table td {{ padding: 8px; border-bottom: 1px solid #e2e8f0; }}
                .txn-table tr:last-child td {{ border-bottom: none; }}
                .role-badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: bold; }}
                .role-badge.dual {{ background: #fed7d7; color: #c53030; }}
                .role-badge.officer {{ background: #feebc8; color: #c05621; }}
                .role-badge.director {{ background: #e9d8fd; color: #6b46c1; }}
                .si-note {{ background: #ebf8ff; border-left: 4px solid #3182ce; padding: 12px; border-radius: 0 8px 8px 0; margin-top: 15px; font-size: 12px; }}
                .footer {{ padding: 20px; text-align: center; font-size: 12px; color: #a0aec0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>&#9888;&#65039; Insider SELL Signals Detected</h1>
                <p>{len(alerts)} ticker{'s' if len(alerts) != 1 else ''} with backtest-validated sell signals &bull; {now.strftime('%B %d, %Y')}</p>
            </div>
            
            <div class="content">
                <div class="summary-grid">
                    <div class="stat-box">
                        <div class="number">{s1_count}</div>
                        <div class="label">Tier 1 (Strongest)</div>
                    </div>
                    <div class="stat-box">
                        <div class="number">{s2_count}</div>
                        <div class="label">Tier 2</div>
                    </div>
                    <div class="stat-box">
                        <div class="number">${total_value/1e6:.1f}M</div>
                        <div class="label">Total Sold</div>
                    </div>
                </div>
                
                <div class="backtest-box">
                    <strong>Backtest Context (432K trades, 2020-2025):</strong><br>
                    Tier 1 (Officer+Director, $250K-$5M): <strong>-2.54% avg 5-day alpha</strong>, p&lt;0.0001<br>
                    Tier 2 (Officer or Director, $250K-$5M): -0.50% to -0.86% avg 5-day alpha<br>
                    <em>LOW short interest amplifies signal. Check cross_signal_scanner for SI data.</em>
                </div>
        '''
        
        for i, alert in enumerate(alerts, 1):
            ticker = alert.get('ticker', 'N/A')
            company = alert.get('company_name', 'Unknown')
            tier_tag = alert.get('tier_tag', 'SELL_WATCH')
            tier_info = alert.get('tier_info', {})
            sells = alert.get('sells', [])
            total_val = alert.get('total_value', 0)
            num_sellers = alert.get('num_sellers', 0)
            
            # Header color class
            header_class = 's1' if tier_tag == 'S1' else ('s2' if tier_tag == 'S2' else 'watch')
            
            # Format total
            if total_val >= 1_000_000:
                val_display = f"${total_val/1e6:.2f}M"
            else:
                val_display = f"${total_val:,.0f}"
            
            html += f'''
                <div class="sell-card">
                    <div class="sell-header {header_class}">
                        <div class="ticker">#{i}: {ticker}</div>
                        <div class="company">{company}</div>
                        <div class="tier-badge">{tier_info.get('name', tier_tag)} &mdash; {tier_info.get('alpha', '')}</div>
                    </div>
                    <div class="sell-body">
                        <div class="sell-stats">
                            <div class="sell-stat">
                                <div class="value">{num_sellers}</div>
                                <div class="label">Seller{'s' if num_sellers != 1 else ''}</div>
                            </div>
                            <div class="sell-stat">
                                <div class="value">{val_display}</div>
                                <div class="label">Total Sold</div>
                            </div>
                            <div class="sell-stat">
                                <div class="value">{len(sells)}</div>
                                <div class="label">Transaction{'s' if len(sells) != 1 else ''}</div>
                            </div>
                        </div>
                        
                        <table class="txn-table">
                            <tr>
                                <th>Insider</th>
                                <th>Role</th>
                                <th>Date</th>
                                <th style="text-align:right">Value</th>
                            </tr>
            '''
            
            for sell in sells[:8]:
                name = sell.get('insider_name', 'Unknown')
                title = sell.get('insider_title', '') or ''
                date = sell.get('transaction_date', 'N/A')
                val = sell.get('total_value', 0)
                is_off = sell.get('is_officer', 0)
                is_dir = sell.get('is_director', 0)
                
                # Role badge
                if is_off and is_dir:
                    role_html = '<span class="role-badge dual">Off+Dir</span>'
                elif is_off:
                    role_html = '<span class="role-badge officer">Officer</span>'
                elif is_dir:
                    role_html = '<span class="role-badge director">Director</span>'
                else:
                    role_html = '<span class="role-badge">Other</span>'
                
                # Truncate title
                if len(title) > 30:
                    title = title[:28] + '...'
                
                if val >= 1_000_000:
                    val_str = f"${val/1e6:.2f}M"
                else:
                    val_str = f"${val:,.0f}"
                
                html += f'''
                            <tr>
                                <td><strong>{name}</strong><br><span style="color:#718096;font-size:12px">{title}</span></td>
                                <td>{role_html}</td>
                                <td>{date}</td>
                                <td style="text-align:right;font-weight:600;color:#c53030">{val_str}</td>
                            </tr>
                '''
            
            if len(sells) > 8:
                html += f'<tr><td colspan="4" style="color:#718096;font-size:12px">+ {len(sells) - 8} more transactions</td></tr>'
            
            html += '''
                        </table>
                        
                        <div class="si-note">
                            <strong>&#128270; Short Interest:</strong> Check cross_signal_scanner for SI overlay. 
                            LOW SI amplifies sell signal (-1.64% 5d vs -0.81% with HIGH SI).
                        </div>
                    </div>
                </div>
            '''
        
        html += f'''
            </div>
            
            <div class="footer">
                <p>Form 4 Insider Sell Scanner | Backtest: -2.54% 5d alpha (Officer+Director, $250K-$5M)</p>
                <p>Generated: {now.strftime('%Y-%m-%d %H:%M:%S')} | This is research output, not financial advice.</p>
            </div>
        </body>
        </html>
        '''
        return html
    
    def _build_sell_text(self, alerts):
        """Build plain text sell alert."""
        lines = [
            f"INSIDER SELL SIGNALS - {len(alerts)} tickers",
            "=" * 55,
            "",
            "Backtest: Officer+Director $250K-$5M = -2.54% avg 5d alpha",
            "LOW SI amplifies signal. Check cross_signal_scanner for SI.",
            "",
        ]
        
        for i, alert in enumerate(alerts, 1):
            ticker = alert.get('ticker', 'N/A')
            company = alert.get('company_name', 'Unknown')
            tier_tag = alert.get('tier_tag', '?')
            tier_info = alert.get('tier_info', {})
            total_val = alert.get('total_value', 0)
            num_sellers = alert.get('num_sellers', 0)
            sells = alert.get('sells', [])
            
            lines.append(f"#{i}: {ticker} - {company}")
            lines.append(f"    Tier: {tier_info.get('name', tier_tag)}")
            lines.append(f"    Sellers: {num_sellers}, Total Sold: ${total_val:,.0f}")
            lines.append(f"    Transactions:")
            
            for sell in sells[:8]:
                name = sell.get('insider_name', 'Unknown')
                title = sell.get('insider_title', '') or ''
                date = sell.get('transaction_date', 'N/A')
                val = sell.get('total_value', 0)
                is_off = sell.get('is_officer', 0)
                is_dir = sell.get('is_director', 0)
                
                role = 'Off+Dir' if (is_off and is_dir) else ('Officer' if is_off else ('Director' if is_dir else 'Other'))
                lines.append(f"      {name} [{role}] {date} ${val:,.0f}")
            
            if len(sells) > 8:
                lines.append(f"      + {len(sells) - 8} more")
            lines.append("")
        
        return "\n".join(lines)
    
    # ============================================================
    #  CLUSTER ALERT (with Phase 4 contamination warnings)
    # ============================================================
    
    def send_status_report(self, stats, message="", dry_run=False):
        subject = f"Form 4 Scanner Status - {datetime.now().strftime('%Y-%m-%d')}"
        html_content = self._build_status_html(stats, message)
        text_content = self._build_status_text(stats, message)
        if dry_run:
            print(f"DRY RUN - Subject: {subject}")
            return True
        return self._send_email(subject, html_content, text_content)
    
    def _build_status_html(self, stats, message=""):
        """Build styled HTML for status report."""
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
        """Build plain text status report."""
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
    
    def _build_cluster_html(self, alerts):
        """
        Build styled HTML for cluster alerts.
        
        Phase 4 Update: Each alert may contain an 'options_contamination'
        dict. If present and contaminated, a warning banner is rendered
        between the cluster stats and the transaction list.
        """
        now = datetime.now()
        
        # Calculate totals
        total_insiders = sum(a.get('unique_insiders', 0) for a in alerts)
        total_dollars = sum(a.get('total_purchased', 0) for a in alerts)
        contaminated_count = sum(
            1 for a in alerts
            if a.get('options_contamination', {}).get('contaminated')
        )
        
        html = f'''
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 700px; margin: 0 auto; padding: 0; }}
                .header {{ background: linear-gradient(135deg, #276749 0%, #38a169 100%); color: white; padding: 25px; border-radius: 8px 8px 0 0; }}
                .header h1 {{ margin: 0; font-size: 24px; }}
                .header p {{ margin: 8px 0 0 0; opacity: 0.9; font-size: 14px; }}
                .content {{ padding: 20px; background: #f8f9fa; }}
                .summary-grid {{ display: flex; gap: 15px; margin-bottom: 25px; }}
                .stat-box {{ background: white; border-radius: 8px; padding: 20px; text-align: center; flex: 1; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
                .stat-box .number {{ font-size: 32px; font-weight: bold; color: #276749; }}
                .stat-box .label {{ font-size: 12px; color: #718096; text-transform: uppercase; margin-top: 5px; }}
                .stat-box.warn .number {{ color: #c53030; }}
                .cluster-card {{ background: white; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden; }}
                .cluster-header {{ background: #276749; color: white; padding: 15px 20px; }}
                .cluster-header.contaminated {{ background: linear-gradient(135deg, #276749 0%, #744210 100%); }}
                .cluster-header .ticker {{ font-size: 24px; font-weight: bold; }}
                .cluster-header .company {{ font-size: 14px; opacity: 0.9; margin-top: 3px; }}
                .cluster-body {{ padding: 20px; }}
                .cluster-stats {{ display: flex; gap: 20px; margin-bottom: 15px; padding-bottom: 15px; border-bottom: 1px solid #e2e8f0; }}
                .cluster-stat {{ }}
                .cluster-stat .value {{ font-size: 20px; font-weight: bold; color: #276749; }}
                .cluster-stat .label {{ font-size: 11px; color: #718096; text-transform: uppercase; }}
                .transactions-title {{ font-weight: bold; color: #2d3748; margin-bottom: 10px; font-size: 14px; }}
                .transaction {{ padding: 10px; background: #f7fafc; border-radius: 6px; margin-bottom: 8px; }}
                .transaction:last-child {{ margin-bottom: 0; }}
                .txn-name {{ font-weight: 600; color: #2d3748; }}
                .txn-title {{ color: #718096; font-size: 13px; }}
                .txn-details {{ display: flex; justify-content: space-between; margin-top: 5px; font-size: 13px; }}
                .txn-date {{ color: #718096; }}
                .txn-value {{ font-weight: 600; color: #276749; }}
                .footer {{ padding: 20px; text-align: center; font-size: 12px; color: #a0aec0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>&#128276; Insider Buying Clusters Detected</h1>
                <p>{len(alerts)} stock{'s' if len(alerts) != 1 else ''} with multiple insiders buying &bull; {now.strftime('%B %d, %Y')}</p>
            </div>
            
            <div class="content">
                <div class="summary-grid">
                    <div class="stat-box">
                        <div class="number">{len(alerts)}</div>
                        <div class="label">Clusters</div>
                    </div>
                    <div class="stat-box">
                        <div class="number">{total_insiders}</div>
                        <div class="label">Total Insiders</div>
                    </div>
                    <div class="stat-box">
                        <div class="number">${total_dollars/1000000:.1f}M</div>
                        <div class="label">Total Purchased</div>
                    </div>
        '''
        
        # Show contamination count if any found
        if contaminated_count > 0:
            html += f'''
                    <div class="stat-box warn">
                        <div class="number">{contaminated_count}</div>
                        <div class="label">⚠️ Options Contaminated</div>
                    </div>
            '''
        
        html += '''
                </div>
        '''
        
        for i, alert in enumerate(alerts, 1):
            ticker = alert.get('ticker', 'N/A')
            company = alert.get('company_name', 'Unknown Company')
            unique_insiders = alert.get('unique_insiders', 0)
            total_purchased = alert.get('total_purchased', 0)
            first_date = alert.get('first_purchase', 'N/A')
            last_date = alert.get('last_purchase', 'N/A')
            transactions = alert.get('transactions', [])
            contam = alert.get('options_contamination', {})
            
            # Format total purchased
            if total_purchased >= 1000000:
                purchased_display = f"${total_purchased/1000000:.2f}M"
            else:
                purchased_display = f"${total_purchased:,.0f}"
            
            # Header class — orange gradient if contaminated
            header_class = 'contaminated' if contam.get('contaminated') else ''
            
            html += f'''
                <div class="cluster-card">
                    <div class="cluster-header {header_class}">
                        <div class="ticker">#{i}: {ticker}</div>
                        <div class="company">{company}</div>
                    </div>
                    <div class="cluster-body">
                        <div class="cluster-stats">
                            <div class="cluster-stat">
                                <div class="value">{unique_insiders}</div>
                                <div class="label">Unique Insiders</div>
                            </div>
                            <div class="cluster-stat">
                                <div class="value">{purchased_display}</div>
                                <div class="label">Total Purchased</div>
                            </div>
                            <div class="cluster-stat">
                                <div class="value">{first_date} to {last_date}</div>
                                <div class="label">Date Range</div>
                            </div>
                        </div>
            '''
            
            # ── Phase 4: Options Volume Contamination Warning ──
            if contam.get('contaminated'):
                warning_html = contam.get('warning_html', '')
                if warning_html:
                    html += warning_html
                else:
                    # Fallback if warning_html wasn't generated
                    max_dev = contam.get('max_deviation', 0)
                    n_anom = len(contam.get('anomalies', []))
                    html += f'''
                        <div style="background:#fff5f5; border-left:4px solid #c53030;
                                    padding:10px 14px; margin:8px 0; border-radius:0 6px 6px 0;
                                    font-size:12px;">
                            <div style="font-weight:bold; color:#c53030; margin-bottom:3px;">
                                ⚠️ OPTIONS VOLUME CONTAMINATION
                            </div>
                            <div style="color:#555;">
                                {n_anom} unusual options volume event{'s' if n_anom != 1 else ''}
                                detected within ±20 trading days ({max_dev:.1f}x deviation).
                            </div>
                            <div style="color:#888; margin-top:4px; font-size:11px;">
                                Phase 4: insider+options = -11.83% alpha at 20d vs +0.99% clean.
                                Consider downgrading conviction.
                            </div>
                        </div>
                    '''
            elif contam.get('error'):
                # Note that check was attempted but failed
                html += f'''
                    <div style="background:#f5f5f5; border-left:4px solid #bbb;
                                padding:6px 12px; margin:6px 0; border-radius:0 4px 4px 0;
                                font-size:11px; color:#999;">
                        ⚪ Options volume check unavailable: {contam['error']}
                    </div>
                '''
            # If no contamination data at all, show nothing (clean)
            
            html += '''
                        <div class="transactions-title">Transactions:</div>
            '''
            
            for txn in transactions[:5]:
                name = txn.get('insider_name', 'Unknown')
                title = txn.get('insider_title', 'N/A')
                if title is None:
                    title = 'N/A'
                date = txn.get('transaction_date', 'N/A')
                value = txn.get('total_value', 0)
                
                html += f'''
                        <div class="transaction">
                            <div class="txn-name">{name} <span class="txn-title">({title})</span></div>
                            <div class="txn-details">
                                <span class="txn-date">{date}</span>
                                <span class="txn-value">${value:,.0f}</span>
                            </div>
                        </div>
                '''
            
            if len(transactions) > 5:
                html += f'<p style="color: #718096; font-size: 13px; margin-top: 10px;">+ {len(transactions) - 5} more transactions</p>'
            
            html += '''
                    </div>
                </div>
            '''
        
        html += f'''
            </div>
            
            <div class="footer">
                <p>Form 4 Insider Buying Scanner | Generated: {now.strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p style="font-size:11px; color:#bbb;">Options contamination filter powered by Phase 4 cross-signal backtest (2021-2026)</p>
            </div>
        </body>
        </html>
        '''
        return html
    
    def _build_cluster_text(self, alerts):
        """Build plain text cluster alert with contamination warnings."""
        lines = [f"INSIDER BUYING CLUSTERS - {len(alerts)} stocks", "=" * 50]
        for i, alert in enumerate(alerts, 1):
            contam = alert.get('options_contamination', {})
            contam_tag = " ⚠️ OPTIONS CONTAMINATED" if contam.get('contaminated') else ""
            
            lines.append(f"\n#{i}: {alert['ticker']} - {alert['company_name']}{contam_tag}")
            lines.append(f"Unique Insiders: {alert['unique_insiders']}")
            lines.append(f"Total Purchased: ${alert['total_purchased']:,.0f}")
            lines.append(f"Date Range: {alert.get('first_purchase', 'N/A')} to {alert.get('last_purchase', 'N/A')}")
            
            if contam.get('contaminated'):
                warning = contam.get('warning_text', '')
                if warning:
                    lines.append(warning)
                else:
                    lines.append(f"  ⚠️ Options volume spike: {contam.get('max_deviation', 0):.1f}x deviation, "
                                 f"{len(contam.get('anomalies', []))} events")
                    lines.append(f"  Phase 4: insider+options = -11.83% alpha vs +0.99% clean")
            
            lines.append("Transactions:")
            for txn in alert.get('transactions', [])[:5]:
                name = txn.get('insider_name', 'Unknown')
                title = txn.get('insider_title', 'N/A')
                date = txn.get('transaction_date', 'N/A')
                value = txn.get('total_value', 0)
                lines.append(f"  - {name} ({title}): {date} - ${value:,.0f}")
        return "\n".join(lines)
    
    def _send_email(self, subject, html_content, text_content):
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.config['sender_email']
            msg['To'] = self.config['recipient_email']
            msg.attach(MIMEText(text_content, 'plain'))
            msg.attach(MIMEText(html_content, 'html'))
            with smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port']) as server:
                server.starttls()
                server.login(self.config['sender_email'], self.config['sender_password'])
                server.send_message(msg)
            print(f"Email sent to {self.config['recipient_email']}")
            return True
        except Exception as e:
            print(f"Failed to send email: {e}")
            return False
