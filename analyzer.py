"""
Insider Trading Cluster Analyzer + Sell Signal Detector
"""
from datetime import datetime, timedelta
from database import (
    get_cluster_candidates, get_recent_purchases, 
    get_significant_sells, get_recent_sells_by_ticker,
    was_alert_sent, record_alert_sent, get_database_stats
)
from config import ALERT_THRESHOLDS
import math


# ============================================================
#  SELL SIGNAL TIER DEFINITIONS (from backtest 2020-2025)
# ============================================================
#
#  Backtest: 432,625 trades, 5,717 tickers
#
#  SELL TIER 1 — Officer+Director, $250K-$5M
#    5d alpha: -2.54%  |  20d alpha: -2.50%  |  p<0.0001
#    This is the strongest signal. Dual-role insiders dumping
#    meaningful size = they know something bad is coming.
#
#  SELL TIER 2 — Officer OR Director, $250K-$5M  
#    5d alpha: -0.50% to -0.86%  |  p<0.0001
#    Still significant but weaker than dual-role.
#
#  SELL WATCH — Other significant sells ($50K-$250K, or $5M+)
#    $5M+ is weaker (-0.85% 5d), likely 10b5-1 pre-scheduled.
#    $50K-$250K is moderate (-1.13% 5d).
#
#  SHORT INTEREST CONTEXT (not available in this scanner):
#    LOW SI + sell = -1.64% 5d (WORST — bad news not priced in)
#    HIGH SI + sell = -0.81% 5d (less bad — shorts already positioned)
#    Check cross_signal_scanner.py for SI overlay.
# ============================================================

SELL_TIERS = {
    'S1': {
        'name': 'SELL TIER 1 — INSIDER DUMP',
        'tag': 'S1',
        'description': 'Officer+Director selling $250K-$5M',
        'alpha': '-2.54% 5d',
        'color': '#c53030',  # dark red
    },
    'S2': {
        'name': 'SELL TIER 2 — NOTABLE SELL',
        'tag': 'S2',
        'description': 'Officer or Director selling $250K-$5M',
        'alpha': '-0.50% to -0.86% 5d',
        'color': '#dd6b20',  # orange
    },
    'SELL_WATCH': {
        'name': 'SELL WATCH',
        'tag': 'SELL_WATCH',
        'description': 'Significant sell outside sweet spot',
        'alpha': 'varies',
        'color': '#718096',  # gray
    },
}


def classify_sell(sell):
    """
    Classify a single sell transaction into a tier.
    
    Args:
        sell: dict with is_officer, is_director, total_value, etc.
    
    Returns:
        (tier_tag, tier_info, notes)
    """
    is_off = sell.get('is_officer', 0)
    is_dir = sell.get('is_director', 0)
    value = sell.get('total_value', 0) or 0
    
    notes = []
    
    # Check for dual role (Officer+Director) — strongest signal
    is_dual_role = is_off and is_dir
    
    # Sweet spot: $250K-$5M
    in_sweet_spot = 250_000 <= value <= 5_000_000
    
    # Build context notes
    if value >= 1_000_000:
        notes.append(f"${value/1e6:.2f}M sale")
    else:
        notes.append(f"${value:,.0f} sale")
    
    if sell.get('insider_title'):
        notes.append(sell['insider_title'])
    
    if value > 5_000_000:
        notes.append("$5M+ (possible 10b5-1 plan)")
    
    # Tier classification
    if is_dual_role and in_sweet_spot:
        return ('S1', SELL_TIERS['S1'], '; '.join(notes))
    
    if (is_off or is_dir) and in_sweet_spot:
        return ('S2', SELL_TIERS['S2'], '; '.join(notes))
    
    # Everything else that made it past the min_value filter
    if is_off or is_dir:
        if value > 5_000_000:
            notes.append("weaker signal at $5M+")
        return ('SELL_WATCH', SELL_TIERS['SELL_WATCH'], '; '.join(notes))
    
    # 10% owners or other roles — still track but lowest priority
    notes.append("non-officer/director")
    return ('SELL_WATCH', SELL_TIERS['SELL_WATCH'], '; '.join(notes))


class SellSignalAnalyzer:
    """Detects and scores insider sell signals from daily Form 4 filings."""
    
    def __init__(self, lookback_days=3, min_value=50000):
        self.lookback_days = lookback_days
        self.min_value = min_value
    
    def find_sell_signals(self):
        """
        Find all significant sells, group by ticker, classify each.
        
        Returns list of dicts, one per ticker, sorted by strongest signal first:
        {
            'ticker': str,
            'company_name': str,
            'tier_tag': str (best tier for this ticker),
            'tier_info': dict,
            'sells': [list of individual sell dicts with tier info],
            'total_value': float,
            'num_sellers': int,
        }
        """
        raw_sells = get_significant_sells(
            days=self.lookback_days, 
            min_value=self.min_value
        )
        
        if not raw_sells:
            return []
        
        # Group by ticker
        by_ticker = {}
        for sell in raw_sells:
            ticker = sell['ticker']
            if ticker not in by_ticker:
                by_ticker[ticker] = {
                    'ticker': ticker,
                    'company_name': sell['company_name'],
                    'sells': [],
                    'total_value': 0,
                    'sellers': set(),
                }
            
            tag, tier_info, notes = classify_sell(sell)
            sell['tier_tag'] = tag
            sell['tier_info'] = tier_info
            sell['notes'] = notes
            
            by_ticker[ticker]['sells'].append(sell)
            by_ticker[ticker]['total_value'] += sell['total_value'] or 0
            by_ticker[ticker]['sellers'].add(sell['insider_name'])
        
        # Determine best (strongest) tier per ticker
        tier_priority = {'S1': 0, 'S2': 1, 'SELL_WATCH': 2}
        
        results = []
        for ticker, data in by_ticker.items():
            best_tag = min(
                [s['tier_tag'] for s in data['sells']], 
                key=lambda t: tier_priority.get(t, 99)
            )
            
            data['tier_tag'] = best_tag
            data['tier_info'] = SELL_TIERS[best_tag]
            data['num_sellers'] = len(data['sellers'])
            del data['sellers']  # not serializable
            
            # Sort individual sells: strongest tier first, then by value
            data['sells'].sort(
                key=lambda s: (tier_priority.get(s['tier_tag'], 99), -(s['total_value'] or 0))
            )
            
            results.append(data)
        
        # Sort tickers: S1 first, then S2, then WATCH; within tier by total value
        results.sort(key=lambda r: (tier_priority.get(r['tier_tag'], 99), -r['total_value']))
        
        return results
    
    def get_new_sell_alerts(self):
        """
        Find sell signals that haven't been alerted on today.
        Only returns S1 and S2 tier signals (not SELL_WATCH).
        """
        all_signals = self.find_sell_signals()
        today = datetime.now().strftime('%Y-%m-%d')
        
        new_alerts = []
        for signal in all_signals:
            # Only alert on S1 and S2 — SELL_WATCH is informational only
            if signal['tier_tag'] == 'SELL_WATCH':
                continue
            
            ticker = signal['ticker']
            alert_type = f"sell_{signal['tier_tag'].lower()}"
            
            if was_alert_sent(alert_type, ticker, today):
                continue
            
            new_alerts.append(signal)
        
        return new_alerts
    
    def mark_sell_alerts_sent(self, alerts):
        """Record that sell alerts were sent to prevent duplicates."""
        today = datetime.now().strftime('%Y-%m-%d')
        for alert in alerts:
            ticker = alert['ticker']
            alert_type = f"sell_{alert['tier_tag'].lower()}"
            details = (
                f"{alert['tier_tag']}: {alert['num_sellers']} seller(s), "
                f"${alert['total_value']:,.0f} total"
            )
            record_alert_sent(alert_type, ticker, today, details)


# ============================================================
#  EXISTING CLUSTER ANALYZER (unchanged)
# ============================================================

class ClusterAnalyzer:
    def __init__(self):
        self.thresholds = ALERT_THRESHOLDS
    
    def find_clusters(self):
        clusters = get_cluster_candidates(
            min_insiders=self.thresholds['min_cluster_size'],
            days=self.thresholds['cluster_window_days'],
            min_value=self.thresholds['min_purchase_value']
        )
        enriched_clusters = []
        for cluster in clusters:
            details = self._enrich_cluster(cluster)
            enriched_clusters.append(details)
        return enriched_clusters
    
    def _enrich_cluster(self, cluster):
        ticker = cluster['ticker']
        purchases = get_recent_purchases(ticker, days=self.thresholds['cluster_window_days'])
        unique_buyers = set()
        total_value = 0
        transactions = []
        for p in purchases:
            unique_buyers.add(p['insider_name'])
            total_value += p['total_value'] or 0
            transactions.append(p)
        transactions.sort(key=lambda x: x['transaction_date'] or '', reverse=True)
        return {
            'ticker': ticker, 'company_name': cluster['company_name'],
            'unique_insiders': len(unique_buyers), 'total_purchased': total_value,
            'first_purchase': cluster['first_purchase'], 'last_purchase': cluster['last_purchase'],
            'transactions': transactions, 'insider_names': list(unique_buyers)
        }
    
    def get_new_alerts(self):
        clusters = self.find_clusters()
        new_alerts = []
        today = datetime.now().strftime('%Y-%m-%d')
        for cluster in clusters:
            if not was_alert_sent('cluster', cluster['ticker'], today):
                cluster['signal_score'] = self._calculate_signal_score(cluster)
                new_alerts.append(cluster)
        new_alerts.sort(key=lambda x: x['signal_score'], reverse=True)
        return new_alerts
    
    def _calculate_signal_score(self, cluster):
        score = cluster['unique_insiders'] * 25
        if cluster['total_purchased'] > 0:
            score += min(50, math.log10(cluster['total_purchased']) * 10)
        if cluster['last_purchase']:
            try:
                last_date = datetime.strptime(cluster['last_purchase'], '%Y-%m-%d')
                days_ago = (datetime.now() - last_date).days
                if days_ago <= 3: score += 20
                elif days_ago <= 7: score += 10
            except: pass
        return score
    
    def mark_alerts_sent(self, alerts):
        today = datetime.now().strftime('%Y-%m-%d')
        for alert in alerts:
            details = f"{alert['unique_insiders']} insiders, ${alert['total_purchased']:,.0f} total"
            record_alert_sent('cluster', alert['ticker'], today, details)
    
    def generate_alert_summary(self, alerts):
        if not alerts:
            return "No new insider buying clusters detected."
        lines = [f"Found {len(alerts)} ticker(s) with cluster buying activity:", ""]
        for i, alert in enumerate(alerts, 1):
            lines.append(f"#{i}: {alert['ticker']} - {alert['company_name']}")
            lines.append(f"    Unique Insiders: {alert['unique_insiders']}, Total: ${alert['total_purchased']:,.0f}")
        return "\n".join(lines)
