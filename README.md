# Form 4 Insider Scanner

**Automated SEC Form 4 filing scanner that detects insider buying clusters, sell signals, and cross-signal patterns (insider activity x short interest). Backtested against 432,625 trades across 5,717 tickers from 2020-2025.**

Corporate insiders — officers, directors, and 10%+ shareholders — are required to report stock transactions to the SEC via Form 4 within two business days. This tool ingests those filings in real time, identifies statistically significant patterns, and delivers prioritized alerts via email.

**Live Deployment:** Buy cluster signals (Tier 1/2) are deployed in an automated equity trading system running on Interactive Brokers, executing positions at next-morning open.

---

## Signal Tiers (Backtested)

### Buy Clusters

The system detects when multiple insiders at the same company buy stock within a 14-day window, then scores each cluster against backtested filters:

| Tier | Filter | 5d Alpha | Win Rate | Sample |
|------|--------|----------|----------|--------|
| **Tier 1 — Conviction Buy** | C-Suite + total value >= $5M | +4.08% | 57.4% | n=230 |
| **Tier 2 — Strong Signal** | C-Suite + total value >= $500K | +1.81-2.43% | 54-57% | n=423-742 |
| **Tier 3 — Mean Reversion** | C-Suite + >= $500K + stock beaten down >10% | +13.44% (60d) | 55.3% | n=320 |
| Watch | Doesn't meet tier criteria | Informational | — | — |
| Avoid | No C-Suite + small dollar | Negative alpha | — | — |

### Sell Signals

| Tier | Filter | 5d Alpha | Significance |
|------|--------|----------|-------------|
| **Sell Tier 1** | Officer+Director dual role, $250K-$5M | -2.54% | p<0.0001 |
| **Sell Tier 2** | Officer OR Director, $250K-$5M | -0.50 to -0.86% | p<0.0001 |
| Sell Watch | Other significant sells ($50K-$250K or $5M+) | -0.85% | Likely 10b5-1 |

### Cross-Signal: Insider Buying x Short Interest

The highest-conviction signal combines insider buying clusters with short squeeze conditions:

| Filter | 5d Alpha | Win Rate | Sample |
|--------|----------|----------|--------|
| **Tier 2 + DTC>5 + SI increasing >10%** | **+6.73%** | **69.7%** | n=142, p<0.0001 |
| Tier 2 + DTC>5 (no SI filter) | +4.67% | 70.2% | n=527, p<0.0001 |

### Options Volume Contamination Filter (Phase 4)

When insider buying coincides with unusual options volume (Vol/OI >= 7x), the insider signal flips negative. The scanner checks for options spikes within +/-20 trading days and flags contaminated signals.

| Condition | 20d Alpha | Win Rate | Sample |
|-----------|----------|----------|--------|
| Insider + options spike | **-11.83%** | 16.7% | p=0.0017 |
| Call-heavy (C/P >= 1.25) + insider | **-13.86%** | 5.9% | p=0.006 |
| Clean insider (no options spike) | **+0.99%** | — | p=0.019 |

Contaminated clusters are flagged with warning banners in email alerts. This is a filter, not a strategy — contamination is rare (1-3% of events) but catastrophic when present.

---

## Architecture

```
main.py                    # Orchestrator — daily pipeline
├── edgar_fetcher.py       # Pulls Form 4 filings from SEC EDGAR RSS + XML
├── form4_parser.py        # Parses Form 4 XML into structured transactions
├── database.py            # SQLite storage, clustering queries, alert tracking
├── analyzer.py            # Cluster detection + sell signal algorithms
├── signal_scorer.py       # Scores clusters against backtested tier filters
├── email_reporter.py      # HTML email alerts + daily status reports
├── options_volume_check.py # Phase 4 options contamination filter
└── config.py              # Credentials and thresholds (not committed)

cross_signal_scanner.py    # Insider x short interest x options volume enrichment
combo_analysis.py          # Multi-factor combination analysis
insider_cluster_backtest.py # Historical backtesting framework
download_sec_form4.py      # Bulk SEC EDGAR data downloader (quarterly TSV files)
```

---

## Data Pipeline

1. **Fetch** — Pulls the latest 100 Form 4 filings from SEC EDGAR RSS feed every run
2. **Parse** — Extracts issuer, insider identity, relationship, transaction type, shares, price, and value from XML
3. **Store** — Inserts into SQLite with deduplication by accession number
4. **Detect** — Runs cluster detection (multiple insiders, same company, 14-day window) and sell signal detection
5. **Score** — Ranks clusters against backtested tier definitions
6. **Filter** — Checks for concurrent options volume spikes that kill insider alpha (Phase 4)
7. **Enrich** — Cross-references with short interest data for highest-conviction signals
8. **Alert** — Sends prioritized HTML email with tier classification, contamination warnings, and context

---

## Setup

```bash
git clone https://github.com/KPH3802/form4-insider-scanner.git
cd form4-insider-scanner
pip install -r requirements.txt
cp config_example.py config.py
python main.py
python main.py --dry-run
python main.py --fetch-only
python main.py --analyze-only
python cross_signal_scanner.py
```

### Requirements
- Python 3.8+
- SEC EDGAR access (free, requires user agent with contact email per SEC policy)
- Gmail account with App Password for alerts

---

## Backtesting

- **432,625 insider trades** across **5,717 tickers** (2020-2025)
- **5,774 cluster signals** evaluated across 5d, 10d, 20d, 40d, and 60d holding windows
- Alpha calculated against SPY benchmark over matching periods
- Statistical significance tested via t-test (p-values reported)

```bash
python insider_cluster_backtest.py
```

---

## Related Projects

- [congress-trade-tracker](https://github.com/KPH3802/congress-trade-tracker) — Congressional stock trade monitoring with 10 detection algorithms
- [options-volume-scanner](https://github.com/KPH3802/options-volume-scanner) — Unusual options volume detection across S&P 500 stocks
- [volatility-scanner](https://github.com/KPH3802/volatility-scanner) — IV rank, HV patterns, and term structure tracking
- [natural-gas-weather-signals](https://github.com/KPH3802/natural-gas-weather-signals) — Weather-driven natural gas storage research
- [trading-utilities](https://github.com/KPH3802/trading-utilities) — Shared data pipeline: 13F filings, FRED data, price history, dividends, earnings, short interest

---

## Connect

[![LinkedIn](https://img.shields.io/badge/LinkedIn-kevin--heaney-blue?logo=linkedin)](https://www.linkedin.com/in/kevin-heaney/)
[![Medium](https://img.shields.io/badge/Medium-@KPH3802-black?logo=medium)](https://medium.com/@KPH3802)

---

## Disclaimer

This tool is for **educational and research purposes only**. SEC Form 4 data is public information. This project does not constitute financial advice. Backtested results reflect historical data and do not guarantee future performance.

---

## License

MIT
