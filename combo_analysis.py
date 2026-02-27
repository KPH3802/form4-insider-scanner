#!/usr/bin/env python3
"""
INSIDER CLUSTER COMBO ANALYSIS
================================
Queries insider_backtest_results.db to test intersections of the
three strongest signal dimensions and rank them.

Usage:
    python3 combo_analysis.py
"""

import sqlite3
import os
import sys

DB_PATH = os.path.expanduser(
    "~/Desktop/Claude_Programs/Trading_Programs/Form4_Scanner/insider_backtest_results.db"
)

if not os.path.exists(DB_PATH):
    print(f"ERROR: Database not found at {DB_PATH}")
    sys.exit(1)

conn = sqlite3.connect(DB_PATH)

# Verify table exists
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
print(f"Tables in database: {tables}")

TABLE = 'backtest_results'
if TABLE not in tables:
    print(f"ERROR: Table '{TABLE}' not found. Available: {tables}")
    sys.exit(1)

# Get columns
cols = [r[1] for r in conn.execute(f"PRAGMA table_info({TABLE})").fetchall()]
print(f"Columns: {cols}\n")

total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
print(f"Total signals: {total:,}")

# Check how returns are stored
sample_ret = conn.execute(f"SELECT ret_5d FROM {TABLE} WHERE ret_5d IS NOT NULL LIMIT 1").fetchone()[0]
print(f"Sample ret_5d value: {sample_ret}  (stored as {'percentage' if abs(sample_ret) > 0.5 else 'decimal'})\n")


# ============================================================
#  HELPER
# ============================================================

def analyze(desc, where):
    """Analyze a filtered group. Returns dict or None."""
    
    n = conn.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE {where} AND ret_5d IS NOT NULL").fetchone()[0]
    
    if n < 10:
        print(f"\n  {desc}: Too few signals ({n})")
        return None
    
    print(f"\n  {desc} (n={n:,})")
    print(f"    Window |  Avg Ret |  Med Ret |   Win% |  Avg Alpha | Alpha Win% |  Std Dev")
    print(f"  -------- | -------- | -------- | ------ | ---------- | ---------- | --------")
    
    result = {}
    
    for w in [5, 10, 20, 40, 60]:
        rc = f"ret_{w}d"
        ac = f"alpha_{w}d"
        
        row = conn.execute(f"""
            SELECT AVG({rc}), AVG({ac}), COUNT(*),
                   SUM(CASE WHEN {rc} > 0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN {ac} > 0 THEN 1 ELSE 0 END)
            FROM {TABLE} WHERE {where} AND {rc} IS NOT NULL
        """).fetchone()
        
        cnt = row[2]
        if cnt == 0:
            continue
        
        avg_r, avg_a = row[0], row[1]
        win_pct = (row[3] / cnt) * 100
        alpha_win = (row[4] / cnt) * 100
        
        # Median
        med = conn.execute(f"""
            SELECT {rc} FROM {TABLE} WHERE {where} AND {rc} IS NOT NULL
            ORDER BY {rc} LIMIT 1 OFFSET {cnt // 2}
        """).fetchone()[0]
        
        # Std dev
        var = conn.execute(f"""
            SELECT AVG(({rc} - {avg_r}) * ({rc} - {avg_r}))
            FROM {TABLE} WHERE {where} AND {rc} IS NOT NULL
        """).fetchone()[0]
        std = var ** 0.5 if var else 0
        
        tag = ""
        if w == 5:
            result = {'n': n, 'a5': avg_a, 'aw5': alpha_win}
            if avg_a > 2.5 and alpha_win > 54:
                tag = " ⭐⭐ STRONG"
            elif avg_a > 1.5 and alpha_win > 52:
                tag = " ⭐ GOOD"
            elif avg_a > 1.0 and alpha_win > 50:
                tag = " ✅ DECENT"
        
        print(f"    {w:4d}d | {avg_r:+7.2f}% | {med:+7.2f}% | {win_pct:5.1f}% | "
              f"  {avg_a:+7.2f}% |     {alpha_win:5.1f}% | {std:7.2f}%{tag}")
    
    return result


# ============================================================
#  FILTER DEFINITIONS
# ============================================================

CSUITE = "has_csuite = 1"
NO_CSUITE = "has_csuite = 0"
CEO = "has_ceo = 1"
CFO = "has_cfo = 1"

D_UNDER_100K = "total_dollars < 100000"
D_100K_500K = "total_dollars >= 100000 AND total_dollars < 500000"
D_500K_1M = "total_dollars >= 500000 AND total_dollars < 1000000"
D_1M_5M = "total_dollars >= 1000000 AND total_dollars < 5000000"
D_5M_PLUS = "total_dollars >= 5000000"
D_500K_PLUS = "total_dollars >= 500000"
D_100K_PLUS = "total_dollars >= 100000"
D_250K_PLUS = "total_dollars >= 250000"

# Stock context: (entry_price / avg_price - 1) * 100
# beaten down = entry price is >10% BELOW cluster avg purchase price
BEATEN = "avg_price > 0 AND ((entry_price * 1.0 / avg_price) - 1) * 100 < -10"
NEUTRAL = "avg_price > 0 AND ((entry_price * 1.0 / avg_price) - 1) * 100 >= -10 AND ((entry_price * 1.0 / avg_price) - 1) * 100 <= 10"
ELEVATED = "avg_price > 0 AND ((entry_price * 1.0 / avg_price) - 1) * 100 > 10"

SIZE_3_5 = "num_insiders >= 3 AND num_insiders <= 5"
SIZE_5_PLUS = "num_insiders >= 5"

POST_2021 = "signal_date >= '2022-01-01'"
YEAR_2022 = "signal_date >= '2022-01-01' AND signal_date < '2023-01-01'"
YEAR_2024_25 = "signal_date >= '2024-01-01'"


# ============================================================
#  RUN ANALYSIS
# ============================================================

print("=" * 70)
print("  COMBO ANALYSIS — FINDING THE OPTIMAL SIGNAL")
print("=" * 70)

print("\n" + "─" * 70)
print("  1. C-SUITE × DOLLAR VALUE")
print("─" * 70)
analyze("C-Suite + Under $100K", f"{CSUITE} AND {D_UNDER_100K}")
analyze("C-Suite + $100K-$500K", f"{CSUITE} AND {D_100K_500K}")
analyze("C-Suite + $500K-$1M", f"{CSUITE} AND {D_500K_1M}")
analyze("C-Suite + $1M-$5M", f"{CSUITE} AND {D_1M_5M}")
analyze("C-Suite + $5M+", f"{CSUITE} AND {D_5M_PLUS}")

print("\n" + "─" * 70)
print("  2. C-SUITE × STOCK CONTEXT")
print("─" * 70)
analyze("C-Suite + Beaten Down", f"{CSUITE} AND {BEATEN}")
analyze("C-Suite + Neutral", f"{CSUITE} AND {NEUTRAL}")
analyze("C-Suite + Elevated", f"{CSUITE} AND {ELEVATED}")

print("\n" + "─" * 70)
print("  3. DOLLAR VALUE × STOCK CONTEXT")
print("─" * 70)
analyze("$500K-$1M + Beaten Down", f"{D_500K_1M} AND {BEATEN}")
analyze("$500K-$1M + Neutral", f"{D_500K_1M} AND {NEUTRAL}")
analyze("$500K-$1M + Elevated", f"{D_500K_1M} AND {ELEVATED}")

print("\n" + "─" * 70)
print("  4. C-SUITE × CLUSTER SIZE")
print("─" * 70)
analyze("C-Suite + 3-5 insiders", f"{CSUITE} AND {SIZE_3_5}")
analyze("C-Suite + 5+ insiders", f"{CSUITE} AND {SIZE_5_PLUS}")

print("\n" + "─" * 70)
print("  5. THREE-WAY COMBOS ⭐⭐⭐")
print("─" * 70)
analyze("C-Suite + $500K-$1M + Beaten Down", f"{CSUITE} AND {D_500K_1M} AND {BEATEN}")
analyze("C-Suite + $500K-$1M + Neutral", f"{CSUITE} AND {D_500K_1M} AND {NEUTRAL}")
analyze("C-Suite + $500K+ + Beaten Down", f"{CSUITE} AND {D_500K_PLUS} AND {BEATEN}")
analyze("C-Suite + $100K+ + Beaten Down", f"{CSUITE} AND {D_100K_PLUS} AND {BEATEN}")
analyze("C-Suite + $250K+ + Beaten Down", f"{CSUITE} AND {D_250K_PLUS} AND {BEATEN}")
analyze("C-Suite + $1M-$5M + Beaten Down", f"{CSUITE} AND {D_1M_5M} AND {BEATEN}")
analyze("C-Suite + $500K-$1M + Elevated", f"{CSUITE} AND {D_500K_1M} AND {ELEVATED}")

print("\n" + "─" * 70)
print("  6. CEO-SPECIFIC COMBOS")
print("─" * 70)
analyze("CEO only", CEO)
analyze("CEO + $500K-$1M", f"{CEO} AND {D_500K_1M}")
analyze("CEO + $500K+", f"{CEO} AND {D_500K_PLUS}")
analyze("CEO + Beaten Down", f"{CEO} AND {BEATEN}")
analyze("CEO + $500K+ + Beaten Down", f"{CEO} AND {D_500K_PLUS} AND {BEATEN}")
analyze("CEO + $250K+ + Beaten Down", f"{CEO} AND {D_250K_PLUS} AND {BEATEN}")
analyze("CEO + $100K+ + Beaten Down", f"{CEO} AND {D_100K_PLUS} AND {BEATEN}")
analyze("CEO + $500K-$1M + Beaten Down", f"{CEO} AND {D_500K_1M} AND {BEATEN}")

print("\n" + "─" * 70)
print("  7. CFO-SPECIFIC COMBOS")
print("─" * 70)
analyze("CFO only", CFO)
analyze("CFO + Beaten Down", f"{CFO} AND {BEATEN}")

print("\n" + "─" * 70)
print("  8. ANTI-PATTERNS ⛔")
print("─" * 70)
analyze("⛔ No C-Suite + Under $100K", f"{NO_CSUITE} AND {D_UNDER_100K}")
analyze("⛔ No C-Suite + $5M+", f"{NO_CSUITE} AND {D_5M_PLUS}")
analyze("⛔ No C-Suite + Elevated", f"{NO_CSUITE} AND {ELEVATED}")
analyze("⛔ Under $100K + Elevated", f"{D_UNDER_100K} AND {ELEVATED}")

print("\n" + "─" * 70)
print("  9. TIME-FILTERED (excluding COVID distortion)")
print("─" * 70)
analyze("Post-2021 (all)", POST_2021)
analyze("Post-2021 + C-Suite + $500K+", f"{POST_2021} AND {CSUITE} AND {D_500K_PLUS}")
analyze("Post-2021 + CS + $500K+ + Beaten", f"{POST_2021} AND {CSUITE} AND {D_500K_PLUS} AND {BEATEN}")
analyze("2022 Bear + C-Suite", f"{YEAR_2022} AND {CSUITE}")
analyze("2022 Bear + C-Suite + $500K+", f"{YEAR_2022} AND {CSUITE} AND {D_500K_PLUS}")
analyze("2024-25 + C-Suite + $500K+", f"{YEAR_2024_25} AND {CSUITE} AND {D_500K_PLUS}")


# ============================================================
#  FINAL RANKING TABLE
# ============================================================

print("\n" + "=" * 70)
print("  FINAL RANKING — ALL COMBOS BY 5-DAY ALPHA")
print("=" * 70)

combos_to_rank = [
    ("ALL SIGNALS (baseline)",                "1=1"),
    ("C-Suite only",                          CSUITE),
    ("No C-Suite",                            NO_CSUITE),
    ("CEO only",                              CEO),
    ("$500K-$1M only",                        D_500K_1M),
    ("Beaten Down only",                      BEATEN),
    ("C-Suite + $100K-$500K",                 f"{CSUITE} AND {D_100K_500K}"),
    ("C-Suite + $500K-$1M",                   f"{CSUITE} AND {D_500K_1M}"),
    ("C-Suite + $1M-$5M",                     f"{CSUITE} AND {D_1M_5M}"),
    ("C-Suite + $5M+",                        f"{CSUITE} AND {D_5M_PLUS}"),
    ("C-Suite + Beaten Down",                 f"{CSUITE} AND {BEATEN}"),
    ("C-Suite + Elevated",                    f"{CSUITE} AND {ELEVATED}"),
    ("$500K-$1M + Beaten Down",               f"{D_500K_1M} AND {BEATEN}"),
    ("C-Suite + $500K-$1M + Beaten Down",     f"{CSUITE} AND {D_500K_1M} AND {BEATEN}"),
    ("C-Suite + $500K+ + Beaten Down",        f"{CSUITE} AND {D_500K_PLUS} AND {BEATEN}"),
    ("C-Suite + $100K+ + Beaten Down",        f"{CSUITE} AND {D_100K_PLUS} AND {BEATEN}"),
    ("C-Suite + $250K+ + Beaten Down",        f"{CSUITE} AND {D_250K_PLUS} AND {BEATEN}"),
    ("C-Suite + $1M-$5M + Beaten Down",       f"{CSUITE} AND {D_1M_5M} AND {BEATEN}"),
    ("CEO + $500K+",                          f"{CEO} AND {D_500K_PLUS}"),
    ("CEO + Beaten Down",                     f"{CEO} AND {BEATEN}"),
    ("CEO + $500K+ + Beaten Down",            f"{CEO} AND {D_500K_PLUS} AND {BEATEN}"),
    ("CEO + $250K+ + Beaten Down",            f"{CEO} AND {D_250K_PLUS} AND {BEATEN}"),
    ("CEO + $100K+ + Beaten Down",            f"{CEO} AND {D_100K_PLUS} AND {BEATEN}"),
    ("Post-2021 + C-Suite + $500K+",          f"{POST_2021} AND {CSUITE} AND {D_500K_PLUS}"),
    ("Post-2021 + CS + $500K+ + Beaten",      f"{POST_2021} AND {CSUITE} AND {D_500K_PLUS} AND {BEATEN}"),
    ("⛔ No C-Suite + Under $100K",           f"{NO_CSUITE} AND {D_UNDER_100K}"),
    ("⛔ No C-Suite + Elevated",              f"{NO_CSUITE} AND {ELEVATED}"),
    ("⛔ Under $100K + Elevated",             f"{D_UNDER_100K} AND {ELEVATED}"),
]

rows = []
for name, where in combos_to_rank:
    try:
        r = conn.execute(f"""
            SELECT COUNT(*), AVG(alpha_5d),
                   SUM(CASE WHEN alpha_5d > 0 THEN 1 ELSE 0 END),
                   AVG(alpha_20d), AVG(alpha_60d)
            FROM {TABLE} WHERE {where} AND ret_5d IS NOT NULL
        """).fetchone()
        
        if r[0] >= 10:
            rows.append({
                'name': name, 'n': r[0],
                'a5': r[1], 'aw5': (r[2]/r[0])*100,
                'a20': r[3] if r[3] else 0,
                'a60': r[4] if r[4] else 0,
            })
    except Exception as e:
        print(f"  Skip '{name}': {e}")

rows.sort(key=lambda x: x['a5'], reverse=True)

print(f"\n  {'Combo':<45s} |    N |  5d α  | 5d Win |  20d α  |  60d α")
print(f"  {'-'*45} | ---- | ------ | ------ | ------- | -------")
for r in rows:
    tag = ""
    if r['a5'] > 2.5 and r['aw5'] > 54:
        tag = " ⭐⭐"
    elif r['a5'] > 1.5 and r['aw5'] > 51:
        tag = " ⭐"
    elif r['a5'] < 0:
        tag = " ⛔"
    
    print(f"  {r['name']:<45s} | {r['n']:>4} | {r['a5']:+5.2f}% | {r['aw5']:5.1f}% | "
          f"{r['a20']:+6.2f}% | {r['a60']:+6.2f}%{tag}")


# ============================================================
#  EXAMPLE SIGNALS FROM BEST COMBO
# ============================================================

best = None
for r in rows:
    if r['n'] >= 25 and r['a5'] > 0 and not r['name'].startswith("⛔") and r['name'] != "ALL SIGNALS (baseline)":
        best = r
        break

if best:
    best_where = None
    for name, where in combos_to_rank:
        if name == best['name']:
            best_where = where
            break
    
    if best_where:
        print(f"\n" + "=" * 70)
        print(f"  BEST TRADEABLE COMBO: {best['name']}")
        print(f"  n={best['n']}  |  5d Alpha: {best['a5']:+.2f}%  |  5d Win: {best['aw5']:.1f}%")
        print(f"  20d Alpha: {best['a20']:+.2f}%  |  60d Alpha: {best['a60']:+.2f}%")
        print(f"=" * 70)
        
        print(f"\n  Most recent signals:")
        exs = conn.execute(f"""
            SELECT ticker, signal_date, num_insiders, total_dollars, roles,
                   ret_5d, alpha_5d, ret_20d, alpha_20d, ret_60d, alpha_60d
            FROM {TABLE} 
            WHERE {best_where} AND ret_5d IS NOT NULL
            ORDER BY signal_date DESC LIMIT 20
        """).fetchall()
        
        print(f"    {'Ticker':<7s} {'Date':<11s} {'#':>2s} {'$Value':>11s} "
              f"{'5d':>7s} {'5dα':>7s} {'20d':>7s} {'20dα':>7s} {'60d':>7s} {'60dα':>7s}  Roles")
        print(f"    {'-'*7} {'-'*11} {'-'*2} {'-'*11} "
              f"{'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7}  {'-'*20}")
        
        for e in exs:
            dv = f"${e[3]:,.0f}" if e[3] else "?"
            def fmt(v): return f"{v:+6.1f}%" if v is not None else "   N/A"
            roles = (e[4] or "")[:25]
            print(f"    {e[0]:<7s} {str(e[1]):<11s} {e[2]:>2d} {dv:>11s} "
                  f"{fmt(e[5])} {fmt(e[6])} {fmt(e[7])} {fmt(e[8])} {fmt(e[9])} {fmt(e[10])}  {roles}")

conn.close()
print("\n  Done!")