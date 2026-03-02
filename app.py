import streamlit as st
import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta
import requests, time, csv, os, json
from datetime import datetime, timezone, timedelta

# ── Fix asyncio.run() inside Colab/Jupyter (uvloop-safe) ────────────────────
# Colab uses uvloop which nest_asyncio can't patch directly.
# Solution: reset to Python's standard DefaultEventLoopPolicy BEFORE applying nest_asyncio.
# This is safe — it only affects the current process and doesn't break ccxt or Streamlit.
try:
    import asyncio, nest_asyncio
    # Force standard event loop policy so nest_asyncio can patch it
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    # Get or create a fresh standard event loop
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    nest_asyncio.apply(loop)
except ImportError:
    pass  # nest_asyncio not installed — install with: !pip install nest_asyncio
except Exception:
    pass  # fallback: proceed without patching

st.set_page_config(page_title="APEX 2 // Pump & Dump Scanner", page_icon="🔥", layout="wide", initial_sidebar_state="expanded")

# ─── STATE ───────────────────────────────────────────────────────────────────
for k,v in [('results',[]),('last_scan',"—"),('scan_count',0),
            ('btc_price',0),('btc_trend',"—"),('fng_val',50),('fng_txt',"Neutral"),
            ('scan_errors',[]),('last_raw_count',0),('logged_sigs',set()),
            ('sentinel_active', False), ('sentinel_results', []),
            ('sentinel_last_check', '—'), ('sentinel_total_checked', 0),
            ('sentinel_signals_found', 0), ('sentinel_universe_size', '?'), ('social_cache', {}),
            ('social_last_fetch', 0),
            ('listing_cache', {}), ('listing_last_fetch', 0),
            ('onchain_cache', {}), ('journal_last_autocheck', 0),
            ('prev_results', {}),
            ('alerted_scores', {})]:
    if k not in st.session_state: st.session_state[k]=v

JOURNAL_FILE="trade_journal.csv"; COOLDOWN_FILE="symbol_cooldowns.json"; SETTINGS_FILE="apex_settings.json"

# ─── SETTINGS PERSISTENCE ────────────────────────────────────────────────────
DEFAULT_SETTINGS = {
    "scan_depth": 40,
    "scan_mode": "mixed",   # volume / gainers / losers / mixed
    "fast_tf": "15m",
    "slow_tf": "4h",
    "min_score": 10,
    "j_imminent": True,
    "j_building": True,
    "j_early": False,
    "whale_min_usdt": 250000,
    "btc_filter": True,
    "cooldown_on": True,
    "cooldown_hrs": 4,
    "auto_scan": False,
    "auto_interval": 5,
    # Notification Controls
    "alert_min_score": 60,
    "alert_longs": True,
    "alert_shorts": True,
    "alert_squeeze": True,
    "alert_breakout": True,
    "alert_early": False,
    "alert_whale": True,
    # Breakout classifier thresholds (tweakable in Settings)
    "cls_breakout_oi_min":  7,
    "cls_breakout_vol_min": 4,
    "cls_breakout_score_min": 25,
    "cls_squeeze_fund_min": 8,
    "cls_squeeze_ob_min":   6,
    # Signal thresholds
    "ob_ratio_low": 1.1,
    "ob_ratio_mid": 1.5,
    "ob_ratio_high": 2.5,
    "funding_low": 0.0002,
    "funding_mid": 0.0005,
    "funding_high": 0.001,
    "oi_price_chg_low": 0.5,
    "oi_price_chg_high": 2.0,
    "vol_surge_low": 1.5,
    "vol_surge_mid": 2.0,
    "vol_surge_high": 3.0,
    "liq_cluster_near": 1.5,
    "liq_cluster_mid": 3.0,
    "liq_cluster_far": 5.0,
    "vol24h_low": 1000000,
    "vol24h_mid": 10000000,
    "vol24h_high": 50000000,
    "rsi_oversold": 40,
    "rsi_overbought": 60,
    "min_reasons": 1,
    "min_rr": 1.5,
    "require_momentum": False,
    # Points
    "pts_ob_low": 5, "pts_ob_mid": 15, "pts_ob_high": 25,
    "pts_funding_low": 5, "pts_funding_mid": 15, "pts_funding_high": 25,
    "pts_oi_low": 7, "pts_oi_high": 18,
    "pts_vol_low": 3, "pts_vol_mid": 8, "pts_vol_high": 14,
    "pts_liq_near": 15, "pts_liq_mid": 8, "pts_liq_far": 4,
    "pts_vol24_low": 3, "pts_vol24_mid": 6, "pts_vol24_high": 12,
    "pts_macd": 4, "pts_rsi": 5, "pts_bb": 4, "pts_ema": 3,
    "pts_session": 4,
    "pts_sentiment": 20,
    "pts_taker": 10,
    "pts_whale_near": 25, "pts_whale_mid": 15, "pts_whale_far": 8,
    # APIs
    "cmc_key": "",
    "tg_token": "",
    "tg_chat_id": "",
    "discord_webhook": "https://discord.com/api/webhooks/1476606856599179265/74wKbIJEXNJ9h10Ab0Q9Vp7ZmeJ52XY18CP3lKxg3eR1BbpZSdX65IT8hbZjpEIXSqEg",
    "okx_key": "",
    "okx_secret": "",
    "okx_passphrase": "",
    "gate_key": "",
    "gate_secret": "",
    # ── Accuracy Filters ─────────────────────────────────────────────────────
    "min_vol_filter":       300000,   # min 24h USDT vol — skip illiquid coins
    "min_active_signals":   3,        # must have signals from ≥N categories
    "spread_max_pct":       0.5,      # max bid-ask spread % (0 = disabled)
    "atr_min_pct":          0.2,      # min ATR/price % — skip flatliners
    "atr_max_pct":          10.0,     # max ATR/price % — skip too noisy
    "mtf_confirm":          True,     # require multi-timeframe trend alignment
    "pts_mtf":              12,       # points for full MTF alignment
    "pts_divergence":       10,       # points for RSI divergence
    "pts_candle_pattern":   8,        # points for candle pattern (engulf/hammer)
    "pts_oi_funding_combo": 10,       # bonus for OI rising + extreme funding together
    "dedup_symbols":        True,     # show each coin once (best score across exchanges)
    "fng_long_threshold":   30,       # below this F&G, raise LONG bar (extreme fear)
    "fng_short_threshold":  70,       # above this F&G, raise SHORT bar (extreme greed)
    "vol_surge_explosive":  5.0,
    "pts_vol_explosive":    20,
    # ── Tier 1 & 2 Intelligence ───────────────────────────────────────────────
    "pts_orderflow":        12,
    "orderflow_lookback":   10,
    "pts_liq_map":          15,
    "listing_alert_pts":    25,
    "onchain_whale_min":    500000,
    "pts_onchain_whale":    15,
    # ── Auto Journal ──────────────────────────────────────────────────────────
    "journal_autocheck_on":   True,
    "journal_autocheck_mins": 15,
}

def load_settings():
    s = DEFAULT_SETTINGS.copy()
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f: s.update(json.load(f))
        except: pass
    return s

def save_settings(s):
    with open(SETTINGS_FILE, 'w', encoding='utf-8') as f: json.dump(s,f,indent=2)

S = load_settings()

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def ensure_journal():
    headers = ["ts","symbol","exchange","type","pump_score","class","price","tp","sl","triggers","status"]
    if not os.path.exists(JOURNAL_FILE):
        with open(JOURNAL_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(headers)
    else:
        try:
            df = pd.read_csv(JOURNAL_FILE)
            updated = False
            if 'status' not in df.columns:
                df['status'] = 'ACTIVE'; updated = True
            if 'exchange' not in df.columns:
                df.insert(2, 'exchange', 'MEXC'); updated = True
            if updated: df.to_csv(JOURNAL_FILE, index=False)
        except: pass

def log_trade(res):
    try:
        ensure_journal()
        with open(JOURNAL_FILE, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow([datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                res['symbol'],res.get('exchange','MEXC'),res['type'],res['pump_score'],res.get('cls','—'),
                round(res['price'],8),round(res.get('tp',0),8),round(res.get('sl',0),8),
                " | ".join(res['reasons']), "ACTIVE"])
    except: pass

def _journal_check_hits(df, prices, s):
    """Walk ACTIVE rows, check TP/SL. Returns (df, hits, updated)."""
    updated = False; hits = []
    for i, row in df.iterrows():
        if row.get('status') != 'ACTIVE': continue
        sym   = str(row.get('symbol', ''))
        price = prices.get(sym, 0)
        if not price: continue
        try: tp = float(row['tp']); sl = float(row['sl'])
        except: continue
        sig = row.get('type', 'LONG'); hit = None
        if sig == 'LONG':
            if price >= tp:  hit = 'TP'
            elif price <= sl: hit = 'SL'
        else:
            if price <= tp:  hit = 'TP'
            elif price >= sl: hit = 'SL'
        if hit:
            df.at[i, 'status'] = hit; updated = True
            hits.append({'symbol': sym, 'hit': hit, 'price': price,
                         'tp': tp, 'sl': sl, 'type': sig})
    return df, hits, updated


def _fire_journal_alerts(hits, s, source="Scan"):
    """Show toast for TP/SL hits — no Discord/Telegram for journal exits (disabled by user)."""
    for h in hits:
        em = "✅" if h['hit'] == 'TP' else "🛑"
        st.toast(f"{em} [{source}] {h['symbol']} {h['hit']} @ ${h['price']:.6f}", icon=em)
        # NOTE: Discord/Telegram notifications for TP/SL hits are intentionally disabled.
        # The journal CSV is updated silently. Only trade ENTRY alerts fire externally.


def process_journal_tracking(tickers_dict, s):
    """Called after every full scan with ccxt tickers dict."""
    if not os.path.exists(JOURNAL_FILE): return
    try:
        df = pd.read_csv(JOURNAL_FILE)
        if df.empty or 'status' not in df.columns: return
        prices = {}
        for t, data in tickers_dict.items():
            base = t.split('/')[0].split(':')[0]
            px = float(data.get('last') or 0)
            if px: prices[base] = px
        df, hits, updated = _journal_check_hits(df, prices, s)
        if updated: df.to_csv(JOURNAL_FILE, index=False)
        if hits: _fire_journal_alerts(hits, s, "Scan")
    except: pass


def autocheck_journal_background(s):
    """
    Background auto-checker — runs every journal_autocheck_mins on every page load.
    Fetches prices via OKX + Gate public REST (no ccxt, no auth).
    """
    if not s.get('journal_autocheck_on', True): return
    if not os.path.exists(JOURNAL_FILE): return
    interval = s.get('journal_autocheck_mins', 15) * 60
    if time.time() - st.session_state.get('journal_last_autocheck', 0) < interval: return
    try:
        df = pd.read_csv(JOURNAL_FILE)
        if df.empty or 'status' not in df.columns:
            st.session_state.journal_last_autocheck = time.time(); return
        active_syms = df[df['status'] == 'ACTIVE']['symbol'].dropna().unique().tolist()
        if not active_syms:
            st.session_state.journal_last_autocheck = time.time(); return
        prices = {}
        for sym in active_syms:
            try:
                r = requests.get("https://www.okx.com/api/v5/market/ticker",
                    params={"instId": f"{sym}-USDT-SWAP"}, timeout=4)
                if r.status_code == 200:
                    d = r.json().get('data', [])
                    if d: prices[sym] = float(d[0].get('last', 0) or 0)
            except: pass
            if not prices.get(sym):
                try:
                    r2 = requests.get("https://fx-api.gateio.ws/api/v4/futures/usdt/tickers",
                        params={"contract": f"{sym}_USDT"}, timeout=4)
                    if r2.status_code == 200:
                        d2 = r2.json()
                        if d2: prices[sym] = float(d2[0].get('last', 0) or 0)
                except: pass
        df, hits, updated = _journal_check_hits(df, prices, s)
        if updated: df.to_csv(JOURNAL_FILE, index=False)
        if hits: _fire_journal_alerts(hits, s, "Auto-Check")
    except: pass
    finally:
        st.session_state.journal_last_autocheck = time.time()

def is_on_cooldown(sym,hrs):
    if not os.path.exists(COOLDOWN_FILE): return False
    try:
        with open(COOLDOWN_FILE, 'r', encoding='utf-8') as f: cd=json.load(f)
        if sym in cd:
            return (datetime.now()-datetime.fromisoformat(cd[sym])).total_seconds()/3600 < hrs
    except: pass
    return False

def set_cooldown(sym):
    cd={}
    if os.path.exists(COOLDOWN_FILE):
        try:
            with open(COOLDOWN_FILE, 'r', encoding='utf-8') as f: cd=json.load(f)
        except: pass
    cd[sym]=datetime.now().isoformat()
    with open(COOLDOWN_FILE, 'w', encoding='utf-8') as f: json.dump(cd,f)

def get_session():
    h=datetime.now(timezone.utc).hour
    if 12<=h<13: return "London/NY Overlap",1.5,"#7c3aed"
    elif 13<=h<17: return "New York Open",1.4,"#059669"
    elif 8<=h<12: return "London Open",1.3,"#2563eb"
    elif 0<=h<4: return "Asian Session",0.85,"#d97706"
    else: return "Off-Hours",0.7,"#9ca3af"

def fmt(n):
    if abs(n)>=1e9: return f"${n/1e9:.2f}B"
    if abs(n)>=1e6: return f"${n/1e6:.2f}M"
    if abs(n)>=1e3: return f"${n/1e3:.1f}K"
    return f"${n:.0f}"

def pump_color(score, is_sniper=False):
    if is_sniper or score>=90: return "#ff0000"
    if score>=70: return "#dc2626"
    if score>=45: return "#d97706"
    if score>=25: return "#2563eb"
    return "#6b7280"

def pump_label(score, sig, is_sniper=False):
    if is_sniper or score>=90: return "🎯 GOD-TIER SETUP"
    if score>=70: return "🔥 PUMP IMMINENT" if sig=="LONG" else "🩸 DUMP IMMINENT"
    if score>=45: return "⚡ BUILDING PUMP" if sig=="LONG" else "⚡ BUILDING DUMP"
    if score>=25: return "📡 EARLY LONG" if sig=="LONG" else "📡 EARLY SHORT"
    return "— WEAK"

def classify(res):
    bd  = res.get('signal_breakdown', {})
    sc  = res['pump_score']
    cfg = res.get('_cls_cfg', {})

    # Read thresholds — defaults match original tuning, overridden by Settings sliders
    br_oi  = cfg.get('breakout_oi_min',  7)
    br_vol = cfg.get('breakout_vol_min', 4)
    br_sc  = cfg.get('breakout_sc_min',  25)
    sq_fd  = cfg.get('squeeze_fund_min', 8)
    sq_ob  = cfg.get('squeeze_ob_min',   6)

    # Composite scores for fallback sorting
    squeeze_score  = bd.get('funding', 0) + bd.get('funding_hist', 0) + bd.get('ob_imbalance', 0)
    breakout_score = bd.get('oi_spike', 0) + bd.get('vol_surge', 0) + bd.get('orderflow', 0)
    whale_score    = bd.get('whale_wall', 0) + bd.get('liq_cluster', 0)

    # ── Squeeze: extreme funding + OB imbalance ───────────────────────────
    if bd.get('funding', 0) >= sq_fd and bd.get('ob_imbalance', 0) >= sq_ob and sc >= 25:
        return 'squeeze'

    # ── Breakout: OI spike + volume surge (user-tunable) ─────────────────
    if bd.get('oi_spike', 0) >= br_oi and bd.get('vol_surge', 0) >= br_vol and sc >= br_sc:
        return 'breakout'
    # Breakout fallback: strong volume + orderflow even without OI data
    if bd.get('vol_surge', 0) >= 8 and bd.get('orderflow', 0) >= 6 and sc >= 30:
        return 'breakout'

    # ── Whale driven: large walls or liq cluster magnetism ────────────────
    if bd.get('whale_wall', 0) >= 8 or (bd.get('liq_cluster', 0) >= 8 and bd.get('vol_surge', 0) >= 3):
        return 'whale_driven'

    # ── God-tier (90+) / High-score (70+): never let strong coins sit in Early
    if sc >= 90:
        return max({'squeeze': squeeze_score, 'breakout': breakout_score, 'whale_driven': whale_score},
                   key=lambda k: {'squeeze': squeeze_score, 'breakout': breakout_score, 'whale_driven': whale_score}[k])
    if sc >= 70:
        if squeeze_score >= breakout_score and squeeze_score >= whale_score: return 'squeeze'
        if breakout_score >= squeeze_score: return 'breakout'

    return 'early'

def send_tg(token, cid, msg):
    if not token or not cid: return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
            timeout=8)
        if r.status_code != 200:
            st.toast(f"⚠️ Telegram failed: {r.status_code} — {r.text[:80]}", icon="⚠️")
    except Exception as _e:
        st.toast(f"⚠️ Telegram error: {_e}", icon="⚠️")

def send_discord(webhook_url, embed_dict):
    if not webhook_url: return
    try:
        r = requests.post(webhook_url, json={"embeds": [embed_dict]}, timeout=8)
        if r.status_code not in (200, 204):
            st.toast(f"⚠️ Discord failed: {r.status_code} — {r.text[:80]}", icon="⚠️")
    except Exception as _e:
        st.toast(f"⚠️ Discord error: {_e}", icon="⚠️")

# ─── CSS ─────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Geist+Mono:wght@300;400;500;600;700&family=Geist:wght@300;400;500;600;700;800&display=swap');
:root{
  --bg:#f7f8fc;--surface:#ffffff;--panel:#f0f2f8;--border:#e2e5f0;--border2:#c8cde0;
  --text:#0f1117;--text2:#3d4461;--muted:#7a82a0;
  --green:#059669;--green-bg:#ecfdf5;--green-bd:#a7f3d0;
  --red:#dc2626;--red-bg:#fef2f2;--red-bd:#fecaca;
  --amber:#d97706;--amber-bg:#fffbeb;--amber-bd:#fde68a;
  --blue:#2563eb;--blue-bg:#eff6ff;--blue-bd:#bfdbfe;
  --purple:#7c3aed;--purple-bg:#f5f3ff;--purple-bd:#ddd6fe;
  --sh:0 1px 4px rgba(15,17,23,.06),0 4px 16px rgba(15,17,23,.04);
  --sh-lg:0 8px 32px rgba(15,17,23,.10),0 2px 8px rgba(15,17,23,.06);
}
*,*::before,*::after{box-sizing:border-box;}
html,body,.stApp{background:var(--bg)!important;font-family:'Geist',sans-serif!important;color:var(--text)!important;}
#MainMenu,footer,.stDeployButton{display:none!important;}
header {background-color: transparent !important;}
section[data-testid="stSidebar"]{background:var(--surface)!important;border-right:1px solid var(--border)!important;}
section[data-testid="stSidebar"] *{color:var(--text)!important;}
section[data-testid="stSidebar"] .stMarkdown h3{font-family:'Geist Mono',monospace!important;font-size:0.58rem!important;letter-spacing:.15em!important;color:var(--muted)!important;text-transform:uppercase!important;border-bottom:1px solid var(--border)!important;padding-bottom:5px!important;margin-bottom:10px!important;}
div.stButton>button:first-child{background:var(--text)!important;color:#fff!important;font-family:'Geist Mono',monospace!important;font-size:0.72rem!important;font-weight:600!important;letter-spacing:.1em!important;text-transform:uppercase!important;border:none!important;border-radius:6px!important;padding:13px 24px!important;transition:all .18s ease!important;}
div.stButton>button:first-child:hover{background:var(--blue)!important;transform:translateY(-1px)!important;box-shadow:0 4px 16px rgba(37,99,235,.28)!important;}
div[data-testid="metric-container"]{background:var(--surface)!important;border:1px solid var(--border)!important;border-radius:8px!important;padding:12px!important;}
div[data-testid="metric-container"] label{font-family:'Geist Mono',monospace!important;font-size:.55rem!important;letter-spacing:.1em!important;text-transform:uppercase!important;color:var(--muted)!important;}
div[data-testid="metric-container"] div[data-testid="metric-value"]{font-family:'Geist Mono',monospace!important;font-size:1rem!important;font-weight:600!important;color:var(--text)!important;}
.stTabs [data-baseweb="tab-list"]{background:transparent!important;border-bottom:2px solid var(--border)!important;gap:0!important;}
.stTabs [data-baseweb="tab"]{font-family:'Geist Mono',monospace!important;font-size:.65rem!important;letter-spacing:.08em!important;font-weight:600!important;color:var(--muted)!important;padding:11px 20px!important;border-bottom:2px solid transparent!important;text-transform:uppercase!important;}
.stTabs [aria-selected="true"]{color:var(--text)!important;border-bottom:2px solid var(--text)!important;background:transparent!important;}
.stProgress>div>div{background:var(--text)!important;}
.stAlert{border-radius:6px!important;font-size:.8rem!important;}
::-webkit-scrollbar{width:4px;height:4px;}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px;}

.ticker-bar{background:#0f1117;color:#fff;border-radius:8px;padding:10px 20px;
  display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;
  margin-bottom:18px;font-family:'Geist Mono',monospace;font-size:.68rem;}
.t-lbl{color:rgba(255,255,255,.38);font-size:.54rem;letter-spacing:.1em;text-transform:uppercase;}
.t-val{color:#fff;font-weight:600;}

.pump-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;
  padding:18px 20px;margin-bottom:10px;transition:box-shadow .18s,border-color .18s;position:relative;overflow:hidden;}
.pump-card:hover{box-shadow:var(--sh-lg);border-color:var(--border2);}
.pump-card::before{content:'';position:absolute;top:0;left:0;width:4px;height:100%;border-radius:10px 0 0 10px;}
.pc-long::before{background:var(--green);}
.pc-short::before{background:var(--red);}

.score-ring{width:54px;height:54px;border-radius:50%;display:flex;align-items:center;justify-content:center;
  font-family:'Geist Mono',monospace;font-size:.95rem;font-weight:700;border:3px solid;flex-shrink:0;}

.px-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:10px 0;}
.px-cell{background:var(--panel);border-radius:6px;padding:8px 10px;text-align:center;}
.px-lbl{font-family:'Geist Mono',monospace;font-size:.52rem;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:3px;}
.px-val{font-family:'Geist Mono',monospace;font-size:.8rem;font-weight:600;color:var(--text);}

.sig-pips{display:flex;flex-wrap:wrap;gap:10px;margin:8px 0;}
.pip-item{display:flex;align-items:center;gap:5px;font-family:'Geist Mono',monospace;font-size:.6rem;color:var(--muted);}
.pip{width:7px;height:7px;border-radius:2px;}
.pip-on{background:var(--text);}
.pip-half{background:var(--border2);}
.pip-off{background:var(--panel);border:1px solid var(--border);}

.reasons-list .r{font-size:.74rem;color:var(--text2);padding:4px 0;border-bottom:1px solid var(--border);}

.tab-desc{background:var(--panel);border:1px solid var(--border);border-radius:8px;
  padding:12px 16px;margin-bottom:14px;font-size:.78rem;color:var(--text2);line-height:1.5;}

.stat-strip{background:var(--surface);border:1px solid var(--border);border-radius:8px;
  padding:12px 18px;display:flex;justify-content:space-between;align-items:center;
  flex-wrap:wrap;gap:10px;margin-bottom:14px;}
.ss-val{font-family:'Geist Mono',monospace;font-size:1.3rem;font-weight:700;color:var(--text);line-height:1;}
.ss-lbl{font-family:'Geist Mono',monospace;font-size:.52rem;letter-spacing:.1em;color:var(--muted);text-transform:uppercase;margin-top:3px;}

.empty-st{text-align:center;padding:50px 20px;color:var(--muted);font-family:'Geist Mono',monospace;font-size:.68rem;letter-spacing:.1em;}

.section-h{font-family:'Geist Mono',monospace;font-size:.58rem;letter-spacing:.18em;text-transform:uppercase;
  color:var(--muted);margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border);}

.stg-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px 20px;margin-bottom:14px;}
.stg-title{font-family:'Geist Mono',monospace;font-size:.62rem;font-weight:700;letter-spacing:.15em;
  text-transform:uppercase;color:var(--text);margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid var(--border);}
.hint{font-size:.72rem;color:var(--muted);line-height:1.4;margin-top:2px;padding:6px 10px;
  background:var(--panel);border-radius:5px;border-left:3px solid var(--border2);}
.hint b{color:var(--text2);}

.sentiment-bar{display:flex;align-items:center;gap:10px;margin:8px 0;padding:8px 12px;
  background:var(--panel);border-radius:6px;font-family:'Geist Mono',monospace;font-size:.62rem;}
.sbar-label{color:var(--muted);width:90px;flex-shrink:0;}
.sbar-track{flex:1;height:8px;background:var(--border);border-radius:4px;overflow:hidden;position:relative;}
.sbar-fill{height:100%;border-radius:4px;transition:width .3s;}
.sbar-val{color:var(--text);font-weight:600;width:40px;text-align:right;}

.whale-wall-card{border-radius:6px;padding:8px 12px;font-family:'Geist Mono',monospace;font-size:.72rem;margin:4px 0;
  display:flex;justify-content:space-between;align-items:center;}
.momentum-badge{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:4px;
  font-family:'Geist Mono',monospace;font-size:.58rem;font-weight:700;letter-spacing:.06em;}
</style>
""", unsafe_allow_html=True)


# ─── ENGINE ──────────────────────────────────────────────────────────────────
class PrePumpScreener:
    def __init__(self, cmc_key="",
                 okx_key="", okx_secret="", okx_passphrase="",
                 gate_key="", gate_secret=""):
        self.cmc_key = cmc_key
        # Binance/Bybit both blocked via CloudFront in PK server IPs.
        # OKX + MEXC + Gate.io: all confirmed no CloudFront geo-block.

        # ── OKX: deep liquidity, full sentiment API, accessible from PK ─────
        okx_params = {
            'enableRateLimit': True, 'rateLimit': 50,
            'timeout': 15000, 'options': {'defaultType': 'swap'}
        }
        if okx_key and okx_secret and okx_passphrase:
            okx_params['apiKey']   = okx_key
            okx_params['secret']   = okx_secret
            okx_params['password'] = okx_passphrase
        self.okx = ccxt.okx(okx_params)

        # ── MEXC: widest altcoin list (3000+ coins), accessible from PK ─────
        self.mexc = ccxt.mexc({
            'enableRateLimit': True, 'rateLimit': 60,
            'timeout': 15000, 'options': {'defaultType': 'swap'}
        })

        # ── Gate.io: large alt list, solid futures API, accessible from PK ──
        gate_params = {
            'enableRateLimit': True, 'rateLimit': 50,
            'timeout': 15000, 'options': {'defaultType': 'swap'}
        }
        if gate_key and gate_secret:
            gate_params['apiKey'] = gate_key
            gate_params['secret'] = gate_secret
        self.gate = ccxt.gateio(gate_params)

    async def fetch_ohlcv(self, exch, sym, tf, n=200):
        try:
            raw = await exch.fetch_ohlcv(sym, tf, limit=n)
            if not raw: return pd.DataFrame()
            df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
            df['ts'] = pd.to_datetime(df['ts'],unit='ms')
            return df
        except: return pd.DataFrame()

    async def fetch_btc(self):
        try:
            df = await self.fetch_ohlcv(self.okx, "BTC/USDT:USDT", "1h", 80)
            if df.empty:
                df = await self.fetch_ohlcv(self.gate, "BTC/USDT:USDT", "1h", 80)
            if df.empty:
                df = await self.fetch_ohlcv(self.mexc, "BTC/USDT:USDT", "1h", 80)
            if df.empty: return "NEUTRAL",0,50
            df.ta.ema(length=20,append=True); df.ta.ema(length=50,append=True); df.ta.rsi(length=14,append=True)
            e20=[c for c in df.columns if 'EMA_20' in c]
            e50=[c for c in df.columns if 'EMA_50' in c]
            rc=[c for c in df.columns if 'RSI' in c]
            if not e20 or not e50 or not rc: return "NEUTRAL",df['close'].iloc[-1],50
            p=df['close'].iloc[-1]; r=df[rc[0]].iloc[-1]
            if p<df[e20[0]].iloc[-1] and p<df[e50[0]].iloc[-1] and df[e20[0]].iloc[-1]<df[e50[0]].iloc[-1] and r<45:
                return "BEARISH",p,r
            elif p>df[e20[0]].iloc[-1] and p>df[e50[0]].iloc[-1] and df[e20[0]].iloc[-1]>df[e50[0]].iloc[-1] and r>55:
                return "BULLISH",p,r
            return "NEUTRAL",p,r
        except: return "NEUTRAL",0,50

    async def safe_fetch(self, exch, sym):
        fi={}; tick={}; ob={'bids':[],'asks':[]}
        try: fi = await exch.fetch_funding_rate(sym)
        except: fi={'fundingRate':0}
        try: tick = await exch.fetch_ticker(sym)
        except: pass
        try: ob = await exch.fetch_order_book(sym,limit=100)
        except: pass
        return fi, tick, ob

    async def fetch_oi(self, exch, sym):
        try:
            oi = await exch.fetch_open_interest(sym)
            v = oi.get('openInterestValue') or oi.get('openInterest') or 0
            return float(v)
        except: return 0.0

    async def fetch_funding_history(self, exch, sym):
        """Fetch funding rate history to detect squeeze building over time."""
        try:
            history = await exch.fetch_funding_rate_history(sym, limit=24)
            rates = [float(h.get('fundingRate', 0)) for h in history if h.get('fundingRate') is not None]
            return rates
        except:
            return []

    async def fetch_oi_history(self, exch, sym):
        """Fetch OI history to detect OI spikes vs just current level."""
        try:
            oi_hist = await exch.fetch_open_interest_history(sym, "1h", limit=24)
            values = [float(h.get('openInterestValue', 0) or h.get('openInterest', 0)) for h in oi_hist]
            return values
        except:
            return []

    async def fetch_recent_trades(self, exch, sym, min_usdt=50000):
        """Fetch recent trades to detect actual whale market orders (more reliable than OB walls)."""
        try:
            trades = await exch.fetch_trades(sym, limit=100)
            whale_buys = []
            whale_sells = []
            for t in trades:
                cost = float(t.get('cost', 0) or 0)
                side = t.get('side', '')
                price = float(t.get('price', 0) or 0)
                if cost >= min_usdt:
                    if side == 'buy':
                        whale_buys.append({'cost': cost, 'price': price})
                    elif side == 'sell':
                        whale_sells.append({'cost': cost, 'price': price})
            return whale_buys, whale_sells
        except:
            return [], []

    async def fetch_orderflow_imbalance(self, exch, sym, lookback=10):
        """Cumulative buy vs sell vol over last N 5m candles."""
        try:
            df = await self.fetch_ohlcv(exch, sym, "5m", lookback + 5)
            if df.empty or len(df) < lookback: return 0.0, 'NEUTRAL'
            r = df.tail(lookback)
            buy_vol  = float(r[r['close'] > r['open']]['volume'].sum())
            sell_vol = float(r[r['close'] <= r['open']]['volume'].sum())
            total = buy_vol + sell_vol
            if total == 0: return 0.0, 'NEUTRAL'
            buy_pct = buy_vol / total * 100
            direction = 'BUY' if buy_pct > 55 else ('SELL' if buy_pct < 45 else 'NEUTRAL')
            return buy_pct, direction
        except: return 0.0, 'NEUTRAL'

    async def fetch_liquidation_map(self, exch, sym, price):
        """
        Fetch real liq clusters from OKX public API.
        Price pulled toward dense liquidation zones.
        """
        clusters = []
        try:
            dsym_c = sym.split(':')[0].replace('/USDT', '')
            r = requests.get(
                "https://www.okx.com/api/v5/public/liquidation-orders",
                params={"instType": "SWAP", "instId": f"{dsym_c}-USDT-SWAP",
                        "state": "unfilled", "limit": "100"}, timeout=5)
            if r.status_code == 200:
                raw   = r.json().get('data', [])
                items = raw[0] if raw else []
                buckets = {}
                for item in items:
                    try:
                        liq_px = float(item.get('bkPx', 0))
                        liq_sz = float(item.get('sz', 0)) * liq_px
                        side   = 'SHORT_LIQ' if item.get('side') == 'sell' else 'LONG_LIQ'
                        if liq_px <= 0 or liq_sz < 10_000: continue
                        bucket = round(liq_px / price, 3)
                        key    = (bucket, side)
                        if key not in buckets:
                            buckets[key] = {'price': liq_px, 'side': side,
                                            'size_usd': 0, 'count': 0}
                        buckets[key]['size_usd'] += liq_sz
                        buckets[key]['count'] += 1
                    except: pass
                for (bkt, side), data in buckets.items():
                    if data['size_usd'] < 50_000: continue
                    dist = abs(data['price'] - price) / price * 100
                    clusters.append({**data, 'dist_pct': dist})
                clusters.sort(key=lambda x: x['dist_pct'])
        except: pass
        return clusters

    def fetch_new_listings(self, s):
        """
        Detect coins newly listed on OKX or Gate.io in last 7 days.
        New listings pump reliably. Cached 30 min.
        """
        cache = st.session_state.get('listing_cache', {})
        if cache.get('ts') and time.time() - cache['ts'] < 1800:
            return cache.get('data', [])
        listings = []; now = time.time()
        try:
            r = requests.get("https://www.okx.com/api/v5/public/instruments",
                params={"instType": "SWAP"}, timeout=7)
            if r.status_code == 200:
                cutoff_ms = (now - 7 * 86400) * 1000
                for inst in r.json().get('data', []):
                    try:
                        lt = float(inst.get('listTime', 0))
                        if lt >= cutoff_ms:
                            sym_c = inst['instId'].replace('-USDT-SWAP', '')
                            listings.append({'symbol': sym_c, 'exchange': 'OKX',
                                             'listed_ts': lt,
                                             'listed_ago_h': (now * 1000 - lt) / 3_600_000})
                    except: pass
        except: pass
        try:
            r2 = requests.get("https://fx-api.gateio.ws/api/v4/futures/usdt/contracts",
                timeout=7)
            if r2.status_code == 200:
                cutoff_s = now - 7 * 86400
                existing = {l['symbol'] for l in listings}
                for c in r2.json():
                    try:
                        ct = float(c.get('create_time', 0))
                        if ct >= cutoff_s:
                            sym_c = c['name'].replace('_USDT', '')
                            if sym_c not in existing:
                                listings.append({'symbol': sym_c, 'exchange': 'GATE',
                                                 'listed_ts': ct * 1000,
                                                 'listed_ago_h': (now - ct) / 3600})
                    except: pass
        except: pass
        st.session_state.listing_cache = {'ts': now, 'data': listings}
        return listings

    def fetch_onchain_whale(self, sym, s):
        """
        On-chain whale flow detection.
        Method 1: Whale Alert free tier (no key for basic data).
        Method 2: CoinGecko vol/price proxy (always works).
        """
        min_usd = s.get('onchain_whale_min', 500_000)
        cache   = st.session_state.get('onchain_cache', {})
        now     = time.time()
        if sym in cache and now - cache[sym].get('ts', 0) < 600:
            return cache[sym]
        result = {'available': False, 'signal': 'NEUTRAL', 'detail': '',
                  'inflow': 0, 'outflow': 0, 'ts': now}
        # Method 1: Whale Alert
        try:
            r = requests.get("https://api.whale-alert.io/v1/transactions",
                params={"api_key": "free", "min_value": str(int(min_usd)),
                        "currency": sym.lower().replace('1000', ''),
                        "limit": "20", "start": str(int(now - 3600))}, timeout=5)
            if r.status_code == 200:
                txns    = r.json().get('transactions', [])
                inflow  = sum(t.get('amount_usd', 0) for t in txns
                              if t.get('to', {}).get('owner_type') == 'exchange')
                outflow = sum(t.get('amount_usd', 0) for t in txns
                              if t.get('from', {}).get('owner_type') == 'exchange')
                if inflow + outflow >= min_usd:
                    net = outflow - inflow
                    result = {'available': True, 'ts': now,
                              'signal': 'BULLISH' if net > 0 else 'BEARISH',
                              'detail': f"ExchIn ${inflow/1e6:.1f}M | ExchOut ${outflow/1e6:.1f}M",
                              'inflow': inflow, 'outflow': outflow}
        except: pass
        # Method 2: CoinGecko proxy
        if not result['available']:
            try:
                CG = {'BTC':'bitcoin','ETH':'ethereum','SOL':'solana','BNB':'binancecoin',
                      'XRP':'ripple','ADA':'cardano','DOGE':'dogecoin','AVAX':'avalanche-2',
                      'LINK':'chainlink','DOT':'polkadot','MATIC':'matic-network',
                      'OP':'optimism','ARB':'arbitrum','SUI':'sui','APT':'aptos',
                      'PEPE':'pepe','WIF':'dogwifcoin','TON':'the-open-network'}
                cg_id = CG.get(sym.upper(), sym.lower())
                r2 = requests.get(f"https://api.coingecko.com/api/v3/coins/{cg_id}",
                    params={"localization": "false", "tickers": "false",
                            "market_data": "true", "developer_data": "false"}, timeout=6)
                if r2.status_code == 200:
                    md  = r2.json().get('market_data', {})
                    vol = float(md.get('total_volume', {}).get('usd', 0) or 0)
                    mcp = float(md.get('market_cap', {}).get('usd', 0) or 0)
                    c1h = float(md.get('price_change_percentage_1h_in_currency',
                                       {}).get('usd', 0) or 0)
                    vr  = vol / mcp if mcp > 0 else 0
                    if vr > 0.08 and abs(c1h) > 0.8:
                        sig = 'BULLISH' if c1h > 0 else 'BEARISH'
                        result = {'available': True, 'ts': now, 'signal': sig,
                                  'detail': f"Vol/MCap {vr:.2f}x | 1h {c1h:+.1f}% (CG proxy)",
                                  'inflow': 0, 'outflow': 0}
            except: pass
        if 'onchain_cache' not in st.session_state:
            st.session_state.onchain_cache = {}
        st.session_state.onchain_cache[sym] = result
        return result

    def fetch_sentiment_data(self, sym_base, exch_name):
        """
        Fetch sentiment data from Gate.io or OKX public APIs (no geo-restriction).
        sym_base for OKX = 'BTC-USDT-SWAP', for Gate = 'BTC_USDT'
        """
        result = {
            'top_long_pct': 50.0,
            'top_short_pct': 50.0,
            'retail_long_pct': 50.0,
            'taker_buy_pct': 50.0,
            'available': False,
            'source': ''
        }
        try:
            if exch_name == "GATE":
                # ── Gate.io Long/Short Ratio (public, no geo-block) ─────────
                # Gate.io futures stats endpoint — no auth needed
                ccy = sym_base.replace("-USDT-SWAP","").replace("USDT","").replace("_USDT","")
                r1 = requests.get(
                    f"https://fx-api.gateio.ws/api/v4/futures/usdt/contract_stats",
                    params={"contract": f"{ccy}_USDT", "interval": "1h", "limit": 8},
                    timeout=4
                )
                if r1.status_code == 200:
                    rows = r1.json()
                    if rows:
                        latest = rows[-1]
                        lsr = float(latest.get('lsr_account', 1.0) or 1.0)
                        long_pct = (lsr / (1 + lsr)) * 100
                        result['top_long_pct'] = long_pct
                        result['top_short_pct'] = 100 - long_pct
                        result['retail_long_pct'] = long_pct
                        result['available'] = True
                        result['source'] = 'Gate'
                # Taker volume from Gate futures
                r2 = requests.get(
                    f"https://fx-api.gateio.ws/api/v4/futures/usdt/trades",
                    params={"contract": f"{ccy}_USDT", "limit": 100},
                    timeout=4
                )
                if r2.status_code == 200:
                    trades = r2.json()
                    buy_vol = sum(abs(float(t.get('size',0))) for t in trades if float(t.get('size',0)) > 0)
                    sell_vol = sum(abs(float(t.get('size',0))) for t in trades if float(t.get('size',0)) < 0)
                    total = buy_vol + sell_vol
                    if total > 0:
                        result['taker_buy_pct'] = (buy_vol / total) * 100

            elif exch_name == "OKX":
                # ── OKX Long/Short Ratio ────────────────────────────────────
                # https://www.okx.com/docs-v5/en/#trading-statistics-rest-api-get-long-short-ratio
                r1 = requests.get(
                    "https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio",
                    params={"ccy": sym_base.replace("-USDT-SWAP", "").replace("USDT", ""),
                            "period": "1H"},
                    timeout=4
                )
                if r1.status_code == 200:
                    d = r1.json()
                    rows = d.get('data', [])
                    if rows:
                        latest = rows[0]
                        ls_ratio = float(latest[1])  # longShortRatio
                        long_pct = (ls_ratio / (ls_ratio + 1)) * 100
                        result['top_long_pct'] = long_pct
                        result['top_short_pct'] = 100 - long_pct
                        result['retail_long_pct'] = long_pct
                        result['available'] = True
                        result['source'] = 'OKX'

                # ── OKX Taker Volume ────────────────────────────────────────
                r2 = requests.get(
                    "https://www.okx.com/api/v5/rubik/stat/taker-volume",
                    params={"ccy": sym_base.replace("-USDT-SWAP", "").replace("USDT", ""),
                            "instType": "CONTRACTS", "period": "5m"},
                    timeout=4
                )
                if r2.status_code == 200:
                    d = r2.json()
                    rows = d.get('data', [])[:12]
                    if rows:
                        buy_vol  = sum(float(r[1]) for r in rows)
                        sell_vol = sum(float(r[2]) for r in rows)
                        total = buy_vol + sell_vol
                        if total > 0:
                            result['taker_buy_pct'] = (buy_vol / total) * 100

        except:
            pass
        return result

    def fetch_reddit_buzz(self, coin_sym, s):
        """
        Multi-source social buzz. 3 methods tried in order:
          1. Reddit public JSON (free, no key) — works if Colab IP not blocked
          2. CoinGecko community data (ALWAYS works — no key, no auth)
          3. Apify Reddit scraper (optional token, from cporter202 repo)
        Cached 5 min per coin so it doesn't slow down scans.
        """
        if not s.get('social_enabled', True):
            return {'mentions': 0, 'score': 0, 'available': False, 'source': ''}
        sym = coin_sym.upper().replace('1000','').replace('10000','')
        cache_key = sym
        now = time.time()
        cache = st.session_state.get('social_cache', {})
        if cache_key in cache and (now - cache[cache_key].get('ts', 0)) < 300:
            return cache[cache_key]

        result    = {'mentions': 0, 'score': 0, 'upvote_avg': 0,
                     'available': False, 'source': '', 'sentiment': 'NEUTRAL',
                     'top_post': '', 'ts': now}
        max_pts   = s.get('social_reddit_weight', 8)
        min_ment  = s.get('social_min_mentions', 3)
        buzz_thr  = s.get('social_buzz_threshold', 10)

        # METHOD 1 ── Reddit free public JSON ─────────────────────────────────
        try:
            subs = "CryptoCurrency+CryptoMoonShots+SatoshiStreetBets+altcoin+CryptoMarkets"
            rr = requests.get(
                f"https://www.reddit.com/r/{subs}/search.json",
                params={"q": sym, "sort": "new", "t": "hour", "limit": 25, "restrict_sr": "1"},
                headers={"User-Agent": "Mozilla/5.0 APEX/3.0"}, timeout=6)
            if rr.status_code == 200:
                posts = rr.json().get('data', {}).get('children', [])
                if posts:
                    mentions  = len(posts)
                    upvotes   = [p['data'].get('score', 0) for p in posts]
                    avg_up    = sum(upvotes) / max(1, len(upvotes))
                    bull_kw   = ['moon','pump','buy','bullish','launch','listing','breakout','gem','surge','ath']
                    bear_kw   = ['dump','crash','sell','bearish','scam','rug','dead','rekt','fraud','fail']
                    bull_h = 0; bear_h = 0
                    for p in posts:
                        txt = (p['data'].get('title','') + ' ' + p['data'].get('selftext','')).lower()
                        bull_h += sum(1 for w in bull_kw if w in txt)
                        bear_h += sum(1 for w in bear_kw if w in txt)
                    sent = ('BULLISH' if bull_h > bear_h + 1
                            else 'BEARISH' if bear_h > bull_h + 1 else 'NEUTRAL')
                    tp   = posts[0]['data'].get('title','')[:60]
                    sc   = min(max_pts, int((mentions/buzz_thr)*max_pts)) if mentions >= min_ment else 0
                    result = {'mentions': mentions, 'score': sc, 'upvote_avg': avg_up,
                              'available': True, 'source': 'Reddit', 'sentiment': sent,
                              'top_post': tp, 'ts': now}
        except: pass

        # METHOD 2 ── CoinGecko community data (always works, no auth) ────────
        if not result['available']:
            try:
                cg_map = {
                    'BTC':'bitcoin','ETH':'ethereum','SOL':'solana','BNB':'binancecoin',
                    'XRP':'ripple','ADA':'cardano','DOGE':'dogecoin','AVAX':'avalanche-2',
                    'LINK':'chainlink','DOT':'polkadot','MATIC':'matic-network','LTC':'litecoin',
                    'UNI':'uniswap','ATOM':'cosmos','XLM':'stellar','ALGO':'algorand',
                    'NEAR':'near','APT':'aptos','ARB':'arbitrum','OP':'optimism',
                    'INJ':'injective-protocol','SUI':'sui','TIA':'celestia','PEPE':'pepe',
                    'SHIB':'shiba-inu','WIF':'dogwifhat','FIL':'filecoin','FLOKI':'floki',
                    'SEI':'sei-network','JTO':'jito-governance-token','PYTH':'pyth-network',
                    'BONK':'bonk','MEME':'memecoin','ORDI':'ordinals','SATS':'1000-sats-ordinals',
                }
                cg_id = cg_map.get(sym, sym.lower())
                cg = requests.get(
                    f"https://api.coingecko.com/api/v3/coins/{cg_id}",
                    params={"localization":"false","tickers":"false","market_data":"true",
                            "community_data":"true","developer_data":"false"},
                    timeout=6)
                if cg.status_code == 200:
                    data = cg.json()
                    cd   = data.get('community_data', {})
                    md   = data.get('market_data', {})
                    subs    = cd.get('reddit_subscribers', 0) or 0
                    active  = cd.get('reddit_accounts_active_48h', 0) or 0
                    tw_foll = cd.get('twitter_followers', 0) or 0
                    # Price change as sentiment proxy
                    chg_1h  = md.get('price_change_percentage_1h_in_currency', {}).get('usd', 0) or 0
                    sent = ('BULLISH' if chg_1h > 1.5
                            else 'BEARISH' if chg_1h < -1.5 else 'NEUTRAL')
                    # active accounts / 50 = rough hourly mention estimate
                    mentions_proxy = min(50, int(active / 50)) if active else (2 if subs > 50000 else 0)
                    sc   = min(max_pts, int((mentions_proxy/buzz_thr)*max_pts)) if mentions_proxy >= min_ment else 0
                    top  = (f"r/ {subs:,} subs | {active:,} active 48h"
                            + (f" | Twitter {tw_foll:,}" if tw_foll else "")
                            + (f" | 1h chg {chg_1h:+.2f}%" if chg_1h else ""))
                    result = {'mentions': mentions_proxy, 'score': sc, 'upvote_avg': 0,
                              'available': True, 'source': 'CoinGecko', 'sentiment': sent,
                              'top_post': top[:80], 'ts': now}
            except: pass

        # METHOD 3 ── Apify (cporter202/social-media-scraping-apis) ──────────
        if not result['available'] and s.get('apify_token'):
            try:
                r3 = requests.post(
                    "https://api.apify.com/v2/acts/trudax~reddit-scraper-lite/run-sync-get-dataset-items",
                    json={"searches":[{"term":sym,"sort":"new","time":"hour"}],"maxItems":20},
                    headers={"Authorization": f"Bearer {s['apify_token']}"}, timeout=15)
                if r3.status_code == 200:
                    items    = r3.json()
                    mentions = len(items)
                    avg_up   = sum(i.get('score',0) for i in items) / max(1, mentions)
                    sc       = min(max_pts, int((mentions/buzz_thr)*max_pts)) if mentions >= min_ment else 0
                    result   = {'mentions': mentions, 'score': sc, 'upvote_avg': avg_up,
                                'available': True, 'source': 'Apify/Reddit',
                                'sentiment': 'NEUTRAL', 'top_post': '', 'ts': now}
            except: pass

        if 'social_cache' not in st.session_state:
            st.session_state.social_cache = {}
        st.session_state.social_cache[cache_key] = result
        return result

    def cmc_data(self, sym):
        if not self.cmc_key: return None
        try:
            r = requests.get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
                headers={"X-CMC_PRO_API_KEY":self.cmc_key},
                params={"symbol":sym.replace("1000","")},timeout=3)
            if r.status_code==200:
                d=r.json()['data']; coin=d[list(d.keys())[0]]; q=coin['quote']['USD']
                return {'rank':coin.get('cmc_rank',9999),'mcap':q.get('market_cap') or 0,
                        'vol24':q.get('volume_24h') or 0,'change24':q.get('percent_change_24h') or 0}
        except: pass
        return None

    def ob_score_calc(self, bids, asks, sig, s):
        try:
            def valid(levels):
                out = []
                for lv in levels:
                    try:
                        p,q = float(lv[0]),float(lv[1])
                        if p>0 and q>0: out.append((p,q))
                    except: pass
                return out
            b = valid(bids); a = valid(asks)
            if not b or not a: return 0,"",{'bid_pct':50,'ratio':1.0,'whale_bid_val':0,'whale_ask_val':0,'whale_bid_px':0,'whale_ask_px':0}

            bv=sum(p*q for p,q in b); av=sum(p*q for p,q in a)
            tot=bv+av; bid_pct=(bv/tot*100) if tot>0 else 50
            ratio=bv/av if av>0 else 1.0

            wb=max(b,key=lambda x:x[0]*x[1]); wa=max(a,key=lambda x:x[0]*x[1])
            wbv=wb[0]*wb[1]; wav=wa[0]*wa[1]

            score=0; msg=""
            if sig=="LONG":
                r=ratio
                if r>=s['ob_ratio_high']:   score=s['pts_ob_high']; msg=f"⚖️ OB {r:.2f}× bid-heavy — strong buy pressure ({bid_pct:.0f}% bid)"
                elif r>=s['ob_ratio_mid']:  score=s['pts_ob_mid'];  msg=f"⚖️ OB {r:.2f}× bid-heavy ({bid_pct:.0f}% bid)"
                elif r>=s['ob_ratio_low']:  score=s['pts_ob_low'];  msg=f"⚖️ OB {r:.2f}× slight bid pressure ({bid_pct:.0f}% bid)"
            else:
                inv=1/ratio if ratio>0 else 1
                if inv>=s['ob_ratio_high']:  score=s['pts_ob_high']; msg=f"⚖️ OB {inv:.2f}× ask-heavy — strong sell pressure ({100-bid_pct:.0f}% ask)"
                elif inv>=s['ob_ratio_mid']: score=s['pts_ob_mid'];  msg=f"⚖️ OB {inv:.2f}× ask-heavy ({100-bid_pct:.0f}% ask)"
                elif inv>=s['ob_ratio_low']: score=s['pts_ob_low'];  msg=f"⚖️ OB {inv:.2f}× slight ask pressure"

            return score, msg, {'bid_pct':bid_pct,'ratio':ratio,'whale_bid_val':wbv,'whale_ask_val':wav,'whale_bid_px':wb[0],'whale_ask_px':wa[0]}
        except: return 0,"",{'bid_pct':50,'ratio':1.0,'whale_bid_val':0,'whale_ask_val':0,'whale_bid_px':0,'whale_ask_px':0}

    def find_whale_walls(self, bids, asks, price, sig, s):
        """Enhanced whale wall detection: finds ALL walls, tiers by distance, stacks multiples."""
        wmin = s['whale_min_usdt']
        whale_sc = 0
        whale_details = []
        reasons_out = []

        def parse_walls(levels, side):
            walls = []
            for lv in levels:
                try:
                    p, q = float(lv[0]), float(lv[1])
                    val = p * q
                    if val >= wmin:
                        dist = abs(p - price) / price * 100
                        walls.append({'price': p, 'value': val, 'dist_pct': dist, 'side': side})
                except: pass
            return sorted(walls, key=lambda x: x['value'], reverse=True)

        bid_walls = parse_walls(bids, 'BID')
        ask_walls = parse_walls(asks, 'ASK')

        near = s.get('pts_whale_near', 25)
        mid  = s.get('pts_whale_mid', 15)
        far  = s.get('pts_whale_far', 8)

        if sig == "LONG" and bid_walls:
            w = bid_walls[0]
            if w['dist_pct'] <= 0.5:   whale_sc = near
            elif w['dist_pct'] <= 1.5: whale_sc = mid
            elif w['dist_pct'] <= 3.0: whale_sc = far
            if whale_sc > 0:
                whale_details.append({'side': 'BUY', 'value': w['value'], 'price': w['price'], 'dist_pct': w['dist_pct']})
                reasons_out.append(f"🐋 BUY WALL {fmt(w['value'])} @ ${w['price']:.6f} ({w['dist_pct']:.2f}% below)")
            if len(bid_walls) >= 3:
                whale_sc += 5
                reasons_out.append(f"🐋 {len(bid_walls)} stacked bid walls — layered buy support")

        elif sig == "SHORT" and ask_walls:
            w = ask_walls[0]
            if w['dist_pct'] <= 0.5:   whale_sc = near
            elif w['dist_pct'] <= 1.5: whale_sc = mid
            elif w['dist_pct'] <= 3.0: whale_sc = far
            if whale_sc > 0:
                whale_details.append({'side': 'SELL', 'value': w['value'], 'price': w['price'], 'dist_pct': w['dist_pct']})
                reasons_out.append(f"🐋 SELL WALL {fmt(w['value'])} @ ${w['price']:.6f} ({w['dist_pct']:.2f}% above)")
            if len(ask_walls) >= 3:
                whale_sc += 5
                reasons_out.append(f"🐋 {len(ask_walls)} stacked ask walls — layered sell resistance")

        # Construct whale_str for display
        whale_str = ""
        if whale_details:
            w = whale_details[0]
            whale_str = f"{'BUY' if w['side']=='BUY' else 'SELL'} {fmt(w['value'])} @ ${w['price']:.6f}"

        return whale_sc, whale_str, whale_details, reasons_out

    async def analyze(self, exch_name, exch_obj, sym, s, btc_trend):
        dsym = sym.split(':')[0].replace('/USDT','')
        if s['cooldown_on'] and is_on_cooldown(dsym, s['cooldown_hrs']): return None

        df_f = await self.fetch_ohlcv(exch_obj, sym, s['fast_tf'], 200)
        df_slow = await self.fetch_ohlcv(exch_obj, sym, s['slow_tf'], 100)
        fi, tick, ob = await self.safe_fetch(exch_obj, sym)
        oi = await self.fetch_oi(exch_obj, sym)

        if df_f.empty or df_slow.empty: return None
        price = float(tick.get('last',0) or 0)
        if price <= 0: return None

        # ─── Indicators ───────────────────────────────────────────────────
        try:
            df_slow.ta.ema(length=50,append=True)
            df_f.ta.rsi(length=14,append=True)
            df_f.ta.atr(length=14,append=True)
            df_f.ta.macd(fast=12,slow=26,signal=9,append=True)
            df_f.ta.bbands(length=20,std=2,append=True)

            e50_cols=[c for c in df_slow.columns if 'EMA_50' in c or ('EMA' in c and '50' in c)]
            rsi_cols=[c for c in df_f.columns if 'RSI_14' in c or ('RSI' in c)]
            atr_cols=[c for c in df_f.columns if 'ATRr_14' in c or ('ATRr' in c)]
            macd_cols=[c for c in df_f.columns if c.startswith('MACD_') and not c.startswith('MACDh') and not c.startswith('MACDs')]
            macds_cols=[c for c in df_f.columns if c.startswith('MACDs_')]
            bbl_cols=[c for c in df_f.columns if c.startswith('BBL_')]
            bbu_cols=[c for c in df_f.columns if c.startswith('BBU_')]

            if not e50_cols or not rsi_cols or not atr_cols: return None

            e50   = float(df_slow[e50_cols[0]].iloc[-1])
            rsi   = float(df_f[rsi_cols[0]].iloc[-1])
            atr   = float(df_f[atr_cols[0]].iloc[-1])
            macd  = float(df_f[macd_cols[0]].iloc[-1]) if macd_cols else 0
            macds = float(df_f[macds_cols[0]].iloc[-1]) if macds_cols else 0
            bbl   = float(df_f[bbl_cols[0]].iloc[-1]) if bbl_cols else price*0.97
            bbu   = float(df_f[bbu_cols[0]].iloc[-1]) if bbu_cols else price*1.03
            vma   = float(df_f['volume'].rolling(20).mean().iloc[-1])
            lvol  = float(df_f['volume'].iloc[-1])
            if vma == 0: vma = 1
        except:
            return None

        sig = "LONG" if float(df_slow['close'].iloc[-1]) > e50 else "SHORT"
        if s['btc_filter'] and btc_trend=="BEARISH" and sig=="LONG": return None

        # ─── ACCURACY GATE 1: Minimum liquidity filter ───────────────────────
        qv_now = float(tick.get('quoteVolume', 0) or 0)
        min_vol = s.get('min_vol_filter', 300_000)
        if min_vol > 0 and qv_now < min_vol: return None  # skip illiquid

        # ─── ACCURACY GATE 2: ATR / volatility sanity check ─────────────────
        atr_pct = (atr / price * 100) if price > 0 else 0
        if atr_pct < s.get('atr_min_pct', 0.2): return None   # flatline / dead coin
        if atr_pct > s.get('atr_max_pct', 10.0): return None  # too chaotic / rug risk

        # ─── ACCURACY GATE 3: Bid-ask spread filter ──────────────────────────
        spread_max = s.get('spread_max_pct', 0.5)
        if spread_max > 0:
            bids_raw = ob.get('bids', []); asks_raw = ob.get('asks', [])
            if bids_raw and asks_raw:
                best_bid = float(bids_raw[0][0]); best_ask = float(asks_raw[0][0])
                if best_bid > 0:
                    spread_pct = (best_ask - best_bid) / best_bid * 100
                    if spread_pct > spread_max: return None  # too wide = illiquid

        pump_score=0; reasons=[]; bd={}

        # 1 ── ORDER BOOK ─────────────────────────────────────────────────────
        ob_sc, ob_msg, ob_data = self.ob_score_calc(ob.get('bids',[]),ob.get('asks',[]),sig,s)
        pump_score+=ob_sc; bd['ob_imbalance']=ob_sc
        if ob_msg: reasons.append(ob_msg)

        # 2 ── WHALE WALLS (ENHANCED — distance tiered, stacked detection) ───
        whale_sc, whale_str, whale_details, whale_reasons = self.find_whale_walls(
            ob.get('bids',[]), ob.get('asks',[]), price, sig, s)
        pump_score += whale_sc
        bd['whale_wall'] = whale_sc
        reasons.extend(whale_reasons)

        # 3 ── FUNDING RATE ───────────────────────────────────────────────────
        fr=float(fi.get('fundingRate',0) or 0)
        fr_sc=0
        if sig=="LONG":
            if fr<=-s['funding_high']:     fr_sc=s['pts_funding_high']; reasons.append(f"⚡ Extreme neg funding {fr*100:.4f}% — shorts will be squeezed")
            elif fr<=-s['funding_mid']:    fr_sc=s['pts_funding_mid'];  reasons.append(f"⚡ Strong neg funding {fr*100:.4f}% — squeeze building")
            elif fr<=-s['funding_low']:    fr_sc=s['pts_funding_low'];  reasons.append(f"⚡ Neg funding {fr*100:.4f}% — mild short pressure")
            elif fr<0:                     fr_sc=3;                     reasons.append(f"⚡ Slightly neg funding {fr*100:.4f}%")
        else:
            if fr>=s['funding_high']:      fr_sc=s['pts_funding_high']; reasons.append(f"⚡ Extreme pos funding {fr*100:.4f}% — longs will be squeezed")
            elif fr>=s['funding_mid']:     fr_sc=s['pts_funding_mid'];  reasons.append(f"⚡ Strong pos funding {fr*100:.4f}% — squeeze building")
            elif fr>=s['funding_low']:     fr_sc=s['pts_funding_low'];  reasons.append(f"⚡ Pos funding {fr*100:.4f}%")
            elif fr>0:                     fr_sc=3;                     reasons.append(f"⚡ Slightly pos funding {fr*100:.4f}%")
        pump_score+=fr_sc; bd['funding']=fr_sc

        # 4 ── FUNDING HISTORY (sustained squeeze building) ───────────────────
        funding_history = await self.fetch_funding_history(exch_obj, sym)
        funding_hist_sc = 0
        if len(funding_history) >= 6:
            last_6 = funding_history[-6:]
            if sig == "LONG" and all(r < 0 for r in last_6):
                funding_hist_sc = 12
                reasons.append(f"⚡ Funding negative for 6+ consecutive periods — deep squeeze setup")
            elif sig == "SHORT" and all(r > 0 for r in last_6):
                funding_hist_sc = 12
                reasons.append(f"⚡ Funding positive for 6+ consecutive periods — prolonged long squeeze")
            elif sig == "LONG" and sum(1 for r in last_6 if r < 0) >= 4:
                funding_hist_sc = 6
                reasons.append(f"⚡ Funding mostly negative last 6 periods — squeeze building")
            elif sig == "SHORT" and sum(1 for r in last_6 if r > 0) >= 4:
                funding_hist_sc = 6
                reasons.append(f"⚡ Funding mostly positive last 6 periods — longs trapped")
        pump_score += funding_hist_sc
        bd['funding_hist'] = funding_hist_sc

        # 5 ── OPEN INTEREST ──────────────────────────────────────────────────
        oi_sc=0
        try: pc20=((price-float(df_f['close'].iloc[-20]))/float(df_f['close'].iloc[-20]))*100
        except: pc20=0

        # OI history: detect actual OI spike vs just level
        oi_history = await self.fetch_oi_history(exch_obj, sym)
        oi_change_6h = 0.0
        if len(oi_history) >= 6 and oi_history[-6] > 0:
            oi_change_6h = (oi_history[-1] - oi_history[-6]) / oi_history[-6] * 100

        if oi > 0:
            if sig=="LONG" and pc20>=s['oi_price_chg_high']:
                oi_sc=s['pts_oi_high']; reasons.append(f"📈 OI + price up {pc20:.1f}% — confirmed accumulation")
            elif sig=="LONG" and pc20>=s['oi_price_chg_low']:
                oi_sc=s['pts_oi_low'];  reasons.append(f"📈 OI growing, price +{pc20:.1f}%")
            elif sig=="SHORT" and pc20<=-s['oi_price_chg_high']:
                oi_sc=s['pts_oi_high']; reasons.append(f"📉 OI + price down {pc20:.1f}% — confirmed distribution")
            elif sig=="SHORT" and pc20<=-s['oi_price_chg_low']:
                oi_sc=s['pts_oi_low'];  reasons.append(f"📉 OI building on drop {pc20:.1f}%")
            # Bonus: OI spike in last 6h
            if abs(oi_change_6h) >= 20:
                oi_sc += 10; reasons.append(f"📈 OI spiked {oi_change_6h:+.1f}% in last 6h — new money entering NOW")
            elif abs(oi_change_6h) >= 10:
                oi_sc += 5; reasons.append(f"📈 OI up {oi_change_6h:+.1f}% in 6h")
        else:
            if sig=="LONG" and pc20>=s['oi_price_chg_high']:
                oi_sc=8; reasons.append(f"📈 Price up {pc20:.1f}% over 20 candles (OI unavailable)")
            elif sig=="SHORT" and pc20<=-s['oi_price_chg_high']:
                oi_sc=8; reasons.append(f"📉 Price down {pc20:.1f}% over 20 candles (OI unavailable)")
        pump_score+=oi_sc; bd['oi_spike']=oi_sc

        # 6 ── VOLUME SURGE ───────────────────────────────────────────────────
        vsurge=lvol/vma
        v_sc=0
        explosive_thresh = s.get('vol_surge_explosive', 5.0)
        if vsurge >= explosive_thresh:
            v_sc = s.get('pts_vol_explosive', 20)
            reasons.append(f"🚀 EXPLOSIVE volume {vsurge:.1f}×avg — institutional move NOW")
        elif vsurge>=s['vol_surge_high']:   v_sc=s['pts_vol_high']; reasons.append(f"🔥 Volume {vsurge:.1f}×avg — major activity NOW")
        elif vsurge>=s['vol_surge_mid']:    v_sc=s['pts_vol_mid'];  reasons.append(f"📊 Volume {vsurge:.1f}×avg — elevated")
        elif vsurge>=s['vol_surge_low']:    v_sc=s['pts_vol_low'];  reasons.append(f"📊 Volume {vsurge:.1f}×avg — above normal")
        pump_score+=v_sc; bd['vol_surge']=v_sc

        # 7 ── LIQUIDATION CLUSTERS ───────────────────────────────────────────
        liq_sc=0; liq_target=0; liq_detail=""
        try:
            top3=df_f.nlargest(3,'volume')
            for _,row in top3.iterrows():
                mid=(float(row['high'])+float(row['low']))/2
                dist=abs(price-mid)/price*100
                target=mid*1.02 if sig=="LONG" else mid*0.98
                if dist<=s['liq_cluster_near']:
                    liq_sc=s['pts_liq_near']; liq_target=target; liq_detail=f"${target:.6f}"
                    reasons.append(f"🧲 Liq cluster {dist:.1f}% away @ ${mid:.5f} — price magnet"); break
                elif dist<=s['liq_cluster_mid'] and liq_sc<s['pts_liq_mid']:
                    liq_sc=s['pts_liq_mid']
                elif dist<=s['liq_cluster_far'] and liq_sc<s['pts_liq_far']:
                    liq_sc=s['pts_liq_far']
        except: pass
        pump_score+=liq_sc; bd['liq_cluster']=liq_sc

        # 8 ── 24H VOLUME (CMC or exchange) ───────────────────────────────────
        cmc=self.cmc_data(dsym); vm_sc=0; vol_mcap_ratio=0
        if cmc and cmc.get('mcap',0)>0:
            vol_mcap_ratio=cmc['vol24']/cmc['mcap']
            if vol_mcap_ratio>1.0:    vm_sc=s['pts_vol24_high']+3; reasons.append(f"🌐 Vol/MCap {vol_mcap_ratio:.2f}× — extreme vs float")
            elif vol_mcap_ratio>0.5:  vm_sc=s['pts_vol24_high'];   reasons.append(f"🌐 Vol/MCap {vol_mcap_ratio:.2f}× — very high")
            elif vol_mcap_ratio>0.15: vm_sc=s['pts_vol24_mid'];    reasons.append(f"🌐 Vol/MCap {vol_mcap_ratio:.2f}×")
            elif vol_mcap_ratio>0.05: vm_sc=s['pts_vol24_low'];    reasons.append(f"🌐 Vol/MCap {vol_mcap_ratio:.2f}×")
        else:
            qv=float(tick.get('quoteVolume',0) or 0)
            if qv>=s['vol24h_high']:   vm_sc=s['pts_vol24_high']; reasons.append(f"📊 24h vol {fmt(qv)} — high activity")
            elif qv>=s['vol24h_mid']:  vm_sc=s['pts_vol24_mid'];  reasons.append(f"📊 24h vol {fmt(qv)}")
            elif qv>=s['vol24h_low']:  vm_sc=s['pts_vol24_low'];  reasons.append(f"📊 24h vol {fmt(qv)}")
        pump_score+=vm_sc; bd['vol_mcap']=vm_sc

        # 9 ── TECHNICALS ─────────────────────────────────────────────────────
        tech_sc=0
        if sig=="LONG" and macd>macds:    tech_sc+=s['pts_macd']; reasons.append("📊 MACD bullish cross")
        elif sig=="SHORT" and macd<macds:  tech_sc+=s['pts_macd']; reasons.append("📊 MACD bearish cross")
        if sig=="LONG" and rsi<s['rsi_oversold']:     tech_sc+=s['pts_rsi']; reasons.append(f"📉 RSI oversold {rsi:.1f}")
        elif sig=="SHORT" and rsi>s['rsi_overbought']: tech_sc+=s['pts_rsi']; reasons.append(f"📈 RSI overbought {rsi:.1f}")
        if sig=="LONG" and float(df_f['close'].iloc[-1])<=bbl:    tech_sc+=s['pts_bb']; reasons.append("🎯 Price at lower BB")
        elif sig=="SHORT" and float(df_f['close'].iloc[-1])>=bbu:  tech_sc+=s['pts_bb']; reasons.append("🎯 Price at upper BB")
        tech_sc+=s['pts_ema']; reasons.append(f"✅ EMA50 trend: {sig}")

        # 9b ── RSI DIVERGENCE (hidden bull/bear — high conviction signal) ────
        try:
            rsi_series  = df_f[rsi_cols[0]].dropna()
            close_series = df_f['close']
            if len(rsi_series) >= 10 and len(close_series) >= 10:
                # Look at last 10 candles: compare lows (for bull div) or highs (bear div)
                rsi_arr   = rsi_series.values[-10:]
                close_arr = close_series.values[-10:]
                # Bullish hidden divergence: price higher low, RSI lower low = trend continuation
                # Regular bullish divergence: price lower low, RSI higher low = reversal up
                p_low_now  = min(close_arr[-3:]);   p_low_prev  = min(close_arr[:5])
                r_low_now  = min(rsi_arr[-3:]);     r_low_prev  = min(rsi_arr[:5])
                p_high_now = max(close_arr[-3:]);   p_high_prev = max(close_arr[:5])
                r_high_now = max(rsi_arr[-3:]);     r_high_prev = max(rsi_arr[:5])
                div_pts = s.get('pts_divergence', 10)
                if sig == "LONG":
                    if p_low_now < p_low_prev and r_low_now > r_low_prev:
                        tech_sc += div_pts
                        reasons.append(f"📐 Bullish RSI divergence — price lower low, RSI higher low (reversal signal)")
                    elif p_low_now > p_low_prev and r_low_now < r_low_prev:
                        tech_sc += div_pts // 2
                        reasons.append(f"📐 Hidden bullish divergence — trend continuation likely")
                elif sig == "SHORT":
                    if p_high_now > p_high_prev and r_high_now < r_high_prev:
                        tech_sc += div_pts
                        reasons.append(f"📐 Bearish RSI divergence — price higher high, RSI lower high (reversal signal)")
                    elif p_high_now < p_high_prev and r_high_now > r_high_prev:
                        tech_sc += div_pts // 2
                        reasons.append(f"📐 Hidden bearish divergence — downtrend continuation likely")
        except: pass

        # 9c ── CANDLE PATTERN RECOGNITION ────────────────────────────────────
        try:
            pat_pts = s.get('pts_candle_pattern', 8)
            c0 = df_f.iloc[-1]; c1 = df_f.iloc[-2]; c2 = df_f.iloc[-3]
            body0 = float(c0['close']) - float(c0['open'])
            body1 = float(c1['close']) - float(c1['open'])
            rng0  = float(c0['high'])  - float(c0['low'])
            rng1  = float(c1['high'])  - float(c1['low'])
            if rng0 > 0 and rng1 > 0:
                if sig == "LONG":
                    # Hammer: small body at top, long lower wick (>60% of range)
                    lower_wick = min(float(c0['open']), float(c0['close'])) - float(c0['low'])
                    if lower_wick / rng0 > 0.6 and abs(body0) / rng0 < 0.3:
                        tech_sc += pat_pts
                        reasons.append("🔨 Hammer candle at support — buyers defended strongly")
                    # Bullish engulfing: current green candle body > previous red candle body
                    elif body0 > 0 and body1 < 0 and abs(body0) > abs(body1) * 1.1:
                        tech_sc += pat_pts
                        reasons.append("🕯️ Bullish engulfing — full reversal candle")
                    # 3 consecutive green candles with rising volume
                    elif (float(c0['close']) > float(c0['open']) and
                          float(c1['close']) > float(c1['open']) and
                          float(c2['close']) > float(c2['open'])):
                        if float(df_f['volume'].iloc[-1]) > float(df_f['volume'].iloc[-2]) > float(df_f['volume'].iloc[-3]):
                            tech_sc += pat_pts
                            reasons.append("📈 3 consecutive green candles with rising volume — strong accumulation")
                else:  # SHORT
                    # Shooting star: small body at bottom, long upper wick
                    upper_wick = float(c0['high']) - max(float(c0['open']), float(c0['close']))
                    if upper_wick / rng0 > 0.6 and abs(body0) / rng0 < 0.3:
                        tech_sc += pat_pts
                        reasons.append("🌠 Shooting star — sellers rejected rally hard")
                    # Bearish engulfing
                    elif body0 < 0 and body1 > 0 and abs(body0) > abs(body1) * 1.1:
                        tech_sc += pat_pts
                        reasons.append("🕯️ Bearish engulfing — full reversal candle")
                    # 3 consecutive red candles with rising volume
                    elif (float(c0['close']) < float(c0['open']) and
                          float(c1['close']) < float(c1['open']) and
                          float(c2['close']) < float(c2['open'])):
                        if float(df_f['volume'].iloc[-1]) > float(df_f['volume'].iloc[-2]) > float(df_f['volume'].iloc[-3]):
                            tech_sc += pat_pts
                            reasons.append("📉 3 consecutive red candles with rising volume — strong distribution")
        except: pass

        pump_score+=tech_sc; bd['technicals']=tech_sc

        # 10 ── SESSION ────────────────────────────────────────────────────────
        sname,smult,_=get_session()
        ses_sc=0
        if smult>=1.4: ses_sc=s['pts_session']; reasons.append(f"⏰ {sname} — peak session")
        elif smult>=1.3: ses_sc=max(1,s['pts_session']//2); reasons.append(f"⏰ {sname}")
        pump_score+=ses_sc; bd['session']=ses_sc

        # 11 ── MOMENTUM CONFIRMATION (new money confirmation) ────────────────
        candle_body = float(df_f['close'].iloc[-1]) - float(df_f['open'].iloc[-1])
        recent_momentum = float(df_f['close'].iloc[-1]) - float(df_f['close'].iloc[-3])
        momentum_confirmed = False
        mom_sc = 0
        if sig == "LONG" and candle_body > 0 and recent_momentum > 0:
            momentum_confirmed = True
            mom_sc = 8
            reasons.append(f"✅ Momentum confirmed: price moving UP last 3 candles ({recent_momentum/price*100:+.2f}%)")
        elif sig == "SHORT" and candle_body < 0 and recent_momentum < 0:
            momentum_confirmed = True
            mom_sc = 8
            reasons.append(f"✅ Momentum confirmed: price moving DOWN last 3 candles ({recent_momentum/price*100:+.2f}%)")
        pump_score += mom_sc
        bd['momentum'] = mom_sc

        # If require_momentum setting is on, skip non-confirmed signals
        if s.get('require_momentum', False) and not momentum_confirmed:
            return None

        # 11b ── MULTI-TIMEFRAME TREND CONFIRMATION ───────────────────────────
        mtf_sc = 0
        if s.get('mtf_confirm', True):
            try:
                # We already have: fast_tf (e.g. 15m), slow_tf (e.g. 4h)
                # Add medium TF (1h) for 3-way confirmation
                med_tf = "1h"
                df_med = await self.fetch_ohlcv(exch_obj, sym, med_tf, 60)
                if not df_med.empty:
                    df_med.ta.ema(length=50, append=True)
                    ema_cols_med = [c for c in df_med.columns if 'EMA_50' in c]
                    if ema_cols_med:
                        e50_med   = float(df_med[ema_cols_med[0]].iloc[-1])
                        close_med = float(df_med['close'].iloc[-1])
                        med_trend = "LONG" if close_med > e50_med else "SHORT"
                        # slow_tf trend already gives us the "higher TF" via sig
                        # fast_tf = momentum direction
                        fast_trend = "LONG" if candle_body > 0 else "SHORT"
                        aligned = sum(1 for t in [sig, med_trend, fast_trend] if t == sig)
                        if aligned == 3:
                            mtf_sc = s.get('pts_mtf', 12)
                            reasons.append(f"🎯 All 3 timeframes aligned {sig} — {s['fast_tf']}+1h+{s['slow_tf']} confirmation")
                        elif aligned == 2:
                            mtf_sc = s.get('pts_mtf', 12) // 2
                            reasons.append(f"🎯 2/3 timeframes aligned {sig}")
                        else:
                            # Conflicting timeframes — penalise
                            mtf_sc = -5
                            reasons.append(f"⚠️ Timeframe conflict — {s['fast_tf']} vs 1h vs {s['slow_tf']} disagree")
            except: pass
        pump_score += mtf_sc
        bd['mtf'] = mtf_sc

        # 12 ── SENTIMENT DATA (Gate.io or OKX public API — no geo-restriction) ──
        sentiment = {'top_long_pct':50,'top_short_pct':50,'retail_long_pct':50,'taker_buy_pct':50,'available':False,'source':''}
        sent_sc = 0
        if exch_name in ("GATE", "OKX"):
            if exch_name == "OKX":
                sym_base = dsym + "-USDT-SWAP"     # e.g. BTC-USDT-SWAP
            else:
                sym_base = dsym + "_USDT"          # Gate format: BTC_USDT
            sentiment = self.fetch_sentiment_data(sym_base, exch_name)
            if sentiment['available']:
                pts_sent = s.get('pts_sentiment', 20)
                pts_taker = s.get('pts_taker', 10)
                if sig == "LONG":
                    # Smart money shorting + retail long = squeeze fuel
                    if sentiment['top_short_pct'] > 65 and sentiment['retail_long_pct'] > 60:
                        sent_sc = pts_sent
                        reasons.append(f"🧠 Smart money {sentiment['top_short_pct']:.0f}% SHORT vs retail {sentiment['retail_long_pct']:.0f}% LONG — squeeze fuel loaded")
                    elif sentiment['top_short_pct'] > 55:
                        sent_sc = pts_sent // 2
                        reasons.append(f"🧠 Top traders {sentiment['top_short_pct']:.0f}% short — potential squeeze")
                    # Taker buy dominating = real buying pressure
                    if sentiment['taker_buy_pct'] > 62:
                        sent_sc += pts_taker
                        reasons.append(f"💚 Taker buy {sentiment['taker_buy_pct']:.0f}% — buyers aggressively entering")
                    elif sentiment['taker_buy_pct'] > 55:
                        sent_sc += pts_taker // 2
                        reasons.append(f"💚 Taker buy slightly dominant {sentiment['taker_buy_pct']:.0f}%")
                else:  # SHORT
                    if sentiment['top_long_pct'] > 65 and sentiment['retail_long_pct'] > 65:
                        sent_sc = pts_sent
                        reasons.append(f"🧠 Smart money {sentiment['top_long_pct']:.0f}% LONG + retail crowded long — distribution likely")
                    elif sentiment['top_long_pct'] > 55:
                        sent_sc = pts_sent // 2
                        reasons.append(f"🧠 Top traders {sentiment['top_long_pct']:.0f}% long — crowded trade")
                    if sentiment['taker_buy_pct'] < 38:
                        sent_sc += pts_taker
                        reasons.append(f"🔴 Taker sell {100-sentiment['taker_buy_pct']:.0f}% — sellers aggressively entering")
                    elif sentiment['taker_buy_pct'] < 45:
                        sent_sc += pts_taker // 2
                        reasons.append(f"🔴 Taker sell slightly dominant")
        pump_score += sent_sc
        bd['sentiment'] = sent_sc

        # 12b ── OI + FUNDING COMBO BONUS (the ultimate squeeze setup) ────────
        combo_sc = 0
        if (bd.get('oi_spike', 0) >= 10 and
            (bd.get('funding', 0) >= s.get('pts_funding_high', 25) or
             bd.get('funding_hist', 0) >= 12)):
            combo_sc = s.get('pts_oi_funding_combo', 10)
            reasons.append("💥 OI surge + extreme funding combo — maximum squeeze pressure")
        pump_score += combo_sc
        bd['oi_funding_combo'] = combo_sc

        # 13 ── SOCIAL MEDIA BUZZ (Reddit free public API — no key needed) ───
        social_data = self.fetch_reddit_buzz(dsym, s)
        social_sc = 0
        if social_data.get('available') and social_data['mentions'] >= s.get('social_min_mentions', 3):
            social_sc = social_data['score']
            reddit_emoji = "🚀" if social_data.get('sentiment') == 'BULLISH' else ("🩸" if social_data.get('sentiment') == 'BEARISH' else "💬")
            reasons.append(f"{reddit_emoji} {social_data.get('source','Reddit')}: {social_data['mentions']} mentions/hr | {social_data.get('sentiment','?')} | avg {social_data.get('upvote_avg',0):.0f} upvotes")
            if social_data.get('top_post'):
                reasons.append(f"📢 Top post: \"{social_data['top_post'][:55]}...\"")
        pump_score += social_sc
        bd['social_buzz'] = social_sc

        # 14 ── ORDER FLOW IMBALANCE ───────────────────────────────────────────
        of_pct, of_dir = await self.fetch_orderflow_imbalance(
            exch_obj, sym, s.get('orderflow_lookback', 10))
        of_sc  = 0
        pts_of = s.get('pts_orderflow', 12)
        sell_pct = 100 - of_pct
        if sig == 'LONG':
            if of_dir == 'BUY' and of_pct >= 65:
                of_sc = pts_of
                reasons.append(f"📊 Order flow {of_pct:.0f}% BUY last {s.get('orderflow_lookback',10)} candles — sustained accumulation")
            elif of_dir == 'BUY' and of_pct >= 58:
                of_sc = pts_of // 2
                reasons.append(f"📊 Order flow mildly bullish ({of_pct:.0f}% buy)")
        elif sig == 'SHORT':
            if of_dir == 'SELL' and sell_pct >= 65:
                of_sc = pts_of
                reasons.append(f"📊 Order flow {sell_pct:.0f}% SELL last {s.get('orderflow_lookback',10)} candles — sustained distribution")
            elif of_dir == 'SELL' and sell_pct >= 58:
                of_sc = pts_of // 2
                reasons.append(f"📊 Order flow mildly bearish ({sell_pct:.0f}% sell)")
        pump_score += of_sc
        bd['orderflow'] = of_sc

        # 15 ── LIQUIDATION MAP (OKX public data) ─────────────────────────────
        liq_map    = await self.fetch_liquidation_map(exch_obj, sym, price)
        liq_map_sc = 0
        pts_lm     = s.get('pts_liq_map', 15)
        if liq_map:
            nearest = liq_map[0]
            d       = nearest['dist_pct']
            sz_m    = nearest['size_usd'] / 1_000_000
            slbl    = nearest['side']
            em_lm   = "💥" if ((slbl == 'SHORT_LIQ' and sig == 'LONG') or
                               (slbl == 'LONG_LIQ'  and sig == 'SHORT')) else "🧲"
            if d <= 1.5:
                liq_map_sc = pts_lm
                reasons.append(f"{em_lm} Liq cluster {slbl} ${sz_m:.1f}M @ ${nearest['price']:.4f} ({d:.2f}% away)")
            elif d <= 3.0:
                liq_map_sc = pts_lm // 2
                reasons.append(f"🧲 Liq cluster {slbl} ${sz_m:.1f}M at {d:.1f}%")
        pump_score += liq_map_sc
        bd['liq_map'] = liq_map_sc

        # 16 ── EXCHANGE LISTING DETECTOR ─────────────────────────────────────
        new_listings = self.fetch_new_listings(s)
        listing_sc   = 0
        listing_info = {}
        pts_lst      = s.get('listing_alert_pts', 25)
        for lst in new_listings:
            if lst['symbol'].upper() == dsym.upper():
                h            = lst.get('listed_ago_h', 999)
                listing_info = lst
                if h <= 24:
                    listing_sc = pts_lst
                    reasons.append(f"🆕 BRAND NEW LISTING on {lst['exchange']} {h:.0f}h ago — high pump probability")
                elif h <= 72:
                    listing_sc = pts_lst // 2
                    reasons.append(f"🆕 Recent listing on {lst['exchange']} {h:.0f}h ago")
                elif h <= 168:
                    listing_sc = pts_lst // 4
                    reasons.append(f"🆕 Listed on {lst['exchange']} this week ({h:.0f}h ago)")
                break
        pump_score += listing_sc
        bd['listing'] = listing_sc

        # 17 ── ON-CHAIN WHALE FLOW ────────────────────────────────────────────
        onchain    = self.fetch_onchain_whale(dsym, s)
        onchain_sc = 0
        pts_oc     = s.get('pts_onchain_whale', 15)
        if onchain.get('available'):
            if onchain['signal'] == 'BULLISH' and sig == 'LONG':
                onchain_sc = pts_oc
                reasons.append(f"🐋 On-chain: {onchain['detail']} — leaving exchanges (accumulation)")
            elif onchain['signal'] == 'BEARISH' and sig == 'SHORT':
                onchain_sc = pts_oc
                reasons.append(f"🐋 On-chain: {onchain['detail']} — entering exchanges (sell pressure)")
            elif onchain['signal'] == 'BULLISH' and sig == 'SHORT':
                onchain_sc = -(pts_oc // 2)
                reasons.append(f"⚠️ On-chain bullish flow conflicts with SHORT — caution")
            elif onchain['signal'] == 'BEARISH' and sig == 'LONG':
                onchain_sc = -(pts_oc // 2)
                reasons.append(f"⚠️ On-chain bearish flow conflicts with LONG — caution")
        pump_score += onchain_sc
        bd['onchain'] = onchain_sc

        pump_score = min(pump_score, 100)

        # ─── ACCURACY GATE 4: Minimum signal categories ──────────────────────
        # Count how many distinct signal categories contributed non-zero score
        active_cats = sum(1 for k, v in bd.items() if v > 0 and k not in ('session', 'mtf'))
        min_cats = s.get('min_active_signals', 3)
        if active_cats < min_cats: return None  # single-category signals = noise

        if len(reasons) < s.get('min_reasons', 1): return None

        # ─── ACCURACY GATE 5: Fear & Greed dynamic threshold ─────────────────
        fng = st.session_state.get('fng_val', 50)
        if sig == "LONG" and fng < s.get('fng_long_threshold', 30):
            # Extreme fear — require higher score for longs (market falling hard)
            if pump_score < 60: return None
        if sig == "SHORT" and fng > s.get('fng_short_threshold', 70):
            # Extreme greed — require higher score for shorts
            if pump_score < 60: return None

        # ─── STRUCTURE-BASED TP/SL ───────────────────────────────────────────
        # Uses swing highs/lows from recent candles for real structure, not just ATR multiples
        try:
            recent_highs = df_f['high'].rolling(5).max().dropna()
            recent_lows  = df_f['low'].rolling(5).min().dropna()
            swing_high = float(recent_highs.iloc[-2]) if len(recent_highs) >= 2 else price * 1.03
            swing_low  = float(recent_lows.iloc[-2])  if len(recent_lows) >= 2 else price * 0.97

            if sig == "LONG":
                # SL: just below last swing low, or whale bid wall — whichever is tighter
                structure_sl = swing_low * 0.995
                whale_bid_px = whale_details[0]['price'] if whale_details and whale_details[0]['side']=='BUY' else 0
                whale_sl = whale_bid_px * 0.995 if whale_bid_px > 0 and whale_bid_px < price else structure_sl
                sl = max(structure_sl, whale_sl)  # tighter stop
                # Ensure SL is actually below price
                if sl >= price: sl = price - atr * 1.5

                # TP: swing high or minimum 2.5R, liq target if it exists
                sl_dist = price - sl
                min_rr_tp = price + sl_dist * max(s.get('min_rr', 1.5), 1.5)
                tp = max(swing_high, min_rr_tp)
                if liq_target > price: tp = min(tp, liq_target) if liq_target < tp else tp

            else:  # SHORT
                # SL: just above last swing high, or whale ask wall — whichever is tighter
                structure_sl = swing_high * 1.005
                whale_ask_px = whale_details[0]['price'] if whale_details and whale_details[0]['side']=='SELL' else 0
                whale_sl = whale_ask_px * 1.005 if whale_ask_px > 0 and whale_ask_px > price else structure_sl
                sl = min(structure_sl, whale_sl)  # tighter stop
                if sl <= price: sl = price + atr * 1.5

                sl_dist = sl - price
                min_rr_tp = price - sl_dist * max(s.get('min_rr', 1.5), 1.5)
                tp = min(swing_low, min_rr_tp)
                if liq_target > 0 and liq_target < price: tp = max(tp, liq_target)

        except:
            # Fallback to ATR-based
            tp = price + atr*3 if sig=="LONG" else price - atr*3
            sl = price - atr*1.5 if sig=="LONG" else price + atr*1.5

        # ─── MULTIPLE TP LEVELS based on structure + liquidity ─────────────
        try:
            sl_dist = abs(price - sl) if abs(price - sl) > 0 else atr
            if sig == "LONG":
                tp1 = price + sl_dist * 1.5        # TP1: quick scalp (1.5R)
                tp2 = tp                            # TP2: main swing high target
                # TP3: extended — liq sweep above swing high, or 4R min
                if liq_target > swing_high:
                    tp3 = liq_target
                else:
                    tp3 = price + sl_dist * 4.0
                tp3 = max(tp3, price + sl_dist * 3.0)
            else:  # SHORT
                tp1 = price - sl_dist * 1.5
                tp2 = tp
                if liq_target > 0 and liq_target < swing_low:
                    tp3 = liq_target
                else:
                    tp3 = price - sl_dist * 4.0
                tp3 = min(tp3, price - sl_dist * 3.0)
        except:
            sl_dist = atr * 1.5
            tp1 = price + atr*1.5 if sig=="LONG" else price - atr*1.5
            tp2 = price + atr*3   if sig=="LONG" else price - atr*3
            tp3 = price + atr*5   if sig=="LONG" else price - atr*5

        # ─── MINIMUM R:R FILTER (based on TP2, the main target) ─────────────
        try:
            rr = abs(tp - price) / abs(price - sl)
            if rr < s.get('min_rr', 1.5):
                return None  # Skip bad R:R setups
        except:
            pass

        result = {
            'symbol': dsym, 'exchange': exch_name, 'price': price, 'pump_score': pump_score,
            'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
            'type': sig, 'reasons': reasons, 'tp': tp, 'sl': sl, 'rsi': rsi,
            'funding': fr, 'atr': atr, 'ob': ob_data, 'cmc': cmc, 'oi': oi,
            'oi_change_6h': oi_change_6h,
            'liq_target': liq_target, 'liq_detail': liq_detail,
            'whale_str': whale_str, 'whale_details': whale_details,
            'vol_mcap': vol_mcap_ratio, 'signal_breakdown': bd,
            'session': sname, 'price_chg_20': pc20,
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'quote_vol': float(tick.get('quoteVolume',0) or 0),
            'sentiment': sentiment,
            'momentum_confirmed': momentum_confirmed,
            'funding_history': funding_history[-8:] if funding_history else [],
            'social_data': social_data,
            'atr_pct': round(atr_pct, 2),
            'pct_24h': float(tick.get('percentage') or tick.get('change') or 0),
            'vol_surge_ratio': round(vsurge, 2),
            'liq_map_data':   liq_map[:3] if liq_map else [],
            'listing_data':   listing_info,
            'onchain_data':   onchain,
            'orderflow_data': {'pct': round(of_pct, 1), 'dir': of_dir},
            # Entry zone: ideal limit-order range based on ATR + nearest OB
            'entry_lo': round(price - atr * 0.35, 8) if sig == 'LONG' else round(price + atr * 0.1, 8),
            'entry_hi': round(price + atr * 0.1,  8) if sig == 'LONG' else round(price + atr * 0.35, 8),
        }
        # Pass classifier thresholds into result so classify() can read them
        result['_cls_cfg'] = {
            'breakout_oi_min':  s.get('cls_breakout_oi_min', 7),
            'breakout_vol_min': s.get('cls_breakout_vol_min', 4),
            'breakout_sc_min':  s.get('cls_breakout_score_min', 25),
            'squeeze_fund_min': s.get('cls_squeeze_fund_min', 8),
            'squeeze_ob_min':   s.get('cls_squeeze_ob_min', 6),
        }
        result['cls'] = classify(result)
        if s['cooldown_on']: set_cooldown(dsym)
        return result

    async def run(self, s):
        btc_trend, btc_px, btc_rsi = await self.fetch_btc()

        # Load markets for all three exchanges concurrently
        await asyncio.gather(
            self.okx.load_markets(),
            self.mexc.load_markets(),
            self.gate.load_markets()
        )

        tickers_okx, tickers_mexc, tickers_gate = {}, {}, {}
        try: tickers_okx  = await self.okx.fetch_tickers()
        except: pass
        try: tickers_mexc = await self.mexc.fetch_tickers()
        except: pass
        try: tickers_gate = await self.gate.fetch_tickers()
        except: pass

        combined_tickers = {**tickers_okx, **tickers_mexc, **tickers_gate}
        process_journal_tracking(combined_tickers, s)

        # Build swap list — OKX overwrites MEXC/Gate for same coin (best liquidity)
        swaps_dict = {}
        for sym, t in tickers_mexc.items():
            if sym.endswith(':USDT') and t.get('quoteVolume'):
                pct = float(t.get('percentage') or t.get('change') or 0)
                swaps_dict[sym] = {'vol': float(t['quoteVolume'] or 0), 'pct': pct,
                                   'exch_name': 'MEXC', 'exch_obj': self.mexc}
        for sym, t in tickers_gate.items():
            if sym.endswith(':USDT') and t.get('quoteVolume'):
                pct = float(t.get('percentage') or t.get('change') or 0)
                swaps_dict[sym] = {'vol': float(t['quoteVolume'] or 0), 'pct': pct,
                                   'exch_name': 'GATE', 'exch_obj': self.gate}
        for sym, t in tickers_okx.items():
            if sym.endswith(':USDT') and t.get('quoteVolume'):
                pct = float(t.get('percentage') or t.get('change') or 0)
                swaps_dict[sym] = {'vol': float(t['quoteVolume'] or 0), 'pct': pct,
                                   'exch_name': 'OKX', 'exch_obj': self.okx}

        scan_mode  = s.get('scan_mode', 'mixed')
        depth      = s['scan_depth']

        if scan_mode == 'gainers':
            # Top 24h gainers — LONG setups most likely
            swaps_list = sorted(swaps_dict.items(), key=lambda x: x[1]['pct'], reverse=True)
        elif scan_mode == 'losers':
            # Top 24h losers — SHORT setups most likely
            swaps_list = sorted(swaps_dict.items(), key=lambda x: x[1]['pct'])
        elif scan_mode == 'mixed':
            # Half gainers + half losers — catch both directions
            by_gain = sorted(swaps_dict.items(), key=lambda x: x[1]['pct'], reverse=True)
            by_loss = sorted(swaps_dict.items(), key=lambda x: x[1]['pct'])
            half = depth // 2
            seen_m = set()
            mixed = []
            for item in by_gain[:half] + by_loss[:half]:
                if item[0] not in seen_m:
                    mixed.append(item); seen_m.add(item[0])
            swaps_list = mixed
        else:
            # Default: highest volume (most liquid, safest fills)
            swaps_list = sorted(swaps_dict.items(), key=lambda x: x[1]['vol'], reverse=True)

        symbols = swaps_list[:depth]

        results=[]; errors=[]
        pb=st.progress(0); st_=st.empty()
        for i, (sym, data) in enumerate(symbols):
            exch_name = data['exch_name']
            exch_obj  = data['exch_obj']
            st_.markdown(
                f"<span style='font-family:Geist Mono,monospace;font-size:.68rem;color:#7a82a0;'>"
                f"Scanning {i+1}/{len(symbols)} — <b>{sym.split(':')[0]}</b> ({exch_name})"
                f" — found so far: <b style='color:#0f1117'>{len(results)}</b></span>",
                unsafe_allow_html=True)
            try:
                r = await self.analyze(exch_name, exch_obj, sym, s, btc_trend)
                if r: results.append(r)
            except Exception as e:
                errors.append(f"{sym} ({exch_name}): {str(e)[:80]}")
            pb.progress((i+1)/len(symbols))
            await asyncio.sleep(0.5)

        st_.empty(); pb.empty()
        try:
            await self.okx.close()
            await self.mexc.close()
            await self.gate.close()
        except: pass

        results.sort(key=lambda x: x['pump_score'], reverse=True)

        # ─── DEDUP: Keep highest-scoring entry per base symbol ───────────────
        if S.get('dedup_symbols', True):
            seen = {}
            for r in results:
                sym_base = r['symbol']
                if sym_base not in seen or r['pump_score'] > seen[sym_base]['pump_score']:
                    seen[sym_base] = r
            results = sorted(seen.values(), key=lambda x: x['pump_score'], reverse=True)
        return results, btc_trend, btc_px, btc_rsi, errors


# ─── CARD ─────────────────────────────────────────────────────────────────────
def render_card(res, is_sniper=False):
    sc=res['pump_score']; col=pump_color(sc, is_sniper); lbl=pump_label(sc, res['type'], is_sniper)
    sig=res['type']; bd=res.get('signal_breakdown',{}); ob=res.get('ob',{})
    cmc=res.get('cmc') or {}; card_cls="pc-long" if sig=="LONG" else "pc-short"
    sig_col="var(--green)" if sig=="LONG" else "var(--red)"
    sentiment = res.get('sentiment', {})
    if not isinstance(sentiment, dict): sentiment = {}
    momentum_confirmed = res.get('momentum_confirmed', False)

    exch = res.get('exchange', 'MEXC')
    # Color coding per exchange
    exch_colors = {"OKX": "#00bcd4", "GATE": "#e040fb", "MEXC": "#2563eb"}
    exch_col = exch_colors.get(exch, "#2563eb")
    # Trade links
    if exch == "OKX":
        trade_link = f"https://www.okx.com/trade-swap/{res['symbol'].lower()}-usdt-swap"
    elif exch == "GATE":
        trade_link = f"https://www.gate.io/futures_trade/USDT/{res['symbol']}_USDT"
    else:
        trade_link = f"https://www.mexc.com/exchange/{res['symbol']}_USDT"

    def pip(v,lo=5,hi=12):
        if v>=hi: return "<span class='pip pip-on'></span>"
        if v>=lo: return "<span class='pip pip-half'></span>"
        return "<span class='pip pip-off'></span>"

    bid_pct = ob.get('bid_pct', 50)

    chg = cmc.get('change24', 0)
    cmc_html = ""
    if cmc:
        chg_c = "var(--green)" if chg>=0 else "var(--red)"
        cmc_html = f"""<div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:6px;">
          <span style="background:var(--panel);border:1px solid var(--border);border-radius:4px;padding:2px 7px;font-family:'Geist Mono',monospace;font-size:.6rem;color:var(--muted);">Rank #{cmc.get('rank','?')}</span>
          <span style="background:var(--panel);border:1px solid var(--border);border-radius:4px;padding:2px 7px;font-family:'Geist Mono',monospace;font-size:.6rem;color:var(--muted);">MCap {fmt(cmc.get('mcap',0))}</span>
          <span style="background:{'var(--green-bg)' if chg>=0 else 'var(--red-bg)'};border:1px solid {'var(--green-bd)' if chg>=0 else 'var(--red-bd)'};border-radius:4px;padding:2px 7px;font-family:'Geist Mono',monospace;font-size:.6rem;color:{chg_c};">{'+' if chg>=0 else ''}{chg:.2f}%</span></div>"""

    # Social buzz HTML — shown on every card, prominent when data available
    social_html = ""
    sd = res.get('social_data', {})
    if not isinstance(sd, dict): sd = {}
    if sd.get('available'):
        sent_cfg = {
            'BULLISH': ('#059669', '#ecfdf5', '#a7f3d0', '📈'),
            'BEARISH': ('#dc2626', '#fef2f2', '#fecaca', '📉'),
            'NEUTRAL': ('#d97706', '#fffbeb', '#fde68a', '💬'),
        }
        sc_color, sc_bg, sc_bd, sc_emoji = sent_cfg.get(sd.get('sentiment','NEUTRAL'), sent_cfg['NEUTRAL'])
        source         = sd.get('source', 'Social')
        mentions       = sd.get('mentions', 0)
        avg_up         = sd.get('upvote_avg', 0)
        social_sentiment = sd.get('sentiment', 'NEUTRAL')   # renamed — don't shadow the exchange sentiment dict
        top_post       = sd.get('top_post', '')

        # Mention bar width
        buzz_thr_card = S.get('social_buzz_threshold', 10) if 'S' in dir() else 10
        bar_pct = min(100, int((mentions / max(1, buzz_thr_card)) * 100))

        _tp60 = top_post[:60].replace("{","{{").replace("}","}}")
        top_line = (f'<div style="font-size:.6rem;color:{sc_color};margin-top:4px;opacity:.85;">'
                    f'"{_tp60}..."</div>') if top_post else ""
        avg_line = (f' &nbsp;·&nbsp; avg {avg_up:.0f} upvotes') if avg_up > 0 else ""

        social_html = (
            f'<div style="background:{sc_bg};border:1px solid {sc_bd};border-left:4px solid {sc_color};'
            f'border-radius:6px;padding:8px 12px;margin:6px 0;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;">'
            f'<span style="font-family:Geist Mono,monospace;font-size:.62rem;font-weight:700;color:{sc_color};">'
            f'{sc_emoji} {source} Buzz</span>'
            f'<span style="font-family:Geist Mono,monospace;font-size:.7rem;font-weight:700;color:{sc_color};">'
            f'{social_sentiment}</span>'
            f'</div>'
            f'<div style="display:flex;align-items:center;gap:8px;margin-top:5px;">'
            f'<div style="flex:1;height:4px;background:rgba(0,0,0,.08);border-radius:2px;">'
            f'<div style="width:{bar_pct}%;height:100%;background:{sc_color};border-radius:2px;"></div>'
            f'</div>'
            f'<span style="font-family:Geist Mono,monospace;font-size:.6rem;color:{sc_color};white-space:nowrap;">'
            f'{mentions} mentions/hr{avg_line}</span>'
            f'</div>'
            f'{top_line}'
            f'</div>'
        )

    # Whale walls HTML (enhanced — show top wall)
    whale_html = ""
    whale_details = res.get('whale_details', [])
    if whale_details:
        w = whale_details[0]
        wc = "var(--green)" if w['side']=='BUY' else "var(--red)"
        wbg = "var(--green-bg)" if w['side']=='BUY' else "var(--red-bg)"
        dist_badge = f"<span style='color:var(--muted);font-size:.58rem;'>({w['dist_pct']:.2f}% {'below' if w['side']=='BUY' else 'above'})</span>"
        whale_html = f"""<div style="background:{wbg};border-left:3px solid {wc};border-radius:6px;padding:7px 12px;font-family:'Geist Mono',monospace;font-size:.72rem;color:{wc};margin:6px 0;font-weight:600;display:flex;justify-content:space-between;align-items:center;">
          <span>🐋 {w['side']} WALL {fmt(w['value'])} @ ${w['price']:.6f}</span>
          {dist_badge}
        </div>"""

    liq_html = ""
    if res.get('liq_target', 0):
        liq_html = f"""<div style="background:var(--purple-bg);border-left:3px solid var(--purple);border-radius:6px;padding:7px 12px;font-size:.72rem;color:var(--purple);margin:6px 0;">🧲 Liq magnet @ {res['liq_detail']}</div>"""

    # Momentum badge
    mom_html = ""
    if momentum_confirmed:
        mom_col = "var(--green)" if sig=="LONG" else "var(--red)"
        mom_bg = "var(--green-bg)" if sig=="LONG" else "var(--red-bg)"
        mom_brd = mom_col.replace(")", "-bd)").replace("var(--green","var(--green").replace("var(--red","var(--red")
        mom_arrow = "▲" if sig=="LONG" else "▼"
        mom_html = f'<span class="momentum-badge" style="background:{mom_bg};color:{mom_col};border:1px solid {mom_brd};">{mom_arrow} MOMENTUM LIVE</span>'

    # Sentiment HTML (OKX and Gate.io coins)
    sentiment_html = ""
    if sentiment.get('available'):
        tl = sentiment['top_long_pct']
        ts = sentiment['top_short_pct']
        rl = sentiment['retail_long_pct']
        tb = sentiment['taker_buy_pct']

        def sbar(label, pct, color_hi, color_lo):
            c = color_hi if pct > 50 else color_lo
            return f"""<div class="sentiment-bar">
              <div class="sbar-label">{label}</div>
              <div class="sbar-track"><div class="sbar-fill" style="width:{pct:.0f}%;background:{c};"></div></div>
              <div class="sbar-val" style="color:{c};">{pct:.0f}%</div>
            </div>"""

        sentiment_html = f"""
        <div style="margin:8px 0;padding:10px;background:var(--panel);border-radius:8px;border:1px solid var(--border);">
          <div style="font-family:'Geist Mono',monospace;font-size:.55rem;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:6px;">{sentiment.get('source','Exchange')} Sentiment</div>
          {sbar("Longs", tl, "var(--green)", "var(--red)")}
          {sbar("Retail Long", rl, "var(--green)", "var(--red)")}
          {sbar("Taker Buy Vol", tb, "var(--green)", "var(--red)")}
        </div>"""

    # OI change badge
    oi_change = res.get('oi_change_6h', 0)
    oi_change_html = ""
    if abs(oi_change) >= 5:
        oi_c  = "var(--green)" if oi_change > 0 else "var(--red)"
        oi_bg = "var(--green-bg)" if oi_change > 0 else "var(--red-bg)"
        oi_bd = "var(--green-bd)" if oi_change > 0 else "var(--red-bd)"
        oi_change_html = (f'<span style="background:{oi_bg};color:{oi_c};border:1px solid {oi_bd};'
                          f'padding:2px 7px;border-radius:4px;font-family:Geist Mono,monospace;'
                          f'font-size:.6rem;font-weight:600;">OI {oi_change:+.1f}% 6h</span>')

    def _esc(s): return str(s).replace("{","{"+"{").replace("}","}"+"}")
    reasons_html = "".join([f"<div class='r'><span style='color:var(--muted);margin-right:6px;'>▸</span>{_esc(r)}</div>" for r in res['reasons'][:12]])

    # Pre-build all color strings with hex suffixes OUTSIDE the main f-string
    # to avoid Python misreading e.g. {col}14 as "format col with width 14"
    exch_bg   = exch_col + "22"
    exch_bd   = exch_col + "44"
    sig_bg    = sig_col + "18"
    sig_bd    = sig_col + "44"
    score_bg  = col + "14"
    sniper_cls = " sniper" if is_sniper else ""
    ts        = res.get('timestamp', '')
    session   = res.get('session', '')
    sym_cls   = res.get('cls', '—').upper()
    _price = float(res.get('price') or 0)
    _tp    = float(res.get('tp')    or 0)
    _sl    = float(res.get('sl')    or 0)
    _rsi   = float(res.get('rsi')   or 0)
    _tp1   = float(res.get('tp1') or _tp or 0)
    _tp2   = float(res.get('tp2') or _tp or 0)
    _tp3   = float(res.get('tp3') or _tp or 0)
    tp_pct = f"+{abs(_tp-_price)/_price*100:.2f}%" if _price > 0 and _tp > 0 else ""
    sl_pct = f"-{abs(_price-_sl)/_price*100:.2f}%" if _price > 0 and _sl > 0 else ""
    tp1_pct = f"+{abs(_tp1-_price)/_price*100:.2f}%" if _price > 0 and _tp1 > 0 else ""
    tp2_pct = f"+{abs(_tp2-_price)/_price*100:.2f}%" if _price > 0 and _tp2 > 0 else ""
    tp3_pct = f"+{abs(_tp3-_price)/_price*100:.2f}%" if _price > 0 and _tp3 > 0 else ""
    rsi_display   = f"{_rsi:.1f}"
    price_display = f"${_price:.6f}"
    tp_display    = f"${_tp:.6f}"
    tp1_display   = f"${_tp1:.6f}"
    tp2_display   = f"${_tp2:.6f}"
    tp3_display   = f"${_tp3:.6f}"
    sl_display    = f"${_sl:.6f}"
    pct_24h       = float(res.get('pct_24h', 0) or 0)
    pct_col       = "var(--green)" if pct_24h >= 0 else "var(--red)"
    pct_sign      = "+" if pct_24h >= 0 else ""
    pct_display   = f"{pct_sign}{pct_24h:.2f}%"
    # NEW badge & score-jump badge
    new_badge     = '<span style="background:#0ea5e9;color:#fff;font-size:.52rem;font-weight:700;padding:1px 6px;border-radius:3px;margin-right:4px;">🆕 NEW</span>' if res.get('is_new') else ''
    jump_sc       = res.get('score_jump', 0)
    jump_badge    = f'<span style="background:#f59e0b22;color:#f59e0b;font-size:.52rem;font-weight:700;padding:1px 6px;border-radius:3px;margin-right:4px;">⬆️ +{jump_sc}pt</span>' if jump_sc >= 15 else ''
    # Entry zone
    _elo_d        = f"${float(res.get('entry_lo') or 0):.6f}"
    _ehi_d        = f"${float(res.get('entry_hi') or 0):.6f}"
    try:
        rr = abs(_tp-_price)/abs(_price-_sl) if abs(_price-_sl) > 0 else 0
        rr_str = f"{rr:.1f}:1"
        rr_col = "var(--green)" if rr >= 2 else ("var(--amber)" if rr >= 1.5 else "var(--red)")
    except:
        rr_str = "N/A"; rr_col = "var(--muted)"

    try:
      card_html = f"""
<div class="pump-card {card_cls}">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;">
    <div style="display:flex;align-items:center;gap:12px;">
      <div class="score-ring{sniper_cls}" style="color:{col};border-color:{col};background:{score_bg};">{sc}</div>
      <div>
        <div style="font-family:'Geist Mono',monospace;font-size:1.2rem;font-weight:700;">{res['symbol']}</div>
        <div style="display:flex;align-items:center;gap:6px;margin-top:3px;flex-wrap:wrap;">
          <span style="background:{exch_bg};border:1px solid {exch_bd};color:{exch_col};padding:1px 7px;border-radius:3px;font-family:'Geist Mono',monospace;font-size:.58rem;font-weight:700;">{exch}</span>
          <span style="background:{sig_bg};border:1px solid {sig_bd};color:{sig_col};padding:1px 7px;border-radius:3px;font-family:'Geist Mono',monospace;font-size:.58rem;font-weight:700;">{sig}</span>
          <span style="font-family:'Geist Mono',monospace;font-size:.6rem;color:{col};font-weight:600;">{lbl}</span>
          {mom_html}
          {oi_change_html}
          {new_badge}{jump_badge}
          <span style="font-family:'Geist Mono',monospace;font-size:.58rem;color:var(--muted);">{ts}</span>
        </div>
      </div>
    </div>
    <div style="text-align:right;font-family:'Geist Mono',monospace;font-size:.58rem;color:var(--muted);">
      R:R <span style="color:{rr_col};font-weight:600;">{rr_str}</span><br>
      RSI <span style="color:var(--text);">{rsi_display}</span><br>
      {session}
    </div>
  </div>

  <div class="sig-pips">
    <div class="pip-item">{pip(bd.get('ob_imbalance',0),4,14)} OB</div>
    <div class="pip-item">{pip(bd.get('funding',0)+bd.get('funding_hist',0),3,15)} FUNDING</div>
    <div class="pip-item">{pip(bd.get('oi_spike',0),5,14)} OI</div>
    <div class="pip-item">{pip(bd.get('vol_surge',0),3,10)} VOLUME</div>
    <div class="pip-item">{pip(bd.get('liq_cluster',0),4,12)} LIQ</div>
    <div class="pip-item">{pip(bd.get('whale_wall',0),4,7)} WHALE</div>
    <div class="pip-item">{pip(bd.get('technicals',0),5,12)} TECH</div>
    <div class="pip-item">{pip(bd.get('sentiment',0),8,20)} SENT</div>
    <div class="pip-item">{pip(bd.get('momentum',0),4,8)} MOM</div>
    <div class="pip-item">{pip(bd.get('social_buzz',0),3,7)} SOC</div>
    <div class="pip-item">{pip(max(0,bd.get('mtf',0)),6,12)} MTF</div>
    <div class="pip-item">{pip(bd.get('orderflow',0),6,12)} FLOW</div>
    <div class="pip-item">{pip(bd.get('liq_map',0),7,15)} LIQ-MAP</div>
    <div class="pip-item">{pip(bd.get('listing',0),6,25)} LISTING</div>
    <div class="pip-item">{pip(max(0,bd.get('onchain',0)),7,15)} ONCHAIN</div>
  </div>

  <div style="display:flex;height:5px;border-radius:3px;overflow:hidden;margin:6px 0;background:var(--panel);">
    <div style="width:{bid_pct:.0f}%;background:var(--green);"></div>
    <div style="width:{100-bid_pct:.0f}%;background:var(--red);"></div>
  </div>
  <div style="display:flex;justify-content:space-between;font-family:'Geist Mono',monospace;font-size:.56rem;color:var(--muted);margin-bottom:8px;">
    <span>BID {bid_pct:.0f}%</span><span>ASK {100-bid_pct:.0f}%</span>
  </div>

  <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:6px;padding:6px 12px;"
       title="Best limit-order zone: wait for price to pull back into this range before entering">
    <span style="font-family:'Geist Mono',monospace;font-size:.55rem;color:#2563eb;font-weight:700;">📍 BEST ENTRY ZONE</span>
    <span style="font-family:'Geist Mono',monospace;font-size:.68rem;color:#1d4ed8;font-weight:700;margin-left:10px;">{_elo_d}</span>
    <span style="font-family:'Geist Mono',monospace;font-size:.55rem;color:#2563eb;margin:0 6px;">–</span>
    <span style="font-family:'Geist Mono',monospace;font-size:.68rem;color:#1d4ed8;font-weight:700;">{_ehi_d}</span>
    <span style="font-family:'Geist Mono',monospace;font-size:.5rem;color:#60a5fa;margin-left:8px;">don't chase — wait for pullback into zone</span>
  </div>
  <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin:8px 0;">
    <div class="px-cell">
      <div class="px-lbl">Entry</div>
      <div class="px-val" style="color:var(--blue);font-size:.72rem;">{price_display}</div>
      <div style="font-family:'Geist Mono',monospace;font-size:.5rem;color:{pct_col};">{pct_display} 24h</div>
    </div>
    <div class="px-cell" style="border-top:2px solid #86efac44;">
      <div class="px-lbl">TP 1 <span style="color:#86efac;font-size:.5rem;">scalp</span></div>
      <div class="px-val" style="color:#86efac;font-size:.68rem;">{tp1_display}</div>
      <div style="font-family:'Geist Mono',monospace;font-size:.5rem;color:var(--muted);">{tp1_pct}</div>
    </div>
    <div class="px-cell" style="border-top:2px solid var(--green);">
      <div class="px-lbl">TP 2 <span style="color:var(--green);font-size:.5rem;">target</span></div>
      <div class="px-val" style="color:var(--green);font-size:.72rem;">{tp2_display}</div>
      <div style="font-family:'Geist Mono',monospace;font-size:.5rem;color:var(--muted);">{tp2_pct}</div>
    </div>
    <div class="px-cell" style="border-top:2px solid #f59e0b;">
      <div class="px-lbl">TP 3 <span style="color:#f59e0b;font-size:.5rem;">max run</span></div>
      <div class="px-val" style="color:#f59e0b;font-size:.68rem;">{tp3_display}</div>
      <div style="font-family:'Geist Mono',monospace;font-size:.5rem;color:var(--muted);">{tp3_pct}</div>
    </div>
    <div class="px-cell" style="border-top:2px solid var(--red);">
      <div class="px-lbl">Stop Loss</div>
      <div class="px-val" style="color:var(--red);font-size:.72rem;">{sl_display}</div>
      <div style="font-family:'Geist Mono',monospace;font-size:.52rem;color:var(--muted);">{sl_pct}</div>
    </div>
  </div>

  {whale_html}{liq_html}{social_html}{cmc_html}
  {sentiment_html}
  <div class="reasons-list" style="margin-top:8px;">{reasons_html}</div>
  <div style="margin-top:10px;display:flex;gap:12px;align-items:center;">
    <a href="{trade_link}" target="_blank"
       style="font-family:'Geist Mono',monospace;font-size:.62rem;color:var(--blue);text-decoration:none;font-weight:600;">
      Trade {res['symbol']} on {exch} →
    </a>
    <span style="font-family:'Geist Mono',monospace;font-size:.58rem;color:var(--muted);">
      Class: <b style="color:var(--text);">{sym_cls}</b>
    </span>
  </div>
</div>"""
      # Strip leading whitespace — Streamlit treats 4+ space-indented lines
      # as markdown code blocks, showing raw HTML instead of rendering it.
      card_html = "\n".join(line.lstrip() for line in card_html.splitlines())
      st.markdown(card_html, unsafe_allow_html=True)
    except Exception as _ce:
        # Fallback: render a minimal safe card so page never goes blank
        _sym = res.get("symbol","?")
        _sc  = res.get("pump_score","?")
        _tp  = res.get("type","?")
        st.error(f"Card render error [{_sym}]: {_ce}")
        st.code(f"{_sym} | Score:{_sc} | {_tp} | Entry:{res.get('price',0):.6f}")


# ─── SIDEBAR NAV ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
<div style="padding:14px 0 18px;border-bottom:1px solid var(--border,#e2e5f0);margin-bottom:14px;">
  <div style="font-family:'Geist Mono',monospace;font-size:.62rem;font-weight:700;letter-spacing:.2em;color:#7a82a0;">⚡ APEX 2</div>
  <div style="font-family:'Geist Mono',monospace;font-size:.52rem;color:#9ca3af;margin-top:2px;letter-spacing:.1em;">Pump & Dump Intelligence</div>
</div>""", unsafe_allow_html=True)
    nav = st.radio("", ["🔥 Scanner","⚙️ Settings","📒 Journal"], label_visibility="collapsed")
    st.markdown("---")

    st.markdown("### ⚡ Quick Controls")
    q_depth = st.slider("Coins to Scan", 10, 100, S['scan_depth'], step=10, key="q_depth")
    q_min   = st.slider("Min display score", 1, 80, S['min_score'], key="q_min")
    q_btc   = st.toggle("BTC bear blocks LONGs", S['btc_filter'], key="q_btc")
    q_mom   = st.toggle("Require momentum confirm", S.get('require_momentum', False), key="q_mom")
    q_mode  = st.radio("Scan Focus",
        ["📊 Volume", "🚀 Gainers", "📉 Losers", "🔀 Mixed"],
        index=["volume","gainers","losers","mixed"].index(S.get('scan_mode','mixed')),
        horizontal=True, key="q_mode",
        help="Gainers=LONG bias · Losers=SHORT bias · Mixed=both · Volume=most liquid")
    _mode_map = {"📊 Volume":"volume","🚀 Gainers":"gainers","📉 Losers":"losers","🔀 Mixed":"mixed"}
    S['scan_mode'] = _mode_map.get(q_mode, 'mixed')

    st.markdown("---")
    st.markdown("### 🤖 Auto-Pilot 24/7")
    q_auto     = st.toggle("Enable Continuous Scan", S.get('auto_scan', False))
    q_auto_int = st.number_input("Scan interval (mins)", 1, 60, S.get('auto_interval', 5))
    if q_auto:
        st.caption("⚠️ Keep this browser tab open. Alerts fire via Discord/Telegram.")

    st.markdown("---")
    if st.button("Clear all cooldowns"):
        if os.path.exists(COOLDOWN_FILE): os.remove(COOLDOWN_FILE)
        st.success("Done")

    st.markdown("---")
    st.markdown("### Sentinel Mode")
    st.caption("Always-on: checks all coins instantly without waiting for your 10-min scan cycle.")
    q_sentinel = st.toggle("Enable Sentinel", st.session_state.get("sentinel_active", False))
    if q_sentinel != st.session_state.get("sentinel_active", False):
        st.session_state.sentinel_active = q_sentinel
        st.session_state.sentinel_total_checked = 0
        st.session_state.sentinel_signals_found = 0
        if q_sentinel: st.toast("Sentinel activated — watching 24/7", icon="🛰️")
    if st.session_state.get("sentinel_active"):
        chk = st.session_state.get("sentinel_total_checked", 0)
        sig = st.session_state.get("sentinel_signals_found", 0)
        last = st.session_state.get("sentinel_last_check", "?")
        st.markdown(f'''<div style="background:#0f1117;border-radius:6px;padding:8px 12px;margin-top:4px;">
        <div style="display:flex;justify-content:space-between;font-family:Geist Mono,monospace;">
        <div><div style="color:#fff;font-size:.82rem;font-weight:700;">{chk}</div><div style="color:#9ca3af;font-size:.5rem;">CHECKED</div></div>
        <div><div style="color:#ff4444;font-size:.82rem;font-weight:700;">{sig}</div><div style="color:#9ca3af;font-size:.5rem;">SIGNALS</div></div>
        <div><div style="color:#9ca3af;font-size:.6rem;">{last}</div></div>
        </div></div>''', unsafe_allow_html=True)

    sn, sm, sc_ = get_session()
    st.markdown(f"""<div style="background:#f7f8fc;border:1px solid #e2e5f0;border-radius:6px;padding:10px 12px;margin-top:10px;">
  <div style="font-family:'Geist Mono',monospace;font-size:.52rem;letter-spacing:.1em;text-transform:uppercase;color:#9ca3af;">Session</div>
  <div style="font-family:'Geist',sans-serif;font-size:.82rem;font-weight:600;color:{sc_};margin-top:3px;">{sn}</div>
  <div style="font-family:'Geist Mono',monospace;font-size:.58rem;color:#9ca3af;margin-top:2px;">{sm}× quality</div>
</div>""", unsafe_allow_html=True)

    st.markdown("---")
    with st.expander("📖 Terminology Guide"):
        st.markdown("""
**Alert Labels:**
* 🔥 **IMMINENT (70+)**: Extremely high confluence. Move starting NOW.
* ⚡ **BUILDING (45+)**: Big players accumulating/distributing. Strong setup.
* 📡 **EARLY (25+)**: First signs. Keep on watchlist.

**Classifications:**
* **Squeeze**: Funding extreme + OB imbalanced. Trapped traders about to be liquidated.
* **Breakout**: OI spiking with volume. New money entering aggressively.
* **Whale Driven**: Big wall + volume surge. Single large actor driving price.

**New Signals:**
* **MOMENTUM LIVE**: Price ALREADY moving in signal direction — highest conviction.
* **SENTIMENT**: OKX/Gate.io long/short ratio + taker buy volume (OKX & Gate coins).
* **OI 6h %**: Open interest change in last 6 hours — spike = new money entering NOW.
* **Funding Hist**: Funding negative/positive for 6+ consecutive periods = sustained squeeze building.
        """)


# ─── HEADER / TICKER ─────────────────────────────────────────────────────────
st.markdown("""<div style="padding:18px 0 14px;">
  <div style="font-family:'Geist Mono',monospace;font-size:1.5rem;font-weight:700;color:#0f1117;">APEX</div>
  <div style="font-family:'Geist Mono',monospace;font-size:.56rem;font-weight:400;letter-spacing:.16em;color:#7a82a0;text-transform:uppercase;margin-top:2px;">Pump & Dump Intelligence Terminal</div>
</div>""", unsafe_allow_html=True)

try:
    fg = requests.get("https://api.alternative.me/fng/?limit=1",timeout=3).json()
    st.session_state.fng_val = int(fg['data'][0]['value'])
    st.session_state.fng_txt = fg['data'][0]['value_classification']
except: pass

fng_v = st.session_state.fng_val; fng_t = st.session_state.fng_txt
fng_c = "#059669" if fng_v>=60 else ("#dc2626" if fng_v<=40 else "#d97706")
btc_c = "#059669" if st.session_state.btc_trend=="BULLISH" else ("#dc2626" if st.session_state.btc_trend=="BEARISH" else "#7a82a0")
sn_, _, sc_now = get_session()
st.markdown(f"""<div class="ticker-bar">
  <div><div class="t-lbl">BTC</div><div class="t-val">${st.session_state.btc_price:,.0f} <span style="color:{btc_c};">{st.session_state.btc_trend}</span></div></div>
  <div><div class="t-lbl">Fear &amp; Greed</div><div class="t-val" style="color:{fng_c};">{fng_v} — {fng_t.upper()}</div></div>
  <div><div class="t-lbl">Session</div><div class="t-val" style="color:{sc_now};">{sn_}</div></div>
  <div><div class="t-lbl">Last Scan</div><div class="t-val">{st.session_state.last_scan}</div></div>
  <div><div class="t-lbl">Raw found</div><div class="t-val">{st.session_state.last_raw_count}</div></div>
  <div><div class="t-lbl">After filter</div><div class="t-val">{len(st.session_state.results)}</div></div>
  <div><div class="t-lbl">Scans</div><div class="t-val">#{st.session_state.scan_count}</div></div>
  <div><div class="t-lbl">UTC</div><div class="t-val">{datetime.now(timezone.utc).strftime('%H:%M:%S')}</div></div>
</div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ═══════════════════════════════════════════════════════════════════════════
if nav == "⚙️ Settings":
    st.markdown('<div class="section-h">Settings — all thresholds and scoring weights</div>', unsafe_allow_html=True)
    st.caption("Changes save instantly and apply to the next scan.")

    with st.form("settings_form"):

        # ── ALERTS ──────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🔔 Notification Controls</div>', unsafe_allow_html=True)
        ac1, ac2, ac3 = st.columns(3)
        with ac1:
            ns_alert_score = st.slider("Minimum Score for Alerts", 10, 100, S.get('alert_min_score', 60))
        with ac2:
            st.markdown("**Types to Alert:**")
            ns_al_long  = st.checkbox("Alert on LONGs (Pumps)", S.get('alert_longs', True))
            ns_al_short = st.checkbox("Alert on SHORTs (Dumps)", S.get('alert_shorts', True))
        with ac3:
            st.markdown("**Classes to Alert:**")
            ns_al_sq = st.checkbox("About to Squeeze", S.get('alert_squeeze', True))
            ns_al_br = st.checkbox("Confirmed Breakout", S.get('alert_breakout', True))
            ns_al_wh = st.checkbox("Whale Driven", S.get('alert_breakout', True))
            ns_al_ea = st.checkbox("Early Signal", S.get('alert_early', False))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SCAN ────────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🔍 Scan Configuration</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_depth = st.slider("Coins to scan", 10, 200, S['scan_depth'], step=10)
            st.markdown('<div class="hint"><b>Higher</b> = more coins, slower scan. Start at 40.</div>', unsafe_allow_html=True)
        with c2:
            ns_fast = st.selectbox("Signal timeframe", ["1m","5m","15m","1h"], index=["1m","5m","15m","1h"].index(S['fast_tf']))
            st.markdown('<div class="hint">15m is the sweet spot. 1m = noisy, 1h = slow.</div>', unsafe_allow_html=True)
        with c3:
            ns_slow = st.selectbox("Trend timeframe", ["1h","4h","1d"], index=["1h","4h","1d"].index(S['slow_tf']))
            st.markdown('<div class="hint">EMA50 trend direction. 4h recommended.</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── FILTERS ─────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🎯 Filters & Quality Gates</div>', unsafe_allow_html=True)
        c1,c2,c3,c4,c5,c6=st.columns(6)
        with c1:
            ns_min = st.slider("Min score to show", 1, 80, S['min_score'])
        with c2:
            st.markdown("**Journal Tags**")
            ns_ji = st.checkbox("🔥 IMMINENT (70+)", S.get('j_imminent', True))
            ns_jb = st.checkbox("⚡ BUILDING (45+)", S.get('j_building', True))
            ns_je = st.checkbox("📡 EARLY (25+)", S.get('j_early', False))
        with c3:
            ns_minr = st.slider("Min signals", 1, 6, S.get('min_reasons', 1))
        with c4:
            ns_btc  = st.toggle("BTC Bear block", S['btc_filter'])
            ns_mom  = st.toggle("Require momentum", S.get('require_momentum', False))
        with c5:
            ns_cd  = st.toggle("Symbol cooldown", S['cooldown_on'])
            ns_cdh = st.slider("Cooldown hrs", 1, 24, S['cooldown_hrs']) if ns_cd else S['cooldown_hrs']
        with c6:
            ns_min_rr = st.slider("Min R:R ratio", 1.0, 4.0, float(S.get('min_rr', 1.5)), step=0.1)
            st.markdown('<div class="hint">Setups below this R:R are skipped. 1.5 = min, 2.0+ = high quality.</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── WHALE WALLS ─────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🐋 Whale Wall Detection (Enhanced)</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Walls are now tiered by distance from price. A wall 0.2% away is more significant than one 2.5% away. Multiple stacked walls add bonus points.</div>', unsafe_allow_html=True)
        c1,c2,c3,c4=st.columns(4)
        with c1:
            ns_whale = st.selectbox("Min wall size (USDT)",
                [50000,100000,250000,500000,1000000,5000000],
                index=[50000,100000,250000,500000,1000000,5000000].index(S['whale_min_usdt']),
                format_func=lambda x: f"${x:,.0f}")
        with c2:
            ns_pts_wh_near = st.slider("Points: wall ≤0.5% away", 10, 35, S.get('pts_whale_near', 25))
        with c3:
            ns_pts_wh_mid  = st.slider("Points: wall 0.5-1.5% away", 5, 25, S.get('pts_whale_mid', 15))
        with c4:
            ns_pts_wh_far  = st.slider("Points: wall 1.5-3% away", 3, 15, S.get('pts_whale_far', 8))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── ORDER BOOK ──────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">⚖️ Order Book Pressure</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Bid/Ask imbalance ratio. 1.5× = 1.5× more buy orders than sell orders. Most books: 0.9–1.3.</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_ob_l = st.number_input("Low threshold", 0.5, 3.0, S['ob_ratio_low'], 0.05, format="%.2f")
            ns_pts_ob_l = st.slider("Points for low", 1, 15, S['pts_ob_low'])
        with c2:
            ns_ob_m = st.number_input("Mid threshold", 0.5, 5.0, S['ob_ratio_mid'], 0.1, format="%.2f")
            ns_pts_ob_m = st.slider("Points for mid", 5, 20, S['pts_ob_mid'])
        with c3:
            ns_ob_h = st.number_input("High threshold", 1.0, 10.0, S['ob_ratio_high'], 0.25, format="%.2f")
            ns_pts_ob_h = st.slider("Points for high", 10, 30, S['pts_ob_high'])
        st.markdown('</div>', unsafe_allow_html=True)

        # ── FUNDING ─────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">⚡ Funding Rate Squeeze</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Negative funding = shorts pay longs = shorts will be squeezed. Now also scores if funding has been negative for 6+ consecutive periods (sustained squeeze building).</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_fr_l = st.number_input("Low threshold", 0.00001, 0.005, S['funding_low'], 0.00005, format="%.5f")
            ns_pts_fr_l = st.slider("Points low", 1, 15, S['pts_funding_low'], key="pfrl")
        with c2:
            ns_fr_m = st.number_input("Mid threshold", 0.0001, 0.01, S['funding_mid'], 0.0001, format="%.5f")
            ns_pts_fr_m = st.slider("Points mid", 5, 20, S['pts_funding_mid'], key="pfrm")
        with c3:
            ns_fr_h = st.number_input("High threshold", 0.0005, 0.05, S['funding_high'], 0.0005, format="%.5f")
            ns_pts_fr_h = st.slider("Points high", 10, 30, S['pts_funding_high'], key="pfrh")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── OI ──────────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">📈 Open Interest</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">OI history is now used to detect actual OI spikes (% change in last 6h). A 20%+ OI spike is much more meaningful than just current OI level.</div>', unsafe_allow_html=True)
        c1,c2,c3,c4=st.columns(4)
        with c1:
            ns_oi_l = st.number_input("Price chg low %", 0.1, 5.0, S['oi_price_chg_low'], 0.1, format="%.1f")
            ns_pts_oi_l = st.slider("Points low OI", 1, 15, S['pts_oi_low'], key="poil")
        with c2:
            ns_oi_h = st.number_input("Price chg high %", 0.5, 10.0, S['oi_price_chg_high'], 0.5, format="%.1f")
            ns_pts_oi_h = st.slider("Points high OI", 5, 25, S['pts_oi_high'], key="poih")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── VOLUME SURGE ────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🔥 Volume Surge</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Latest candle vs 20-candle average. 2× = double normal volume.</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_vs_l = st.number_input("Low multiplier", 1.0, 3.0, S['vol_surge_low'], 0.1, format="%.1f")
            ns_pts_vs_l = st.slider("Points low vol", 1, 10, S['pts_vol_low'], key="pvl")
        with c2:
            ns_vs_m = st.number_input("Mid multiplier", 1.5, 5.0, S['vol_surge_mid'], 0.25, format="%.1f")
            ns_pts_vs_m = st.slider("Points mid vol", 3, 15, S['pts_vol_mid'], key="pvm")
        with c3:
            ns_vs_h = st.number_input("High multiplier", 2.0, 10.0, S['vol_surge_high'], 0.5, format="%.1f")
            ns_pts_vs_h = st.slider("Points high vol", 5, 20, S['pts_vol_high'], key="pvh")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── LIQUIDITY CLUSTERS ──────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🧲 Liquidation Clusters</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_lq_n = st.number_input("Near % (max dist)", 0.5, 5.0, S['liq_cluster_near'], 0.25, format="%.2f")
            ns_pts_lq_n = st.slider("Points near", 5, 20, S['pts_liq_near'], key="pln")
        with c2:
            ns_lq_m = st.number_input("Mid %", 1.0, 10.0, S['liq_cluster_mid'], 0.5, format="%.1f")
            ns_pts_lq_m = st.slider("Points mid", 3, 15, S['pts_liq_mid'], key="plm")
        with c3:
            ns_lq_f = st.number_input("Far %", 2.0, 20.0, S['liq_cluster_far'], 1.0, format="%.1f")
            ns_pts_lq_f = st.slider("Points far", 1, 10, S['pts_liq_far'], key="plf")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── 24H VOLUME ──────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🌐 24h Volume Activity</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_v24_l = st.number_input("Low ($)", 100000, 10000000, S['vol24h_low'], 100000, format="%d")
            ns_pts_v24_l = st.slider("Points", 1, 8, S['pts_vol24_low'], key="pv24l")
        with c2:
            ns_v24_m = st.number_input("Mid ($)", 1000000, 50000000, S['vol24h_mid'], 1000000, format="%d")
            ns_pts_v24_m = st.slider("Points", 2, 12, S['pts_vol24_mid'], key="pv24m")
        with c3:
            ns_v24_h = st.number_input("High ($)", 5000000, 200000000, S['vol24h_high'], 5000000, format="%d")
            ns_pts_v24_h = st.slider("Points", 4, 15, S['pts_vol24_high'], key="pv24h")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── TECHNICALS ──────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">📊 Technical Indicators</div>', unsafe_allow_html=True)
        c1,c2,c3,c4,c5=st.columns(5)
        with c1:
            ns_rsi_os = st.slider("RSI oversold (LONG)", 20, 55, S['rsi_oversold'])
        with c2:
            ns_rsi_ob = st.slider("RSI overbought (SHORT)", 45, 80, S['rsi_overbought'])
        with c3:
            ns_pts_macd = st.slider("MACD pts", 1, 10, S['pts_macd'])
        with c4:
            ns_pts_rsi = st.slider("RSI pts", 1, 10, S['pts_rsi'])
        with c5:
            ns_pts_bb  = st.slider("BB pts", 1, 10, S['pts_bb'])
            ns_pts_ema = st.slider("EMA pts (base)", 1, 8, S['pts_ema'])
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SENTIMENT (OKX / Gate.io public API) ────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🧠 OKX / Gate Sentiment Scoring</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Uses OKX and Gate.io public APIs (no auth, no geo-block). Long/short ratio + taker buy volume.</div>', unsafe_allow_html=True)
        c1,c2=st.columns(2)
        with c1:
            ns_pts_sent  = st.slider("Smart money divergence pts", 5, 30, S.get('pts_sentiment', 20))
        with c2:
            ns_pts_taker = st.slider("Taker buy/sell pts", 3, 20, S.get('pts_taker', 10))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── ACCURACY & FILTERS ──────────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#059669;"><div class="stg-title" style="color:#059669;">🎯 Accuracy Filters — Cut False Positives</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">These gates run before scoring. Each one blocks low-quality signals at the door. Start conservative, tighten as needed.</div>', unsafe_allow_html=True)
        af1, af2, af3 = st.columns(3)
        with af1:
            ns_min_vol    = st.number_input("Min 24h volume ($)", 0, 5_000_000, int(S.get('min_vol_filter', 300_000)), 50_000, format="%d", help="Skip coins below this 24h volume — illiquid = unreliable signals")
            ns_min_sigs   = st.slider("Min signal categories", 1, 6, int(S.get('min_active_signals', 3)), help="Require signals from N+ different categories (OB, funding, OI, vol, tech…)")
            ns_dedup      = st.toggle("Dedup coins across exchanges", S.get('dedup_symbols', True), help="Show each coin once — keep highest score if same coin appears on multiple exchanges")
        with af2:
            ns_atr_min    = st.number_input("ATR min % (flatline filter)", 0.0, 2.0, float(S.get('atr_min_pct', 0.2)), 0.05, format="%.2f", help="Skip coins barely moving — ATR/price < this %")
            ns_atr_max    = st.number_input("ATR max % (chaos filter)", 2.0, 20.0, float(S.get('atr_max_pct', 10.0)), 0.5, format="%.1f", help="Skip coins too volatile — ATR/price > this %")
            ns_spread_max = st.number_input("Max spread % (0=off)", 0.0, 2.0, float(S.get('spread_max_pct', 0.5)), 0.05, format="%.2f", help="Skip coins with wide bid-ask spread — sign of low liquidity")
        with af3:
            ns_fng_lt     = st.slider("F&G LONG min (extreme fear)", 10, 45, int(S.get('fng_long_threshold', 30)), help="When F&G < this, require score ≥60 for LONG signals")
            ns_fng_st     = st.slider("F&G SHORT min (extreme greed)", 55, 90, int(S.get('fng_short_threshold', 70)), help="When F&G > this, require score ≥60 for SHORT signals")
            ns_mtf        = st.toggle("Multi-timeframe confirmation", S.get('mtf_confirm', True), help="Add/subtract points based on whether fast/mid/slow TFs agree")

        af4, af5 = st.columns(2)
        with af4:
            ns_pts_mtf    = st.slider("MTF alignment pts", 0, 20, int(S.get('pts_mtf', 12)))
            ns_pts_div    = st.slider("RSI divergence pts", 0, 20, int(S.get('pts_divergence', 10)))
        with af5:
            ns_pts_cpat   = st.slider("Candle pattern pts", 0, 15, int(S.get('pts_candle_pattern', 8)))
            ns_pts_combo  = st.slider("OI+Funding combo bonus pts", 0, 20, int(S.get('pts_oi_funding_combo', 10)))
            ns_vol_exp_t  = st.number_input("Explosive vol threshold (×avg)", 3.0, 20.0, float(S.get('vol_surge_explosive', 5.0)), 0.5, format="%.1f")
            ns_pts_vol_e  = st.slider("Explosive vol pts", 10, 30, int(S.get('pts_vol_explosive', 20)))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SENTINEL MODE ────────────────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#7c3aed;"><div class="stg-title" style="color:#7c3aed;">🛰️ Sentinel Mode — Always-On Background Scanner</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Instead of waiting 10 minutes, Sentinel continuously cycles through ALL coins one-by-one in the background. The moment any coin hits your threshold it fires an alert instantly. Enable it in the sidebar — no button click needed. Each Streamlit rerun checks a small batch so the page stays responsive.</div>', unsafe_allow_html=True)
        sc1, sc2, sc3 = st.columns(3)
        with sc1: ns_sent_score = st.slider("Alert threshold score", 40, 95, int(S.get('sentinel_score_threshold', 70)))
        with sc2: ns_sent_batch = st.slider("Coins per rerun batch", 2, 20, int(S.get('sentinel_batch_size', 5)))
        with sc3: ns_sent_interval = st.slider("Seconds between batches", 5, 120, int(S.get('sentinel_check_interval', 30)))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SOCIAL MEDIA SENTIMENT ───────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">📡 Social Media — Reddit Buzz Scoring (Free, No Key)</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Uses Reddit public JSON API — no account, no key, no cost. Monitors r/CryptoCurrency, r/CryptoMoonShots, r/SatoshiStreetBets for coin mentions in the last hour. Optionally paste your <b>Apify token</b> (from <a href="https://github.com/cporter202/social-media-scraping-apis" target="_blank">cporter202/social-media-scraping-apis</a>) for higher rate limits — free plan gives $5/month credits.</div>', unsafe_allow_html=True)
        so1, so2, so3, so4 = st.columns(4)
        with so1: ns_social_en = st.toggle("Enable Reddit Buzz", S.get('social_enabled', True))
        with so2: ns_social_wt = st.slider("Max Reddit score pts", 3, 20, int(S.get('social_reddit_weight', 8)))
        with so3: ns_social_min = st.slider("Min mentions to score", 1, 10, int(S.get('social_min_mentions', 3)))
        with so4: ns_social_buzz = st.slider("Mentions for max pts", 5, 50, int(S.get('social_buzz_threshold', 10)))
        ns_apify = st.text_input("Apify Token (optional)", S.get('apify_token', ''), type="password", help="Get free token at apify.com — $5 free credits/month")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── CLASSIFIER THRESHOLDS ─────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🏷️ Tab Classifier Thresholds</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:10px;">Control which coins go into each tab. Lower = more coins in tab. Raise = only stronger setups qualify.</div>', unsafe_allow_html=True)
        _ca, _cb, _cc = st.columns(3)
        with _ca:
            st.caption('🟡 Breakout')
            ns_br_oi  = st.slider('Min OI spike pts', 1, 20, int(S.get('cls_breakout_oi_min', 7)), key='br_oi')
            ns_br_vol = st.slider('Min Vol surge pts', 1, 15, int(S.get('cls_breakout_vol_min', 4)), key='br_vol')
            ns_br_sc  = st.slider('Min score', 10, 60, int(S.get('cls_breakout_score_min', 25)), key='br_sc')
        with _cb:
            st.caption('🔴 Squeeze')
            ns_sq_fd  = st.slider('Min Funding pts', 1, 20, int(S.get('cls_squeeze_fund_min', 8)), key='sq_fd')
            ns_sq_ob  = st.slider('Min OB imbalance pts', 1, 15, int(S.get('cls_squeeze_ob_min', 6)), key='sq_ob')
        with _cc:
            st.caption('ℹ️ Note')
            st.info('Coins not matching Squeeze/Breakout/Whale criteria fall into Early tab.')
        st.markdown('</div>', unsafe_allow_html=True)

        # ── AUTO-JOURNAL TRACKING ───────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#059669;"><div class="stg-title" style="color:#059669;">📒 Auto-Journal Exit Tracking</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Checks all ACTIVE journal trades every N minutes using live OKX/Gate prices. Fires Discord + Telegram the moment TP or SL is hit — fully automatic.</div>', unsafe_allow_html=True)
        aj1, aj2 = st.columns(2)
        with aj1:
            ns_aj_on   = st.toggle("Enable Auto-Journal Tracking", S.get('journal_autocheck_on', True))
            ns_aj_mins = st.slider("Check interval (minutes)", 1, 60, int(S.get('journal_autocheck_mins', 15)))
        with aj2:
            _last_j = st.session_state.get('journal_last_autocheck', 0)
            if _last_j:
                _next_j   = _last_j + S.get('journal_autocheck_mins', 15) * 60
                _secs_lft = max(0, int(_next_j - time.time()))
                st.metric("Next auto-check in", f"{_secs_lft//60}m {_secs_lft%60}s")
            else:
                st.info("Auto-check will run on next page load")
            # st.button cannot be used inside st.form — use a checkbox flag instead
            # Tick it and hit Save to force an immediate check on the next page load
            ns_aj_force = st.checkbox("🔄 Force check on Save", value=False,
                help="Tick this + hit Save to reset the timer and trigger an immediate journal check")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── INTELLIGENCE SIGNALS (TIER 1 & 2) ───────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🧠 Intelligence Signals — Tier 1 & 2</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Order flow imbalance · OKX liquidation map · Exchange listing detector (OKX + Gate) · On-chain whale flow (Whale Alert + CoinGecko)</div>', unsafe_allow_html=True)
        ti1, ti2, ti3 = st.columns(3)
        with ti1:
            ns_of_lb  = st.slider("Order flow lookback (candles)", 5, 30, int(S.get('orderflow_lookback', 10)))
            ns_pts_of = st.slider("Order flow pts", 3, 20, int(S.get('pts_orderflow', 12)))
        with ti2:
            ns_pts_lm  = st.slider("Liq map pts", 3, 25, int(S.get('pts_liq_map', 15)))
            ns_pts_lst = st.slider("New listing bonus pts", 5, 35, int(S.get('listing_alert_pts', 25)))
        with ti3:
            ns_oc_min  = st.number_input("On-chain whale min ($)", 100_000, 5_000_000, int(S.get('onchain_whale_min', 500_000)), 100_000, format="%d")
            ns_pts_oc  = st.slider("On-chain whale pts", 3, 25, int(S.get('pts_onchain_whale', 15)))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── APIs ────────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🔑 API Keys</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">All API keys are optional — the screener works without them using public endpoints. Keys unlock higher rate limits. <b>OKX + Gate.io + MEXC</b> replace Binance/Bybit (both CloudFront-blocked in PK).</div>', unsafe_allow_html=True)
        c1,c2=st.columns(2)
        with c1:
            ns_tg_tok     = st.text_input("Telegram Bot Token", S.get('tg_token',''), type="password")
            ns_tg_cid     = st.text_input("Telegram Chat ID", S.get('tg_chat_id',''))
            ns_okx_key    = st.text_input("OKX API Key (optional)", S.get('okx_key',''), type="password")
            ns_okx_sec    = st.text_input("OKX API Secret (optional)", S.get('okx_secret',''), type="password")
            ns_okx_pass   = st.text_input("OKX Passphrase (optional)", S.get('okx_passphrase',''), type="password")
        with c2:
            ns_dc_web     = st.text_input("Discord Webhook URL", S.get('discord_webhook',''), type="password")
            ns_cmc        = st.text_input("CMC API Key (optional)", S.get('cmc_key',''), type="password")
            ns_gate_key   = st.text_input("Gate.io API Key (optional)", S.get('gate_key',''), type="password")
            ns_gate_sec   = st.text_input("Gate.io API Secret (optional)", S.get('gate_secret',''), type="password")
        st.markdown('</div>', unsafe_allow_html=True)

        submitted = st.form_submit_button("💾  SAVE ALL SETTINGS", use_container_width=True)
        if submitted:
            new_s = {
                'scan_depth': ns_depth, 'fast_tf': ns_fast, 'slow_tf': ns_slow,
                'min_score': ns_min, 'j_imminent': ns_ji, 'j_building': ns_jb, 'j_early': ns_je,
                'min_reasons': ns_minr, 'btc_filter': ns_btc, 'require_momentum': ns_mom,
                'cooldown_on': ns_cd, 'cooldown_hrs': ns_cdh, 'whale_min_usdt': ns_whale,
                'min_rr': ns_min_rr,
                'alert_min_score': ns_alert_score, 'alert_longs': ns_al_long,
                'alert_shorts': ns_al_short, 'alert_squeeze': ns_al_sq,
                'alert_breakout': ns_al_br, 'alert_early': ns_al_ea,
                'ob_ratio_low': ns_ob_l, 'ob_ratio_mid': ns_ob_m, 'ob_ratio_high': ns_ob_h,
                'pts_ob_low': ns_pts_ob_l, 'pts_ob_mid': ns_pts_ob_m, 'pts_ob_high': ns_pts_ob_h,
                'funding_low': ns_fr_l, 'funding_mid': ns_fr_m, 'funding_high': ns_fr_h,
                'pts_funding_low': ns_pts_fr_l, 'pts_funding_mid': ns_pts_fr_m, 'pts_funding_high': ns_pts_fr_h,
                'oi_price_chg_low': ns_oi_l, 'oi_price_chg_high': ns_oi_h,
                'pts_oi_low': ns_pts_oi_l, 'pts_oi_high': ns_pts_oi_h,
                'vol_surge_low': ns_vs_l, 'vol_surge_mid': ns_vs_m, 'vol_surge_high': ns_vs_h,
                'pts_vol_low': ns_pts_vs_l, 'pts_vol_mid': ns_pts_vs_m, 'pts_vol_high': ns_pts_vs_h,
                'liq_cluster_near': ns_lq_n, 'liq_cluster_mid': ns_lq_m, 'liq_cluster_far': ns_lq_f,
                'pts_liq_near': ns_pts_lq_n, 'pts_liq_mid': ns_pts_lq_m, 'pts_liq_far': ns_pts_lq_f,
                'vol24h_low': ns_v24_l, 'vol24h_mid': ns_v24_m, 'vol24h_high': ns_v24_h,
                'pts_vol24_low': ns_pts_v24_l, 'pts_vol24_mid': ns_pts_v24_m, 'pts_vol24_high': ns_pts_v24_h,
                'rsi_oversold': ns_rsi_os, 'rsi_overbought': ns_rsi_ob,
                'pts_macd': ns_pts_macd, 'pts_rsi': ns_pts_rsi, 'pts_bb': ns_pts_bb, 'pts_ema': ns_pts_ema,
                'pts_session': S['pts_session'],
                'pts_sentiment': ns_pts_sent, 'pts_taker': ns_pts_taker,
                'pts_whale_near': ns_pts_wh_near, 'pts_whale_mid': ns_pts_wh_mid, 'pts_whale_far': ns_pts_wh_far,
                'min_vol_filter': ns_min_vol, 'min_active_signals': ns_min_sigs, 'dedup_symbols': ns_dedup,
                'atr_min_pct': ns_atr_min, 'atr_max_pct': ns_atr_max, 'spread_max_pct': ns_spread_max,
                'fng_long_threshold': ns_fng_lt, 'fng_short_threshold': ns_fng_st, 'mtf_confirm': ns_mtf,
                'pts_mtf': ns_pts_mtf, 'pts_divergence': ns_pts_div, 'pts_candle_pattern': ns_pts_cpat,
                'pts_oi_funding_combo': ns_pts_combo, 'vol_surge_explosive': ns_vol_exp_t, 'pts_vol_explosive': ns_pts_vol_e,
                'sentinel_score_threshold': ns_sent_score, 'sentinel_batch_size': ns_sent_batch, 'sentinel_check_interval': ns_sent_interval,
                'cls_breakout_oi_min': ns_br_oi, 'cls_breakout_vol_min': ns_br_vol,
                'cls_breakout_score_min': ns_br_sc, 'cls_squeeze_fund_min': ns_sq_fd,
                'cls_squeeze_ob_min': ns_sq_ob,
                'journal_autocheck_on': ns_aj_on, 'journal_autocheck_mins': ns_aj_mins,
                'orderflow_lookback': ns_of_lb, 'pts_orderflow': ns_pts_of,
                'pts_liq_map': ns_pts_lm, 'listing_alert_pts': ns_pts_lst,
                'onchain_whale_min': ns_oc_min, 'pts_onchain_whale': ns_pts_oc,
                'social_enabled': ns_social_en, 'social_reddit_weight': ns_social_wt, 'social_min_mentions': ns_social_min, 'social_buzz_threshold': ns_social_buzz, 'apify_token': ns_apify,
                'cmc_key': ns_cmc, 'tg_token': ns_tg_tok, 'tg_chat_id': ns_tg_cid,
                'discord_webhook': ns_dc_web,
                'okx_key': ns_okx_key, 'okx_secret': ns_okx_sec, 'okx_passphrase': ns_okx_pass,
                'gate_key': ns_gate_key, 'gate_secret': ns_gate_sec,
                'auto_scan': q_auto, 'auto_interval': q_auto_int,
            }
            save_settings(new_s)
            if ns_aj_force:
                st.session_state.journal_last_autocheck = 0  # force immediate check on next load
                st.success("✅ Settings saved! Journal check will run on next page load.")
            else:
                st.success("✅ Settings saved!")
            st.balloons()
    st.stop()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: JOURNAL
# ═══════════════════════════════════════════════════════════════════════════
if nav == "📒 Journal":
    ensure_journal()
    col1, col2 = st.columns([5, 1])
    with col1:
        st.markdown('<div class="section-h">Trade Journal & Tracking Dashboard</div>', unsafe_allow_html=True)
    with col2:
        if st.button("🗑️ Clear Journal"):
            if os.path.exists(JOURNAL_FILE): os.remove(JOURNAL_FILE)
            st.rerun()

    try: df_j = pd.read_csv(JOURNAL_FILE)
    except: df_j = pd.DataFrame()

    if df_j.empty:
        st.markdown('<div class="empty-st"><div style="font-size:2rem;opacity:.2;margin-bottom:10px;">📒</div>No signals logged yet</div>', unsafe_allow_html=True)
    else:
        df_j['ts'] = pd.to_datetime(df_j['ts'], errors='coerce')
        now = datetime.now()
        df_24h = df_j[df_j['ts'] >= (now - timedelta(hours=24))]

        longs    = len(df_24h[df_24h['type']=='LONG'])
        shorts   = len(df_24h[df_24h['type']=='SHORT'])
        tps      = len(df_24h[df_24h['status']=='TP'])
        sls      = len(df_24h[df_24h['status']=='SL'])
        active   = len(df_24h[df_24h['status']=='ACTIVE'])
        win_rate = (tps / (tps + sls) * 100) if (tps+sls) > 0 else 0

        st.markdown(f"""<div class="stat-strip">
          <div><div class="ss-val">{len(df_24h)}</div><div class="ss-lbl">24h Total</div></div>
          <div><div class="ss-val">{longs} / {shorts}</div><div class="ss-lbl">Longs / Shorts</div></div>
          <div><div class="ss-val" style="color:var(--amber);">{active}</div><div class="ss-lbl">Active</div></div>
          <div><div class="ss-val" style="color:var(--green);">{tps}</div><div class="ss-lbl">TP Hits</div></div>
          <div><div class="ss-val" style="color:var(--red);">{sls}</div><div class="ss-lbl">SL Hits</div></div>
          <div><div class="ss-val" style="color:var(--blue);">{win_rate:.1f}%</div><div class="ss-lbl">Win Rate</div></div>
        </div>""", unsafe_allow_html=True)

        fc1,fc2,fc3,fc4,fc5 = st.columns(5)
        with fc1: jt      = st.selectbox("Type", ["ALL","LONG","SHORT"])
        with fc2: jc      = st.selectbox("Class", ["ALL","squeeze","breakout","whale_driven","early"])
        with fc3: js_stat = st.selectbox("Status", ["ALL","ACTIVE","TP","SL"])
        with fc4: jx      = st.selectbox("Exchange", ["ALL","OKX","GATE","MEXC"])
        with fc5: js      = st.selectbox("Sort By", ["ts","pump_score","symbol"])

        dv = df_j.copy()
        if jt != "ALL" and 'type' in dv.columns:     dv = dv[dv['type']==jt]
        if jc != "ALL" and 'class' in dv.columns:    dv = dv[dv['class']==jc]
        if js_stat != "ALL" and 'status' in dv.columns: dv = dv[dv['status']==js_stat]
        if jx != "ALL" and 'exchange' in dv.columns: dv = dv[dv['exchange']==jx]
        if js in dv.columns: dv = dv.sort_values(js, ascending=(js!='pump_score'))

        st.dataframe(dv, use_container_width=True, height=500)
        st.download_button("⬇️ Export CSV", dv.to_csv(index=False).encode(),
            file_name=f"apex_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
    st.stop()


# ─── AUTO-JOURNAL CHECK ─────────────────────────────────────────────────────
autocheck_journal_background(S)

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: SCANNER
# ═══════════════════════════════════════════════════════════════════════════
eff_s = S.copy()
eff_s['scan_depth']      = q_depth
eff_s['min_score']       = q_min
eff_s['btc_filter']      = q_btc
eff_s['require_momentum'] = q_mom
eff_s['auto_scan']       = q_auto
eff_s['auto_interval']   = q_auto_int

col_btn, _ = st.columns([2, 5])
with col_btn: do_scan = st.button("⚡  RUN PUMP/DUMP SCAN", use_container_width=True)
if eff_s.get('auto_scan'): do_scan = True; time.sleep(0.3)

if do_scan:
    try:
        screener = PrePumpScreener(
            cmc_key=eff_s.get('cmc_key',''),
            okx_key=eff_s.get('okx_key',''),
            okx_secret=eff_s.get('okx_secret',''),
            okx_passphrase=eff_s.get('okx_passphrase',''),
            gate_key=eff_s.get('gate_key',''),
            gate_secret=eff_s.get('gate_secret','')
        )
        raw_results, btc_t, btc_p, btc_r, scan_errs = asyncio.run(screener.run(eff_s))
        st.session_state.last_raw_count = len(raw_results)
        # Snapshot previous scan for NEW-badge + re-alert detection
        st.session_state.prev_results = {
            f"{r['symbol']}_{r['type']}": r['pump_score']
            for r in st.session_state.results}
        st.session_state.results = [r for r in raw_results if r['pump_score'] >= eff_s['min_score']]
        st.session_state.last_scan  = datetime.now().strftime('%H:%M:%S')
        st.session_state.scan_count += 1
        st.session_state.btc_price  = btc_p
        st.session_state.btc_trend  = btc_t
        st.session_state.scan_errors = scan_errs

        for r in st.session_state.results:
            ak   = f"{r['symbol']}_{r['type']}_{datetime.now().hour}"
            lbl  = pump_label(r['pump_score'], r['type'])
            prev = st.session_state.prev_results
            sym_key = f"{r['symbol']}_{r['type']}"
            is_new_signal = sym_key not in prev  # wasn't in last scan at all
            prev_score    = prev.get(sym_key, 0)
            score_jumped  = (r['pump_score'] - prev_score) >= 15  # jumped 15+ pts
            r['is_new']   = is_new_signal
            r['score_jump'] = r['pump_score'] - prev_score if prev_score else 0
            # Re-alert key: use score bracket so a jump re-triggers even same hour
            score_bracket = r['pump_score'] // 15
            ak_recheck = f"{r['symbol']}_{r['type']}_{datetime.now().hour}_{score_bracket}"

            # JOURNAL LOGGING
            if ak not in st.session_state.logged_sigs:
                log_it = False
                if "GOD-TIER" in lbl: log_it = True  # always log god-tier
                elif "IMMINENT" in lbl and eff_s.get('j_imminent', True): log_it = True
                elif "BUILDING" in lbl and eff_s.get('j_building', True): log_it = True
                elif "EARLY" in lbl and eff_s.get('j_early', False): log_it = True
                if log_it: log_trade(r)
                st.session_state.logged_sigs.add(ak)

            # NOTIFICATION CHECK
            send_alert = False
            if r['pump_score'] >= eff_s.get('alert_min_score', 60):
                type_ok  = (r['type']=='LONG' and eff_s.get('alert_longs',True)) or (r['type']=='SHORT' and eff_s.get('alert_shorts',True))
                cls_ok   = (r['cls']=='squeeze'     and eff_s.get('alert_squeeze',True))  or \
                           (r['cls']=='breakout'     and eff_s.get('alert_breakout',True)) or \
                           (r['cls']=='whale_driven' and eff_s.get('alert_whale',True))    or \
                           (r['cls']=='early'        and eff_s.get('alert_early',False))   or \
                           r['cls'] not in ('squeeze','breakout','whale_driven','early')
                if type_ok and cls_ok: send_alert = True

            if send_alert:
                if 'alerted_sigs' not in st.session_state: st.session_state.alerted_sigs = set()
                _alert_key = ak_recheck  # score-bracketed key allows re-alert on big jumps
                if _alert_key not in st.session_state.alerted_sigs:
                    bd = r.get('signal_breakdown', {})
                    sentiment = r.get('sentiment', {})

                    def epip(v,lo=5,hi=12):
                        if v>=hi: return "🟩"
                        if v>=lo: return "🟨"
                        return "⬛"

                    pips_str = (f"OB {epip(bd.get('ob_imbalance',0),4,14)} | FD {epip(bd.get('funding',0)+bd.get('funding_hist',0),3,15)} | OI {epip(bd.get('oi_spike',0),5,14)}\n"
                                f"VOL {epip(bd.get('vol_surge',0),3,10)} | LQ {epip(bd.get('liq_cluster',0),4,12)} | WH {epip(bd.get('whale_wall',0),4,7)} | SENT {epip(bd.get('sentiment',0),8,20)}")

                    sent_line = ""
                    if sentiment.get('available'):
                        sent_line = f"\n📊 {sentiment.get('source','Exchange')} L/S: {sentiment['top_long_pct']:.0f}% / {sentiment['top_short_pct']:.0f}% | Taker Buy: {sentiment['taker_buy_pct']:.0f}%"

                    mom_line = "\n✅ MOMENTUM CONFIRMED — price already moving" if r.get('momentum_confirmed') else ""
                    reasons_str = "\n".join([f"▸ {rsn}" for rsn in r['reasons'][:6]])
                    rr_ratio = f"{abs(r['tp']-r['price'])/abs(r['price']-r['sl']):.1f}:1" if r.get('sl') else "N/A"
                    oi_ch = r.get('oi_change_6h', 0)
                    oi_line = f"\n📈 OI Change 6h: {oi_ch:+.1f}%" if abs(oi_ch) >= 5 else ""

                    # Build TP + entry strings
                    _tp1 = float(r.get('tp1') or r.get('tp') or 0)
                    _tp2 = float(r.get('tp2') or r.get('tp') or 0)
                    _tp3 = float(r.get('tp3') or r.get('tp') or 0)
                    _elo = float(r.get('entry_lo') or r['price'])
                    _ehi = float(r.get('entry_hi') or r['price'])
                    _new_flag  = '🆕 NEW SIGNAL\n' if is_new_signal else ''
                    _jump_flag = f'⬆️ Score jumped +{r["score_jump"]}pts\n' if score_jumped and prev_score else ''
                    # Telegram
                    if eff_s.get('tg_token') and eff_s.get('tg_chat_id'):
                        msg_tg = (f'{_new_flag}{_jump_flag}'
                                  f'<b>{lbl}: {r["symbol"]} ({r["cls"].upper()})</b>\n'
                                  f'━━━━━━━━━━━━━━━━━━\n'
                                  f'🏦 <b>Exch:</b> {r.get("exchange","MEXC")} | <b>Score:</b> {r["pump_score"]}/100 | <b>RSI:</b> {r.get("rsi",0):.1f}\n'
                                  f'📍 <b>Best Entry Zone:</b> ${_elo:.6f} – ${_ehi:.6f}\n'
                                  f'🎯 <b>TP1 (scalp):</b> ${_tp1:.6f}\n'
                                  f'✅ <b>TP2 (target):</b> ${_tp2:.6f}  (R:R {rr_ratio})\n'
                                  f'🚀 <b>TP3 (max run):</b> ${_tp3:.6f}\n'
                                  f'🛑 <b>SL:</b> ${r["sl"]:.6f}\n'
                                  f'{oi_line}{sent_line}{mom_line}\n\n'
                                  f'📊 <b>Signals:</b>\n{pips_str}\n\n'
                                  f'📝 <b>Key Drivers:</b>\n{reasons_str}')
                        send_tg(eff_s['tg_token'], eff_s['tg_chat_id'], msg_tg)

                    # Discord
                    if eff_s.get('discord_webhook'):
                        dc_color = 0x059669 if r['type']=='LONG' else 0xdc2626
                        _dc_flags = ('🆕 **NEW SIGNAL**\n' if is_new_signal else '') + (f'⬆️ **Score jumped +{r["score_jump"]}pts**\n' if score_jumped and prev_score else '')
                        stats_text = (_dc_flags +
                                      f'**Exchange:** {r.get("exchange","MEXC")} | **Score:** {r["pump_score"]}/100 | **RSI:** {r.get("rsi",0):.1f} | **R:R:** {rr_ratio}\n'
                                      f'📍 **Entry Zone:** `${_elo:.6f}` – `${_ehi:.6f}`\n'
                                      f'🎯 **TP1 (scalp):** `${_tp1:.6f}` | ✅ **TP2:** `${_tp2:.6f}` | 🚀 **TP3:** `${_tp3:.6f}`\n'
                                      f'🛑 **SL:** `${r["sl"]:.6f}`'
                                      + (f'\n**OI 6h:** {oi_ch:+.1f}%' if abs(oi_ch) >= 5 else '')
                                      + (f'\n**{sentiment.get("source","Exchange")} L/S:** {sentiment["top_long_pct"]:.0f}% Long / {sentiment["top_short_pct"]:.0f}% Short' if sentiment.get('available') else ''))
                        dc_embed = {
                            'title': f'{lbl}: {r["symbol"]} ({r["cls"].upper()})',
                            'color': dc_color,
                            'description': stats_text,
                            'fields': [
                                {'name': '📊 Signal Breakdown', 'value': pips_str, 'inline': False},
                                {'name': '📝 Top Reasons', 'value': reasons_str, 'inline': False}
                            ],
                            'footer': {'text': f'APEX Intelligence Terminal • {datetime.now(timezone.utc).strftime("%H:%M:%S")} UTC'}
                        }
                        send_discord(eff_s['discord_webhook'], dc_embed)

                    st.session_state.alerted_sigs.add(_alert_key)
                    _ch = []
                    if eff_s.get('discord_webhook'): _ch.append('Discord')
                    if eff_s.get('tg_token') and eff_s.get('tg_chat_id'): _ch.append('Telegram')
                    _jump_note = f' ⬆️ +{r["score_jump"]}pt jump' if score_jumped and prev_score else ''
                    _new_note  = ' 🆕 NEW SIGNAL' if is_new_signal else ''
                    if _ch:
                        st.toast(f'📨{_new_note}{_jump_note} {r["symbol"]} → {"+".join(_ch)}', icon='📨')
                    else:
                        st.toast(f'⚠️ {r["symbol"]} scored {r["pump_score"]} but no Discord/Telegram configured!', icon='⚠️')

    except Exception as e:
        if any(x in str(e) for x in ["510","429","too frequent"]): st.error("🚦 Rate limited — wait 60s")
        else: st.error(f"Error: {e}")


# ─── SENTINEL RUNNER ─────────────────────────────────────────────────────────
# Piggybacks on the main scan: after every auto-pilot scan, checks a fresh batch
# of coins NOT already in the main results for additional signals.
# This way Sentinel and Auto-Pilot work TOGETHER, not in competition.
if st.session_state.get('sentinel_active') and do_scan and st.session_state.scan_count > 0:
    sent_ph = st.empty()

    # Show "scanning…" banner immediately so user sees it's alive
    chk_now     = st.session_state.get('sentinel_total_checked', 0)
    sig_now     = st.session_state.get('sentinel_signals_found', 0)
    last_now    = st.session_state.get('sentinel_last_check', 'starting…')
    uni_now     = st.session_state.get('sentinel_universe_size', '?')
    sent_ph.markdown(
        f'<div style="background:#0f1117;border-radius:8px;padding:10px 18px;'
        f'margin-bottom:10px;display:flex;justify-content:space-between;'
        f'align-items:center;flex-wrap:wrap;gap:8px;border:1px solid #7c3aed44;">'
        f'<span style="color:#7c3aed;font-family:Geist Mono,monospace;'
        f'font-size:.65rem;font-weight:700;">🛰️ SENTINEL SCANNING…</span>'
        f'<span style="font-family:Geist Mono,monospace;font-size:.6rem;'
        f'color:#9ca3af;">{chk_now} checked / {uni_now} universe</span>'
        f'<span style="font-family:Geist Mono,monospace;font-size:.62rem;'
        f'color:#ff4444;font-weight:700;">{sig_now} signals found</span>'
        f'<span style="font-family:Geist Mono,monospace;font-size:.58rem;'
        f'color:#9ca3af;">Last: {last_now}</span>'
        '</div>',
        unsafe_allow_html=True)

    try:
        # ── ensure nest_asyncio is active — uvloop-safe ──────────────────────
        try:
            import nest_asyncio
            asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
            try:
                _loop = asyncio.get_event_loop()
                if _loop.is_closed():
                    _loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(_loop)
            except RuntimeError:
                _loop = asyncio.new_event_loop()
                asyncio.set_event_loop(_loop)
            nest_asyncio.apply(_loop)
        except Exception:
            pass

        s_screener = PrePumpScreener(
            cmc_key=eff_s.get('cmc_key',''), okx_key=eff_s.get('okx_key',''),
            okx_secret=eff_s.get('okx_secret',''), okx_passphrase=eff_s.get('okx_passphrase',''),
            gate_key=eff_s.get('gate_key',''), gate_secret=eff_s.get('gate_secret',''))

        async def sentinel_tick(screener, s):
            btc_trend, btc_px, _ = await screener.fetch_btc()
            # Load markets concurrently
            await asyncio.gather(
                screener.okx.load_markets(),
                screener.mexc.load_markets(),
                screener.gate.load_markets(),
                return_exceptions=True)
            # Fetch tickers concurrently
            tk_okx, tk_mexc, tk_gate = {}, {}, {}
            results_tickers = await asyncio.gather(
                screener.okx.fetch_tickers(),
                screener.mexc.fetch_tickers(),
                screener.gate.fetch_tickers(),
                return_exceptions=True)
            if not isinstance(results_tickers[0], Exception): tk_okx  = results_tickers[0]
            if not isinstance(results_tickers[1], Exception): tk_mexc = results_tickers[1]
            if not isinstance(results_tickers[2], Exception): tk_gate = results_tickers[2]

            # Build universe sorted by volume (high-vol coins move fastest)
            all_swaps = {}
            for sym, t in tk_mexc.items():
                if sym.endswith(':USDT') and t.get('quoteVolume'):
                    all_swaps[sym] = {'vol': float(t['quoteVolume'] or 0), 'exch_name': 'MEXC', 'exch_obj': screener.mexc}
            for sym, t in tk_gate.items():
                if sym.endswith(':USDT') and t.get('quoteVolume'):
                    all_swaps[sym] = {'vol': float(t['quoteVolume'] or 0), 'exch_name': 'GATE', 'exch_obj': screener.gate}
            for sym, t in tk_okx.items():
                if sym.endswith(':USDT') and t.get('quoteVolume'):
                    all_swaps[sym] = {'vol': float(t['quoteVolume'] or 0), 'exch_name': 'OKX', 'exch_obj': screener.okx}

            sorted_swaps = sorted(all_swaps.items(), key=lambda x: x[1]['vol'], reverse=True)
            uni_size  = len(sorted_swaps)
            checked   = st.session_state.get('sentinel_total_checked', 0)
            batch_sz  = max(1, s.get('sentinel_batch_size', 5))
            # Wrap-around cycling through full universe
            start     = checked % max(1, uni_size)
            end       = min(start + batch_sz, uni_size)
            coin_batch = sorted_swaps[start:end]

            new_sigs = []
            for sym, data in coin_batch:
                try:
                    r = await screener.analyze(data['exch_name'], data['exch_obj'], sym, s, btc_trend)
                    if r and r['pump_score'] >= s.get('sentinel_score_threshold', 70):
                        new_sigs.append(r)
                except Exception:
                    pass

            # Cleanup exchange connections
            for exch in [screener.okx, screener.mexc, screener.gate]:
                try: await exch.close()
                except: pass

            return new_sigs, btc_px, uni_size

        # Run the async tick — nest_asyncio makes this safe in Colab
        new_sigs, s_btc, total_uni = asyncio.run(sentinel_tick(s_screener, eff_s))

        # Update state counters
        st.session_state.sentinel_total_checked = (
            st.session_state.get('sentinel_total_checked', 0) +
            eff_s.get('sentinel_batch_size', 5))
        st.session_state.sentinel_last_check    = datetime.now().strftime('%H:%M:%S')
        st.session_state.sentinel_universe_size = total_uni
        st.session_state.btc_price              = s_btc

        # Fire alerts for new signals
        if new_sigs:
            st.session_state.sentinel_signals_found = (
                st.session_state.get('sentinel_signals_found', 0) + len(new_sigs))
            existing_syms = {r['symbol'] for r in st.session_state.results}
            if 'alerted_sigs' not in st.session_state:
                st.session_state.alerted_sigs = set()
            for r in new_sigs:
                st.toast(f"🛰️ {r['symbol']} — {r['pump_score']}/100 {r['type']}", icon="🚨")
                # Only add to results + alert once per symbol per hour
                ak = f"{r['symbol']}_{r['type']}_{datetime.now().strftime('%Y%m%d%H')}_sent"
                # Add to dedicated sentinel list (separate from manual scan results)
                if 'sentinel_results' not in st.session_state:
                    st.session_state.sentinel_results = []
                sent_syms = {x['symbol'] for x in st.session_state.sentinel_results}
                if r['symbol'] not in sent_syms:
                    st.session_state.sentinel_results.insert(0, r)
                    # Also log to journal
                    sent_lbl = pump_label(r['pump_score'], r['type'])
                    if "GOD-TIER" in sent_lbl or "IMMINENT" in sent_lbl:
                        log_trade(r)
                if ak not in st.session_state.alerted_sigs:
                    _p = float(r.get('price') or 0)
                    _t = float(r.get('tp') or 0)
                    _s = float(r.get('sl') or 0)
                    rr_r = f"{abs(_t-_p)/abs(_p-_s):.1f}:1" if abs(_p-_s) > 0 else "N/A"
                    rsns = "\n".join([f"• {x}" for x in r['reasons'][:5]])
                    if eff_s.get('tg_token') and eff_s.get('tg_chat_id'):
                        send_tg(eff_s['tg_token'], eff_s['tg_chat_id'],
                            f"<b>🛰️ [SENTINEL {r['pump_score']}/100] {r['symbol']}</b>\n"
                            f"{r.get('cls','').upper()} | {r['type']} | R:R {rr_r}\n"
                            f"Entry: ${_p:.6f}  TP: ${_t:.6f}  SL: ${_s:.6f}\n{rsns}")
                    if eff_s.get('discord_webhook'):
                        send_discord(eff_s['discord_webhook'], {
                            "title": f"🛰️ SENTINEL: {r['symbol']} ({r['pump_score']}/100)",
                            "color": 0x7c3aed,
                            "description": (
                                f"**{r['type']}** | **Class:** {r.get('cls','').upper()} | "
                                f"**R:R:** {rr_r}\n"
                                f"Entry: `${_p:.6f}` | TP: `${_t:.6f}` | SL: `${_s:.6f}`\n\n"
                                f"{rsns}"),
                            "footer": {"text": f"APEX Sentinel | {datetime.now(timezone.utc).strftime('%H:%M UTC')}"}})
                    st.session_state.alerted_sigs.add(ak)

        # Update status bar with fresh counts
        chk_f    = st.session_state.get('sentinel_total_checked', 0)
        sig_f    = st.session_state.get('sentinel_signals_found', 0)
        last_f   = st.session_state.get('sentinel_last_check', '?')
        pct_done = int((chk_f % max(1,total_uni)) / max(1,total_uni) * 100)
        sent_ph.markdown(
            f'<div style="background:#0f1117;border-radius:8px;padding:10px 18px;'
            f'margin-bottom:10px;border:1px solid #7c3aed66;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">'
            f'<span style="color:#7c3aed;font-family:Geist Mono,monospace;font-size:.65rem;font-weight:700;">🛰️ SENTINEL LIVE</span>'
            f'<span style="font-family:Geist Mono,monospace;font-size:.6rem;color:#9ca3af;">{chk_f} checked / {total_uni} universe ({pct_done}% cycle)</span>'
            f'<span style="font-family:Geist Mono,monospace;font-size:.62rem;color:#ff4444;font-weight:700;">{sig_f} signals</span>'
            f'<span style="font-family:Geist Mono,monospace;font-size:.58rem;color:#9ca3af;">Last: {last_f}</span>'
            f'</div>'
            f'<div style="height:3px;background:#1f2937;border-radius:2px;margin-top:6px;">'
            f'<div style="width:{pct_done}%;height:100%;background:#7c3aed;border-radius:2px;transition:width .3s;"></div>'
            f'</div>'
            '</div>',
            unsafe_allow_html=True)

    except Exception as e:
        import traceback
        st.session_state.sentinel_active = False
        st.error(f"Sentinel error (auto-disabled): {e}\n\nRun in Colab: `!pip install nest_asyncio --quiet` then restart runtime")
        st.code(traceback.format_exc(), language="text")

    # Sentinel runs inline with the auto-pilot scan cycle.
    # No separate sleep/rerun needed — auto-pilot drives the cadence.


# ─── RESULTS DISPLAY ─────────────────────────────────────────────────────────
results = st.session_state.results
errs    = getattr(st.session_state, 'scan_errors', [])
btc_t   = getattr(st.session_state, 'btc_trend', "NEUTRAL")

if not results:
    st.markdown('<div class="empty-st"><div style="font-size:2.5rem;opacity:.2;margin-bottom:12px;">🔥</div>Run a scan — or lower Min Score to 1 in sidebar</div>', unsafe_allow_html=True)
    if st.session_state.scan_count > 0:
        with st.expander("🔍 Debug Info", expanded=True):
            raw = st.session_state.last_raw_count
            st.markdown(f"""
**Scan #{st.session_state.scan_count}** at **{st.session_state.last_scan}**

| Check | Status |
|---|---|
| Raw coins found (before filter) | **{raw}** |
| Min score filter | **{eff_s['min_score']}** — set to 1 to see everything |
| Min R:R filter | **{eff_s.get('min_rr', 1.5)}** — lowering this shows more results |
| Momentum required | **{'YES — turn off in sidebar to see all' if eff_s.get('require_momentum') else 'NO'}** |
| BTC filter | **{btc_t}** — {"⚠️ BEARISH blocking all LONGs! Turn off." if btc_t=='BEARISH' else "✅ OK"} |
| Cooldown | **{'ON' if eff_s['cooldown_on'] else 'OFF'}** |
| Errors | **{len(errs)}** out of {eff_s['scan_depth']} coins |

**Fix steps:**
1. Set **Min Score = 1** in left sidebar
2. Turn off **Require Momentum** in sidebar
3. Set **Min R:R = 1.0** in Settings
4. Turn **BTC Bear filter OFF**
5. If still 0 raw coins — exchange throttling. Wait 60s.
            """)
            if errs: st.code("\n".join(errs[:15]), language="text")
else:
    sq = [r for r in results if r['cls']=="squeeze"]
    br = [r for r in results if r['cls']=="breakout"]
    wh = [r for r in results if r['cls']=="whale_driven"]
    ea = [r for r in results if r['cls']=="early"]
    top = results[0]

    mom_count = sum(1 for r in results if r.get('momentum_confirmed'))
    sent_count = sum(1 for r in results if r.get('sentiment',{}).get('available'))

    st.markdown(f"""<div class="stat-strip">
      <div><div class="ss-val" style="color:var(--red);">{len(sq)}</div><div class="ss-lbl">Squeeze</div></div>
      <div><div class="ss-val" style="color:var(--amber);">{len(br)}</div><div class="ss-lbl">Breakout</div></div>
      <div><div class="ss-val" style="color:var(--purple);">{len(wh)}</div><div class="ss-lbl">Whale Driven</div></div>
      <div><div class="ss-val" style="color:var(--blue);">{len(ea)}</div><div class="ss-lbl">Early</div></div>
      <div><div class="ss-val">{len(results)}</div><div class="ss-lbl">Total</div></div>
      <div><div class="ss-val">{st.session_state.last_raw_count}</div><div class="ss-lbl">Raw Scanned</div></div>
      <div><div class="ss-val" style="color:var(--green);">{mom_count}</div><div class="ss-lbl">Momentum Live</div></div>
      <div><div class="ss-val" style="color:var(--purple);">{sent_count}</div><div class="ss-lbl">With Sentiment</div></div>
      <div><div class="ss-val" style="color:{pump_color(top['pump_score'], top.get('is_sniper', False))};">{top['symbol']}</div><div class="ss-lbl">Hottest ({top['pump_score']})</div></div>
    </div>""", unsafe_allow_html=True)

    # ── SENTINEL RESULTS (live background findings) ─────────────────────────
    sent_res = st.session_state.get('sentinel_results', [])
    if sent_res:
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#1a0a2e,#0f1117);'
            f'border:1px solid #7c3aed66;border-radius:10px;padding:10px 16px;margin-bottom:12px;">'
            f'<span style="font-family:Geist Mono,monospace;font-size:.65rem;font-weight:700;color:#a78bfa;">'
            f'🛰️ SENTINEL LIVE — {len(sent_res)} background signal{"s" if len(sent_res)>1 else ""} found</span>'
            f'<span style="font-family:Geist Mono,monospace;font-size:.55rem;color:#6b7280;margin-left:12px;">'
            f'auto-scanning 24/7 · cards stack here as they qualify</span></div>',
            unsafe_allow_html=True)
        for _sr in sent_res[:10]:
            render_card(_sr, _sr.get('pump_score', 0) >= 90)
        if st.button("🗑️ Clear Sentinel Results", key="clr_sent"):
            st.session_state.sentinel_results = []
            st.rerun()
        st.markdown("---")

    tab_s, tab_b, tab_w, tab_e = st.tabs([
        f"🔴  SQUEEZE  ({len(sq)})",
        f"🟡  BREAKOUT  ({len(br)})",
        f"🐋  WHALE DRIVEN  ({len(wh)})",
        f"🔵  EARLY  ({len(ea)})"
    ])
    with tab_s:
        st.markdown('<div class="tab-desc"><b>About to Squeeze</b> — Funding extreme + OB heavily imbalanced. Trapped shorts/longs about to be liquidated. Highest urgency.</div>', unsafe_allow_html=True)
        [render_card(r, r.get('is_sniper', False)) for r in sq] if sq else st.markdown('<div class="empty-st"><div>⚡</div>No squeeze setups this scan</div>', unsafe_allow_html=True)
    with tab_b:
        st.markdown('<div class="tab-desc"><b>Confirmed Breakout</b> — OI spiking with volume. New money entering. Enter on first pullback.</div>', unsafe_allow_html=True)
        [render_card(r, r.get('is_sniper', False)) for r in br] if br else st.markdown('<div class="empty-st"><div>📈</div>No confirmed breakouts</div>', unsafe_allow_html=True)
    with tab_w:
        st.markdown('<div class="tab-desc"><b>Whale Driven</b> — Large wall + volume surge. A single large actor is driving price. Follow the whale, not the crowd.</div>', unsafe_allow_html=True)
        [render_card(r, r.get('is_sniper', False)) for r in wh] if wh else st.markdown('<div class="empty-st"><div>🐋</div>No whale-driven setups</div>', unsafe_allow_html=True)
    with tab_e:
        st.markdown('<div class="tab-desc"><b>Early Signal</b> — Pre-pump alignment. Watchlist these — often move into Squeeze/Breakout within 1–4h.</div>', unsafe_allow_html=True)
        [render_card(r, r.get('is_sniper', False)) for r in ea] if ea else st.markdown('<div class="empty-st"><div>📡</div>No early signals</div>', unsafe_allow_html=True)

if eff_s.get('auto_scan'):
    st.markdown(f'<div style="text-align:center;font-family:Geist Mono,monospace;font-size:.62rem;color:#9ca3af;padding:16px 0;">🤖 Auto-Pilot Active — Next scan in {eff_s["auto_interval"]} minute(s)</div>', unsafe_allow_html=True)
    time.sleep(eff_s['auto_interval']*60)
    st.rerun()
