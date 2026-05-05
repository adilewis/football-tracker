import requests
import psycopg2
import time
import os
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from flask import Flask, request

IL_TZ = ZoneInfo("Asia/Jerusalem")
app = Flask(__name__)

def il_now():
    return datetime.now(IL_TZ)

# ──────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────
API_BASE = "https://v3.football.api-sports.io"
API_KEY   = os.environ.get("API_KEY", "8c18c97267fd1e32b0be49849a2b4d83")
TG_TOKEN  = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

COMPOSITE_MIN = 1.0   # minimum composite to send alert

# (league_id, season) — European 2025-26 season = "2025"; Americas/Sweden calendar year = "2026"
SPECIFIC_LEAGUES = [
    (39,  "2025"),   # Premier League
    (140, "2025"),   # La Liga
    (78,  "2025"),   # Bundesliga
    (135, "2025"),   # Serie A
    (61,  "2025"),   # Ligue 1
    (88,  "2025"),   # Eredivisie
    (94,  "2025"),   # Primeira Liga
    (203, "2025"),   # Süper Lig (Turkey)
    (40,  "2025"),   # Championship (England)
    (2,   "2025"),   # UEFA Champions League
    (3,   "2025"),   # UEFA Europa League
    (848, "2025"),   # UEFA Conference League
    (271, "2025"),   # Ligat Ha'al (Israel)
    (179, "2025"),   # Scottish Premiership
    (144, "2025"),   # Belgian Pro League
    (197, "2025"),   # Greek Super League
    (71,  "2026"),   # Brazilian Série A
    (253, "2026"),   # MLS (USA)
    (113, "2026"),   # Allsvenskan (Sweden)
    (13,  "2026"),   # Copa Libertadores
]
SPECIFIC_LEAGUE_IDS = {l[0] for l in SPECIFIC_LEAGUES}

TRACKED_GAMES = {}

# ──────────────────────────────────────────────────────
# DB
# ──────────────────────────────────────────────────────
def get_conn():
    url = os.environ.get("DATABASE_URL", "")
    for attempt in range(3):
        try:
            return psycopg2.connect(url, connect_timeout=10)
        except Exception as e:
            print(f"DB connect error (attempt {attempt+1}): {e}")
            time.sleep(3)
    raise RuntimeError("Cannot connect to DB after 3 attempts")

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS odds_snapshots (
            id SERIAL PRIMARY KEY,
            game_id TEXT NOT NULL,
            snapshot_time TEXT NOT NULL,
            home_team TEXT, away_team TEXT, league TEXT, game_time TEXT,
            ml_home REAL, ml_draw REAL, ml_away REAL,
            ou_line REAL, ou_over REAL, ou_under REAL
        )
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_snapshots_game_id ON odds_snapshots (game_id)
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sent_alerts (
            alert_key TEXT PRIMARY KEY,
            sent_time TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS pick_log (
            id SERIAL PRIMARY KEY,
            game_id TEXT,
            home_team TEXT, away_team TEXT, league TEXT,
            game_time TEXT, direction TEXT,
            composite REAL,
            result TEXT DEFAULT 'PENDING',
            home_score INT, away_score INT,
            logged_at TIMESTAMP DEFAULT NOW(),
            checked_at TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def try_claim(key):
    """Atomic insert — returns True only if THIS instance was first."""
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO sent_alerts (alert_key, sent_time) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (str(key), datetime.utcnow().isoformat())
    )
    claimed = c.rowcount > 0
    conn.commit()
    conn.close()
    return claimed

def cleanup_old_alerts():
    cutoff = (datetime.utcnow() - timedelta(days=3)).isoformat()
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM sent_alerts WHERE sent_time < %s", (cutoff,))
    conn.commit()
    conn.close()

def save_snapshot(game_id, info, odds):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO odds_snapshots
        (game_id, snapshot_time, home_team, away_team, league, game_time,
         ml_home, ml_draw, ml_away, ou_line, ou_over, ou_under)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        game_id, datetime.utcnow().isoformat(),
        info["home"], info["away"], info["league"], info["game_time"],
        odds["ml_home"], odds["ml_draw"], odds["ml_away"],
        odds["ou_line"], odds["ou_over"], odds["ou_under"],
    ))
    conn.commit()
    conn.close()

def get_snapshots(game_id):
    """Returns rows: (snapshot_time[0], ml_home[1], ml_draw[2], ml_away[3], ou_line[4], ou_over[5], ou_under[6])"""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT snapshot_time, ml_home, ml_draw, ml_away, ou_line, ou_over, ou_under
        FROM odds_snapshots WHERE game_id=%s ORDER BY snapshot_time ASC
    """, (game_id,))
    rows = c.fetchall()
    conn.close()
    return rows

# ──────────────────────────────────────────────────────
# TELEGRAM
# ──────────────────────────────────────────────────────
def send_telegram(msg):
    if not TG_TOKEN or not TG_CHAT_ID:
        print(f"[TG skip — no token] {msg[:80]}")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"Telegram error: {e}")

# ──────────────────────────────────────────────────────
# FOOTBALL API
# ──────────────────────────────────────────────────────
def get_games_today():
    games = []
    seen_ids = set()
    headers = {"x-apisports-key": API_KEY}
    dates = [(datetime.utcnow() + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(5)]

    for date in dates:
        # General date scan
        try:
            r = requests.get(f"{API_BASE}/fixtures?date={date}&timezone=UTC", headers=headers, timeout=15)
            for g in r.json().get("response", []):
                gid = str(g["fixture"]["id"])
                if gid not in seen_ids:
                    seen_ids.add(gid)
                    games.append(g)
        except Exception as e:
            print(f"API error (date={date}): {e}")
        time.sleep(0.7)

        # Specific leagues to ensure coverage
        for lid, season in SPECIFIC_LEAGUES:
            try:
                url = f"{API_BASE}/fixtures?league={lid}&season={season}&date={date}&timezone=UTC"
                r = requests.get(url, headers=headers, timeout=15)
                for g in r.json().get("response", []):
                    gid = str(g["fixture"]["id"])
                    if gid not in seen_ids:
                        seen_ids.add(gid)
                        games.append(g)
            except Exception as e:
                print(f"API error (league={lid}): {e}")
            time.sleep(0.4)

    print(f"Fetched {len(games)} football fixtures total")
    return games

def update_tracked_games():
    games = get_games_today()
    now = datetime.utcnow()
    added = 0
    for g in games:
        try:
            gid       = str(g["fixture"]["id"])
            league_id = int(g["league"].get("id", 0))
            league    = g["league"]["name"]
            home      = g["teams"]["home"]["name"]
            away      = g["teams"]["away"]["name"]
            game_time = g["fixture"]["date"]

            if league_id not in SPECIFIC_LEAGUE_IDS:
                continue
            if any(x in league for x in ["Women", " W", "women", "Femenina"]):
                continue

            try:
                game_dt = datetime.fromisoformat(game_time.replace("Z", "+00:00")).replace(tzinfo=None)
            except:
                continue

            hours_until = (game_dt - now).total_seconds() / 3600
            if -2 < hours_until <= 96:
                TRACKED_GAMES[gid] = {
                    "home": home, "away": away, "league": league,
                    "league_id": league_id, "game_time": game_time,
                }
                added += 1
        except Exception as e:
            print(f"update_tracked_games item error: {e}")

    print(f"Tracking {len(TRACKED_GAMES)} football games ({added} added/refreshed)")

def get_odds(fixture_id):
    headers = {"x-apisports-key": API_KEY}
    try:
        r = requests.get(f"{API_BASE}/odds?fixture={fixture_id}", headers=headers, timeout=10)
        data = r.json()
        if not data.get("response"):
            return None

        ml_home_list, ml_draw_list, ml_away_list = [], [], []
        ou_over_by_line, ou_under_by_line = {}, {}

        for bookmaker in data["response"][0].get("bookmakers", []):
            for bet in bookmaker.get("bets", []):
                if bet["name"] == "Match Winner":
                    for v in bet["values"]:
                        try:
                            if v["value"] == "Home":  ml_home_list.append(float(v["odd"]))
                            elif v["value"] == "Draw": ml_draw_list.append(float(v["odd"]))
                            elif v["value"] == "Away": ml_away_list.append(float(v["odd"]))
                        except:
                            pass
                elif bet["name"] == "Goals Over/Under":
                    for v in bet["values"]:
                        try:
                            parts = v["value"].split(" ")
                            if len(parts) == 2:
                                line = float(parts[1])
                                odd  = float(v["odd"])
                                if parts[0] == "Over":
                                    ou_over_by_line.setdefault(line, []).append(odd)
                                elif parts[0] == "Under":
                                    ou_under_by_line.setdefault(line, []).append(odd)
                        except:
                            pass

        if not ml_home_list:
            return None

        def median(lst):
            s = sorted(lst)
            return s[len(s) // 2]

        ml_home = median(ml_home_list)
        ml_draw = median(ml_draw_list) if ml_draw_list else None
        ml_away = median(ml_away_list) if ml_away_list else None

        ou_line = ou_over = ou_under = None
        target = 2.5
        if target in ou_over_by_line:
            ou_line  = target
            ou_over  = median(ou_over_by_line[target])
            ou_under = median(ou_under_by_line[target]) if ou_under_by_line.get(target) else None

        return {
            "ml_home": ml_home, "ml_draw": ml_draw, "ml_away": ml_away,
            "ou_line": ou_line, "ou_over": ou_over, "ou_under": ou_under,
        }
    except Exception as e:
        print(f"get_odds error {fixture_id}: {e}")
        return None

def get_result(fixture_id):
    """Returns (home_score, away_score, status_short) or (None, None, None)."""
    headers = {"x-apisports-key": API_KEY}
    try:
        r = requests.get(f"{API_BASE}/fixtures?id={fixture_id}", headers=headers, timeout=10)
        resp = r.json().get("response", [])
        if not resp:
            return None, None, None
        g = resp[0]
        status = g.get("fixture", {}).get("status", {}).get("short", "")
        goals  = g.get("goals", {})
        return goals.get("home"), goals.get("away"), status
    except:
        return None, None, None

# ──────────────────────────────────────────────────────
# COMPOSITE SCORE ENGINE — FOOTBALL 1X2 + O/U
# ──────────────────────────────────────────────────────
LEAGUE_EFFICIENCY = {
    "premier league":     0.88,
    "champions league":   0.90, "uefa champions": 0.90,
    "la liga":            0.90,
    "bundesliga":         0.92,
    "serie a":            0.92,
    "ligue 1":            0.93,
    "europa league":      0.93, "uefa europa": 0.93,
    "eredivisie":         0.96,
    "primeira liga":      0.96,
    "championship":       0.96,
    "conference league":  0.97, "uefa conference": 0.97,
    "super lig":          0.98, "süper lig": 0.98,
    "scottish premiership": 0.99,
    "pro league":         1.00, "belgian":    1.00,
    "super league":       1.02, "greek super": 1.02,
    "mls":                1.03,
    "série a":            1.04, "brasileirao": 1.04,
    "allsvenskan":        1.05,
    "copa libertadores":  1.04,
    "ligat ha":           1.08,
}

def get_league_efficiency(league: str) -> float:
    ll = league.lower()
    for key, val in LEAGUE_EFFICIENCY.items():
        if key in ll:
            return val
    return 1.0

def get_hours_to_game(game_time_str):
    try:
        gt = datetime.fromisoformat(game_time_str.replace("Z", "+00:00"))
        if gt.tzinfo is None:
            gt = gt.replace(tzinfo=timezone.utc)
        return (gt - datetime.now(timezone.utc)).total_seconds() / 3600
    except:
        return 999.0

def compute_composite(rows, game_time_str, league):
    """
    rows: (snap_time, ml_home, ml_draw, ml_away, ou_line, ou_over, ou_under)
    Returns dict or None.
    """
    if len(rows) < 2:
        return None

    first, last = rows[0], rows[-1]
    fh, fd, fa = first[1], first[2], first[3]
    lh, ld, la = last[1],  last[2],  last[3]

    if fh is None or lh is None:
        return None

    # Delta per direction: positive = shortened = money coming in
    dh = (fh - lh) if (fh and lh) else 0.0
    dd = (fd - ld) if (fd and ld) else 0.0
    da = (fa - la) if (fa and la) else 0.0

    moves     = {"home": dh, "draw": dd, "away": da}
    direction = max(moves, key=moves.get)
    delta     = moves[direction]

    if delta < 0.03:
        return None

    h2g = get_hours_to_game(game_time_str)
    if h2g < 0:
        return None  # game started

    # F_mag — size of movement
    F_mag = min(delta / 0.10, 3.0)

    # F_time — proximity to kickoff
    if h2g < 2:       F_time = 2.0
    elif h2g < 6:     F_time = 1.5
    elif h2g < 24:    F_time = 1.2
    else:             F_time = 1.0

    # F_dual — did opposite side drift out (confirming one-way action)?
    if direction == "home":
        other_delta = max(dd, da)
    elif direction == "away":
        other_delta = max(dh, dd)
    else:  # draw
        other_delta = max(dh, da)
    F_dual = 1.5 if other_delta < -0.03 else 1.0

    # F_steam — movement in last 30 minutes
    recent_cutoff = (datetime.utcnow() - timedelta(minutes=30)).isoformat()
    recent = [r for r in rows if r[0] >= recent_cutoff]
    has_steam = False
    if len(recent) >= 2:
        if direction == "home":
            recent_move = (recent[0][1] or 0) - (recent[-1][1] or 0)
        elif direction == "away":
            recent_move = (recent[0][3] or 0) - (recent[-1][3] or 0)
        else:
            recent_move = (recent[0][2] or 0) - (recent[-1][2] or 0)
        has_steam = recent_move > 0.02
    F_steam = 1.3 if has_steam else 1.0

    # F_ou — O/U line moved (market confirmation)
    ou_delta = 0.0
    if first[4] is not None and last[4] is not None:
        ou_delta = abs(last[4] - first[4])
    F_ou = 1.1 if ou_delta >= 0.5 else 1.0

    # F_league — market efficiency adjustment
    F_league = get_league_efficiency(league)

    composite = F_mag * F_time * F_dual * F_steam * F_ou * F_league

    if composite >= 2.0:
        verdict = "🚀 SIGNAL FIRE — סיגנל פצצה"
        confidence = "~82%"
    elif composite >= 1.5:
        verdict = "🔥 חזק מאוד — שקול בט"
        confidence = "~75%"
    elif composite >= 1.0:
        verdict = "⚡ טוב — מעניין"
        confidence = "~67%"
    elif composite >= 0.5:
        verdict = "👀 חלש — מידע בלבד"
        confidence = "~58%"
    else:
        verdict = "💤 רעש שוק"
        confidence = "~52%"

    return {
        "direction":      direction,
        "composite":      round(composite, 4),
        "delta":          delta,
        "h2g":            h2g,
        "has_steam":      has_steam,
        "dual_confirmed": F_dual > 1.0,
        "verdict":        verdict,
        "confidence":     confidence,
        "ml_home_first":  fh,   "ml_home_last":  lh,
        "ml_draw_first":  fd,   "ml_draw_last":  ld,
        "ml_away_first":  fa,   "ml_away_last":  la,
        "ou_first":       first[4], "ou_last": last[4],
        "ou_over":        last[5],  "ou_under": last[6],
        "factors": {
            "F_mag":    round(F_mag, 3),
            "F_time":   F_time,
            "F_dual":   F_dual,
            "F_steam":  F_steam,
            "F_ou":     F_ou,
            "F_league": round(F_league, 3),
        },
    }

# ──────────────────────────────────────────────────────
# ALERT TIER + DEDUP
# ──────────────────────────────────────────────────────
ALERT_TIERS = [0.40, 0.60, 0.80, 1.00, 1.20, 1.50, 2.00]

def composite_tier(comp):
    for t in reversed(ALERT_TIERS):
        if comp >= t:
            return f"{t:.2f}"
    return "below"

# ──────────────────────────────────────────────────────
# ALERT FORMATTING
# ──────────────────────────────────────────────────────
def direction_name(direction, home, away):
    if direction == "home":  return home
    if direction == "away":  return away
    return "שוויון"

def format_alert(info, signal):
    direction = signal["direction"]
    backed    = direction_name(direction, info["home"], info["away"])
    comp      = signal["composite"]
    h2g       = signal["h2g"]
    f         = signal["factors"]
    bar       = "█" * min(int(comp / 3 * 10), 10)

    try:
        gt_il = datetime.fromisoformat(
            info["game_time"].replace("Z", "+00:00")
        ).astimezone(IL_TZ).strftime("%d/%m %H:%M")
    except:
        gt_il = info["game_time"][:16]

    steam_tag = "⚡ STEAM " if signal["has_steam"] else ""
    dual_tag  = "🔁 DUAL "  if signal["dual_confirmed"] else ""

    fh = signal["ml_home_first"] or 0
    lh = signal["ml_home_last"]  or 0
    fd = signal["ml_draw_first"] or 0
    ld = signal["ml_draw_last"]  or 0
    fa = signal["ml_away_first"] or 0
    la = signal["ml_away_last"]  or 0

    dirs_line = (
        f"🏠 {info['home'][:12]}: {fh:.2f}→{lh:.2f}  "
        f"🤝 שוויון: {fd:.2f}→{ld:.2f}  "
        f"✈️ {info['away'][:12]}: {fa:.2f}→{la:.2f}"
    )

    lines = [
        f"⚽ <b>{info['away']} @ {info['home']}</b>",
        f"🏆 {info['league']}  |  🗓 {gt_il}  |  ⏱ בעוד {h2g:.1f}h",
        "",
        f"🎯 <b>כסף על: {backed}</b>  [{direction.upper()}]",
        dirs_line,
        "",
        f"{steam_tag}{dual_tag}",
        f"🧮 <b>COMPOSITE: {comp:.3f}</b>  [{bar}]",
        f"  F_mag={f['F_mag']:.2f} × F_time={f['F_time']:.1f} × F_dual={f['F_dual']:.1f}"
        f" × F_steam={f['F_steam']:.1f} × F_ou={f['F_ou']:.1f} × F_league={f['F_league']:.2f}",
        "",
        f"{'━'*6} {signal['verdict']} ({signal['confidence']}) {'━'*6}",
    ]
    if signal["ou_over"] and signal["ou_under"]:
        lines.insert(-1, f"📊 O/U 2.5: Over {signal['ou_over']:.2f}  |  Under {signal['ou_under']:.2f}")

    return "\n".join(lines)

def log_pick(game_id, info, direction, composite):
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO pick_log
              (game_id, home_team, away_team, league, game_time, direction, composite)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (game_id, info["home"], info["away"], info["league"],
              info["game_time"], direction, composite))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"log_pick error: {e}")

def check_movements(game_id, info):
    try:
        gt = datetime.fromisoformat(info["game_time"].replace("Z", "+00:00"))
        if gt.tzinfo is None:
            gt = gt.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) > gt:
            return
    except:
        pass

    rows = get_snapshots(game_id)
    if len(rows) < 2:
        return

    signal = compute_composite(rows, info["game_time"], info["league"])
    if signal is None:
        return

    comp = signal["composite"]
    if comp < COMPOSITE_MIN:
        return

    tier      = composite_tier(comp)
    steam_tag = "ST" if signal["has_steam"] else "G"
    alert_key = f"{game_id}|{signal['direction']}|{tier}|{steam_tag}"

    if not try_claim(alert_key):
        return

    log_pick(game_id, info, signal["direction"], comp)
    msg = format_alert(info, signal)
    send_telegram(msg)
    print(f"[ALERT] {info['away']}@{info['home']} comp={comp:.3f} tier={tier} dir={signal['direction']}")

# ──────────────────────────────────────────────────────
# RESULT CHECKER
# ──────────────────────────────────────────────────────
def check_results():
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT id, game_id, direction, game_time, home_team, away_team
            FROM pick_log WHERE result='PENDING'
        """)
        pending = c.fetchall()
        conn.close()
    except:
        return

    now_utc = datetime.now(timezone.utc)
    for pid, gid, direction, gt_str, home, away in pending:
        try:
            gt = datetime.fromisoformat(gt_str.replace("Z", "+00:00"))
            if gt.tzinfo is None:
                gt = gt.replace(tzinfo=timezone.utc)
            if now_utc < gt + timedelta(hours=2):
                continue  # too early to check

            hs, as_, status = get_result(gid)
            if status not in ("FT", "AET", "PEN"):
                continue
            if hs is None or as_ is None:
                continue

            if direction == "home":
                result = "WIN" if hs > as_ else ("PUSH" if hs == as_ else "LOSS")
            elif direction == "away":
                result = "WIN" if as_ > hs else ("PUSH" if hs == as_ else "LOSS")
            else:  # draw
                result = "WIN" if hs == as_ else "LOSS"

            conn = get_conn()
            c = conn.cursor()
            c.execute("""
                UPDATE pick_log
                SET result=%s, home_score=%s, away_score=%s, checked_at=NOW()
                WHERE id=%s
            """, (result, hs, as_, pid))
            conn.commit()
            conn.close()
            print(f"[RESULT] {home} vs {away} → {direction} = {result} ({hs}-{as_})")
        except Exception as e:
            print(f"check_results error: {e}")

# ──────────────────────────────────────────────────────
# DASHBOARD
# ──────────────────────────────────────────────────────
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="{refresh_secs}">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Football Odds Dashboard</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: Arial, sans-serif; background: #0f0f1a; color: #e0e0e0; margin: 0; padding: 16px; }}
  h1 {{ color: #a78bfa; margin-bottom: 2px; font-size: 22px; }}
  .header-meta {{ color: #666; font-size: 12px; margin-bottom: 20px; }}
  .next-refresh {{ color: #a78bfa; font-size: 12px; }}
  .game-card {{ background: #1a1a2e; border: 1px solid #2a2a4a; border-radius: 10px; padding: 14px 16px; margin-bottom: 14px; }}
  .game-card.trend-big {{ border-color: #f59e0b; box-shadow: 0 0 8px #f59e0b44; }}
  .game-card.trend-massive {{ border-color: #ef4444; box-shadow: 0 0 12px #ef444466; }}
  .card-badges {{ display: flex; gap: 6px; margin-bottom: 8px; flex-wrap: wrap; }}
  .badge {{ font-size: 11px; font-weight: bold; padding: 2px 10px; border-radius: 12px; }}
  .badge-fire {{ background: #7f1d1d; color: #fca5a5; }}
  .badge-strong {{ background: #78350f; color: #fbbf24; }}
  .badge-watch {{ background: #1a2a1a; color: #86efac; }}
  .badge-steam {{ background: #1e3a5f; color: #93c5fd; }}
  .badge-gap {{ background: #2d1b4e; color: #c4b5fd; }}
  .game-title {{ font-size: 16px; font-weight: bold; color: #fff; margin-bottom: 4px; }}
  .game-meta {{ font-size: 12px; color: #888; margin-bottom: 6px; }}
  .composite-bar {{ height: 6px; background: #12122a; border-radius: 3px; margin: 8px 0; }}
  .composite-fill {{ height: 100%; border-radius: 3px; background: linear-gradient(90deg, #3b82f6, #a78bfa, #ef4444); }}
  .timeline {{ display: flex; gap: 16px; font-size: 11px; color: #777; background: #0d0d20; border-radius: 6px; padding: 6px 10px; margin-bottom: 12px; flex-wrap: wrap; }}
  .tl-item {{ display: flex; flex-direction: column; }}
  .tl-label {{ color: #555; font-size: 10px; margin-bottom: 1px; }}
  .tl-val {{ color: #a0aec0; }}
  .odds-grid {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 8px; }}
  @media(max-width:700px) {{ .odds-grid {{ grid-template-columns: repeat(3,1fr); }} }}
  .odds-box {{ background: #12122a; border-radius: 8px; padding: 8px 6px; text-align: center; border: 1px solid transparent; }}
  .odds-label {{ font-size: 10px; color: #666; margin-bottom: 5px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
  .odds-open {{ font-size: 10px; color: #444; margin-bottom: 2px; }}
  .odds-cur {{ font-size: 14px; font-weight: bold; line-height: 1.3; }}
  .odds-delta {{ font-size: 11px; margin-top: 3px; font-weight: bold; }}
  .up   {{ color: #34d399; }}
  .down {{ color: #f87171; }}
  .same {{ color: #9ca3af; }}
  .box-up       {{ border-color: #34d39966; background: #0f2a1e; }}
  .box-down     {{ border-color: #f8717166; background: #2a0f0f; }}
  .box-big-up   {{ border-color: #34d399;   background: #1a3a2a; }}
  .box-big-down {{ border-color: #f87171;   background: #3a1a1a; }}
  .no-games {{ color: #888; text-align: center; padding: 60px; font-size: 16px; }}
  .composite-score {{ font-size: 13px; color: #a78bfa; margin: 6px 0; font-weight: bold; }}
  .first-move {{ font-size: 11px; background: #0d0d20; border-radius: 6px; padding: 8px 10px; margin-top: 10px; display: flex; flex-direction: column; gap: 6px; }}
  .fm-row {{ display: flex; gap: 6px; align-items: baseline; flex-wrap: wrap; }}
  .fm-label {{ color: #555; font-size: 10px; min-width: 100px; }}
  .fm-time {{ color: #555; font-size: 10px; }}
  .fm-ago  {{ color: #f59e0b; font-size: 10px; font-weight: bold; }}
  .fm-val  {{ font-weight: bold; }}
  .top5-box {{ background: #0d0d1e; border: 2px solid #a78bfa; border-radius: 12px; padding: 14px 16px; margin-bottom: 20px; }}
  .top5-title {{ color: #c4b5fd; font-size: 15px; font-weight: bold; margin-bottom: 10px; }}
  .top5-item {{ background: #1a1a35; border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; border-left: 3px solid #a78bfa; }}
</style>
<script>
  var secs = {refresh_secs};
  setInterval(function(){{
    secs--;
    var el = document.getElementById('countdown');
    if(el) el.textContent = secs > 0 ? secs + 's' : 'מתרענן...';
    if(secs <= 0) location.reload();
  }}, 1000);
</script>
</head>
<body>
<h1>⚽ Football Odds Dashboard</h1>
<div class="header-meta">
  עדכון אחרון: {updated} &nbsp;|&nbsp;
  <span class="next-refresh">רענון — <span id="countdown">{refresh_secs}s</span></span>
</div>
{content}
</body>
</html>"""


def fmt_odds(v):
    return f"{v:.2f}" if v is not None else "—"

def fmt_time_il(iso):
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IL_TZ).strftime("%H:%M")
    except:
        return "—"

def fmt_ago(iso):
    """Returns 'לפני X:YY שעות' or 'לפני X דקות'."""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        diff = (datetime.now(timezone.utc) - dt).total_seconds()
        h = int(diff // 3600)
        m = int((diff % 3600) // 60)
        if h > 0:
            return f"לפני {h}:{m:02d} שעות"
        return f"לפני {m} דקות"
    except:
        return ""

def find_first_move(rows, field_idx, threshold=0.03):
    """Return (snapshot_time, base_val, moved_val) of first significant move, or None."""
    if len(rows) < 2:
        return None
    base = rows[0][field_idx]
    if base is None:
        return None
    for row in rows[1:]:
        val = row[field_idx]
        if val is None:
            continue
        if abs(val - base) >= threshold:
            return (row[0], base, val)
    return None

def odds_box(label, opn, cur):
    if cur is None:
        return (f'<div class="odds-box">'
                f'<div class="odds-label">{label}</div>'
                f'<div class="odds-cur same">—</div>'
                f'</div>')
    delta = (cur - opn) if opn is not None else 0
    if delta <= -0.10:    box_cls, dc = "box-big-down", "down"
    elif delta <= -0.03:  box_cls, dc = "box-down",     "down"
    elif delta >= 0.10:   box_cls, dc = "box-big-up",   "up"
    elif delta >= 0.03:   box_cls, dc = "box-up",       "up"
    else:                 box_cls, dc = "",              "same"
    sign = "+" if delta > 0 else ""
    open_str  = f'<div class="odds-open">פתיחה: {fmt_odds(opn)}</div>' if opn is not None else ""
    delta_str = f'<div class="odds-delta {dc}">{sign}{delta:.2f}</div>' if opn is not None else ""
    return (f'<div class="odds-box {box_cls}">'
            f'<div class="odds-label">{label}</div>'
            f'{open_str}'
            f'<div class="odds-cur {dc}">{fmt_odds(cur)}</div>'
            f'{delta_str}'
            f'</div>')

def render_top5(picks):
    if not picks:
        return ""
    items = []
    for i, g in enumerate(picks, 1):
        sig     = g["signal"]
        backed  = direction_name(sig["direction"], g["home"], g["away"])
        comp    = sig["composite"]
        try:
            gt_il = datetime.fromisoformat(
                g["game_time"].replace("Z", "+00:00")
            ).astimezone(IL_TZ).strftime("%d/%m %H:%M")
        except:
            gt_il = g["game_time"][:16]
        items.append(
            f'<div class="top5-item">'
            f'<div style="font-size:13px;font-weight:bold;color:#fff;">#{i} {g["away"]} @ {g["home"]}</div>'
            f'<div style="font-size:11px;color:#888;">{g["league"]} | {gt_il}</div>'
            f'<div style="font-size:12px;color:#a78bfa;margin-top:4px;">'
            f'כסף על: <b style="color:#fff">{backed}</b> | Composite: <b>{comp:.3f}</b></div>'
            f'<div style="font-size:11px;color:#fbbf24;">{sig["verdict"]}</div>'
            f'</div>'
        )
    return (
        f'<div class="top5-box">'
        f'<div class="top5-title">🎯 TOP 5 המלצות</div>'
        + "".join(items) +
        f'</div>'
    )

def render_dashboard():
    now   = il_now()
    rsecs = 90

    games = []
    for gid, info in list(TRACKED_GAMES.items()):
        rows   = get_snapshots(gid)
        signal = compute_composite(rows, info["game_time"], info["league"]) if len(rows) >= 2 else None
        games.append({
            "game_id":   gid,
            "home":      info["home"],
            "away":      info["away"],
            "league":    info["league"],
            "game_time": info["game_time"],
            "snapshots": len(rows),
            "all_rows":  rows,
            "signal":    signal,
        })

    def sort_key(g):
        comp = g["signal"]["composite"] if g["signal"] else 0
        try:
            gt  = datetime.fromisoformat(g["game_time"].replace("Z", "+00:00"))
            if gt.tzinfo is None:
                gt = gt.replace(tzinfo=timezone.utc)
            h2g = (gt - datetime.now(timezone.utc)).total_seconds() / 3600
        except:
            h2g = 999
        return (-comp, h2g)

    games.sort(key=sort_key)

    if not games:
        content = '<div class="no-games">⚽ אין משחקים במעקב כרגע</div>'
        return HTML_TEMPLATE.format(
            updated=now.strftime("%H:%M:%S IL"),
            refresh_secs=rsecs, content=content
        )

    top5_candidates = [g for g in games if g["signal"] and g["signal"]["composite"] >= COMPOSITE_MIN]
    top5_candidates.sort(key=lambda g: -g["signal"]["composite"])
    top5_html = render_top5(top5_candidates[:5])

    cards = []
    for g in games:
        signal = g["signal"]
        all_rows = g["all_rows"]

        try:
            gt_dt = datetime.fromisoformat(g["game_time"].replace("Z", "+00:00"))
            if gt_dt.tzinfo is None:
                gt_dt = gt_dt.replace(tzinfo=timezone.utc)
            gt_il = gt_dt.astimezone(IL_TZ)
            h2g   = (gt_dt - datetime.now(timezone.utc)).total_seconds() / 3600
            if h2g < 0:       time_label = "🔴 משחק החל"
            elif h2g < 1:     time_label = f"⚡ בעוד {int(h2g*60)} דק'"
            else:             time_label = f"⏱ בעוד {h2g:.1f}h"
            gt_str = gt_il.strftime("%d/%m %H:%M")
        except:
            gt_str = g["game_time"][:16]
            time_label = ""

        # Card border
        if signal:
            comp = signal["composite"]
            trend_cls = "trend-massive" if comp >= 2.0 else ("trend-big" if comp >= 1.0 else "")
        else:
            comp      = 0
            trend_cls = ""

        # Badges
        badges = []
        if signal:
            if comp >= 2.0:   badges.append('<span class="badge badge-fire">🔥 FIRE</span>')
            elif comp >= 1.5: badges.append('<span class="badge badge-fire">🚀 STRONG</span>')
            elif comp >= 1.0: badges.append('<span class="badge badge-strong">⚡ SIGNAL</span>')
            elif comp >= 0.5: badges.append('<span class="badge badge-watch">👀 WATCH</span>')
            if signal["has_steam"]:
                badges.append('<span class="badge badge-steam">⚡ STEAM</span>')
            else:
                badges.append('<span class="badge badge-gap">📊 GAP</span>')
        badges_html = f'<div class="card-badges">{"".join(badges)}</div>' if badges else ""

        # Composite bar
        comp_html = ""
        if signal and comp > 0:
            bar_w  = min(int(comp / 3.0 * 100), 100)
            backed = direction_name(signal["direction"], g["home"], g["away"])
            comp_html = (
                f'<div class="composite-score">COMPOSITE: {comp:.3f} — כסף על {backed}</div>'
                f'<div class="composite-bar"><div class="composite-fill" style="width:{bar_w}%"></div></div>'
            )

        # Timeline
        if all_rows:
            f_row, l_row = all_rows[0], all_rows[-1]
            timeline = (
                f'<div class="timeline">'
                f'<div class="tl-item"><div class="tl-label">מעקב מ</div><div class="tl-val">{fmt_time_il(f_row[0])}</div></div>'
                f'<div class="tl-item"><div class="tl-label">עדכון</div><div class="tl-val">{fmt_time_il(l_row[0])}</div></div>'
                f'<div class="tl-item"><div class="tl-label">snapshots</div><div class="tl-val">{g["snapshots"]}</div></div>'
                f'<div class="tl-item"><div class="tl-label">משחק</div><div class="tl-val">{time_label}</div></div>'
                f'</div>'
            )
            home_short = g["home"][:10]
            away_short = g["away"][:10]
            grid = (
                f'<div class="odds-grid">'
                f'{odds_box("🏠 " + home_short, f_row[1], l_row[1])}'
                f'{odds_box("🤝 שוויון",        f_row[2], l_row[2])}'
                f'{odds_box("✈️ " + away_short, f_row[3], l_row[3])}'
                f'{odds_box("קו O/U",           f_row[4], l_row[4])}'
                f'{odds_box("Over",             f_row[5], l_row[5])}'
                f'{odds_box("Under",            f_row[6], l_row[6])}'
                f'</div>'
            )
        else:
            timeline = (
                '<div class="timeline">'
                '<div class="tl-item"><div class="tl-label">סטטוס</div>'
                '<div class="tl-val">ממתין לאודס</div></div></div>'
            )
            grid = ""

        # ── First movement section ──
        fm_rows_html = []
        if all_rows:
            # Check all 3 ML directions — find which fired first
            home_fm = find_first_move(all_rows, 1, 0.03)
            draw_fm = find_first_move(all_rows, 2, 0.03)
            away_fm = find_first_move(all_rows, 3, 0.03)

            candidates = []
            if home_fm: candidates.append(("home", home_fm, g["home"]))
            if draw_fm: candidates.append(("draw", draw_fm, "שוויון"))
            if away_fm: candidates.append(("away", away_fm, g["away"]))
            candidates.sort(key=lambda x: x[1][0])  # sort by snapshot_time

            if candidates:
                first_dir, first_fm, first_name = candidates[0]
                fm_t, fm_from, fm_to = first_fm
                fm_delta = fm_to - fm_from
                fm_arrow = "⬇" if fm_delta < 0 else "⬆"
                fm_cls   = "down" if fm_delta < 0 else "up"
                fm_sign  = "+" if fm_delta > 0 else ""
                fm_rows_html.append(
                    f'<div class="fm-row">'
                    f'<span class="fm-label">תנועה ראשונה:</span>'
                    f'<span class="fm-val {fm_cls}">{fm_arrow} {first_name[:14]}: '
                    f'{fmt_odds(fm_from)}→{fmt_odds(fm_to)} ({fm_sign}{fm_delta:.2f})</span>'
                    f'<span class="fm-time"> @ {fmt_time_il(fm_t)}</span>'
                    f'<span class="fm-ago"> {fmt_ago(fm_t)}</span>'
                    f'</div>'
                )
            else:
                fm_rows_html.append(
                    '<div class="fm-row">'
                    '<span class="fm-label">תנועה ראשונה:</span>'
                    '<span class="same">אין תנועה משמעותית עדיין</span>'
                    '</div>'
                )

            # O/U first move (threshold 0.5 pts)
            ou_fm = find_first_move(all_rows, 4, 0.5)
            if ou_fm:
                lf_t, lf_from, lf_to = ou_fm
                lf_delta = lf_to - lf_from
                lf_arrow = "⬆" if lf_delta > 0 else "⬇"
                lf_cls   = "up" if lf_delta > 0 else "down"
                lf_sign  = "+" if lf_delta > 0 else ""
                fm_rows_html.append(
                    f'<div class="fm-row">'
                    f'<span class="fm-label">קו O/U ראשון:</span>'
                    f'<span class="fm-val {lf_cls}">{lf_arrow} '
                    f'{fmt_odds(lf_from)}→{fmt_odds(lf_to)} ({lf_sign}{lf_delta:.1f})</span>'
                    f'<span class="fm-time"> @ {fmt_time_il(lf_t)}</span>'
                    f'<span class="fm-ago"> {fmt_ago(lf_t)}</span>'
                    f'</div>'
                )
            else:
                fm_rows_html.append(
                    f'<div class="fm-row">'
                    f'<span class="fm-label">קו O/U:</span>'
                    f'<span class="same">יציב ({fmt_odds(all_rows[0][4])})</span>'
                    f'</div>'
                )

            # Current backed odds (when signal exists)
            if signal:
                dir_ = signal["direction"]
                backed_name = direction_name(dir_, g["home"], g["away"])
                backed_odds = all_rows[-1][1] if dir_ == "home" else (
                    all_rows[-1][3] if dir_ == "away" else all_rows[-1][2]
                )
                fm_rows_html.append(
                    f'<div class="fm-row">'
                    f'<span class="fm-label">אודס על {backed_name[:12]}:</span>'
                    f'<span class="fm-val">{fmt_odds(backed_odds)}</span>'
                    f'</div>'
                )

        first_move_html = (
            '<div class="first-move">' + "".join(fm_rows_html) + '</div>'
        ) if fm_rows_html else ""

        cards.append(
            f'<div class="game-card {trend_cls}">'
            f'{badges_html}'
            f'<div class="game-title">{g["away"]} @ {g["home"]}</div>'
            f'<div class="game-meta">{g["league"]} &nbsp;|&nbsp; {gt_str}</div>'
            f'{comp_html}'
            f'{timeline}'
            f'{grid}'
            f'{first_move_html}'
            f'</div>'
        )

    return HTML_TEMPLATE.format(
        updated=now.strftime("%H:%M:%S IL"),
        refresh_secs=rsecs,
        content=top5_html + "\n".join(cards)
    )

# ──────────────────────────────────────────────────────
# FLASK ROUTES
# ──────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_dashboard()

@app.route("/health")
def health():
    lines = [f"Status: OK", f"Tracked games: {len(TRACKED_GAMES)}"]
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT MAX(snapshot_time) FROM odds_snapshots")
        last_snap = c.fetchone()[0]
        c.execute("""SELECT COUNT(*) FROM odds_snapshots
                     WHERE snapshot_time::timestamptz > NOW() - INTERVAL '1 hour'""")
        snaps_1h = c.fetchone()[0]
        c.execute("""SELECT COUNT(*) FROM odds_snapshots
                     WHERE snapshot_time::timestamptz > NOW() - INTERVAL '24 hours'""")
        snaps_24h = c.fetchone()[0]
        conn.close()
        lines.append(f"Last snapshot: {last_snap}")
        lines.append(f"Snapshots last 1h:  {snaps_1h}")
        lines.append(f"Snapshots last 24h: {snaps_24h}")
    except Exception as e:
        lines.append(f"DB error: {e}")
    return "<pre>" + "\n".join(lines) + "</pre>"

@app.route("/api/history")
def api_history():
    import json as _json
    date_from = request.args.get("from", "2026-05-01")
    date_to   = request.args.get("to",   "2026-05-05")
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT game_id, home_team, away_team, league, game_time,
                   direction, composite, result, home_score, away_score, logged_at
            FROM pick_log
            WHERE logged_at >= %s AND logged_at < %s
            ORDER BY logged_at ASC
        """, (date_from, date_to + " 23:59:59"))
        pick_cols = ["game_id","home_team","away_team","league","game_time",
                     "direction","composite","result","home_score","away_score","logged_at"]
        picks = [dict(zip(pick_cols, [str(x) if x is not None else None for x in r]))
                 for r in c.fetchall()]

        c.execute("""
            SELECT game_id, home_team, away_team, league, game_time,
                   COUNT(*) as snap_count,
                   MIN(snapshot_time) as first_snap,
                   MAX(snapshot_time) as last_snap
            FROM odds_snapshots
            WHERE snapshot_time >= %s AND snapshot_time < %s
            GROUP BY game_id, home_team, away_team, league, game_time
        """, (date_from, date_to + " 23:59:59"))
        snap_cols = ["game_id","home_team","away_team","league","game_time",
                     "snap_count","first_snap","last_snap"]
        snapshots = [dict(zip(snap_cols, [str(x) if x is not None else None for x in r]))
                     for r in c.fetchall()]
        conn.close()
        return _json.dumps({"picks": picks, "snapshots": snapshots},
                           ensure_ascii=False, indent=2), 200, {"Content-Type": "application/json"}
    except Exception as e:
        return _json.dumps({"error": str(e)}), 500

@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    data = request.json or {}
    threading.Thread(target=handle_telegram_update, args=(data,), daemon=True).start()
    return "ok"

# ──────────────────────────────────────────────────────
# TELEGRAM COMMANDS
# ──────────────────────────────────────────────────────
_tg_offset = 0

def tg_reply(chat_id, msg):
    if not TG_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"tg_reply error: {e}")

def handle_telegram_update(data):
    msg  = data.get("message", {})
    chat_id = msg.get("chat", {}).get("id")
    text    = msg.get("text", "").strip()
    if not chat_id or not text:
        return
    handle_command(chat_id, text)

def handle_command(chat_id, text):
    cmd = text.split()[0].lower().split("@")[0]
    now_utc = datetime.now(timezone.utc)

    if cmd == "/status":
        lines = [
            "⚽ <b>Football Tracker — סטטוס</b>",
            f"🏟 משחקים במעקב: {len(TRACKED_GAMES)}",
        ]
        try:
            conn = get_conn()
            c = conn.cursor()
            c.execute("""SELECT COUNT(*) FROM odds_snapshots
                         WHERE snapshot_time::timestamptz > NOW() - INTERVAL '1 hour'""")
            snaps_1h = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM pick_log WHERE result='PENDING'")
            pending = c.fetchone()[0]
            conn.close()
            lines.append(f"📸 Snapshots (1h): {snaps_1h}")
            lines.append(f"⏳ Picks ממתינים: {pending}")
        except Exception as e:
            lines.append(f"DB error: {e}")
        tg_reply(chat_id, "\n".join(lines))

    elif cmd == "/picks":
        picks = []
        for gid, info in list(TRACKED_GAMES.items()):
            try:
                gt = datetime.fromisoformat(info["game_time"].replace("Z", "+00:00"))
                if gt.tzinfo is None:
                    gt = gt.replace(tzinfo=timezone.utc)
                if gt <= now_utc:
                    continue
                rows = get_snapshots(gid)
                sig  = compute_composite(rows, info["game_time"], info["league"])
                if sig and sig["composite"] >= COMPOSITE_MIN:
                    picks.append((sig["composite"], info, sig))
            except:
                continue
        picks.sort(key=lambda x: -x[0])
        picks = picks[:5]

        if not picks:
            tg_reply(chat_id, "⚽ אין המלצות פעילות כרגע (composite < 1.0)")
            return

        lines = ["⚽ <b>Top 5 Picks — Football</b>", ""]
        for i, (comp, info, sig) in enumerate(picks, 1):
            backed = direction_name(sig["direction"], info["home"], info["away"])
            try:
                gt_il = datetime.fromisoformat(
                    info["game_time"].replace("Z", "+00:00")
                ).astimezone(IL_TZ).strftime("%d/%m %H:%M")
            except:
                gt_il = info["game_time"][:16]
            lines += [
                f"#{i} <b>{info['away']} @ {info['home']}</b>",
                f"   {info['league']} | {gt_il}",
                f"   כסף על: <b>{backed}</b> | Composite: {comp:.3f}",
                f"   {sig['verdict']}", "",
            ]
        tg_reply(chat_id, "\n".join(lines))

    elif cmd == "/signals":
        sigs = []
        for gid, info in list(TRACKED_GAMES.items()):
            try:
                gt = datetime.fromisoformat(info["game_time"].replace("Z", "+00:00"))
                if gt.tzinfo is None:
                    gt = gt.replace(tzinfo=timezone.utc)
                if gt <= now_utc:
                    continue
                rows = get_snapshots(gid)
                sig  = compute_composite(rows, info["game_time"], info["league"])
                if sig and sig["composite"] >= 0.4:
                    sigs.append((sig["composite"], info, sig))
            except:
                continue
        sigs.sort(key=lambda x: -x[0])

        if not sigs:
            tg_reply(chat_id, "⚽ אין סיגנלים פעילים כרגע")
            return

        lines = [f"⚽ <b>כל הסיגנלים הפעילים ({len(sigs)})</b>", ""]
        for comp, info, sig in sigs[:10]:
            backed = direction_name(sig["direction"], info["home"], info["away"])
            lines.append(f"• <b>{info['away']} @ {info['home']}</b>  [{backed}]  {comp:.3f}")
        tg_reply(chat_id, "\n".join(lines))

    elif cmd == "/stats":
        try:
            conn = get_conn()
            c = conn.cursor()
            c.execute("""
                SELECT result, COUNT(*) FROM pick_log
                WHERE logged_at > NOW() - INTERVAL '30 days'
                GROUP BY result
            """)
            counts  = {r[0]: r[1] for r in c.fetchall()}
            conn.close()
            wins    = counts.get("WIN",     0)
            losses  = counts.get("LOSS",    0)
            push    = counts.get("PUSH",    0)
            pending = counts.get("PENDING", 0)
            total   = wins + losses + push
            wr      = f"{wins/total*100:.1f}%" if total > 0 else "N/A"
            tg_reply(chat_id,
                f"⚽ <b>סטטיסטיקות 30 ימים</b>\n"
                f"✅ WIN: {wins}\n❌ LOSS: {losses}\n"
                f"🔄 PUSH: {push}\n⏳ PENDING: {pending}\n"
                f"📊 Win Rate: {wr}"
            )
        except Exception as e:
            tg_reply(chat_id, f"שגיאה: {e}")

    elif cmd == "/top":
        moves = []
        for gid, info in list(TRACKED_GAMES.items()):
            try:
                gt = datetime.fromisoformat(info["game_time"].replace("Z", "+00:00"))
                if gt.tzinfo is None:
                    gt = gt.replace(tzinfo=timezone.utc)
                if gt <= now_utc:
                    continue
                rows = get_snapshots(gid)
                sig  = compute_composite(rows, info["game_time"], info["league"])
                if sig:
                    moves.append((sig["composite"], info, sig))
            except:
                continue
        moves.sort(key=lambda x: -x[0])
        moves = moves[:5]

        if not moves:
            tg_reply(chat_id, "⚽ אין תנועות משמעותיות כרגע")
            return

        lines = ["⚽ <b>Top Moves</b>", ""]
        for comp, info, sig in moves:
            backed = direction_name(sig["direction"], info["home"], info["away"])
            lines.append(f"• <b>{info['home']} vs {info['away']}</b>  → {backed}  ({comp:.3f})")
        tg_reply(chat_id, "\n".join(lines))

def handle_commands_polling():
    global _tg_offset
    if not TG_TOKEN:
        return
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates",
            params={"offset": _tg_offset, "timeout": 5},
            timeout=15
        )
        for upd in r.json().get("result", []):
            _tg_offset = upd["update_id"] + 1
            handle_telegram_update(upd)
    except Exception as e:
        print(f"handle_commands_polling error: {e}")

def commands_loop():
    while True:
        try:
            handle_commands_polling()
        except Exception as e:
            print(f"commands_loop error: {e}")
        time.sleep(10)

# ──────────────────────────────────────────────────────
# MAIN LOOP
# ──────────────────────────────────────────────────────
_last_snapshot_time = datetime.utcnow()

def main_loop():
    global _last_snapshot_time
    last_game_refresh  = datetime.utcnow() - timedelta(hours=2)
    last_result_check  = datetime.utcnow() - timedelta(hours=2)
    last_cleanup       = datetime.utcnow() - timedelta(hours=12)

    print("Football-tracker starting...")
    init_db()
    update_tracked_games()

    while True:
        now = datetime.utcnow()

        # Refresh tracked game list every 60 minutes
        if (now - last_game_refresh).total_seconds() >= 3600:
            try:
                update_tracked_games()
                last_game_refresh = now
            except Exception as e:
                print(f"update_tracked_games error: {e}")

        # Check results every 2 hours
        if (now - last_result_check).total_seconds() >= 7200:
            try:
                check_results()
                last_result_check = now
            except Exception as e:
                print(f"check_results error: {e}")

        # Cleanup old alerts once per day
        if (now - last_cleanup).total_seconds() >= 86400:
            try:
                cleanup_old_alerts()
                last_cleanup = now
            except Exception as e:
                print(f"cleanup_old_alerts error: {e}")

        # Per-game: get odds → save snapshot → check movements
        game_ids = list(TRACKED_GAMES.keys())
        for game_id in game_ids:
            info = TRACKED_GAMES.get(game_id)
            if not info:
                continue
            try:
                # Remove games that ended >2h ago
                gt = datetime.fromisoformat(info["game_time"].replace("Z", "+00:00"))
                if gt.tzinfo is None:
                    gt = gt.replace(tzinfo=timezone.utc)
                if datetime.now(timezone.utc) > gt + timedelta(hours=2):
                    TRACKED_GAMES.pop(game_id, None)
                    continue

                odds = get_odds(game_id)
                if odds:
                    save_snapshot(game_id, info, odds)
                    _last_snapshot_time = datetime.utcnow()
                    check_movements(game_id, info)
                time.sleep(0.7)  # ~85 games/min — well under 100/min API limit
            except Exception as e:
                print(f"Game loop error {game_id}: {e}")

        print(f"[CYCLE] {len(TRACKED_GAMES)} games | {now.strftime('%H:%M:%S')} UTC")
        time.sleep(300)  # 5-minute cycle


if __name__ == "__main__":
    threading.Thread(target=main_loop,    daemon=True).start()
    threading.Thread(target=commands_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
