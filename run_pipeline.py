import subprocess
import sys
import os
import json
import requests
from datetime import datetime

# --- CONFIGURATION (SENSITIVE DATA REMOVED) ---
BASE        = r"" # Isi dengan path folder proyekmu
PIPELINE    = os.path.join(BASE, "pipeline")
DIGEST_FILE = os.path.join(BASE, "out", "alerts_digest.json")

BOT_TOKEN = "" # Isi dengan Telegram Bot Token kamu
CHAT_ID   = "" # Isi dengan Chat ID tujuan
# ----------------------------------------------

def run(script_name):
    script_path = os.path.join(PIPELINE, script_name)
    print(f"\n{'='*55}")
    print(f"  >> {script_name}")
    print(f"  {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*55}")

    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    result = subprocess.run(
        [sys.executable, script_path],
        cwd=BASE,
        env=env,
    )

    if result.returncode != 0:
        print(f"\n[X] ERROR in {script_name} - pipeline stopped.")
        sys.exit(1)

    print(f"[OK] {script_name} completed - {datetime.now().strftime('%H:%M:%S')}")

def escape_md(text):
    if text is None:
        return ""
    text = str(text)
    for ch in ['_', '*', '[', ']', '(', ')', '~', '`', '>',
               '#', '+', '-', '=', '|', '{', '}', '.', '!']:
        text = text.replace(ch, '\\' + ch)
    return text

def format_message(data):
    summary    = data["summary"]
    alerts     = data["alerts_count"]
    renewal    = data["renewal_pipeline"]
    top_alerts = data["top_alerts"][:5]
    date_str   = datetime.now().strftime("%A, %d %B %Y")

    msg = f"""📊 *SCHOOL HEALTH DAILY*
📅 {date_str}

*📈 Summary*
• Total Schools: *{summary['total_schools']:,}*
• Active: *{summary['aktif']:,}* \\- Expired: *{summary['expired']:,}*
• Ghost Schools: *{summary['ghost_schools']:,}*
• Users Ever Login: *{summary['users_ever_login']:,}*

*🚨 Alerts*
• CRITICAL: *{alerts['CRITICAL']:,}*
• HIGH: *{alerts['HIGH']:,}*
• MEDIUM: *{alerts['MEDIUM']:,}*

*🔄 Renewal Pipeline*
• Expire ≤30 days: *{renewal['expire_30_days']}* \\[URGENT\\]
• Expire ≤60 days: *{renewal['expire_60_days']}*
• Expire ≤90 days: *{renewal['expire_90_days']}*

*🔥 Top Priority*
"""

    for i, alert in enumerate(top_alerts, 1):
        users    = alert.get('total_real_users', 0) or 0
        emoji    = "👥" if users > 0 else "👻"
        name     = escape_md(alert['school_name'])
        degree   = escape_md(alert['degree'])
        stype    = escape_md(alert['school_type'])
        amsg     = escape_md(alert['alert_message'])
        dte      = alert['days_to_expire']
        tier     = alert.get('engagement_tier', 'Unknown')

        msg += f"""
{i}\\. *{name}* \\({degree}, {stype}\\)
   ⏰ {dte} days left \\- {emoji} {int(users)} users \\- {tier}
   → {amsg}"""

    msg += "\n"
    return msg

def send_telegram(message):
    url     = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    CHAT_ID,
        "text":       message,
        "parse_mode": "MarkdownV2",
    }
    try:
        res    = requests.post(url, json=payload, timeout=30)
        result = res.json()
        if result.get("ok"):
            print(f"[OK] Telegram sent (message_id: {result['result']['message_id']})")
            return True
        else:
            print(f"[ERROR] Telegram: {result.get('description', 'Unknown error')}")
            return False
    except Exception as e:
        print(f"[ERROR] Telegram exception: {e}")
        return False

if __name__ == "__main__":
    print(f"\n{'='*55}")
    print(f"  SCHOOL HEALTH PIPELINE")
    print(f"  Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*55}")

    run("extract_to_ducklake.py")
    run("transform.py")

    print(f"\n{'='*55}")
    print(f"  [OK] PIPELINE COMPLETED")
    print(f"  Output : {os.path.join(BASE, 'out')}")
    print(f"  End    : {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*55}\n")

    try:
        with open(DIGEST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        send_telegram(f"⚠️ *PIPELINE ERROR*\n\nFailed to read digest: {e}")
        sys.exit(1)

    print("[INFO] Sending report to Telegram...")
    message = format_message(data)
    send_telegram(message)

    print(f"\n  Dashboard:")
    print(f"  streamlit run dashboard.py\n")
