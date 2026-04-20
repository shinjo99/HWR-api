"""core/config.py — 환경변수 및 전역 설정"""
import os

JWT_SECRET     = os.environ.get("JWT_SECRET", "hwr-secret-change-this")
ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
FB_URL      = os.environ.get("FB_URL", "https://team-dashboard-c0d7b-default-rtdb.asia-southeast1.firebasedatabase.app")
FB_SECRET   = os.environ.get("FB_SECRET", "")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")


def get_users():
    raw = os.environ.get("USERS", "team@hwr.com:hanwha2024:viewer")
    users = {}
    for entry in raw.split(","):
        parts = entry.strip().split(":")
        if len(parts) >= 3:
            email, password, role = parts[0], parts[1], parts[2]
            users[email] = {"password": password, "role": role}
    return users
