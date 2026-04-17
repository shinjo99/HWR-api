from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, Form
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

MACRS_5YR = [0.20, 0.32, 0.192, 0.1152, 0.1152, 0.0576]

def _irr_robust(cfs, guess=0.08):
    """여러 초기값으로 Newton 반복 → 양수 수렴값 반환"""
    import numpy as np
    def newton(g):
        r = g
        for _ in range(2000):
            npv  = sum(cf/(1+r)**t for t,cf in enumerate(cfs))
            dnpv = sum(-t*cf/(1+r)**(t+1) for t,cf in enumerate(cfs))
            if abs(dnpv) < 1e-12: break
            r_new = r - npv/dnpv
            if r_new <= -0.999: r_new = 0.001
            if abs(r_new - r) < 1e-8: return r_new
            r = r_new
        return r
    for g in [guess, 0.01, 0.03, 0.05, 0.10, 0.15]:
        r = newton(g)
        if r > 0:
            chk = sum(cf/(1+r)**t for t,cf in enumerate(cfs))
            if abs(chk) < 500:  # $500K 오차 허용
                return r
    try:
        r0 = float(npf.irr(cfs))
        return r0 if not np.isnan(r0) else 0.0
    except Exception:
        return 0.0
import requests
import jwt
import os
import datetime
import json
import tempfile
from pyxlsb import open_workbook

app = FastAPI(title="HWR Dashboard API")

# ── CORS (대시보드에서 호출 허용) ──────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 추후 shinjo99.github.io 로 제한
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 설정 ──────────────────────────────────────────
JWT_SECRET     = os.environ.get("JWT_SECRET", "hwr-secret-change-this")
ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
FB_URL      = os.environ.get("FB_URL", "https://team-dashboard-c0d7b-default-rtdb.asia-southeast1.firebasedatabase.app")
FB_SECRET   = os.environ.get("FB_SECRET", "")  # Firebase Database Secret
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")  # https://fred.stlouisfed.org/docs/api/api_key.html

# ── 사용자 계정 (환경변수로 관리) ─────────────────
# 형식: "email:password:role,email:password:role"
# 예: "team@hwr.com:hanwha2024:viewer,admin@hwr.com:admin1234:admin"
def get_users():
    raw = os.environ.get("USERS", "team@hwr.com:hanwha2024:viewer")
    users = {}
    for entry in raw.split(","):
        parts = entry.strip().split(":")
        if len(parts) >= 3:
            email, password, role = parts[0], parts[1], parts[2]
            users[email] = {"password": password, "role": role}
    return users

# ── JWT 헬퍼 ──────────────────────────────────────
def create_token(email: str, role: str) -> str:
    payload = {
        "email": email,
        "role": role,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def verify_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="토큰이 만료되었습니다. 다시 로그인하세요.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="유효하지 않은 토큰입니다.")

security = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return verify_token(credentials.credentials)

def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")
    return user

# ── Firebase 헬퍼 ──────────────────────────────────
def fb_auth_param():
    if FB_SECRET:
        return {"auth": FB_SECRET}
    return {}

def fb_read(path: str):
    try:
        res = requests.get(
            f"{FB_URL}/{path}.json",
            params=fb_auth_param(),
            timeout=5
        )
        return res.json() or {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB 읽기 오류: {str(e)}")

def fb_write(path: str, data: dict):
    try:
        res = requests.patch(
            f"{FB_URL}/{path}.json",
            json=data,
            params=fb_auth_param(),
            timeout=5
        )
        if res.status_code != 200:
            raise HTTPException(status_code=500, detail="DB 저장 실패")
        return res.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB 쓰기 오류: {str(e)}")

def fb_put(path: str, data: dict):
    try:
        res = requests.put(
            f"{FB_URL}/{path}.json",
            json=data,
            params=fb_auth_param(),
            timeout=5
        )
        if res.status_code != 200:
            raise HTTPException(status_code=500, detail="DB 저장 실패")
        return res.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB 쓰기 오류: {str(e)}")

def fb_patch(path: str, data: dict):
    """Firebase PATCH — 필드 일부만 업데이트"""
    try:
        requests.patch(
            f"{FB_URL}/{path}.json",
            json=data,
            params=fb_auth_param(),
            timeout=5
        )
    except Exception:
        pass

# ══════════════════════════════════════════════════
#  인증
# ══════════════════════════════════════════════════
class LoginRequest(BaseModel):
    email: str
    password: str

class ValuationCalcRequest(BaseModel):
    project_id: str = ""
    inputs: dict = {}

@app.post("/auth/login")
def login(req: LoginRequest):
    users = get_users()
    user = users.get(req.email)
    if not user or user["password"] != req.password:
        raise HTTPException(status_code=401, detail="이메일 또는 비밀번호가 틀렸습니다.")
    token = create_token(req.email, user["role"])
    return {
        "token": token,
        "email": req.email,
        "role": user["role"],
        "expires_in": 86400
    }

@app.get("/auth/me")
def me(user=Depends(get_current_user)):
    return user

# ══════════════════════════════════════════════════
#  PPV
# ══════════════════════════════════════════════════
@app.get("/ppv")
def get_ppv(user=Depends(get_current_user)):
    return fb_read("ppv")

@app.get("/ppv/summary")
def get_ppv_summary(user=Depends(get_current_user)):
    return fb_read("ppv/summary")

class PPVSummary(BaseModel):
    totalRisked: float
    byStage: dict
    projectCount: int

@app.post("/ppv/summary")
def save_ppv_summary(data: PPVSummary, user=Depends(get_current_user)):
    payload = data.dict()
    payload["updatedAt"] = datetime.datetime.now().isoformat()
    payload["updatedBy"] = user["email"]
    fb_write("ppv/summary", payload)
    return {"ok": True, "data": payload}

@app.post("/ppv/snapshot")
def save_snapshot(data: dict, user=Depends(get_current_user)):
    data["ts"] = datetime.datetime.now().isoformat()
    data["by"] = user["email"]
    requests.post(
        f"{FB_URL}/ppv/snapshots.json",
        json=data,
        params=fb_auth_param(),
        timeout=5
    )
    return {"ok": True}

@app.post("/ppv/event")
def save_event(data: dict, user=Depends(get_current_user)):
    data["ts"] = datetime.datetime.now().isoformat()
    data["by"] = user["email"]
    requests.post(
        f"{FB_URL}/ppv/events.json",
        json=data,
        params=fb_auth_param(),
        timeout=5
    )
    return {"ok": True}

@app.post("/ppv/override/{project_name}")
def save_override(project_name: str, data: dict, user=Depends(get_current_user)):
    safe_name = project_name.replace("/", "_").replace(".", "_")
    data["updatedAt"] = datetime.datetime.now().isoformat()
    data["updatedBy"] = user["email"]
    fb_write(f"ppv/overrides/{safe_name}", data)
    return {"ok": True}

# ══════════════════════════════════════════════════
#  재무 (P&L / B/S / C/F)
# ══════════════════════════════════════════════════
@app.get("/financial")
def get_financial(user=Depends(get_current_user)):
    return fb_read("financial")

@app.get("/financial/{stmt}")
def get_stmt(stmt: str, user=Depends(get_current_user)):
    if stmt not in ["pl", "bs", "cf"]:
        raise HTTPException(status_code=400, detail="stmt는 pl, bs, cf 중 하나여야 합니다.")
    return fb_read(f"financial/{stmt}")

@app.get("/financial/{stmt}/{year}")
def get_stmt_year(stmt: str, year: int, user=Depends(get_current_user)):
    if stmt not in ["pl", "bs", "cf"]:
        raise HTTPException(status_code=400, detail="stmt는 pl, bs, cf 중 하나여야 합니다.")
    return fb_read(f"financial/{stmt}/{year}")

class FinancialData(BaseModel):
    year: int
    month: int
    data: dict

@app.post("/financial/{stmt}")
def save_financial(stmt: str, req: FinancialData, user=Depends(get_current_user)):
    if stmt not in ["pl", "bs", "cf"]:
        raise HTTPException(status_code=400, detail="stmt는 pl, bs, cf 중 하나여야 합니다.")
    payload = req.data.copy()
    payload["updatedAt"] = datetime.datetime.now().isoformat()[:16]
    payload["updatedBy"] = user["email"]
    fb_write(f"financial/{stmt}/{req.year}/{req.month}", payload)
    return {"ok": True, "path": f"financial/{stmt}/{req.year}/{req.month}", "data": payload}

# ══════════════════════════════════════════════════
#  매각 현황
# ══════════════════════════════════════════════════
@app.get("/divest")
def get_divest(user=Depends(get_current_user)):
    return fb_read("divest")

@app.post("/divest/{project_name}")
def update_divest(project_name: str, data: dict, user=Depends(get_current_user)):
    safe_name = project_name.replace("/", "_").replace(".", "_")
    data["updatedAt"] = datetime.datetime.now().isoformat()[:16]
    data["updatedBy"] = user["email"]
    fb_write(f"divest/{safe_name}", data)
    return {"ok": True}

# ══════════════════════════════════════════════════
#  Atlas Milestone
# ══════════════════════════════════════════════════
@app.get("/atlas")
def get_atlas(user=Depends(get_current_user)):
    return fb_read("atlas")

@app.post("/atlas/{milestone_id}")
def update_atlas(milestone_id: str, data: dict, user=Depends(get_current_user)):
    data["updatedAt"] = datetime.datetime.now().isoformat()[:16]
    data["updatedBy"] = user["email"]
    fb_write(f"atlas/{milestone_id}", data)
    return {"ok": True}

# ══════════════════════════════════════════════════
#  전체 데이터 (대시보드 초기 로딩용)
# ══════════════════════════════════════════════════
@app.get("/dashboard")
def get_dashboard(user=Depends(get_current_user)):
    return {
        "ppv_summary": fb_read("ppv/summary"),
        "financial": fb_read("financial"),
        "divest": fb_read("divest"),
        "atlas": fb_read("atlas"),
    }

# ══════════════════════════════════════════════════
#  외부 시장 벤치마크 (FRED + LevelTen)
# ══════════════════════════════════════════════════

# FRED 시리즈 매핑: HEUH 투자 의사결정에 의미있는 지표만
FRED_SERIES = {
    # 금리 (할인율 기준점)
    "us_10y":       {"id": "DGS10",        "label": "US 10Y Treasury",    "unit": "%",       "group": "rates"},
    "us_2y":        {"id": "DGS2",         "label": "US 2Y Treasury",     "unit": "%",       "group": "rates"},
    "fed_funds":    {"id": "DFF",          "label": "Fed Funds Rate",     "unit": "%",       "group": "rates"},
    "bbb_spread":   {"id": "BAMLC0A4CBBB", "label": "BBB Corp Spread",    "unit": "%",       "group": "rates"},
    # 에너지/인플레
    "henry_hub":    {"id": "DHHNGSP",      "label": "Henry Hub NatGas",   "unit": "$/MMBtu", "group": "energy"},
    "cpi":          {"id": "CPIAUCSL",     "label": "US CPI (Index)",     "unit": "Index",   "group": "macro"},
    # 환율
    "krw_usd":      {"id": "DEXKOUS",      "label": "KRW/USD",            "unit": "KRW",     "group": "fx"},
}

# TAN ETF는 FRED에 없음 — Stooq로 별도 조회
STOOQ_SYMBOLS = {
    "tan":          {"symbol": "tan.us",   "label": "TAN (Solar ETF)",    "unit": "$",       "group": "equity"},
    "icln":         {"symbol": "icln.us",  "label": "ICLN (Clean Energy)", "unit": "$",      "group": "equity"},
}

def _fred_fetch(series_id: str, days: int = 180):
    """FRED API에서 시리즈 데이터 조회. 최근 N일치."""
    if not FRED_API_KEY:
        return None
    try:
        end = datetime.date.today()
        start = end - datetime.timedelta(days=days)
        res = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": series_id,
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "observation_start": start.isoformat(),
                "observation_end": end.isoformat(),
                "sort_order": "asc",
            },
            timeout=10,
        )
        if res.status_code != 200:
            return None
        obs = res.json().get("observations", [])
        # "." = 결측, 제외
        points = [
            {"date": o["date"], "value": float(o["value"])}
            for o in obs if o.get("value") not in (".", "", None)
        ]
        return points
    except Exception:
        return None

def _stooq_fetch(symbol: str, days: int = 180):
    """Stooq에서 일별 종가 조회 (CSV, 키 불필요)."""
    try:
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        res = requests.get(url, timeout=10)
        if res.status_code != 200 or "Date,Open" not in res.text:
            return None
        lines = res.text.strip().split("\n")[1:]
        end = datetime.date.today()
        start = end - datetime.timedelta(days=days)
        points = []
        for ln in lines:
            parts = ln.split(",")
            if len(parts) < 5:
                continue
            try:
                d = datetime.date.fromisoformat(parts[0])
                if d < start:
                    continue
                points.append({"date": parts[0], "value": float(parts[4])})
            except Exception:
                continue
        return points
    except Exception:
        return None

def _summarize_series(points):
    """시계열 → 최신값/변동/스파크라인 요약."""
    if not points:
        return None
    latest = points[-1]
    prev = points[-2] if len(points) >= 2 else latest
    # 약 일주일 전 (영업일 5개 전)
    week_ago = points[-6] if len(points) >= 6 else points[0]
    # 약 한달 전 (영업일 21개 전)
    month_ago = points[-22] if len(points) >= 22 else points[0]
    return {
        "latest": latest["value"],
        "latest_date": latest["date"],
        "d_1d": latest["value"] - prev["value"],
        "d_1w": latest["value"] - week_ago["value"],
        "d_1m": latest["value"] - month_ago["value"],
        "spark": [p["value"] for p in points[-30:]],  # 최근 30포인트
        "n_points": len(points),
    }

@app.get("/benchmark/market")
def get_market_benchmark(force: int = 0, user=Depends(get_current_user)):
    """FRED + Stooq 시장 벤치마크. 6시간 캐시."""
    today = datetime.date.today().isoformat()
    cache_key = f"benchmark_cache/market/{today}"

    # 캐시 확인 (force=1이면 무시)
    if not force:
        cached = fb_read(cache_key)
        if cached and cached.get("fetched_at"):
            try:
                fetched = datetime.datetime.fromisoformat(cached["fetched_at"])
                age_hrs = (datetime.datetime.utcnow() - fetched).total_seconds() / 3600
                if age_hrs < 6:
                    return cached
            except Exception:
                pass

    if not FRED_API_KEY:
        raise HTTPException(500, "FRED_API_KEY 환경변수 미설정")

    result = {
        "fetched_at": datetime.datetime.utcnow().isoformat()[:19],
        "source": "FRED + Stooq",
        "series": {},
    }

    # FRED 시리즈
    for key, meta in FRED_SERIES.items():
        pts = _fred_fetch(meta["id"], days=400 if key == "cpi" else 180)
        summary = _summarize_series(pts) if pts else None
        result["series"][key] = {
            **meta,
            "data": summary,
            "ok": summary is not None,
        }

    # Stooq 시리즈
    for key, meta in STOOQ_SYMBOLS.items():
        pts = _stooq_fetch(meta["symbol"], days=180)
        summary = _summarize_series(pts) if pts else None
        result["series"][key] = {
            **meta,
            "data": summary,
            "ok": summary is not None,
        }

    # CPI는 YoY % 변화율로 계산 (Index 자체는 의미가 없음)
    cpi = result["series"].get("cpi", {}).get("data")
    if cpi and cpi.get("spark") and len(cpi["spark"]) >= 13:
        try:
            yoy = (cpi["spark"][-1] / cpi["spark"][-13] - 1) * 100
            result["series"]["cpi"]["yoy_pct"] = round(yoy, 2)
        except Exception:
            pass

    # 캐시 저장
    try:
        fb_put(cache_key, result)
    except Exception:
        pass  # 캐시 실패해도 응답은 반환
    return result


# ── LevelTen PPA Index 업로드/조회 ─────────────────
@app.post("/benchmark/levelten/upload")
async def upload_levelten(
    file: UploadFile = File(...),
    quarter: str = Form(...),  # e.g. "2026-Q1"
    user=Depends(get_current_user),
):
    """LevelTen PPA Index 리포트 업로드 → Claude API로 파싱 → Firebase 저장."""
    if not ANTHROPIC_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY 환경변수 미설정")

    raw = await file.read()
    filename = file.filename or "levelten.pdf"
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""

    # 1) 파일 타입별 텍스트 추출
    source_text = ""
    parse_mode = ""
    parsed = None

    if ext in ("csv", "tsv"):
        try:
            source_text = raw.decode("utf-8", errors="ignore")
        except Exception:
            source_text = raw.decode("latin-1", errors="ignore")
        parse_mode = "csv"

    elif ext in ("xlsx", "xls", "xlsb"):
        parse_mode = "excel"
        tmp_path = None
        try:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}")
            tmp_path = tmp.name
            tmp.write(raw)
            tmp.close()

            if ext == "xlsb":
                lines = []
                with open_workbook(tmp_path) as wb:
                    for sheet_name in wb.sheets[:5]:
                        lines.append(f"\n=== Sheet: {sheet_name} ===")
                        with wb.get_sheet(sheet_name) as sh:
                            for row in sh.rows():
                                vals = [str(c.v) if c.v is not None else "" for c in row]
                                lines.append("\t".join(vals))
                source_text = "\n".join(lines)[:40000]
            else:
                try:
                    from openpyxl import load_workbook
                except ImportError:
                    raise HTTPException(400, "openpyxl이 설치되지 않았습니다. requirements.txt에 추가하세요.")
                wb = load_workbook(tmp_path, data_only=True, read_only=True)
                lines = []
                for sheet_name in wb.sheetnames[:5]:
                    lines.append(f"\n=== Sheet: {sheet_name} ===")
                    for row in wb[sheet_name].iter_rows(values_only=True):
                        vals = [str(v) if v is not None else "" for v in row]
                        lines.append("\t".join(vals))
                wb.close()
                source_text = "\n".join(lines)[:40000]
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(400, f"Excel 파싱 실패: {str(e)}")
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try: os.unlink(tmp_path)
                except Exception: pass

    elif ext == "pdf":
        parse_mode = "pdf"
        import base64
        pdf_b64 = base64.standard_b64encode(raw).decode("utf-8")

        headers = {
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        prompt = (
            "You are parsing a LevelTen Energy PPA Price Index report. "
            "Extract ALL regional PPA pricing data into strict JSON (no markdown, no prose).\n\n"
            "Required schema:\n"
            "{\n"
            '  "quarter": "YYYY-QN",\n'
            '  "report_date": "YYYY-MM-DD or null",\n'
            '  "entries": [\n'
            '    {"tech":"solar|wind|storage", "region":"ERCOT|PJM|MISO|CAISO|SPP|NYISO|ISO-NE|WECC|SERC|...",\n'
            '     "term_yr":10, "p25": <number $/MWh>, "p50": <number or null>, "p75": <number or null>}\n'
            "  ],\n"
            '  "notes": "brief description of trends mentioned in the report (1-2 sentences)"\n'
            "}\n\n"
            "Rules:\n"
            "- p25/p50/p75 are USD per MWh.\n"
            "- If only one price tier is shown, put it in p25.\n"
            "- Extract every region × tech × term combination found.\n"
            "- Return ONLY the JSON object. No explanation. No code fences."
        )
        body = {
            "model": "claude-sonnet-4-5",
            "max_tokens": 4000,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "document", "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": pdf_b64,
                    }},
                    {"type": "text", "text": prompt},
                ],
            }],
        }
        try:
            res = requests.post("https://api.anthropic.com/v1/messages",
                                headers=headers, json=body, timeout=90)
            if res.status_code != 200:
                raise HTTPException(502, f"Claude API 오류: {res.text[:300]}")
            ai_text = res.json()["content"][0]["text"].strip()
            if ai_text.startswith("```"):
                ai_text = ai_text.split("```", 2)[1]
                if ai_text.startswith("json"): ai_text = ai_text[4:]
                ai_text = ai_text.rsplit("```", 1)[0]
            parsed = json.loads(ai_text.strip())
        except HTTPException:
            raise
        except json.JSONDecodeError as e:
            raise HTTPException(500, f"AI 응답 JSON 파싱 실패: {str(e)}")
    else:
        raise HTTPException(400, f"지원하지 않는 파일 형식: .{ext} (PDF, CSV, XLSX, XLSB 지원)")

    # CSV/Excel인 경우 Claude에게 텍스트 파싱 요청
    if parse_mode in ("csv", "excel"):
        headers = {
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        prompt = (
            "You are parsing LevelTen Energy PPA Price Index tabular data. "
            f"The data below is from a {parse_mode.upper()} export.\n\n"
            f"DATA:\n{source_text[:30000]}\n\n"
            "Extract into strict JSON (no markdown, no prose):\n"
            "{\n"
            '  "quarter": "YYYY-QN",\n'
            '  "report_date": "YYYY-MM-DD or null",\n'
            '  "entries": [\n'
            '    {"tech":"solar|wind|storage", "region":"ERCOT|PJM|MISO|...",\n'
            '     "term_yr":10, "p25":<number $/MWh>, "p50":<number or null>, "p75":<number or null>}\n'
            "  ],\n"
            '  "notes": "brief trend summary"\n'
            "}\n"
            "Return ONLY the JSON object. No code fences."
        )
        body = {
            "model": "claude-sonnet-4-5",
            "max_tokens": 4000,
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            res = requests.post("https://api.anthropic.com/v1/messages",
                                headers=headers, json=body, timeout=90)
            if res.status_code != 200:
                raise HTTPException(502, f"Claude API 오류: {res.text[:300]}")
            ai_text = res.json()["content"][0]["text"].strip()
            if ai_text.startswith("```"):
                ai_text = ai_text.split("```", 2)[1]
                if ai_text.startswith("json"): ai_text = ai_text[4:]
                ai_text = ai_text.rsplit("```", 1)[0]
            parsed = json.loads(ai_text.strip())
        except HTTPException:
            raise
        except json.JSONDecodeError as e:
            raise HTTPException(500, f"AI 응답 JSON 파싱 실패: {str(e)}")

    if not parsed:
        raise HTTPException(500, "파싱 결과가 비어있습니다.")

    # 쿼터 형식 검증 (YYYY-QN)
    import re
    if not re.match(r'^\d{4}-Q[1-4]$', quarter.upper()):
        raise HTTPException(400, "쿼터 형식은 YYYY-Q1 ~ YYYY-Q4 여야 합니다.")
    quarter = quarter.upper()

    # 쿼터 덮어쓰기 (사용자가 명시한 값이 우선)
    parsed["quarter"] = quarter
    parsed["uploaded_at"] = datetime.datetime.utcnow().isoformat()[:19]
    parsed["uploaded_by"] = user["email"]
    parsed["filename"] = filename
    parsed["parse_mode"] = parse_mode

    # Firebase 저장: benchmark/levelten/{quarter}
    fb_put(f"benchmark/levelten/{quarter}", parsed)

    return {"ok": True, "quarter": quarter, "entries_count": len(parsed.get("entries", [])), "data": parsed}


@app.get("/benchmark/levelten")
def get_levelten_all(user=Depends(get_current_user)):
    """모든 분기별 LevelTen 데이터."""
    return fb_read("benchmark/levelten") or {}


@app.get("/benchmark/levelten/latest")
def get_levelten_latest(user=Depends(get_current_user)):
    """가장 최신 분기의 LevelTen 데이터."""
    all_data = fb_read("benchmark/levelten") or {}
    if not all_data:
        return {}
    # 쿼터 문자열 정렬 (YYYY-QN 포맷이라 사전순 정렬로 충분)
    latest_key = sorted(all_data.keys())[-1]
    return {"quarter": latest_key, **all_data[latest_key]}


@app.delete("/benchmark/levelten/{quarter}")
def delete_levelten(quarter: str, user=Depends(require_admin)):
    """특정 분기 LevelTen 데이터 삭제."""
    import re
    if not re.match(r'^\d{4}-Q[1-4]$', quarter.upper()):
        raise HTTPException(400, "쿼터 형식은 YYYY-Q1 ~ YYYY-Q4 여야 합니다.")
    quarter = quarter.upper()
    try:
        requests.delete(f"{FB_URL}/benchmark/levelten/{quarter}.json",
                        params=fb_auth_param(), timeout=5)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, f"삭제 오류: {str(e)}")


# ── 피어 IRR 벤치마크 (내부 수동 입력값) ────────────
@app.get("/benchmark/peer-irr")
def get_peer_irr(user=Depends(get_current_user)):
    """저장된 피어 IRR 벤치마크 조회."""
    return fb_read("benchmark/peer_irr") or {}


@app.post("/benchmark/peer-irr")
def save_peer_irr(payload: dict, user=Depends(get_current_user)):
    """피어 IRR 벤치마크 저장 (Levered Pre-Tax IRR 레인지)."""
    # 필드 검증
    required_numeric = ["solar_min", "solar_max", "hybrid_min", "hybrid_max", "wind_min", "wind_max"]
    data = {}
    for k in required_numeric:
        v = payload.get(k)
        if v is None:
            raise HTTPException(400, f"누락된 필드: {k}")
        try:
            fv = float(v)
            if fv < 0 or fv > 50:
                raise HTTPException(400, f"{k}: 0~50% 범위여야 합니다.")
            data[k] = round(fv, 2)
        except (ValueError, TypeError):
            raise HTTPException(400, f"{k}: 숫자여야 합니다.")
    # min < max 검증
    for tech in ("solar", "hybrid", "wind"):
        if data[f"{tech}_min"] >= data[f"{tech}_max"]:
            raise HTTPException(400, f"{tech}: min < max 이어야 합니다.")
    # 비고
    note = payload.get("note", "")
    if isinstance(note, str):
        data["note"] = note[:200]
    data["updated_at"] = datetime.datetime.utcnow().isoformat()[:19]
    data["updated_by"] = user["email"]
    fb_put("benchmark/peer_irr", data)
    return {"ok": True, "data": data}


# ══════════════════════════════════════════════════
#  Valuation Calculate (Stage 1 Engine)
# ══════════════════════════════════════════════════
import numpy as np
import numpy_financial as npf

def _calc_engine(inputs: dict) -> dict:
    pv_mwac   = inputs.get('pv_mwac', 199)
    pv_mwdc   = pv_mwac * inputs.get('dc_ac_ratio', 1.34)
    bess_mw   = inputs.get('bess_mw', 199)
    bess_mwh  = inputs.get('bess_mwh', 796)
    life      = int(inputs.get('life', 35))

    # CAPEX
    module_cwp   = inputs.get('module_cwp', 31.5)
    bos_cwp      = inputs.get('bos_cwp', 42.9)
    ess_per_kwh  = inputs.get('ess_per_kwh', 234.5)
    epc_cont_pct = inputs.get('epc_cont_pct', 8.0)
    owner_pct    = inputs.get('owner_pct', 3.0)
    softcost_pct = inputs.get('softcost_pct', 5.0)
    intercon_m   = inputs.get('intercon_m', 120.0)
    dev_cost_m   = inputs.get('dev_cost_m', 20.0)
    capex_etc    = inputs.get('capex_etc', 0)

    pv_module = pv_mwdc*1000*module_cwp/100
    pv_bos    = pv_mwdc*1000*bos_cwp/100
    ess_equip = bess_mwh*ess_per_kwh
    epc_base  = pv_module + pv_bos + ess_equip
    epc_total = epc_base * (1 + epc_cont_pct/100)
    pre_capex = (epc_total*(1+owner_pct/100+softcost_pct/100)
                 + intercon_m*1000 + dev_cost_m*1000 + capex_etc*1000)
    int_rate   = inputs.get('int_rate', 5.5) / 100
    debt_ratio = inputs.get('debt_ratio', 47.6) / 100
    base_capex = pre_capex * (1 + debt_ratio*int_rate*0.75 + 0.012)
    total_capex = float(inputs['capex_total_override'])*1000 if inputs.get('capex_total_override') else base_capex

    dev_margin  = pv_mwac*1000*inputs.get('dev_margin_kwac', 200)/1000
    epc_margin  = epc_base * inputs.get('epc_margin_pct', 7.95)/100
    total_margin = dev_margin + epc_margin

    loan_term  = int(inputs.get('loan_term', 18))
    debt       = total_capex * debt_ratio
    ann_ds     = float(npf.pmt(int_rate, loan_term, -debt)) if debt > 0 else 0

    # TE Flip
    _fy_raw = inputs.get('flip_yield', 8.75)
    if _fy_raw > 50: _fy_raw = _fy_raw / 100   # 875 → 8.75 자동 보정
    flip_yield = _fy_raw / 100
    flip_term  = int(inputs.get('flip_term', 7))
    itc_elig   = inputs.get('itc_elig', 97) / 100
    itc_rate   = inputs.get('itc_rate') or inputs.get('credit_val', 30)
    itc_rate   = itc_rate / 100
    te_mult    = inputs.get('te_mult', 1.115)
    yield_adj  = 1 / (1 + (flip_yield - 0.0875) * 8)
    te_invest  = min(total_capex * itc_elig * itc_rate * te_mult * yield_adj, total_capex * 0.36)
    sponsor_eq = total_capex - debt - te_invest
    effective_eq = sponsor_eq * (1 - int_rate * 0.75)

    # MACRS depreciation
    tax_rate    = inputs.get('tax_rate', 21) / 100
    macrs_basis = total_capex * itc_elig * (1 - itc_rate/2)
    depr_sched  = {i+1: macrs_basis*r for i,r in enumerate(MACRS_5YR)}
    depr_share  = inputs.get('depr_share', 0.7721)  # calibrated to Neptune

    # Cash allocation
    pre_flip_cash_te  = inputs.get('pre_flip_cash_te', 25) / 100
    post_flip_cash_te = inputs.get('post_flip_cash_te', 5) / 100

    # Revenue
    cf_pct       = inputs.get('cf_pct', 21.24)
    net_prod_yr1 = inputs.get('net_prod_yr1', None)
    ann_prod_yr1 = float(net_prod_yr1) if net_prod_yr1 else pv_mwac*cf_pct/100*8760
    ppa_price   = inputs.get('ppa_price', 68.82)
    ppa_term    = int(inputs.get('ppa_term', 25))
    ppa_esc     = inputs.get('ppa_esc', 0) / 100
    # bess_toll: CF_Annual Y1 실제값 우선, 없으면 Summary 파싱값
    bess_toll   = inputs.get('bess_toll_y1_effective') or inputs.get('bess_toll', 14.50)
    bess_toll_t = int(inputs.get('bess_toll_term', 20))
    merch_ppa   = inputs.get('merchant_ppa', 45.0)
    merch_esc   = inputs.get('merchant_esc', 3.0) / 100
    degradation = inputs.get('degradation', 0.0064)
    avail_1     = inputs.get('availability_yr1', 0.977)
    avail_2     = inputs.get('availability_yr2', 0.982)

    # OPEX
    pv_om=inputs.get('pv_om',4.5); pv_om_nc=inputs.get('pv_om_nc',1.0)
    pv_aux=inputs.get('pv_aux',1.56); bess_om=inputs.get('bess_om',8.64)
    bess_om_nc=inputs.get('bess_om_nc',1.0); bess_aux=inputs.get('bess_aux',3.84)
    ins_pv=inputs.get('insurance_pv',10.57); ins_bess=inputs.get('insurance_bess',5.05)
    asset_mgmt=inputs.get('asset_mgmt',210); prop_tax=inputs.get('prop_tax_yr1',3162)
    land_rent=inputs.get('land_rent_yr1',437); opex_etc=inputs.get('opex_etc',0)
    opex_esc=inputs.get('opex_esc',2.0)/100

    # Augmentation
    aug_price=inputs.get('aug_price',150); aug_mwh_pct=inputs.get('aug_mwh_pct',18.8)
    aug_mwh_ea=bess_mwh*aug_mwh_pct/100
    aug_years=[int(y) for y in [inputs.get('aug_y1',4),inputs.get('aug_y2',8),inputs.get('aug_y3',12)] if y and int(y)>0]
    aug_cost_ea=aug_mwh_ea*aug_price

    # Full 35-year CF schedule
    cashflows=[-effective_eq]; unlev_cfs=[-total_capex]; sponsor_cfs=[-effective_eq]; pretax_cfs=[-effective_eq]
    debt_bal=debt; detail=[]; ebitda_yr1=None

    for yr in range(1, life+1):
        avail = avail_1 if yr==1 else avail_2
        prod  = ann_prod_yr1 * avail * ((1-degradation)**(yr-1))

        # CF_Annual parsed schedule 우선 사용 (실제 Neptune 모델값)
        pv_sched   = inputs.get('pv_rev_schedule', [])
        bess_sched = inputs.get('bess_rev_schedule', [])
        merch_sched= inputs.get('merch_rev_schedule', [])

        if pv_sched and yr-1 < len(pv_sched):
            pv_rev = pv_sched[yr-1]
        elif yr <= ppa_term:
            pv_rev = prod*ppa_price*((1+ppa_esc)**(yr-1))/1000
        else:
            pv_rev = prod*merch_ppa*((1+merch_esc)**(yr-1))/1000

        if bess_sched and yr-1 < len(bess_sched):
            bess_rev = bess_sched[yr-1]
        else:
            bess_rev = bess_mw*1000*bess_toll*12/1000 if yr<=bess_toll_t else 0

        if merch_sched and yr-1 < len(merch_sched) and merch_sched[yr-1] > 0:
            pv_rev = merch_sched[yr-1]  # merchant 기간은 merch_sched 우선

        total_rev = pv_rev + bess_rev

        esc=(1+opex_esc)**(yr-1); prop_esc=max(0.35,1-0.025*(yr-1))
        opex=(pv_mwdc*1000*pv_om/1000*esc + pv_mwac*1000*pv_om_nc/1000*esc +
              pv_mwac*1000*pv_aux/1000*esc + bess_mw*1000*bess_om/1000*esc +
              bess_mw*1000*bess_om_nc/1000*esc + bess_mw*1000*bess_aux/1000*esc +
              pv_mwac*1000*ins_pv/1000*esc + bess_mw*1000*ins_bess/1000*esc +
              asset_mgmt*esc + prop_tax*prop_esc + land_rent*esc + opex_etc*1000*esc)

        ebitda = total_rev - opex
        if yr==1: ebitda_yr1=ebitda
        aug_c = aug_cost_ea if yr in aug_years else 0

        if yr<=loan_term and debt_bal>0:
            int_p=debt_bal*int_rate; prin=ann_ds-int_p
            ds=ann_ds; debt_bal=max(0,debt_bal-prin)
        else: ds=0

        # MACRS depreciation tax benefit to sponsor
        depr = depr_sched.get(yr, 0)
        s_tax = depr * tax_rate * depr_share  # sponsor keeps depr_share of tax benefit

        op_cf = ebitda - ds - aug_c
        if yr<=flip_term:
            s_cf = op_cf*(1-pre_flip_cash_te) + s_tax
        else:
            s_cf = op_cf*(1-post_flip_cash_te) + s_tax

        s_cf_pretax = op_cf*(1-pre_flip_cash_te) if yr<=flip_term else op_cf*(1-post_flip_cash_te)
        cashflows.append(op_cf); unlev_cfs.append(ebitda-aug_c); sponsor_cfs.append(s_cf); pretax_cfs.append(s_cf_pretax)
        if yr<=10:
            detail.append({'yr':yr,'rev':round(total_rev,0),'opex':round(opex,0),
                'ebitda':round(ebitda,0),'ds':round(ds,0),'aug':round(aug_c,0),
                'depr':round(depr,0),'s_cf':round(s_cf,0)})

    lirr = _irr_robust(pretax_cfs, guess=0.10)   # Sponsor pretax levered (Neptune Row 26 ~10%)
    uirr = _irr_robust(unlev_cfs, guess=0.05)    # Asset-level unlevered (Neptune Row 27 ~8%)
    sirr = _irr_robust(sponsor_cfs, guess=0.10)  # Sponsor after-tax w/ MACRS
    sirr_c = float(npf.irr(sponsor_cfs[:ppa_term+1]))
    ebitda_yield = ebitda_yr1/total_capex*100 if total_capex else 0

    return {
        'capex_total':   round(total_capex,0),
        'epc_base':      round(epc_base,0),
        'debt':          round(debt,0),
        'equity':        round(sponsor_eq+te_invest,0),
        'te_invest':     round(te_invest,0),
        'sponsor_equity':round(sponsor_eq,0),
        'dev_margin':    round(dev_margin,0),
        'epc_margin':    round(epc_margin,0),
        'total_margin':  round(total_margin,0),
        'levered_irr':   round(lirr,6) if not np.isnan(lirr) else None,
        'unlevered_irr': round(uirr,6) if not np.isnan(uirr) else None,
        'sponsor_irr':   round(sirr,6) if not np.isnan(sirr) else None,
        'sponsor_irr_contract': round(sirr_c,6) if not np.isnan(sirr_c) else None,
        'ebitda_yield':  round(ebitda_yield,2),
        'aug_cost_ea':   round(aug_cost_ea,0),
        'annual_detail': detail,
        'cashflows':     [round(x,0) for x in cashflows[:36]],
    }


@app.post("/valuation/calculate")
def calculate_valuation(req: ValuationCalcRequest, user=Depends(get_current_user)):
    """Run PF calculation engine with given inputs"""
    try:
        result = _calc_engine(req.inputs)
        return {"ok": True, "project_id": req.project_id, "result": result}
    except Exception as e:
        raise HTTPException(500, f"Calculation error: {str(e)}")


@app.get("/valuation/calculate/defaults")
def get_calc_defaults(user=Depends(get_current_user)):
    """Return default input values for the calculator"""
    return {
        "pv_mwac": 199, "dc_ac_ratio": 1.34,
        "bess_mw": 199, "bess_mwh": 796,
        "cf_pct": 21.24, "life": 35,
        "module_cwp": 31.5, "bos_cwp": 42.9, "ess_per_kwh": 234.5,
        "epc_cont_pct": 8.0, "owner_pct": 3.0, "softcost_pct": 5.0,
        "intercon_m": 120.0, "dev_cost_m": 20.0,
        "dev_margin_kwac": 200, "epc_margin_pct": 7.95,
        "ppa_price": 68.82, "ppa_term": 25, "ppa_esc": 0,
        "bess_toll": 13.68, "bess_toll_term": 20, "merchant_ppa": 45.0,
        "degradation": 0.0064,
        "pv_om": 4.5, "bess_om": 8.64, "insurance_pv": 10.57,
        "insurance_bess": 5.05, "asset_mgmt": 210,
        "prop_tax_yr1": 3162, "land_rent_yr1": 437, "opex_esc": 2.0,
        "aug_price": 150, "aug_mwh_pct": 18.8, "aug_y1": 4, "aug_y2": 8, "aug_y3": 12,
        "debt_ratio": 47.6, "int_rate": 5.5, "loan_term": 18,
        "te_pct": 0,
    }

# ══════════════════════════════════════════════════
#  헬스체크
# ══════════════════════════════════════════════════
@app.get("/")
def root():
    return {"status": "ok", "service": "HWR Dashboard API"}

@app.get("/health")
def health():
    return {"status": "ok", "ts": datetime.datetime.now().isoformat()}

# ══════════════════════════════════════════════════
#  프로젝트 데이터 (인허가/예산/일정/메모)
# ══════════════════════════════════════════════════
@app.get("/project/{project_id}")
def get_project(project_id: str, user=Depends(get_current_user)):
    return fb_read(f"projects/{project_id}")

@app.post("/project/{project_id}")
def save_project(project_id: str, data: dict, user=Depends(get_current_user)):
    data["updatedAt"] = datetime.datetime.now().isoformat()[:16]
    data["updatedBy"] = user["email"]
    fb_write(f"projects/{project_id}", data)
    return {"ok": True}

@app.get("/projects")
def get_all_projects(user=Depends(get_current_user)):
    return fb_read("projects")

# ══════════════════════════════════════════════════
#  Valuation (PF 모델 업로드 / 조회)
# ══════════════════════════════════════════════════

def parse_pf_model(filepath: str) -> dict:
    """xlsb에서 핵심 가정값과 아웃풋 추출"""
    assumptions = {}
    outputs = {}

    with open_workbook(filepath) as wb:

        # ── PF Intake → assumptions ───────────────
        try:
            with wb.get_sheet("PF Intake") as ws:
                for row in ws.rows():
                    vals = [c.v for c in row if c.v is not None]
                    if len(vals) < 2:
                        continue
                    label = str(vals[0]).strip()
                    val   = vals[1] if len(vals) > 1 else None

                    intake_map = {
                        "Project Name":             ("project_name", str),
                        "PJ Characteristic":        ("technology",   str),
                        "State":                    ("state",        str),
                        "PV : Project Size (MWac)": ("pv_mwac",      float),
                        "NTP Date":                 ("ntp",          str),
                        "COD":                      ("cod",          str),
                        "DC/AC Ratio":              ("dc_ac_ratio",  float),
                        "Total Site Area":          ("site_area_ac", float),
                    }
                    if label in intake_map:
                        key, typ = intake_map[label]
                        try:
                            assumptions[key] = typ(val) if val is not None else None
                        except Exception:
                            assumptions[key] = str(val) if val is not None else None

                    # BESS: ESS size 행은 "4hr" 같은 duration값 → bess_duration으로 저장
                    if label == "ESS size (MW)":
                        assumptions["bess_duration"] = str(val) if val is not None else None

                    # BESS: ESS Duration 행에서 실제 MW 추출
                    if label == "ESS Duration (Hours)":
                        try:
                            assumptions["bess_mw"] = float(val)
                        except Exception:
                            pass

                    # BESS: ESS storage MWh — hex/int 모두 처리
                    if label == "ESS storage size (MWh)":
                        try:
                            v = val
                            if isinstance(v, str) and v.startswith("0x"):
                                assumptions["bess_mwh"] = float(int(v, 16))
                            else:
                                assumptions["bess_mwh"] = float(v)
                        except Exception:
                            assumptions["bess_mwh"] = None
        except Exception:
            pass

        # ── Quarterly Assumptions → 운영 가정 ─────
        try:
            with wb.get_sheet("Quarterly Assumptions") as ws:
                for row in ws.rows():
                    vals = [c.v for c in row if c.v is not None]
                    if len(vals) < 3:
                        continue
                    label = str(vals[0]).strip()
                    val   = vals[2] if len(vals) > 2 else None

                    qa_map = {
                        "Degradation":              ("degradation",       float),
                        "Availability (yr 1)":      ("availability_yr1",  float),
                        "Availability (yr 2+)":     ("availability_yr2",  float),
                        "PV Covered O&M":           ("pv_om_covered",     float),
                        "PV Non-covered O&M":       ("pv_om_noncovered",  float),
                        "Asset management < 200MW": ("asset_mgmt_sm",     float),
                        "Asset management > 200MW": ("asset_mgmt_lg",     float),
                        "PV Merchant Haircut":      ("merchant_haircut",  float),
                    }
                    if label in qa_map:
                        key, typ = qa_map[label]
                        try:
                            assumptions[key] = typ(val) if val is not None else None
                        except Exception:
                            pass
        except Exception:
            pass

        # ── Summary → outputs (Case 2 = PV+BESS 컬럼 기준, index 3) ──
        try:
            with wb.get_sheet("Summary") as ws:
                for row in ws.rows():
                    vals = [c.v for c in row if c.v is not None]
                    if len(vals) < 3:
                        continue
                    label = str(vals[0]).strip()

                    summary_map = {
                        "levered project IRR (full life)":   "levered_irr",
                        "Unlevered project IRR (full life)": "unlevered_irr",
                        "Sponsor levered IRR (full life)":   "sponsor_irr",
                        "Sponsor levered IRR (contract)":    "sponsor_irr_contract",
                        # 신규: After-Tax IRR (Class B 관점) + WACC
                        "Sponsor levered after-tax IRR (before NOL)":  "sponsor_irr_aftertax_before_nol",
                        "Sponsor levered after-tax IRR (after NOL)":   "sponsor_irr_aftertax_after_nol",
                        "Weighted average cost of capital":  "wacc",
                        "WACC":                              "wacc",
                        "Total Project Cost":                "capex_total",
                        "Debt":                              "debt",
                        "Tax Equity Investment":             "tax_equity",
                        "Sponsor Equity Investment":         "sponsor_equity",
                        "PV : PPA Price":                    "ppa_price",
                        "PV : PPA term":                     "ppa_term",
                        "BESS : Toll rate":                  "bess_toll",
                        "BESS : Toll term":                  "bess_toll_term",
                        "HQC DEV Margin (000$)":             "dev_margin",
                        "Total Margin (000$)":               "total_margin",
                    }
                    if label in summary_map:
                        key = summary_map[label]
                        # Case 2 (PV+BESS) = vals[3], fallback to vals[2]
                        val = vals[3] if len(vals) > 3 else (vals[2] if len(vals) > 2 else None)
                        try:
                            v = float(val)
                            if key in ("levered_irr", "unlevered_irr",
                                       "sponsor_irr", "sponsor_irr_contract",
                                       "sponsor_irr_aftertax_before_nol",
                                       "sponsor_irr_aftertax_after_nol",
                                       "wacc"):
                                outputs[key] = round(v, 6)
                            else:
                                outputs[key] = round(v, 2)
                        except Exception:
                            pass
        except Exception:
            pass

        # ── Returns 시트 → After-Tax IRR (Before/After NOL) 및 기타 세분화 IRR ──
        # Returns 시트의 'Sponsor net aftertax cashflow' 줄에 IRR이 있음
        # 각 IRR 값은 보통 4~5번째 컬럼 위치에 있고, 레이블은 맨 앞
        try:
            with wb.get_sheet("Returns") as ws:
                rows_list = list(ws.rows())
                # 라인 순서대로 처리 (NOL 이전 aftertax는 첫 번째 매칭, 이후는 두 번째)
                aftertax_matches = []
                unlevered_aftertax_matches = []
                for row in rows_list:
                    vals = [c.v for c in row]
                    label = ""
                    # 첫 번째 문자열 셀을 레이블로
                    for v in vals[:3]:
                        if isinstance(v, str) and v.strip():
                            label = v.strip()
                            break
                    if not label:
                        continue
                    # IRR 숫자 찾기 (0 < v < 1 범위의 float)
                    irr_val = None
                    for v in vals:
                        if isinstance(v, float) and 0.001 < v < 0.5 and v != 1.0:
                            # 첫 번째 그럴듯한 IRR 값 (label 이후)
                            irr_val = round(v, 6)
                            break
                    if irr_val is None:
                        continue

                    # Sponsor net pretax cashflow — Levered Pre-Tax
                    #   "(without ITC or PTC)" = baseline (Line 25, ~10.02%)
                    #   "(with PTC)" 는 PTC 모델이므로 제외
                    if (label.startswith("Sponsor net pretax cashflow")
                        and "unlevered" not in label.lower()
                        and "with ptc" not in label.lower()
                        and "with itc" not in label.lower()):
                        if "sponsor_irr_levered_pretax" not in outputs:
                            outputs["sponsor_irr_levered_pretax"] = irr_val
                    # Sponsor net unlevered pretax cashflow
                    elif (label.startswith("Sponsor net unlevered pretax")
                          and "with ptc" not in label.lower()
                          and "with itc" not in label.lower()):
                        if "sponsor_irr_unlevered_pretax" not in outputs:
                            outputs["sponsor_irr_unlevered_pretax"] = irr_val
                    # Sponsor net aftertax cashflow (level IRR, NOL 전/후 두 줄)
                    #   - 첫 등장 = Before NOL (~13.62%)
                    #   - 두 번째 등장 (NOL effect 처리 후) = After NOL (~10.51%)
                    # "(including Residual Value)" 및 State Tax 버전은 제외
                    elif (label == "Sponsor net aftertax cashflow"
                          and "residual" not in label.lower()
                          and "state" not in label.lower()):
                        aftertax_matches.append(irr_val)
                    # Sponsor net unlevered aftertax cashflow
                    elif (label == "Sponsor net unlevered aftertax cashflow"
                          or label == "Sponsor net unlevered aftertax cashflow with NOL"):
                        unlevered_aftertax_matches.append(irr_val)

                # 매칭 순서 기반: first = before NOL, second = after NOL
                if len(aftertax_matches) >= 1:
                    outputs["sponsor_irr_aftertax_before_nol"] = aftertax_matches[0]
                if len(aftertax_matches) >= 2:
                    # 두 번째 매칭이 After NOL (세 번째 이상은 State Tax 변형)
                    outputs["sponsor_irr_aftertax_after_nol"] = aftertax_matches[1]
                if len(unlevered_aftertax_matches) >= 1:
                    outputs["sponsor_irr_unlevered_aftertax_before_nol"] = unlevered_aftertax_matches[0]
                if len(unlevered_aftertax_matches) >= 2:
                    outputs["sponsor_irr_unlevered_aftertax_after_nol"] = unlevered_aftertax_matches[1]
        except Exception:
            pass

        # ── Sensitivities 시트 → WACC, Cost of Debt ──
        # "Weighted average cost of capital" 레이블이 있는 행에서 2번째 컬럼 값
        try:
            with wb.get_sheet("Sensitivities") as ws:
                for row in ws.rows():
                    vals = [c.v for c in row]
                    label = ""
                    for v in vals[:4]:
                        if isinstance(v, str) and v.strip():
                            label = v.strip()
                            break
                    if not label:
                        continue
                    low = label.lower()
                    # WACC - "Weighted average cost of capital"
                    if "weighted average cost of capital" in low and "wacc" not in outputs:
                        for v in vals:
                            if isinstance(v, float) and 0.01 < v < 0.3:
                                outputs["wacc"] = round(v, 6)
                                break
                    # Cost of debt
                    elif label.lower().strip() == "cost of debt" and "cost_of_debt" not in outputs:
                        for v in vals:
                            if isinstance(v, float) and 0.01 < v < 0.3:
                                outputs["cost_of_debt"] = round(v, 6)
                                break
        except Exception:
            pass

    # ── CF_Annual → 연도별 실제 수익 추출
    try:
        with wb.get_sheet("CF_Annual") as ws:
            for row in ws.rows():
                vals = [c.v for c in row if c.v is not None]
                if len(vals) < 5: continue
                label = str(vals[0]).strip()
                # Y1 시작 인덱스: 앞 4개(total, pre-COD, 0, 0) 제거 후 운영연도
                op_vals = [v for v in vals[1:] if isinstance(v, (int, float))]

                if "PPA #2 BESS" in label and "Revenue" in label:
                    try:
                        bess_rev_y1 = float(op_vals[4]) if len(op_vals) > 4 else 0
                        bess_mw = assumptions.get("bess_mw") or 199
                        if bess_rev_y1 > 0 and bess_mw > 0:
                            outputs["bess_toll_y1_effective"] = round(bess_rev_y1/(bess_mw*1000*12)*1000, 4)
                            outputs["bess_rev_y1"] = round(bess_rev_y1, 0)
                        # 연도별 BESS 수익 (인덱스 4~38 = Y1~Y35)
                        outputs["bess_rev_schedule"] = [round(float(v),0) for v in op_vals[4:39] if isinstance(v,(int,float))]
                    except: pass

                if "PPA #1 PV" in label and "Revenue" in label:
                    try:
                        pv_rev_y1 = float(op_vals[4]) if len(op_vals) > 4 else 0
                        if pv_rev_y1 > 0:
                            outputs["pv_rev_y1"] = round(pv_rev_y1, 0)
                        outputs["pv_rev_schedule"] = [round(float(v),0) for v in op_vals[4:39] if isinstance(v,(int,float))]
                    except: pass

                if "Merchant PV Power Revenue" in label:
                    try:
                        outputs["merch_rev_schedule"] = [round(float(v),0) for v in op_vals[4:39] if isinstance(v,(int,float))]
                    except: pass
    except Exception:
        pass

    # bess_mwh 보정: xlsb hex 파싱 한계 → pv_mwac × duration(숫자)으로 계산
    try:
        duration_str = assumptions.get("bess_duration", "")
        duration_h = float("".join(x for x in str(duration_str) if x.isdigit() or x=="."))
        pv_mwac = assumptions.get("pv_mwac") or assumptions.get("bess_mw")
        if pv_mwac and duration_h:
            assumptions["bess_mwh"] = round(float(pv_mwac) * duration_h, 1)
    except Exception:
        pass

    return {"assumptions": assumptions, "outputs": outputs}


@app.post("/valuation/upload")
async def upload_valuation(
    project_id: str = Form(...),
    scenario:   str = Form(default=""),
    reason:     str = Form(default=""),
    approver:   str = Form(default=""),
    file: UploadFile = File(...),
    user=Depends(get_current_user)
):
    """PF 재무모델(xlsb/xlsx) 업로드 → AI 파싱 → Firebase 저장"""
    if not (file.filename.endswith(".xlsb") or file.filename.endswith(".xlsx")):
        raise HTTPException(400, "xlsb 또는 xlsx 파일만 업로드 가능합니다.")

    suffix = ".xlsb" if file.filename.endswith(".xlsb") else ".xlsx"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        parsed = parse_pf_model(tmp_path)
    except Exception as e:
        raise HTTPException(500, f"모델 파싱 실패: {str(e)}")
    finally:
        os.unlink(tmp_path)

    ts      = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    safe_id = project_id.replace("/", "_").replace(".", "_")

    payload = {
        "uploaded_at": datetime.datetime.now().isoformat(),
        "uploaded_by": user["email"],
        "filename":    file.filename,
        "scenario":    scenario,
        "reason":      reason,
        "approver":    approver,
        "assumptions": parsed["assumptions"],
        "outputs":     parsed["outputs"],
    }

    fb_put(f"valuation/{safe_id}/versions/{ts}", payload)
    fb_put(f"valuation/{safe_id}/latest", payload)

    return {
        "ok":         True,
        "project_id": safe_id,
        "timestamp":  ts,
        "parsed":     parsed,
    }


@app.get("/valuation")
def get_all_valuations(user=Depends(get_current_user)):
    """전체 프로젝트 latest 비교 (Valuation 탭용)"""
    all_data = fb_read("valuation") or {}
    result = {}
    for pid, pdata in all_data.items():
        if isinstance(pdata, dict) and "latest" in pdata:
            result[pid] = pdata["latest"]
    return result


@app.get("/valuation/{project_id}")
def get_valuation(project_id: str, user=Depends(get_current_user)):
    safe_id = project_id.replace("/", "_").replace(".", "_")
    return fb_read(f"valuation/{safe_id}")


@app.get("/valuation/{project_id}/latest")
def get_valuation_latest(project_id: str, user=Depends(get_current_user)):
    safe_id = project_id.replace("/", "_").replace(".", "_")
    return fb_read(f"valuation/{safe_id}/latest")


@app.get("/valuation/{project_id}/versions")
def get_valuation_versions(project_id: str, user=Depends(get_current_user)):
    safe_id = project_id.replace("/", "_").replace(".", "_")
    return fb_read(f"valuation/{safe_id}/versions")


@app.post("/valuation/generate-ic-summary")
async def generate_ic_summary(payload: dict, user=Depends(get_current_user)):
    """Claude API로 IC Summary 전문 보고서 생성"""
    if not ANTHROPIC_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY 환경변수 미설정")

    proj    = payload.get("project_name", "Project")
    metrics = payload.get("metrics", {})
    scenarios = payload.get("scenarios", [])
    assumptions = payload.get("assumptions", {})
    history = payload.get("history", [])
    today   = payload.get("date", "")

    scen_text = ""
    if scenarios:
        scen_text = "\n\nScenario Analysis:\n"
        for s in scenarios:
            scen_text += f"  {s.get('name','')}: IRR {s.get('irr','—')}, Dev Margin {s.get('margin','—')}\n"

    hist_text = ""
    if history:
        hist_text = "\n\nVersion History (recent):\n"
        for h in history[:3]:
            hist_text += f"  {h.get('date','')} — {h.get('reason','')}\n"

    prompt = (
        "You are a senior investment analyst at a US renewable energy developer. "
        "Write a concise, professional Investment Committee (IC) Summary in Korean (with key financial metrics in English). "
        "Use formal Korean business writing style. Structure it with clear sections.\n\n"
        f"Project: {proj}\n"
        f"Date: {today}\n\n"
        "Financial Metrics:\n"
        f"  Sponsor IRR: {metrics.get('sirr','—')}\n"
        f"  Dev Margin: {metrics.get('dev_margin','—')}\n"
        f"  Levered IRR: {metrics.get('lirr','—')}\n"
        f"  Unlevered IRR: {metrics.get('uirr','—')}\n"
        f"  EBITDA Yield: {metrics.get('ebitda_yield','—')}\n"
        f"  Total CAPEX: {metrics.get('capex','—')}\n"
        f"  Debt: {metrics.get('debt','—')} ({metrics.get('debt_pct','—')})\n"
        f"  Tax Equity: {metrics.get('te','—')}\n"
        f"  Sponsor Equity: {metrics.get('eq','—')}\n"
        f"  PPA: {metrics.get('ppa','—')}\n"
        f"  BESS Toll: {metrics.get('toll','—')}\n"
        f"  ITC/PTC: {metrics.get('credit','—')}\n"
        f"  Flip Yield: {metrics.get('flip','—')}\n"
        f"{scen_text}{hist_text}\n\n"
        "Write the IC Summary with these sections:\n"
        "1. 프로젝트 개요 (2-3 sentences)\n"
        "2. 핵심 재무 지표 (bullet points with brief commentary)\n"
        "3. Deal Structure 특징 (TE flip structure, debt terms 등)\n"
        "4. 리스크 요인 (2-3 key risks)\n"
        "5. 투자 의견 (1 paragraph recommendation)\n\n"
        "Keep it concise — suitable for a 1-page print. "
        "Return ONLY valid JSON: "
        '{{"sections":[{{"title":"섹션제목","content":"내용"}}]}}'
    )

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": prompt}]
        },
        timeout=40
    )
    if resp.status_code != 200:
        raise HTTPException(500, f"Claude API 오류: {resp.text[:200]}")

    data = resp.json()
    text = "".join(b.get("text","") for b in data.get("content",[]))
    clean = text.replace("```json","").replace("```","").strip()
    return {"ok": True, "result": clean}


# ══════════════════════════════════════════════════
#  IC Summary PDF Export (WeasyPrint — world-class formatting)
# ══════════════════════════════════════════════════
import base64 as _base64
from fastapi.responses import Response as _Response

def _esc_html(s):
    """HTML escape helper."""
    if s is None:
        return ""
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))

def _fmt_pct(v, decimals=2):
    """숫자 → 퍼센트 문자열. 이미 문자열이면 그대로."""
    if v is None or v == "—":
        return "—"
    if isinstance(v, str):
        return v
    try:
        return f"{float(v)*100:.{decimals}f}%"
    except Exception:
        return "—"

def _fmt_usd_m(v):
    """$M 포맷 (input in thousands)."""
    if v is None or v == "—":
        return "—"
    if isinstance(v, str):
        return v
    try:
        return f"${float(v)/1000:.1f}M"
    except Exception:
        return "—"

def _build_ic_pdf_html(data: dict) -> str:
    """IC Summary HTML (WeasyPrint용). World-class IB/PE 수준 포맷."""
    proj_name = data.get("project_name", "Project")
    today = data.get("date", datetime.date.today().isoformat())
    verdict = (data.get("verdict") or "").upper() or "—"
    verdict_color = data.get("verdict_color", "amber")

    # 색상 매핑
    color_map = {
        "green": "#059669",  # emerald-600
        "amber": "#D97706",  # amber-600
        "red":   "#DC2626",  # red-600
    }
    v_color = color_map.get(verdict_color, "#6B7280")

    # 지표
    outputs = data.get("outputs", {}) or {}
    assumptions = data.get("assumptions", {}) or {}
    pv_mwac = assumptions.get("pv_mwac") or outputs.get("pv_mwac") or "—"
    bess_mw = assumptions.get("bess_mw") or "—"
    cod = assumptions.get("cod") or "—"
    ntp = assumptions.get("ntp") or "—"
    state = data.get("state") or assumptions.get("state") or "—"
    iso = data.get("iso") or assumptions.get("iso") or "—"

    # 5 IRR 지표
    irr_lev_pre  = _fmt_pct(outputs.get("sponsor_irr_levered_pretax") or outputs.get("sponsor_irr"))
    irr_at_before = _fmt_pct(outputs.get("sponsor_irr_aftertax_before_nol"))
    irr_at_after  = _fmt_pct(outputs.get("sponsor_irr_aftertax_after_nol"))
    irr_unlev    = _fmt_pct(outputs.get("sponsor_irr_unlevered_pretax") or outputs.get("unlevered_irr"))
    wacc_val     = _fmt_pct(outputs.get("wacc"))

    # 재무 요약
    capex = _fmt_usd_m(outputs.get("capex_total"))
    debt  = _fmt_usd_m(outputs.get("debt"))
    te    = _fmt_usd_m(outputs.get("tax_equity"))
    eq    = _fmt_usd_m(outputs.get("sponsor_equity"))
    dev_margin = _fmt_usd_m(outputs.get("dev_margin"))
    margin_cwp = outputs.get("margin_cwp")
    margin_cwp_str = f"{margin_cwp:.2f} c/Wp" if isinstance(margin_cwp, (int, float)) else "—"
    ppa_price = outputs.get("ppa_price") or assumptions.get("ppa_price") or "—"
    ppa_term  = outputs.get("ppa_term") or assumptions.get("ppa_term") or "—"
    bess_toll = outputs.get("bess_toll") or assumptions.get("bess_toll") or "—"
    ebitda_y  = outputs.get("ebitda_yield")
    ebitda_y_str = f"{ebitda_y:.2f}%" if isinstance(ebitda_y, (int, float)) else "—"

    # AI 분석 결과 (IC Opinion 에서 생성된 것)
    ic_analysis = data.get("ic_analysis", {}) or {}
    thesis = ic_analysis.get("thesis", "")
    rec    = ic_analysis.get("rec", "")
    risks  = ic_analysis.get("risks", []) or []
    threshold_status = ic_analysis.get("threshold_status", {}) or {}
    dev_ic = ic_analysis.get("dev_ic", {}) or {}

    # Sensitivity (프론트에서 계산된 값)
    scenarios = data.get("scenarios", []) or []

    # Threshold 메타
    thresholds = data.get("thresholds", {}) or {}
    thr_irr = thresholds.get("sponsor_irr_pct", 9.0)
    thr_margin = thresholds.get("dev_margin_cwp", 10.0)

    # 위험 항목 렌더
    risks_html = ""
    sev_color = {"Critical": "#DC2626", "Watch": "#D97706", "OK": "#059669"}
    for i, r in enumerate(risks[:8]):
        sev = r.get("severity", "OK")
        c = sev_color.get(sev, "#6B7280")
        title = _esc_html(r.get("title", ""))
        detail = _esc_html(r.get("detail", ""))
        risks_html += f"""
        <div class="risk-item">
          <div class="risk-header">
            <span class="risk-num">{i+1:02d}</span>
            <span class="risk-title">{title}</span>
            <span class="risk-sev" style="background:{c}">{sev}</span>
          </div>
          <div class="risk-detail">{detail}</div>
        </div>
        """

    # Scenario 테이블
    scen_rows = ""
    for s in scenarios:
        scen_rows += f"""
        <tr>
          <td class="scen-name">{_esc_html(s.get('name','—'))}</td>
          <td class="scen-val">{_esc_html(s.get('irr','—'))}</td>
          <td class="scen-val">{_esc_html(s.get('margin','—'))}</td>
        </tr>
        """

    # Threshold 체크
    def _chk(ok):
        return ('<span style="color:#059669;font-weight:700">✓ PASS</span>' if ok
                else '<span style="color:#DC2626;font-weight:700">✗ FAIL</span>')
    thr_irr_ok = threshold_status.get("irr_ok", False)
    thr_margin_ok = threshold_status.get("margin_ok", False)
    thr_irr_gap = _esc_html(threshold_status.get("irr_gap", ""))
    thr_margin_gap = _esc_html(threshold_status.get("margin_gap", ""))

    # HTML 조립
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>IC Summary - {_esc_html(proj_name)}</title>
<style>
@page {{
  size: A4 portrait;
  margin: 18mm 16mm 18mm 16mm;
  @bottom-center {{
    content: counter(page) " / " counter(pages);
    font-family: 'Noto Sans KR', 'Helvetica', sans-serif;
    font-size: 8pt;
    color: #6B7280;
  }}
  @bottom-left {{
    content: "Hanwha Energy USA Holdings · Internal IC Memo";
    font-family: 'Noto Sans KR', 'Helvetica', sans-serif;
    font-size: 7pt;
    color: #9CA3AF;
  }}
  @bottom-right {{
    content: "{_esc_html(today)}";
    font-family: 'Noto Sans KR', 'Helvetica', sans-serif;
    font-size: 7pt;
    color: #9CA3AF;
  }}
}}
@page :first {{
  @bottom-center {{ content: none; }}
  @bottom-left {{ content: none; }}
  @bottom-right {{ content: none; }}
}}
* {{ box-sizing: border-box; }}
body {{
  font-family: 'Noto Sans KR', 'Helvetica Neue', Helvetica, sans-serif;
  font-size: 10pt;
  line-height: 1.55;
  color: #111827;
  margin: 0;
  padding: 0;
  -webkit-font-smoothing: antialiased;
}}

/* ── Cover ────────────────────────────────────── */
.cover {{
  height: 260mm;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  padding: 20mm 8mm 8mm 8mm;
}}
.cover-header {{
  font-size: 8pt;
  font-weight: 600;
  color: #6B7280;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  border-bottom: 1px solid #E5E7EB;
  padding-bottom: 12pt;
}}
.cover-main {{
  margin-top: 40mm;
}}
.cover-tag {{
  font-size: 9pt;
  font-weight: 600;
  color: {v_color};
  letter-spacing: 0.16em;
  text-transform: uppercase;
  margin-bottom: 10pt;
}}
.cover-title {{
  font-size: 36pt;
  font-weight: 800;
  color: #111827;
  letter-spacing: -1.2pt;
  line-height: 1.05;
  margin-bottom: 14pt;
}}
.cover-sub {{
  font-size: 12pt;
  color: #4B5563;
  font-weight: 400;
  margin-bottom: 40pt;
}}
.cover-verdict {{
  display: inline-block;
  padding: 10pt 22pt;
  border: 2pt solid {v_color};
  border-radius: 2pt;
  font-size: 22pt;
  font-weight: 800;
  color: {v_color};
  letter-spacing: 4pt;
}}
.cover-stats {{
  display: flex;
  gap: 20pt;
  margin-top: 24pt;
}}
.cover-stat {{
  flex: 1;
  border-left: 2pt solid #E5E7EB;
  padding-left: 10pt;
}}
.cover-stat-label {{
  font-size: 7pt;
  font-weight: 700;
  color: #6B7280;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  margin-bottom: 3pt;
}}
.cover-stat-value {{
  font-size: 16pt;
  font-weight: 700;
  color: #111827;
  font-variant-numeric: tabular-nums;
}}
.cover-footer {{
  margin-top: auto;
  padding-top: 20pt;
  border-top: 1px solid #E5E7EB;
  display: flex;
  justify-content: space-between;
  font-size: 8pt;
  color: #6B7280;
}}

/* ── Content Pages ────────────────────────────── */
.page-break {{ page-break-before: always; }}

h1 {{
  font-size: 14pt;
  font-weight: 800;
  color: #111827;
  margin: 0 0 3pt 0;
  letter-spacing: -0.3pt;
}}
.section-sub {{
  font-size: 8pt;
  color: #6B7280;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  margin-bottom: 12pt;
  border-bottom: 1pt solid #E5E7EB;
  padding-bottom: 6pt;
}}
h2 {{
  font-size: 10pt;
  font-weight: 700;
  color: #111827;
  margin: 14pt 0 6pt 0;
  letter-spacing: 0;
}}
p {{ margin: 4pt 0; color: #1F2937; }}

/* Metrics Grid */
.metrics-grid {{
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 6pt;
  margin-bottom: 14pt;
}}
.metric-card {{
  padding: 8pt 10pt;
  border: 0.5pt solid #D1D5DB;
  border-radius: 2pt;
}}
.metric-card-primary {{
  border-left: 2.5pt solid #059669;
}}
.metric-card-secondary {{
  border-left: 2.5pt solid #D97706;
}}
.metric-card-wacc {{
  border-left: 2.5pt solid #2563EB;
}}
.metric-label {{
  font-size: 7pt;
  font-weight: 700;
  color: #6B7280;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  margin-bottom: 3pt;
}}
.metric-value {{
  font-size: 15pt;
  font-weight: 700;
  color: #111827;
  font-variant-numeric: tabular-nums;
  line-height: 1.1;
}}
.metric-sub {{
  font-size: 7pt;
  color: #6B7280;
  margin-top: 2pt;
}}

/* Financial table */
.fin-table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 9pt;
  margin: 8pt 0 14pt 0;
}}
.fin-table th {{
  text-align: left;
  padding: 6pt 8pt;
  border-bottom: 1pt solid #111827;
  font-size: 7pt;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #374151;
}}
.fin-table td {{
  padding: 5pt 8pt;
  border-bottom: 0.3pt solid #E5E7EB;
  font-variant-numeric: tabular-nums;
}}
.fin-table td.val {{ text-align: right; font-weight: 600; }}
.fin-table tr.subtotal td {{
  background: #F9FAFB;
  font-weight: 700;
  border-top: 0.5pt solid #6B7280;
}}

/* Threshold check */
.thr-box {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10pt;
  margin: 10pt 0;
}}
.thr-item {{
  padding: 10pt 12pt;
  border: 1pt solid #E5E7EB;
  border-radius: 2pt;
  background: #F9FAFB;
}}
.thr-label {{
  font-size: 8pt;
  font-weight: 600;
  color: #6B7280;
  margin-bottom: 4pt;
}}
.thr-status {{ font-size: 11pt; margin-bottom: 3pt; }}
.thr-gap {{ font-size: 9pt; color: #374151; }}

/* Thesis / Recommendation boxes */
.thesis-box {{
  padding: 12pt 14pt;
  background: #F9FAFB;
  border-left: 3pt solid #2563EB;
  border-radius: 0 2pt 2pt 0;
  margin: 10pt 0;
  font-size: 10pt;
  line-height: 1.7;
  color: #1F2937;
}}
.rec-box {{
  padding: 14pt 16pt;
  background: #FEF3C7;
  border-left: 3pt solid #D97706;
  border-radius: 0 2pt 2pt 0;
  margin: 10pt 0;
  font-size: 10pt;
  line-height: 1.7;
  color: #78350F;
  font-weight: 500;
}}

/* Risk items */
.risk-item {{
  padding: 10pt 0;
  border-bottom: 0.5pt solid #E5E7EB;
}}
.risk-item:last-child {{ border-bottom: none; }}
.risk-header {{
  display: flex;
  align-items: center;
  gap: 8pt;
  margin-bottom: 4pt;
}}
.risk-num {{
  font-size: 8pt;
  font-weight: 700;
  color: #9CA3AF;
  font-variant-numeric: tabular-nums;
  min-width: 18pt;
}}
.risk-title {{
  font-size: 10pt;
  font-weight: 700;
  color: #111827;
  flex: 1;
}}
.risk-sev {{
  color: #fff;
  font-size: 7pt;
  font-weight: 700;
  padding: 2pt 7pt;
  border-radius: 2pt;
  letter-spacing: 0.05em;
}}
.risk-detail {{
  font-size: 9pt;
  color: #4B5563;
  line-height: 1.6;
  margin-left: 26pt;
}}

/* Scenario table */
.scen-table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 9pt;
  margin: 8pt 0;
}}
.scen-table th {{
  text-align: left;
  padding: 7pt 10pt;
  background: #111827;
  color: #fff;
  font-size: 7.5pt;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}}
.scen-table td {{
  padding: 7pt 10pt;
  border-bottom: 0.5pt solid #E5E7EB;
}}
.scen-name {{ font-weight: 700; color: #111827; }}
.scen-val {{ font-variant-numeric: tabular-nums; text-align: right; }}

/* Dev IC Grid */
.devic-grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10pt;
  margin: 10pt 0;
}}
.devic-item {{
  padding: 9pt 12pt;
  border: 0.5pt solid #E5E7EB;
  border-radius: 2pt;
  background: #FEFEFE;
}}
.devic-label {{
  font-size: 7pt;
  font-weight: 700;
  color: #6B7280;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  margin-bottom: 3pt;
}}
.devic-value {{
  font-size: 9pt;
  color: #1F2937;
  line-height: 1.5;
}}

/* Footer note */
.confidential-note {{
  margin-top: 20pt;
  padding-top: 10pt;
  border-top: 0.3pt solid #E5E7EB;
  font-size: 7pt;
  color: #9CA3AF;
  font-style: italic;
  text-align: center;
}}
</style>
</head>
<body>

<!-- ══ COVER PAGE ══ -->
<div class="cover">
  <div>
    <div class="cover-header">Hanwha Energy USA Holdings · Investment Committee Memo</div>
  </div>

  <div class="cover-main">
    <div class="cover-tag">Confidential · Internal Use Only</div>
    <div class="cover-title">{_esc_html(proj_name)}</div>
    <div class="cover-sub">{_esc_html(pv_mwac)} MWac Solar + BESS · {_esc_html(state)} ({_esc_html(iso)}) · COD {_esc_html(cod)}</div>

    <div class="cover-verdict">{_esc_html(verdict)}</div>

    <div class="cover-stats">
      <div class="cover-stat">
        <div class="cover-stat-label">Sponsor IRR</div>
        <div class="cover-stat-value">{irr_lev_pre}</div>
      </div>
      <div class="cover-stat">
        <div class="cover-stat-label">Dev Margin</div>
        <div class="cover-stat-value">{dev_margin}</div>
      </div>
      <div class="cover-stat">
        <div class="cover-stat-label">Total CAPEX</div>
        <div class="cover-stat-value">{capex}</div>
      </div>
      <div class="cover-stat">
        <div class="cover-stat-label">WACC</div>
        <div class="cover-stat-value">{wacc_val}</div>
      </div>
    </div>
  </div>

  <div class="cover-footer">
    <span>Prepared: {_esc_html(today)}</span>
    <span>{_esc_html(data.get("prepared_by",""))}</span>
  </div>
</div>

<!-- ══ PAGE 2 — EXECUTIVE SUMMARY ══ -->
<div class="page-break">
  <h1>Executive Summary</h1>
  <div class="section-sub">투자 의견 · 핵심 논거</div>

  <h2>투자 논거 (Investment Thesis)</h2>
  <div class="thesis-box">{_esc_html(thesis) if thesis else "(AI 분석 미완료 — IC Opinion 탭에서 Run AI Analysis 실행 후 재생성)"}</div>

  <h2>행동 권고 (Recommendation)</h2>
  <div class="rec-box">{_esc_html(rec) if rec else "(AI 분석 미완료)"}</div>

  <h2>Development IC 평가</h2>
  <div class="devic-grid">
    <div class="devic-item">
      <div class="devic-label">NTP 달성 확률</div>
      <div class="devic-value">{_esc_html(dev_ic.get("ntp_prob",""))}</div>
    </div>
    <div class="devic-item">
      <div class="devic-label">ITC Expiry 평가</div>
      <div class="devic-value">{_esc_html(dev_ic.get("itc_expiry_verdict",""))}</div>
    </div>
    <div class="devic-item">
      <div class="devic-label">Safe Harbor</div>
      <div class="devic-value">{_esc_html(dev_ic.get("safe_harbor",""))}</div>
    </div>
    <div class="devic-item">
      <div class="devic-label">개발 단계</div>
      <div class="devic-value">{_esc_html(dev_ic.get("stage_ok",""))}</div>
    </div>
  </div>
</div>

<!-- ══ PAGE 3 — FINANCIAL SUMMARY ══ -->
<div class="page-break">
  <h1>Financial Summary</h1>
  <div class="section-sub">재무 지표 · 자본 구조 · 계약 조건</div>

  <h2>Returns Detail</h2>
  <div class="metrics-grid">
    <div class="metric-card metric-card-primary">
      <div class="metric-label">Sponsor IRR</div>
      <div class="metric-value">{irr_lev_pre}</div>
      <div class="metric-sub">Levered · Pre-Tax</div>
    </div>
    <div class="metric-card">
      <div class="metric-label">Sponsor IRR</div>
      <div class="metric-value">{irr_at_before}</div>
      <div class="metric-sub">After-Tax (Before NOL)</div>
    </div>
    <div class="metric-card metric-card-secondary">
      <div class="metric-label">Sponsor IRR</div>
      <div class="metric-value">{irr_at_after}</div>
      <div class="metric-sub">After-Tax (After NOL)</div>
    </div>
    <div class="metric-card">
      <div class="metric-label">Project IRR</div>
      <div class="metric-value">{irr_unlev}</div>
      <div class="metric-sub">Unlevered · Pre-Tax</div>
    </div>
    <div class="metric-card metric-card-wacc">
      <div class="metric-label">WACC</div>
      <div class="metric-value">{wacc_val}</div>
      <div class="metric-sub">Capital Cost · Hurdle</div>
    </div>
  </div>

  <h2>Investment Thresholds (기준 달성)</h2>
  <div class="thr-box">
    <div class="thr-item">
      <div class="thr-label">Sponsor IRR (Levered Pre-Tax, min {thr_irr}%)</div>
      <div class="thr-status">{_chk(thr_irr_ok)}</div>
      <div class="thr-gap">{thr_irr_gap}</div>
    </div>
    <div class="thr-item">
      <div class="thr-label">Dev Margin (min {thr_margin} c/Wp)</div>
      <div class="thr-status">{_chk(thr_margin_ok)}</div>
      <div class="thr-gap">{thr_margin_gap}</div>
    </div>
  </div>

  <h2>Capital Structure & Deal Terms</h2>
  <table class="fin-table">
    <thead><tr><th>Item</th><th style="text-align:right">Value</th></tr></thead>
    <tbody>
      <tr><td>Total CAPEX</td><td class="val">{capex}</td></tr>
      <tr><td>Senior Debt</td><td class="val">{debt}</td></tr>
      <tr><td>Tax Equity</td><td class="val">{te}</td></tr>
      <tr><td>Sponsor Equity</td><td class="val">{eq}</td></tr>
      <tr class="subtotal"><td>Dev Margin</td><td class="val">{dev_margin} ({margin_cwp_str})</td></tr>
      <tr><td>EBITDA Yield (Y1)</td><td class="val">{ebitda_y_str}</td></tr>
      <tr><td>PPA Price × Term</td><td class="val">${_esc_html(ppa_price)}/MWh × {_esc_html(ppa_term)}yr</td></tr>
      <tr><td>BESS Toll</td><td class="val">${_esc_html(bess_toll)}/kW-mo</td></tr>
    </tbody>
  </table>

  <h2>Scenario Analysis</h2>
  <table class="scen-table">
    <thead><tr><th>Scenario</th><th style="text-align:right">Sponsor IRR</th><th style="text-align:right">Dev Margin</th></tr></thead>
    <tbody>{scen_rows if scen_rows else '<tr><td colspan="3" style="color:#9CA3AF;text-align:center">시나리오 미실행</td></tr>'}</tbody>
  </table>
</div>

<!-- ══ PAGE 4 — RISK ASSESSMENT ══ -->
<div class="page-break">
  <h1>Risk Assessment</h1>
  <div class="section-sub">개발 리스크 · 재무 리스크 · 외부 리스크</div>

  {risks_html if risks_html else '<p style="color:#9CA3AF">AI 분석 미완료 — IC Opinion 탭에서 Run AI Analysis 실행 후 재생성</p>'}

  <div class="confidential-note">
    본 문서는 Hanwha Energy USA Holdings 내부 투자심의 목적으로만 작성되었으며, 외부 유출을 금합니다.<br>
    수치 및 가정은 {_esc_html(today)} 기준 엑셀 재무모델 및 시장 데이터를 근거로 하며, 시장 변동에 따라 달라질 수 있습니다.
  </div>
</div>

</body>
</html>"""
    return html


@app.post("/valuation/export-pdf")
async def export_ic_pdf(payload: dict, user=Depends(get_current_user)):
    """IC Summary PDF 생성 (WeasyPrint, world-class formatting)."""
    import traceback
    import sys

    # Step 1: WeasyPrint import
    try:
        from weasyprint import HTML
        print(f"[export-pdf] WeasyPrint import OK", flush=True)
    except Exception as e:
        print(f"[export-pdf] WeasyPrint import FAILED: {e}", flush=True)
        traceback.print_exc(file=sys.stdout)
        raise HTTPException(500, f"WeasyPrint import 실패: {str(e)[:300]}")

    # Step 2: HTML 문자열 생성
    try:
        html_str = _build_ic_pdf_html(payload)
        print(f"[export-pdf] HTML built, length={len(html_str)}", flush=True)
    except Exception as e:
        print(f"[export-pdf] HTML build FAILED: {e}", flush=True)
        traceback.print_exc(file=sys.stdout)
        raise HTTPException(500, f"HTML 생성 오류: {str(e)[:300]}")

    # Step 3: PDF 렌더링
    try:
        pdf_bytes = HTML(string=html_str).write_pdf()
        print(f"[export-pdf] PDF rendered, size={len(pdf_bytes)} bytes", flush=True)
    except Exception as e:
        print(f"[export-pdf] PDF render FAILED: {e}", flush=True)
        traceback.print_exc(file=sys.stdout)
        raise HTTPException(500, f"PDF 렌더링 오류: {str(e)[:300]}")

    proj_name = payload.get("project_name", "IC_Summary").replace(" ", "_")
    date_str = payload.get("date", datetime.date.today().isoformat()).replace("-", "")
    filename = f"IC_Summary_{proj_name}_{date_str}.pdf"

    return _Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# ── WeasyPrint 진단용 미니멀 테스트 엔드포인트 ───────
@app.get("/valuation/export-pdf-test")
async def export_pdf_test(user=Depends(get_current_user)):
    """WeasyPrint가 살아있는지 간단히 테스트."""
    import traceback, sys
    try:
        from weasyprint import HTML
        simple_html = "<html><body><h1>Test</h1><p>안녕하세요, WeasyPrint 테스트</p></body></html>"
        pdf = HTML(string=simple_html).write_pdf()
        return _Response(
            content=pdf,
            media_type="application/pdf",
            headers={"Content-Disposition": 'attachment; filename="weasyprint_test.pdf"'}
        )
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        raise HTTPException(500, f"테스트 실패: {str(e)[:500]}")


@app.post("/valuation/analyze-cf")
async def analyze_cf(payload: dict, user=Depends(get_current_user)):
    """CF 데이터를 Claude API로 분석하여 인사이트 반환"""
    if not ANTHROPIC_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY 환경변수 미설정")

    cf_text   = payload.get("cf_text", "")
    proj_name = payload.get("project_name", "프로젝트")

    context        = payload.get("context", "")
    proj_context   = payload.get("project_context", "")  # PPV 탭 프로젝트 메타데이터
    lang           = payload.get("lang", "en")
    mode           = payload.get("mode", "full")
    proj_meta      = payload.get("project_meta", {})
    stage    = proj_meta.get("stage", "")
    iso      = proj_meta.get("iso", "")
    proj_type= proj_meta.get("type", "")
    ntp_date = proj_meta.get("ntp", "")
    cod_date = proj_meta.get("cod", "")
    risk_pct = proj_meta.get("risk_factor", "")
    if risk_pct != "": risk_pct = f"{float(risk_pct)*100:.0f}%"
    itc_risk = proj_meta.get("itc_expiry_risk", "")
    proj_ctx = proj_meta.get("proj_ctx", "")
    thresholds     = payload.get("thresholds", {})
    current_metrics= payload.get("current_metrics", {})

    irr_thr    = thresholds.get("sponsor_irr_pct", 9.0)
    margin_thr = thresholds.get("dev_margin_cwp", 10.0)
    itc_thr    = thresholds.get("itc_min_pct", 30.0)

    curr_irr    = current_metrics.get("sponsor_irr_pct", "?")
    curr_margin = current_metrics.get("dev_margin_cwp", "?")
    curr_itc    = current_metrics.get("itc_rate_pct", "?")
    ppa_term    = current_metrics.get("ppa_term", "?")
    toll_term   = current_metrics.get("toll_term", "?")
    pv_mwac     = current_metrics.get("pv_mwac", "?")

    if mode == "interp":
        prompt = (
            "미국 태양광+BESS PF 전문가로서 아래 연도별 Sponsor CF 패턴을 분석해줘.\n"
            f"프로젝트: {proj_name}\n"
            f"CF: {cf_text}\n\n"
            "3~4개 핵심 인사이트를 JSON으로 반환 (다른 텍스트 없이):\n"
            '{"insights":[{"title":"제목","detail":"설명(80자이내)"}]}'
        )
    else:
        # 동적 날짜 계산
        _today = datetime.date.today()
        _current_year = _today.year
        _current_quarter = f"{_current_year}-Q{(_today.month - 1) // 3 + 1}"
        _prev_q_month = _today.month - 3
        _prev_q_year = _current_year
        if _prev_q_month <= 0:
            _prev_q_month += 12
            _prev_q_year -= 1
        _prev_quarter = f"{_prev_q_year}-Q{(_prev_q_month - 1) // 3 + 1}"

        # 시장 데이터 컨텍스트 (payload에서 주입, 없으면 기본)
        market_context = payload.get("market_context", {}) or {}
        rates_txt = market_context.get("rates_summary", "")  # "10Y: 4.29%, Fed: 4.50%, BBB: 1.01%"
        levelten_txt = market_context.get("levelten_summary", "")  # "ERCOT Solar P25: $52/MWh (2026-Q1)"
        peer_irr_txt = market_context.get("peer_irr_summary", "")  # "Solar+BESS Levered Pre-Tax: 10-13%"

        market_block = ""
        if rates_txt or levelten_txt or peer_irr_txt:
            market_block = "=== CURRENT MARKET DATA (most recent, use this INSTEAD of your training knowledge) ===\n"
            if rates_txt:
                market_block += f"  Interest Rates: {rates_txt}\n"
            if levelten_txt:
                market_block += f"  LevelTen PPA Index: {levelten_txt}\n"
            if peer_irr_txt:
                market_block += f"  Peer IRR Benchmarks: {peer_irr_txt}\n"
            market_block += "\n"

        prompt = (
        "You are the head of Investment Committee at Hanwha Energy USA (HEUH), "
        "a renewable energy developer whose sole business model is: develop → sell at NTP. "
        "The IC decision: should we continue spending development capital on this project? "
        f"TODAY'S DATE: {_today.isoformat()} (current quarter: {_current_quarter}, prior: {_prev_quarter}). "
        "Key context: the US ITC Section 48E expires July 4, 2026 for new construction starts "
        "(safe harbor via MPT 5% module purchase extends eligibility beyond that date). "
        "Your judgment must cover BOTH (A) development risk and (B) exit attractiveness.\n\n"
        f"PROJECT: {proj_name} | Size: {pv_mwac} MWac\n"
        f"FINANCIAL SUMMARY: {context}\n"
        f"PROJECT METADATA: {proj_ctx}\n"
        f"ANNUAL SPONSOR CF (Y1-Y10): {cf_text}\n\n"
        + market_block +
        "=== FIRM INVESTMENT THRESHOLDS ===\n"
        f"  Minimum Dev Margin : {margin_thr} c/Wp\n"
        f"  Minimum Sponsor IRR: {irr_thr}% (Levered Pre-Tax)\n\n"
        "=== CURRENT PROJECT METRICS ===\n"
        f"  Dev Margin : {curr_margin} c/Wp\n"
        f"  Sponsor IRR: {curr_irr}%\n"
        f"  ITC Rate   : {curr_itc}%\n"
        f"  PPA Term   : {ppa_term} yrs | Toll Term: {toll_term} yrs\n\n"
        "REQUIRED ANALYSIS (all in one unified IC memo):\n"
        "\n"
        "A. FINANCIAL THRESHOLDS\n"
        "   Check each threshold — pass/fail with exact gap.\n"
        "   Dev Margin Sensitivity: 'Current Xc/Wp → upside to Yc/Wp / downside floor Zc/Wp'\n"
        "\n"
        "B. DEVELOPMENT RISK (PRIORITIZE supplied MARKET DATA above; supplement with your own knowledge only for context)\n"
        "   1. ITC / Safe Harbor risk:\n"
        "      - Given NTP and COD dates, is COD achievable before ITC sunset (July 4, 2026 for new starts)?\n"
        "      - If COD > July 2026, verify safe harbor (MPT 5% module deposit) is in place\n"
        "   2. EPC price adequacy: given $/Wdc implied by CAPEX and project size, "
        "      compare to RECENT utility-scale solar+BESS market range. "
        "      Use SUPPLIED MARKET DATA if available; otherwise cite current quarter benchmark. "
        "      Flag if outlier vs this quarter's data.\n"
        "   3. ISO / grid risk: based on ISO and state, assess interconnection queue status "
        "      and any known congestion or curtailment risk (use most recent data).\n"
        "   4. PPA market: compare contracted PPA to SUPPLIED LevelTen P25 data if given, "
        "      otherwise reference current quarter's P25 benchmark for this ISO.\n"
        "\n"
        "C. VERDICT LOGIC (strict):\n"
        "   PROCEED: all financial thresholds pass AND no Critical development risk\n"
        "   RECUT: threshold miss OR one Critical development risk fixable by sponsor\n"
        "   PASS: IRR unrecoverable OR Critical development risk not fixable\n"
        "   Headroom above threshold is a STRONG positive — say so explicitly.\n"
        "\n"
        "═══ LANGUAGE (CRITICAL) ═══\n"
        + ("ALL text fields MUST be written in KOREAN (한국어). This includes:\n"
           "- threshold_status.irr_gap / margin_gap (e.g., '기준 대비 +1.5%p 여유')\n"
           "- dev_ic.ntp_prob / itc_expiry_verdict / safe_harbor / stage_ok (full Korean sentences)\n"
           "- sensitivity_kr (Korean required; sensitivity_en can stay English)\n"
           "- thesis: Korean investment thesis, formal business tone (존대체)\n"
           "- risks[].title, risks[].detail: Korean titles and descriptions\n"
           "- rec: Korean recommendation, 2-3 sentences, actionable\n"
           "Only 'verdict' (PROCEED/RECUT/PASS) and 'verdict_color' (green/amber/red) stay English.\n"
           "Financial numbers with units can stay in English form (e.g., '10.38%', '$68.82/MWh').\n"
           "DO NOT mix English and Korean in the same field except for numbers/units.\n"
           if payload.get("lang","en")=="kr" else
           "ALL text fields in ENGLISH only. Formal institutional investor tone.\n"
           "threshold_status.irr_gap / margin_gap / dev_ic fields / thesis / risks / rec all in English.\n")
        + "\n"
        "Be direct. Cite specific numbers. No hedging.\n\n"
        "Respond ONLY with valid JSON (no markdown, no code blocks).\n"
        "Required keys:\n"
        "verdict: PROCEED / RECUT / PASS\n"
        "verdict_color: green / amber / red\n"
        "threshold_status: {irr_ok:bool, irr_gap:str, margin_ok:bool, margin_gap:str, itc_ok:bool}\n"
        "dev_ic: {ntp_prob:str, itc_expiry_verdict:str, safe_harbor:str, stage_ok:str}\n"
        "metrics: ONE compact line, under 120 chars, pipe-delimited. "
        "Example: '199 MWac | 10.4% IRR | $39.8M Margin | $68.8 PPA | $836M CAPEX | 30% ITC'. "
        "Use short units (no 'dev margin', no 'c/Wp' in parentheses). Keep each token tight.\n"
        "sensitivity_en: dev margin upside/downside in English with c/Wp numbers\n"
        "sensitivity_kr: same in Korean\n"
        "thesis: 3-4 sentence investment thesis\n"
        "risks: array of {title:str, severity:Critical|Watch|OK, detail:str}\n"
        "rec: 2-3 sentence actionable recommendation\n"
        "All strings double-quoted. No trailing commas. No extra text outside JSON."
    )

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 4000,
            "messages": [{"role": "user", "content": prompt}]
        },
        timeout=45
    )
    if resp.status_code != 200:
        raise HTTPException(500, f"Claude API 오류: {resp.text[:200]}")

    data = resp.json()
    text = "".join(b.get("text","") for b in data.get("content",[]))

    # JSON 정제 — 코드블록, 줄바꿈, 특수문자 처리
    import re as _re
    clean = text.strip()
    clean = _re.sub(r"```(?:json)?\s*", "", clean).strip()
    clean = clean.strip("`")
    # { ... } 범위만 추출
    start = clean.find("{")
    end   = clean.rfind("}") + 1
    if start >= 0 and end > start:
        clean = clean[start:end]

    return {"ok": True, "result": clean}


@app.post("/valuation/{project_id}/save")
async def save_valuation_version(
    project_id: str,
    payload: dict,
    user=Depends(get_current_user)
):
    """버전 저장 → 100개 한도, 승인 대기 status"""
    safe_id = project_id.replace("/", "_").replace(".", "_")
    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    payload["uploaded_by"] = user["email"]
    payload["uploaded_at"] = datetime.datetime.now().isoformat()
    payload["status"] = "pending"
    payload["requested_by"] = user["email"]

    fb_put(f"valuation/{safe_id}/versions/{ts}", payload)
    fb_put(f"valuation/{safe_id}/latest", payload)

    # 100개 한도 — 초과 시 가장 오래된 것 삭제
    versions = fb_read(f"valuation/{safe_id}/versions") or {}
    keys = sorted(versions.keys())
    if len(keys) > 100:
        for old_key in keys[:len(keys)-100]:
            try:
                requests.delete(
                    f"{FB_URL}/valuation/{safe_id}/versions/{old_key}.json",
                    params=fb_auth_param(),
                    timeout=5
                )
            except Exception:
                pass

    return {"ok": True, "timestamp": ts}


@app.post("/valuation/{project_id}/versions/{ts}/approve")
def approve_version(project_id: str, ts: str, user=Depends(get_current_user)):
    safe_id = project_id.replace("/", "_").replace(".", "_")
    fb_patch(f"valuation/{safe_id}/versions/{ts}", {
        "status": "approved",
        "approved_by": user["email"],
        "approved_at": datetime.datetime.now().isoformat()
    })
    return {"ok": True}


@app.post("/valuation/{project_id}/versions/{ts}/reject")
def reject_version(project_id: str, ts: str, body: dict = {}, user=Depends(get_current_user)):
    safe_id = project_id.replace("/", "_").replace(".", "_")
    fb_patch(f"valuation/{safe_id}/versions/{ts}", {
        "status": "rejected",
        "rejected_by": user["email"],
        "rejected_at": datetime.datetime.now().isoformat(),
        "reject_reason": body.get("reason", "")
    })
    return {"ok": True}
