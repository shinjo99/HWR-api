from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import jwt
import os
import datetime
import json

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
JWT_SECRET  = os.environ.get("JWT_SECRET", "hwr-secret-change-this")
FB_URL      = os.environ.get("FB_URL", "https://team-dashboard-c0d7b-default-rtdb.asia-southeast1.firebasedatabase.app")
FB_SECRET   = os.environ.get("FB_SECRET", "")  # Firebase Database Secret

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

# ══════════════════════════════════════════════════
#  인증
# ══════════════════════════════════════════════════
class LoginRequest(BaseModel):
    email: str
    password: str

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
    # Firebase push (랜덤 키 생성)
    res = requests.post(
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
