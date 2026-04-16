from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, Form
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

MACRS_5YR = [0.20, 0.32, 0.192, 0.1152, 0.1152, 0.0576]
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
    flip_yield = inputs.get('flip_yield', 8.75) / 100
    flip_term  = int(inputs.get('flip_term', 7))
    itc_elig   = inputs.get('itc_elig', 97) / 100
    itc_rate   = inputs.get('itc_rate', 30) / 100
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
    cashflows=[- total_capex]; unlev_cfs=[-total_capex]; sponsor_cfs=[-effective_eq]
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

        cashflows.append(op_cf); unlev_cfs.append(ebitda-aug_c); sponsor_cfs.append(s_cf)
        if yr<=10:
            detail.append({'yr':yr,'rev':round(total_rev,0),'opex':round(opex,0),
                'ebitda':round(ebitda,0),'ds':round(ds,0),'aug':round(aug_c,0),
                'depr':round(depr,0),'s_cf':round(s_cf,0)})

    lirr = float(npf.irr(cashflows))
    uirr = float(npf.irr(unlev_cfs))
    sirr = float(npf.irr(sponsor_cfs))
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
                                       "sponsor_irr", "sponsor_irr_contract"):
                                outputs[key] = round(v, 6)
                            else:
                                outputs[key] = round(v, 2)
                        except Exception:
                            pass
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


@app.post("/valuation/{project_id}/save")
async def save_valuation_version(
    project_id: str,
    payload: dict,
    user=Depends(get_current_user)
):
    """Calculate 결과 또는 수동 저장 → Firebase versions에 기록"""
    safe_id = project_id.replace("/", "_").replace(".", "_")
    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    payload["uploaded_by"] = user["email"]
    payload["uploaded_at"] = datetime.datetime.now().isoformat()
    fb_put(f"valuation/{safe_id}/versions/{ts}", payload)
    fb_put(f"valuation/{safe_id}/latest", payload)
    return {"ok": True, "timestamp": ts}
