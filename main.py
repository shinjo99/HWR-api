from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, Form
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from engine import _irr_robust, _calc_engine, _CALIB_STRUCTURAL, _apply_calibration_defaults, _decompose_irr_difference
from audit import _integrity_check_pf_model
from pdf_report import _esc_html, _fmt_pct, _fmt_usd_m, _build_ic_pdf_html

from core.config import JWT_SECRET, ANTHROPIC_KEY, FB_URL, FB_SECRET, FRED_API_KEY, get_users
from core.deps import create_token, verify_token, security, get_current_user, require_admin
from core.firebase import fb_auth_param, fb_read, fb_write, fb_put, fb_patch

from schemas import (
    LoginRequest, ValuationCalcRequest, PPVSummary, FinancialData,
    DecomposeIRRRequest, ExplainDiffRequest, BreakEvenRequest,
)

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


# ══════════════════════════════════════════════════
#  라우터 등록 (Phase 4 Step 4D)
# ══════════════════════════════════════════════════
from routers import (
    meta, dashboard, divest, atlas, auth,
    ppv, financial, project, benchmark, valuation,
)
app.include_router(meta.router)
app.include_router(dashboard.router)
app.include_router(divest.router)
app.include_router(atlas.router)
app.include_router(auth.router)
app.include_router(ppv.router)
app.include_router(financial.router)
app.include_router(project.router)
app.include_router(benchmark.router)
app.include_router(valuation.router)


# ══════════════════════════════════════════════════
#  외부 시장 벤치마크 (FRED + LevelTen)
# ══════════════════════════════════════════════════


# ══════════════════════════════════════════════════
#  Valuation Calculate (Stage 1 Engine)
# ══════════════════════════════════════════════════
import numpy as np
import numpy_financial as npf


# ══════════════════════════════════════════════════════════════
# Calibration Auto-Merge
# ──────────────────────────────────────────────────────────────
# 프런트 사이드바는 Neptune 구조적 파라미터를 전송하지 않으므로,
# calibration_mode='calibration' 일 때 백엔드가 자동 주입한다.
# (값은 get_calc_defaults endpoint와 동일 — single source로 가려면 추후 리팩터)
# ══════════════════════════════════════════════════════════════

_CALIB_FILL_IF_MISSING = {
    'availability_yr1': 1.0,
    'availability_yr2': 1.0,
    'capex_total_override': 836.7,
    'te_ratio_override': 32.52,
    'flip_yield': 8.75,
}


# ══════════════════════════════════════════════════
#  Valuation (PF 모델 업로드 / 조회)
# ══════════════════════════════════════════════════


# ══════════════════════════════════════════════════
#  IC Summary PDF Export (WeasyPrint — world-class formatting)
# ══════════════════════════════════════════════════
import base64 as _base64
from fastapi.responses import Response as _Response


