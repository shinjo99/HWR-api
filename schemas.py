"""schemas.py — Pydantic 모델"""
from pydantic import BaseModel


# ══════════════════════════════════════════════════
#  인증
# ══════════════════════════════════════════════════
class LoginRequest(BaseModel):
    email: str
    password: str


class ValuationCalcRequest(BaseModel):
    project_id: str = ""
    inputs: dict = {}


class PPVSummary(BaseModel):
    totalRisked: float
    byStage: dict
    projectCount: int


class FinancialData(BaseModel):
    year: int
    month: int
    data: dict


class DecomposeIRRRequest(BaseModel):
    project_id: str = ""
    inputs: dict


class ExplainDiffRequest(BaseModel):
    project_id: str = ""
    decomposition: dict  # _decompose_irr_difference 결과
    lang: str = 'ko'


# ── Break-Even Analysis (Newton-Raphson) ─────────
class BreakEvenRequest(BaseModel):
    project_id: str = ""
    inputs: dict
    target_irr_pct: float  # e.g., 11.0 for 11%
    target_var: str = "ppa_price"  # 현재는 PPA만 지원 (확장 가능)
