"""routers/financial.py — Phase 4 Step 4D refactoring."""

from fastapi import APIRouter
from fastapi import Depends, HTTPException
import datetime
from core.deps import get_current_user
from core.firebase import fb_read, fb_write
from schemas import FinancialData


router = APIRouter()


# ══════════════════════════════════════════════════
#  재무 (P&L / B/S / C/F)
# ══════════════════════════════════════════════════
@router.get("/financial")
def get_financial(user=Depends(get_current_user)):
    return fb_read("financial")


@router.get("/financial/{stmt}")
def get_stmt(stmt: str, user=Depends(get_current_user)):
    if stmt not in ["pl", "bs", "cf"]:
        raise HTTPException(status_code=400, detail="stmt는 pl, bs, cf 중 하나여야 합니다.")
    return fb_read(f"financial/{stmt}")


@router.get("/financial/{stmt}/{year}")
def get_stmt_year(stmt: str, year: int, user=Depends(get_current_user)):
    if stmt not in ["pl", "bs", "cf"]:
        raise HTTPException(status_code=400, detail="stmt는 pl, bs, cf 중 하나여야 합니다.")
    return fb_read(f"financial/{stmt}/{year}")


@router.post("/financial/{stmt}")
def save_financial(stmt: str, req: FinancialData, user=Depends(get_current_user)):
    if stmt not in ["pl", "bs", "cf"]:
        raise HTTPException(status_code=400, detail="stmt는 pl, bs, cf 중 하나여야 합니다.")
    payload = req.data.copy()
    payload["updatedAt"] = datetime.datetime.now().isoformat()[:16]
    payload["updatedBy"] = user["email"]
    fb_write(f"financial/{stmt}/{req.year}/{req.month}", payload)
    return {"ok": True, "path": f"financial/{stmt}/{req.year}/{req.month}", "data": payload}
