"""routers/dashboard.py — Phase 4 Step 4D refactoring."""

from fastapi import APIRouter
from fastapi import Depends
from core.deps import get_current_user
from core.firebase import fb_read


router = APIRouter()


# ══════════════════════════════════════════════════
#  전체 데이터 (대시보드 초기 로딩용)
# ══════════════════════════════════════════════════
@router.get("/dashboard")
def get_dashboard(user=Depends(get_current_user)):
    return {
        "ppv_summary": fb_read("ppv/summary"),
        "financial": fb_read("financial"),
        "divest": fb_read("divest"),
        "atlas": fb_read("atlas"),
    }
