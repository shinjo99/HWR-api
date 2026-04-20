"""routers/meta.py — Phase 4 Step 4D refactoring."""

from fastapi import APIRouter
import datetime


router = APIRouter()


# ══════════════════════════════════════════════════
#  헬스체크
# ══════════════════════════════════════════════════
@router.get("/")
def root():
    return {"status": "ok", "service": "HWR Dashboard API"}


@router.get("/health")
def health():
    return {"status": "ok", "ts": datetime.datetime.now().isoformat()}
