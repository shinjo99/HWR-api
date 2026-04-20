"""routers/atlas.py — Phase 4 Step 4D refactoring."""

from fastapi import APIRouter
import datetime
from fastapi import Depends
from core.deps import get_current_user
from core.firebase import fb_read, fb_write


router = APIRouter()


# ══════════════════════════════════════════════════
#  Atlas Milestone
# ══════════════════════════════════════════════════
@router.get("/atlas")
def get_atlas(user=Depends(get_current_user)):
    return fb_read("atlas")


@router.post("/atlas/{milestone_id}")
def update_atlas(milestone_id: str, data: dict, user=Depends(get_current_user)):
    data["updatedAt"] = datetime.datetime.now().isoformat()[:16]
    data["updatedBy"] = user["email"]
    fb_write(f"atlas/{milestone_id}", data)
    return {"ok": True}
