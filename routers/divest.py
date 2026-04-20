"""routers/divest.py — Phase 4 Step 4D refactoring."""

from fastapi import APIRouter
import datetime
from fastapi import Depends
from core.deps import get_current_user
from core.firebase import fb_read, fb_write


router = APIRouter()


# ══════════════════════════════════════════════════
#  매각 현황
# ══════════════════════════════════════════════════
@router.get("/divest")
def get_divest(user=Depends(get_current_user)):
    return fb_read("divest")


@router.post("/divest/{project_name}")
def update_divest(project_name: str, data: dict, user=Depends(get_current_user)):
    safe_name = project_name.replace("/", "_").replace(".", "_")
    data["updatedAt"] = datetime.datetime.now().isoformat()[:16]
    data["updatedBy"] = user["email"]
    fb_write(f"divest/{safe_name}", data)
    return {"ok": True}
