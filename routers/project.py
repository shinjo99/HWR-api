"""routers/project.py — Phase 4 Step 4D refactoring."""

from fastapi import APIRouter
import datetime
from fastapi import Depends
from core.deps import get_current_user
from core.firebase import fb_read, fb_write


router = APIRouter()


# ══════════════════════════════════════════════════
#  프로젝트 데이터 (인허가/예산/일정/메모)
# ══════════════════════════════════════════════════
@router.get("/project/{project_id}")
def get_project(project_id: str, user=Depends(get_current_user)):
    return fb_read(f"projects/{project_id}")


@router.post("/project/{project_id}")
def save_project(project_id: str, data: dict, user=Depends(get_current_user)):
    data["updatedAt"] = datetime.datetime.now().isoformat()[:16]
    data["updatedBy"] = user["email"]
    fb_write(f"projects/{project_id}", data)
    return {"ok": True}


@router.get("/projects")
def get_all_projects(user=Depends(get_current_user)):
    return fb_read("projects")
