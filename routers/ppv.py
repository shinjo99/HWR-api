"""routers/ppv.py — Phase 4 Step 4D refactoring."""

from fastapi import APIRouter
from fastapi import Depends, HTTPException
from core.deps import get_current_user
from core.firebase import fb_read, fb_write, fb_put, fb_auth_param
from core.config import FB_URL
import requests
from schemas import PPVSummary
import datetime


router = APIRouter()


# ══════════════════════════════════════════════════
#  PPV
# ══════════════════════════════════════════════════
@router.get("/ppv")
def get_ppv(user=Depends(get_current_user)):
    return fb_read("ppv")


@router.get("/ppv/summary")
def get_ppv_summary(user=Depends(get_current_user)):
    return fb_read("ppv/summary")


@router.post("/ppv/summary")
def save_ppv_summary(data: PPVSummary, user=Depends(get_current_user)):
    payload = data.dict()
    payload["updatedAt"] = datetime.datetime.now().isoformat()
    payload["updatedBy"] = user["email"]
    fb_write("ppv/summary", payload)
    return {"ok": True, "data": payload}


@router.post("/ppv/snapshot")
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


@router.post("/ppv/event")
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


@router.post("/ppv/override/{project_name}")
def save_override(project_name: str, data: dict, user=Depends(get_current_user)):
    safe_name = project_name.replace("/", "_").replace(".", "_")
    data["updatedAt"] = datetime.datetime.now().isoformat()
    data["updatedBy"] = user["email"]
    fb_write(f"ppv/overrides/{safe_name}", data)
    return {"ok": True}
