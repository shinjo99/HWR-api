"""core/firebase.py — Firebase Realtime DB 헬퍼"""
import requests
from fastapi import HTTPException

from core.config import FB_URL, FB_SECRET


def fb_auth_param():
    if FB_SECRET:
        return {"auth": FB_SECRET}
    return {}


def fb_read(path: str):
    try:
        res = requests.get(f"{FB_URL}/{path}.json", params=fb_auth_param(), timeout=5)
        return res.json() or {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB 읽기 오류: {str(e)}")


def fb_write(path: str, data: dict):
    try:
        res = requests.patch(f"{FB_URL}/{path}.json", json=data, params=fb_auth_param(), timeout=5)
        if res.status_code != 200:
            raise HTTPException(status_code=500, detail="DB 저장 실패")
        return res.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB 쓰기 오류: {str(e)}")


def fb_put(path: str, data: dict):
    try:
        res = requests.put(f"{FB_URL}/{path}.json", json=data, params=fb_auth_param(), timeout=5)
        if res.status_code != 200:
            raise HTTPException(status_code=500, detail="DB 저장 실패")
        return res.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB 쓰기 오류: {str(e)}")


def fb_patch(path: str, data: dict):
    try:
        requests.patch(f"{FB_URL}/{path}.json", json=data, params=fb_auth_param(), timeout=5)
    except Exception:
        pass
