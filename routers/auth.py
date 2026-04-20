"""routers/auth.py — Phase 4 Step 4D refactoring."""

from fastapi import APIRouter
from fastapi import Depends, HTTPException
from core.deps import get_current_user, create_token
from core.config import get_users
from schemas import LoginRequest


router = APIRouter()


@router.post("/auth/login")
def login(req: LoginRequest):
    users = get_users()
    user = users.get(req.email)
    if not user or user["password"] != req.password:
        raise HTTPException(status_code=401, detail="이메일 또는 비밀번호가 틀렸습니다.")
    token = create_token(req.email, user["role"])
    return {
        "token": token,
        "email": req.email,
        "role": user["role"],
        "expires_in": 86400
    }


@router.get("/auth/me")
def me(user=Depends(get_current_user)):
    return user


@router.get("/auth/admins")
def get_admins(user=Depends(get_current_user)):
    """승인자 드롭다운용 admin 목록. 로그인한 사용자라면 조회 가능."""
    users = get_users()
    admins = [email for email, u in users.items() if u.get("role") == "admin"]
    return {"admins": sorted(admins)}
