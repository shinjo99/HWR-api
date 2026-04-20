# HWR-api

**Hanwha Energy USA Holdings — Dashboard 백엔드 API**

FastAPI 기반 REST API. PF (Project Finance) 모델 계산, 정합성 검증, PDF 리포트 생성, AI 기반 시장 리서치 제공.

프론트엔드: [HWR_Dashboard](https://github.com/shinjo99/HWR_Dashboard) (GitHub Pages)

---

## 프로젝트 배경

- **구축 기간**: 약 1주 (2026년 3~4월, Claude 4.6 Opus 활용)
- **구축 환경**: Git CLI 미사용. GitHub 웹 인터페이스 드래그&드롭으로 배포
- **사용 범위**: Hanwha Renewables USA 내부 (Solar + BESS 프로젝트 평가)
- **엔진 검증 기준 모델**: Neptune (UT, WECC) PF Model
  - Excel Sponsor IRR 9.92% vs 엔진 10.14% (±0.22%p 오차)

---

## 주요 기능

| 카테고리 | 기능 |
|---|---|
| **Valuation** | Partnership Flip + Sculpted Debt + MACRS 5YR 기반 IRR/NPV 계산 |
| | Calibration 모드 (Excel 복제) vs Prediction 모드 (Excel 없이 예비 평가) |
| | Break-Even Analysis (Newton-Raphson, PPA 역산) |
| | IRR 분해 + Claude API 기반 자연어 해설 |
| **Integrity Check** | 업로드된 xlsb/xlsx 파일의 42개 검증 항목 자동 체크 |
| **IC Report PDF** | WeasyPrint 기반 Investment Committee 리포트 생성 |
| **Benchmark** | FRED (10Y Treasury, Fed Funds 등) + Stooq (KRW/USD, WTI) 실시간 조회 |
| | LevelTen Solar PPA 시장 데이터 (Admin 업로드) |
| | BESS Tolling AI 리서치 (Claude Sonnet 4.5 + web search) |
| **Dashboard** | PPV (Project Pipeline Value) 스테이지별 리스크 조정 총액 |
| | Divest, Atlas Milestone, Financial (P&L/BS/CF) 관리 |
| **Auth** | JWT 기반 인증. USERS 환경변수로 계정 관리 (viewer/admin) |

---

## 아키텍처

Phase 4D 리팩터링 완료 (main.py 4,632 → 100줄, -97.8%). FastAPI 표준 구조.

```
main.py                  FastAPI app 초기화 + 라우터 등록만 (100줄)
engine.py                PF 계산 엔진 (_irr_robust, _calc_engine, calibration 상수)
audit.py                 정합성 검증 (_integrity_check_pf_model, 42개 체크)
pdf_report.py            IC Report HTML 생성 (WeasyPrint 입력)
schemas.py               Pydantic 모델 7개 (LoginRequest, ValuationCalcRequest 등)

core/                    공통 기반 모듈
├── config.py            환경변수 + get_users()
├── deps.py              JWT 인증 (create_token, get_current_user, require_admin)
└── firebase.py          Firebase Realtime DB 헬퍼 (fb_read, fb_write, fb_put, fb_patch)

routers/                 FastAPI 라우터 (엔드포인트별 분리)
├── meta.py              /, /health (2)
├── auth.py              /auth/* (3)
├── dashboard.py         /dashboard (1)
├── divest.py            /divest/* (2)
├── atlas.py             /atlas/* (2)
├── ppv.py               /ppv/* (6)
├── financial.py         /financial/* (4)
├── project.py           /project/*, /projects (3)
├── benchmark.py         /benchmark/* (10) + FRED_SERIES/STOOQ_SYMBOLS 상수
└── valuation.py         /valuation/* (18) + parse_pf_model() 포함
```

**총 51개 엔드포인트** (+ FastAPI 자동 OPTIONS = 55 routes)

### 의존성 플로우

```
request → main.py → router → core.deps (auth) → core.firebase (DB)
                                              → engine / audit / pdf_report
                                              → schemas (validation)
```

---

## 로컬 실행

```bash
pip install -r requirements.txt

# 환경변수 설정 (또는 .env 파일)
export JWT_SECRET="..."
export FB_URL="..."
export FB_SECRET="..."
export ANTHROPIC_API_KEY="..."
export FRED_API_KEY="..."
export USERS="team@hwr.com:password:viewer,admin@hwr.com:pw:admin"

uvicorn main:app --reload --port 8080
```

테스트: `curl http://localhost:8080/health` → `{"status":"ok","ts":"..."}`

API 문서 자동 생성: `http://localhost:8080/docs` (FastAPI Swagger UI)

---

## 배포 (Railway)

**Git CLI 없이 GitHub 웹 드래그로 배포.**

1. 수정한 파일을 로컬에서 준비
2. GitHub `HWR-api` repo → **Add file → Upload files**
3. 파일 드래그 → Commit message 입력 → Commit
4. Railway가 자동 감지해서 rebuild (2-5분)
5. Railway Dashboard → **Deploy Logs** 에서 `Application startup complete` 확인

**배포 환경:**
- Railway (현재). 향후 AWS ECS Fargate 또는 Lambda 이전 검토 중
- Dockerfile: Python 3.11-slim + libpango/libcairo/fonts-noto-cjk (WeasyPrint용)
- 참고: libreoffice는 Railway OOM (512MB 제한)으로 제거. Model Audit 기능 제약.

---

## 환경변수

| 변수 | 용도 | 예시 |
|---|---|---|
| `JWT_SECRET` | JWT 서명 키 | `hwr-secret-2026-prod` |
| `FB_URL` | Firebase Realtime DB URL | `https://team-dashboard-c0d7b-default-rtdb.asia-southeast1.firebasedatabase.app` |
| `FB_SECRET` | Firebase Database Secret | `...` |
| `ANTHROPIC_API_KEY` | Claude API (BESS 리서치, IRR 해설, CF 분석) | `sk-ant-...` |
| `FRED_API_KEY` | FRED 시계열 API | `...` (https://fred.stlouisfed.org/docs/api/api_key.html) |
| `USERS` | 사용자 계정 (email:password:role, 쉼표 구분) | `team@hwr.com:pw1:viewer,admin@hwr.com:pw2:admin` |

실제 값은 Railway Dashboard → Variables 참조.

---

## 주요 엔드포인트

### Auth
- `POST /auth/login` — 로그인, JWT 반환
- `GET /auth/me` — 현재 사용자 정보
- `GET /auth/admins` — admin 계정 목록

### Valuation (핵심)
- `POST /valuation/calculate` — IRR/NPV 계산 (Calibration/Prediction 모드)
- `GET /valuation/calculate/defaults` — Neptune 기준 기본값
- `POST /valuation/integrity-check` — PF Model xlsb/xlsx 정합성 42개 검증
- `POST /valuation/upload` — xlsb/xlsx 업로드 & parse
- `POST /valuation/decompose-irr` — Calibration vs Prediction IRR 차이 분해
- `POST /valuation/explain-diff` — IRR 차이 자연어 해설 (Claude API)
- `POST /valuation/breakeven` — PPA 역산 (Newton-Raphson)
- `POST /valuation/export-pdf` — IC Summary PDF 생성 (WeasyPrint)
- `POST /valuation/generate-ic-summary` — AI 기반 IC Opinion 초안
- `POST /valuation/analyze-cf` — Cash Flow AI 해설
- `GET /valuation`, `/valuation/{id}`, `/valuation/{id}/latest`, `/valuation/{id}/versions`
- `POST /valuation/{id}/save`
- `POST /valuation/{id}/versions/{ts}/approve` | `/reject` (admin)

### Benchmark
- `GET /benchmark/market` — FRED + Stooq 시장 지표 (10Y Treasury, KRW/USD 등)
- `GET|POST /benchmark/levelten` — Solar PPA 시장 데이터 (admin 업로드)
- `GET|POST /benchmark/peer-irr` — 경쟁사 IRR 비교 데이터
- `POST /benchmark/bess-tolling/research` — BESS Tolling AI 리서치 (30-60초)
- `GET /benchmark/bess-tolling`, `/history`

### Dashboard / PPV / Financial / Project / Divest / Atlas
- `GET /dashboard` — 홈 화면 초기 데이터
- `GET|POST /ppv`, `/ppv/summary`, `/ppv/snapshot`, `/ppv/event`
- `GET|POST /financial/{pl|bs|cf}`
- `GET|POST /project/{id}`, `GET /projects`
- `GET|POST /divest`, `/atlas`

---

## 계산 엔진 상세

### 2가지 계산 모드

| 모드 | 용도 | 특징 |
|---|---|---|
| **Calibration** | Excel 모델 복제 | Sculpted debt, NOL offset, custom Partnership Flip. Neptune 기준 ±0.22%p 정확도 |
| **Prediction** | Excel 없이 예비 평가 | 99/5 flip 표준, level debt, MACRS standard, FMV = CAPEX total |

### Calibration 상수 (engine.py)
- `_CALIB_STRUCTURAL`: Neptune 기준 구조적 파라미터 (TE ratio, flip yield 등)
- `_CALIB_FILL_IF_MISSING`: availability_yr1/2, capex_total_override, te_ratio_override, flip_yield
- `MACRS_5YR = [0.20, 0.32, 0.192, 0.1152, 0.1152, 0.0576]`

### Break-Even (Newton-Raphson)
1. Phase 1: PPA ±25% 11점 민감도 스캔
2. Phase 2: Newton-Raphson iteration (tolerance 0.01% IRR, max 10 iter)
3. Clamping: PPA 값 `base * 0.33 ~ base * 3.0`

---

## Phase 진행도

| Phase | 내용 | 상태 |
|---|---|---|
| Phase 1 | Dead code 제거 | ✅ 완료 |
| Phase 2 | (skip) | — |
| Phase 3 | 프론트 index.html 분리 (13,869 → 7,743줄, -44.2%) | ✅ 완료 |
| **Phase 4A** | engine.py 분리 | ✅ 완료 |
| **Phase 4B** | audit.py 분리 | ✅ 완료 |
| **Phase 4C** | pdf_report.py 분리 | ✅ 완료 |
| **Phase 4D** | core/ + schemas + routers/ 분리 (main.py 4,632 → 100줄) | ✅ 완료 (2026-04-20) |
| Phase 5 | 문서화 | 🟡 진행 중 (README 작성) |
| Phase 6 | CI/CD (GitHub Actions) | ⏳ 예정 |

---

## 알려진 이슈 / 다음 과제

### 🔴 High Priority
- **Prediction 모드 Sponsor IRR -5% 비정상 출력** — 99/5 flip + level debt 가정 검증 필요
- **Sponsor IRR Levered Pre-Tax, After-Tax (Before NOL) 필드 공란** — 엔진이 return 안 함
- **엔진 과적합 위험** — Neptune 단일 모델로만 calibration. 추가 PF 모델 2-3개 검증 필요

### 🟡 Medium Priority
- **PDF IC Report 영문 템플릿** — 현재 한국어 하드코딩 ("투자 의견", "핵심 논거")
- **BESS Tolling AI 리서치 한국어 출력** — 영문 옵션 추가 필요
- **Sculpted debt 엄밀 구현** — 현재 단순 근사
- **ITC 캐시플로 적용** — `effective_itc_value` 변수 dead code 상태
- **NOL 80% IRS 규정** — 현재 단순 offset 방식

### 🟢 Low Priority
- **Model Audit libreoffice 복원** — Railway Pro ($20/mo, 8GB RAM) 또는 AWS ECS 이전 시 가능
- **CF 분석 AI 프론트 UI 버튼 추가**
- **favicon.ico 404** (무해, cosmetic)

---

## 기술 스택

- **FastAPI** 0.115+ (uvicorn)
- **Pydantic** (schemas)
- **numpy, numpy_financial** (IRR 계산, MACRS)
- **openpyxl, pyxlsb** (Excel 파일 파싱)
- **WeasyPrint** (PDF 생성, libpango/libcairo)
- **anthropic** Python SDK (Claude Sonnet 4.5)
- **requests** (Firebase REST, FRED, Stooq)
- **PyJWT** (인증)

Python 3.11+. 전체 의존성 `requirements.txt` 참조.

---

## 트러블슈팅

### 500 Internal Server Error
1. Railway Deploy Logs는 stdout만 표시 → **Python traceback 안 보임**
2. F12 Network 탭 → 빨간 요청 클릭 → **Response 탭**의 JSON에 `detail: "name 'X' is not defined"` 같은 실제 에러 표시
3. 주로 **import 누락**. 해당 라우터 파일에 import 추가 후 재배포

### Phase 4D 배포 시 학습된 히든 의존성 버그 패턴
- AST `NameCollector` 도구로 모든 라우터 파일의 undefined 이름 사전 검출
- 그래도 **함수 내부 참조**는 AST가 못 잡음 → **최소 호출 테스트** 필수
- 예: `pdf_report.py` 내부 `datetime.date.today()` 호출이 import 누락으로 런타임 에러

### Railway rebuild 실패
- `Dockerfile`에 `COPY . .` 있어서 새 파일 자동 포함됨
- requirements.txt 수정 시 build 시간 2~5분 → 8~12분으로 증가
- libreoffice 같은 무거운 패키지는 Railway Pro 필요

---

## 라이선스

Proprietary. Hanwha Renewables USA 내부 사용.

---

## 작업 재개용 컨텍스트 (Claude 새 세션)

**이 repo에서 작업 재개 시, Claude에게 다음을 알려주세요:**

1. Repo 구조: FastAPI + core/ + schemas + routers/ (10개) + engine/audit/pdf_report (최상위)
2. 배포: GitHub 웹 드래그 → Railway 자동 rebuild. Git CLI 미사용.
3. 기준 모델: Neptune_PF_Model (UT, WECC). Excel Sponsor IRR 9.92% vs 엔진 10.14%.
4. 주의사항:
   - 라우터 추가 시 `main.py`의 `from routers import (...)` 와 `app.include_router(...)` 둘 다 수정
   - 새 helper 함수는 적절한 파일에 추가 (engine/audit/pdf_report/해당 router)
   - **런타임 NameError 방지**: 함수 본문에서 참조하는 `datetime`, `json`, `os`, `requests` 등은 반드시 파일 상단 import 확인
5. 참고 문서: 이 README 전체 + 프론트 `HWR_Dashboard` README
