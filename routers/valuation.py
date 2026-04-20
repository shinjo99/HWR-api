"""routers/valuation.py — Phase 4 Step 4D refactoring."""

from fastapi import APIRouter
from fastapi import Depends, HTTPException, UploadFile, File, Form
from core.deps import get_current_user, require_admin
from core.config import ANTHROPIC_KEY
from core.firebase import fb_read, fb_write, fb_put, fb_patch, fb_auth_param
from core.config import FB_URL
from schemas import ValuationCalcRequest, DecomposeIRRRequest, ExplainDiffRequest, BreakEvenRequest
from engine import _irr_robust, _calc_engine, _CALIB_STRUCTURAL, _apply_calibration_defaults, _decompose_irr_difference
from audit import _integrity_check_pf_model
from pdf_report import _esc_html, _fmt_pct, _fmt_usd_m, _build_ic_pdf_html
from fastapi.responses import Response as _Response
import requests
import json
import datetime
import tempfile
import os
from pyxlsb import open_workbook

# main.py에 있었던 상수 (valuation 엔드포인트에서 참조)
MACRS_5YR = [0.20, 0.32, 0.192, 0.1152, 0.1152, 0.0576]



# ── PF Model 파서 (main.py에서 이동) ──
def parse_pf_model(filepath: str) -> dict:
    """xlsb에서 핵심 가정값과 아웃풋 추출"""
    assumptions = {}
    outputs = {}

    with open_workbook(filepath) as wb:

        # ── PF Intake → assumptions ───────────────
        try:
            with wb.get_sheet("PF Intake") as ws:
                for row in ws.rows():
                    vals = [c.v for c in row if c.v is not None]
                    if len(vals) < 2:
                        continue
                    label = str(vals[0]).strip()
                    val   = vals[1] if len(vals) > 1 else None

                    intake_map = {
                        "Project Name":             ("project_name", str),
                        "PJ Characteristic":        ("technology",   str),
                        "State":                    ("state",        str),
                        "PV : Project Size (MWac)": ("pv_mwac",      float),
                        "NTP Date":                 ("ntp",          str),
                        "COD":                      ("cod",          str),
                        "DC/AC Ratio":              ("dc_ac_ratio",  float),
                        "Total Site Area":          ("site_area_ac", float),
                    }
                    if label in intake_map:
                        key, typ = intake_map[label]
                        try:
                            assumptions[key] = typ(val) if val is not None else None
                        except Exception:
                            assumptions[key] = str(val) if val is not None else None

                    # BESS: ESS size 행은 "4hr" 같은 duration값 → bess_duration으로 저장
                    if label == "ESS size (MW)":
                        assumptions["bess_duration"] = str(val) if val is not None else None

                    # BESS: ESS Duration 행에서 실제 MW 추출
                    if label == "ESS Duration (Hours)":
                        try:
                            assumptions["bess_mw"] = float(val)
                        except Exception:
                            pass

                    # BESS: ESS storage MWh — hex/int 모두 처리
                    if label == "ESS storage size (MWh)":
                        try:
                            v = val
                            if isinstance(v, str) and v.startswith("0x"):
                                assumptions["bess_mwh"] = float(int(v, 16))
                            else:
                                assumptions["bess_mwh"] = float(v)
                        except Exception:
                            assumptions["bess_mwh"] = None
        except Exception:
            pass

        # ── Quarterly Assumptions → 운영 가정 ─────
        try:
            with wb.get_sheet("Quarterly Assumptions") as ws:
                for row in ws.rows():
                    vals = [c.v for c in row if c.v is not None]
                    if len(vals) < 3:
                        continue
                    label = str(vals[0]).strip()
                    val   = vals[2] if len(vals) > 2 else None

                    qa_map = {
                        "Degradation":              ("degradation",       float),
                        "Availability (yr 1)":      ("availability_yr1",  float),
                        "Availability (yr 2+)":     ("availability_yr2",  float),
                        "PV Covered O&M":           ("pv_om_covered",     float),
                        "PV Non-covered O&M":       ("pv_om_noncovered",  float),
                        "Asset management < 200MW": ("asset_mgmt_sm",     float),
                        "Asset management > 200MW": ("asset_mgmt_lg",     float),
                        "PV Merchant Haircut":      ("merchant_haircut",  float),
                    }
                    if label in qa_map:
                        key, typ = qa_map[label]
                        try:
                            assumptions[key] = typ(val) if val is not None else None
                        except Exception:
                            pass
        except Exception:
            pass

        # ── Summary → outputs (Case 2 = PV+BESS 컬럼 기준, index 3) ──
        try:
            with wb.get_sheet("Summary") as ws:
                for row in ws.rows():
                    vals = [c.v for c in row if c.v is not None]
                    if len(vals) < 3:
                        continue
                    label = str(vals[0]).strip()

                    summary_map = {
                        "levered project IRR (full life)":   "levered_irr",
                        "Unlevered project IRR (full life)": "unlevered_irr",
                        "Sponsor levered IRR (full life)":   "sponsor_irr",
                        "Sponsor levered IRR (contract)":    "sponsor_irr_contract",
                        # 신규: After-Tax IRR (Class B 관점) + WACC
                        "Sponsor levered after-tax IRR (before NOL)":  "sponsor_irr_aftertax_before_nol",
                        "Sponsor levered after-tax IRR (after NOL)":   "sponsor_irr_aftertax_after_nol",
                        "Weighted average cost of capital":  "wacc",
                        "WACC":                              "wacc",
                        "Total Project Cost":                "capex_total",
                        "Debt":                              "debt",
                        "Tax Equity Investment":             "tax_equity",
                        "Sponsor Equity Investment":         "sponsor_equity",
                        "PV : PPA Price":                    "ppa_price",
                        "PV : PPA term":                     "ppa_term",
                        "BESS : Toll rate":                  "bess_toll",
                        "BESS : Toll term":                  "bess_toll_term",
                        "HQC DEV Margin (000$)":             "dev_margin",
                        "Total Margin (000$)":               "total_margin",
                    }
                    if label in summary_map:
                        key = summary_map[label]
                        # Case 2 (PV+BESS) = vals[3], fallback to vals[2]
                        val = vals[3] if len(vals) > 3 else (vals[2] if len(vals) > 2 else None)
                        try:
                            v = float(val)
                            if key in ("levered_irr", "unlevered_irr",
                                       "sponsor_irr", "sponsor_irr_contract",
                                       "sponsor_irr_aftertax_before_nol",
                                       "sponsor_irr_aftertax_after_nol",
                                       "wacc"):
                                outputs[key] = round(v, 6)
                            else:
                                outputs[key] = round(v, 2)
                        except Exception:
                            pass
        except Exception:
            pass

        # ── Returns 시트 → After-Tax IRR (Before/After NOL) 및 기타 세분화 IRR ──
        # Returns 시트의 'Sponsor net aftertax cashflow' 줄에 IRR이 있음
        # 각 IRR 값은 보통 4~5번째 컬럼 위치에 있고, 레이블은 맨 앞
        try:
            with wb.get_sheet("Returns") as ws:
                rows_list = list(ws.rows())
                # 라인 순서대로 처리 (NOL 이전 aftertax는 첫 번째 매칭, 이후는 두 번째)
                aftertax_matches = []
                unlevered_aftertax_matches = []
                for row in rows_list:
                    vals = [c.v for c in row]
                    label = ""
                    # 첫 번째 문자열 셀을 레이블로
                    for v in vals[:3]:
                        if isinstance(v, str) and v.strip():
                            label = v.strip()
                            break
                    if not label:
                        continue
                    # IRR 숫자 찾기 (0 < v < 1 범위의 float)
                    irr_val = None
                    for v in vals:
                        if isinstance(v, float) and 0.001 < v < 0.5 and v != 1.0:
                            # 첫 번째 그럴듯한 IRR 값 (label 이후)
                            irr_val = round(v, 6)
                            break
                    if irr_val is None:
                        continue

                    # Sponsor net pretax cashflow — Levered Pre-Tax
                    #   "(without ITC or PTC)" = baseline (Line 25, ~10.02%)
                    #   "(with PTC)" 는 PTC 모델이므로 제외
                    if (label.startswith("Sponsor net pretax cashflow")
                        and "unlevered" not in label.lower()
                        and "with ptc" not in label.lower()
                        and "with itc" not in label.lower()):
                        if "sponsor_irr_levered_pretax" not in outputs:
                            outputs["sponsor_irr_levered_pretax"] = irr_val
                    # Sponsor net unlevered pretax cashflow
                    elif (label.startswith("Sponsor net unlevered pretax")
                          and "with ptc" not in label.lower()
                          and "with itc" not in label.lower()):
                        if "sponsor_irr_unlevered_pretax" not in outputs:
                            outputs["sponsor_irr_unlevered_pretax"] = irr_val
                    # Sponsor net aftertax cashflow (level IRR, NOL 전/후 두 줄)
                    #   - 첫 등장 = Before NOL (~13.62%)
                    #   - 두 번째 등장 (NOL effect 처리 후) = After NOL (~10.51%)
                    # "(including Residual Value)" 및 State Tax 버전은 제외
                    elif (label == "Sponsor net aftertax cashflow"
                          and "residual" not in label.lower()
                          and "state" not in label.lower()):
                        aftertax_matches.append(irr_val)
                    # Sponsor net unlevered aftertax cashflow
                    elif (label == "Sponsor net unlevered aftertax cashflow"
                          or label == "Sponsor net unlevered aftertax cashflow with NOL"):
                        unlevered_aftertax_matches.append(irr_val)

                # 매칭 순서 기반: first = before NOL, second = after NOL
                if len(aftertax_matches) >= 1:
                    outputs["sponsor_irr_aftertax_before_nol"] = aftertax_matches[0]
                if len(aftertax_matches) >= 2:
                    # 두 번째 매칭이 After NOL (세 번째 이상은 State Tax 변형)
                    outputs["sponsor_irr_aftertax_after_nol"] = aftertax_matches[1]
                if len(unlevered_aftertax_matches) >= 1:
                    outputs["sponsor_irr_unlevered_aftertax_before_nol"] = unlevered_aftertax_matches[0]
                if len(unlevered_aftertax_matches) >= 2:
                    outputs["sponsor_irr_unlevered_aftertax_after_nol"] = unlevered_aftertax_matches[1]
        except Exception:
            pass

        # ── Sensitivities 시트 → WACC, Cost of Debt ──
        # "Weighted average cost of capital" 레이블이 있는 행에서 2번째 컬럼 값
        try:
            with wb.get_sheet("Sensitivities") as ws:
                for row in ws.rows():
                    vals = [c.v for c in row]
                    label = ""
                    for v in vals[:4]:
                        if isinstance(v, str) and v.strip():
                            label = v.strip()
                            break
                    if not label:
                        continue
                    low = label.lower()
                    # WACC - "Weighted average cost of capital"
                    if "weighted average cost of capital" in low and "wacc" not in outputs:
                        for v in vals:
                            if isinstance(v, float) and 0.01 < v < 0.3:
                                outputs["wacc"] = round(v, 6)
                                break
                    # Cost of debt
                    elif label.lower().strip() == "cost of debt" and "cost_of_debt" not in outputs:
                        for v in vals:
                            if isinstance(v, float) and 0.01 < v < 0.3:
                                outputs["cost_of_debt"] = round(v, 6)
                                break
        except Exception:
            pass

    # ── CF_Annual → 연도별 실제 수익 추출
    try:
        with wb.get_sheet("CF_Annual") as ws:
            for row in ws.rows():
                vals = [c.v for c in row if c.v is not None]
                if len(vals) < 5: continue
                label = str(vals[0]).strip()
                # Y1 시작 인덱스: 앞 4개(total, pre-COD, 0, 0) 제거 후 운영연도
                op_vals = [v for v in vals[1:] if isinstance(v, (int, float))]

                if "PPA #2 BESS" in label and "Revenue" in label:
                    try:
                        bess_rev_y1 = float(op_vals[4]) if len(op_vals) > 4 else 0
                        bess_mw = assumptions.get("bess_mw") or 199
                        if bess_rev_y1 > 0 and bess_mw > 0:
                            outputs["bess_toll_y1_effective"] = round(bess_rev_y1/(bess_mw*1000*12)*1000, 4)
                            outputs["bess_rev_y1"] = round(bess_rev_y1, 0)
                        # 연도별 BESS 수익 (인덱스 4~38 = Y1~Y35)
                        outputs["bess_rev_schedule"] = [round(float(v),0) for v in op_vals[4:39] if isinstance(v,(int,float))]
                    except: pass

                if "PPA #1 PV" in label and "Revenue" in label:
                    try:
                        pv_rev_y1 = float(op_vals[4]) if len(op_vals) > 4 else 0
                        if pv_rev_y1 > 0:
                            outputs["pv_rev_y1"] = round(pv_rev_y1, 0)
                        outputs["pv_rev_schedule"] = [round(float(v),0) for v in op_vals[4:39] if isinstance(v,(int,float))]
                    except: pass

                if "Merchant PV Power Revenue" in label:
                    try:
                        outputs["merch_rev_schedule"] = [round(float(v),0) for v in op_vals[4:39] if isinstance(v,(int,float))]
                    except: pass
    except Exception:
        pass

    # bess_mwh 보정: xlsb hex 파싱 한계 → pv_mwac × duration(숫자)으로 계산
    try:
        duration_str = assumptions.get("bess_duration", "")
        duration_h = float("".join(x for x in str(duration_str) if x.isdigit() or x=="."))
        pv_mwac = assumptions.get("pv_mwac") or assumptions.get("bess_mw")
        if pv_mwac and duration_h:
            assumptions["bess_mwh"] = round(float(pv_mwac) * duration_h, 1)
    except Exception:
        pass

    return {"assumptions": assumptions, "outputs": outputs}


router = APIRouter()


@router.post("/valuation/calculate")
def calculate_valuation(req: ValuationCalcRequest, user=Depends(get_current_user)):
    """Run PF calculation engine with given inputs"""
    try:
        inputs = _apply_calibration_defaults(dict(req.inputs))
        result = _calc_engine(inputs)
        return {"ok": True, "project_id": req.project_id, "result": result}
    except Exception as e:
        raise HTTPException(500, f"Calculation error: {str(e)}")


@router.post("/valuation/decompose-irr")
def decompose_irr(req: DecomposeIRRRequest, user=Depends(get_current_user)):
    """Calibration vs Prediction IRR 차이를 4개 요인별로 분해"""
    try:
        result = _decompose_irr_difference(req.inputs)
        return {"ok": True, "project_id": req.project_id, "result": result}
    except Exception as e:
        raise HTTPException(500, f"Decomposition error: {str(e)}")


@router.post("/valuation/explain-diff")
def explain_diff(req: ExplainDiffRequest, user=Depends(get_current_user)):
    """Claude API로 IRR 차이에 대한 자연어 해설 + 엑셀 수정 제안"""
    try:
        import anthropic
        client = anthropic.Anthropic()
        
        d = req.decomposition
        lang = req.lang
        
        # 프롬프트 구성 (결정적 숫자를 context로 제공 → 환각 방지)
        if lang == 'en':
            system_prompt = """You are a PF (Project Finance) Solar+BESS expert. 
Based on the decomposition data provided, explain why Calibration IRR differs from Prediction IRR 
and suggest specific Excel modifications. Be concise, practical, and cite actual numbers. 
Max 5 paragraphs. Do not invent data not in context."""
            user_prompt = f"""
Calibration IRR: {d['calib_irr']}%
Prediction IRR: {d['predict_irr']}%
Total Difference: {d['total_delta']}%p

Factor Breakdown:
"""
            for f in d['factors']:
                user_prompt += f"\n• {f['name_en']}: {f['delta_pp']:+.2f}%p\n  From (Calib): {f['from_calib']}\n  To (Predict): {f['to_predict']}\n  Excel hint: {f['excel_hint_en']}\n"
            user_prompt += "\nExplain the key drivers of the difference and what the Excel modeler should verify/modify in their spreadsheet. Focus on actionable Excel-level advice."
        else:
            system_prompt = """당신은 PF (Project Finance) Solar+BESS 전문가입니다.
제공된 분해 데이터를 바탕으로 Calibration IRR과 Prediction IRR 차이의 원인을 설명하고, 
구체적인 엑셀 수정 제안을 해주세요. 간결하고 실용적으로, 실제 수치를 인용하세요.
최대 5문단. context에 없는 데이터를 지어내지 마세요."""
            user_prompt = f"""
Calibration IRR: {d['calib_irr']}%
Prediction IRR: {d['predict_irr']}%
총 차이: {d['total_delta']:+.2f}%p

요인별 분해:
"""
            for f in d['factors']:
                user_prompt += f"\n• {f['name_ko']}: {f['delta_pp']:+.2f}%p\n  Calib → : {f['from_calib']}\n  Predict → : {f['to_predict']}\n  Excel 힌트: {f['excel_hint_ko']}\n"
            user_prompt += "\n이 차이의 핵심 원인을 설명하고, 엑셀 모델러가 스프레드시트에서 확인/수정해야 할 사항을 알려주세요. 실행 가능한 엑셀 레벨 조언에 집중."
        
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1500,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}]
        )
        
        explanation = response.content[0].text if response.content else ""
        return {"ok": True, "explanation": explanation, "lang": lang}
    except Exception as e:
        raise HTTPException(500, f"Explain error: {str(e)}")


@router.post("/valuation/breakeven")
def break_even(req: BreakEvenRequest, user=Depends(get_current_user)):
    """
    Newton-Raphson 기반 정확한 PPA 역산.
    Phase 1: PPA ±25% 11점 민감도 스캔
    Phase 2: Newton-Raphson (tolerance 0.01% IRR)
    """
    try:
        base_inputs = dict(req.inputs)
        base_ppa = float(base_inputs.get("ppa_price", 68.82))
        target_irr = req.target_irr_pct / 100.0  # 0.11
        tol = 0.0001  # 0.01% IRR tolerance
        h = 0.50  # finite difference step $/MWh
        max_iter = 10

        def calc_irr(ppa_val):
            """Calc engine 호출 → Sponsor IRR (Full Life 우선) 반환.
            엔진 발산 시 None 반환 (caller가 처리)."""
            inp = dict(base_inputs)
            inp["ppa_price"] = ppa_val
            try:
                res = _calc_engine(inp)
            except Exception:
                return None
            # sponsor_irr 우선, 없으면 contract, 둘 다 None이면 None
            s_irr = res.get("sponsor_irr")
            if s_irr is None:
                s_irr = res.get("sponsor_irr_contract")
            return s_irr  # None or float

        # ── Phase 1: ±25% 민감도 스캔 ──
        pcts = [-25, -20, -15, -10, -5, 0, 5, 10, 15, 20, 25]
        sensitivity = []
        for pct in pcts:
            ppa_p = base_ppa * (1 + pct / 100.0)
            irr_p = calc_irr(ppa_p)
            sensitivity.append({
                "pct": pct,
                "ppa": round(ppa_p, 2),
                "irr_pct": round(irr_p * 100, 4) if irr_p is not None else None
            })

        # ── Phase 2: Newton-Raphson ──
        iterations = []
        ppa = base_ppa  # 초기값
        status = "not_started"
        solution = None

        # Sensitivity에서 유효한 값만 추출
        valid_sens = [s for s in sensitivity if s["irr_pct"] is not None]
        if not valid_sens:
            # 엔진이 모든 PPA에서 발산 — break-even 불가
            status = "engine_diverged"
            solution = {
                "ppa": base_ppa,
                "irr_pct": 0,
                "error_pct": 0,
                "iterations": 0,
                "converged": False,
            }
            base_res = _calc_engine(base_inputs)
            return {
                "ok": True,
                "base_ppa": base_ppa,
                "target_irr_pct": req.target_irr_pct,
                "sensitivity": sensitivity,
                "iterations": iterations,
                "solution": solution,
                "status": status,
                "tolerance_pct": tol * 100,
                "dev_margin_k": base_res.get("dev_margin", 0),
            }

        min_irr = min(s["irr_pct"] for s in valid_sens)
        max_irr = max(s["irr_pct"] for s in valid_sens)
        target_irr_pct = target_irr * 100

        # Target이 sensitivity 범위 안에 있는지 먼저 확인
        # 범위 안에 있으면 linear interpolation으로 초기값 설정 (Newton-Raphson에 좋은 시작점)
        if min_irr <= target_irr_pct <= max_irr:
            # 두 인접 sens point 사이에서 linear interp
            sorted_sens = sorted(valid_sens, key=lambda s: s["ppa"])
            for i in range(len(sorted_sens) - 1):
                lo = sorted_sens[i]
                hi = sorted_sens[i+1]
                if (lo["irr_pct"] <= target_irr_pct <= hi["irr_pct"]) or \
                   (hi["irr_pct"] <= target_irr_pct <= lo["irr_pct"]):
                    # linear interp on PPA
                    if hi["irr_pct"] != lo["irr_pct"]:
                        frac = (target_irr_pct - lo["irr_pct"]) / (hi["irr_pct"] - lo["irr_pct"])
                        ppa = lo["ppa"] + frac * (hi["ppa"] - lo["ppa"])
                    else:
                        ppa = (lo["ppa"] + hi["ppa"]) / 2
                    break
        elif target_irr_pct < min_irr:
            ppa = base_ppa * 0.75
            status = "target_below_range"
        else:
            ppa = base_ppa * 1.25
            status = "target_above_range"

        for i in range(max_iter):
            irr_cur = calc_irr(ppa)
            if irr_cur is None:
                # 엔진 발산 → 이 PPA에선 IRR 못 구함, loop 종료
                status = "engine_diverged_mid"
                break
            err = irr_cur - target_irr
            iterations.append({
                "iter": i,
                "ppa": round(ppa, 4),
                "irr_pct": round(irr_cur * 100, 4),
                "error_pct": round(err * 100, 4),
                "status": "converged" if abs(err) < tol else "iterating"
            })

            if abs(err) < tol:
                solution = {
                    "ppa": round(ppa, 4),
                    "irr_pct": round(irr_cur * 100, 4),
                    "error_pct": round(err * 100, 4),
                    "iterations": i + 1,
                    "converged": True
                }
                status = "converged"
                break

            # 미분 (central difference) — 양쪽 발산 체크
            irr_plus = calc_irr(ppa + h)
            irr_minus = calc_irr(ppa - h)
            if irr_plus is None or irr_minus is None:
                status = "engine_diverged_deriv"
                break
            derivative = (irr_plus - irr_minus) / (2 * h)

            if abs(derivative) < 1e-8:
                status = "flat_derivative"
                break

            # Newton step + 안전장치 (최대 20% 한 스텝)
            delta = -err / derivative
            max_step = base_ppa * 0.20
            if abs(delta) > max_step:
                delta = max_step if delta > 0 else -max_step
            ppa = ppa + delta

            # 음수/비정상 방지
            if ppa < 1.0:
                ppa = 1.0
            elif ppa > base_ppa * 3:
                ppa = base_ppa * 3

        if not solution:
            # 수렴 실패 - 마지막 iteration 값 사용
            last = iterations[-1] if iterations else {"ppa": base_ppa, "irr_pct": 0}
            solution = {
                "ppa": last["ppa"],
                "irr_pct": last["irr_pct"],
                "error_pct": last.get("error_pct", 0),
                "iterations": len(iterations),
                "converged": False
            }
            if status == "not_started":
                status = "max_iter_reached"

        # 추가 meta: Dev Margin 고정값 (참고용)
        base_res = _calc_engine(base_inputs)
        dev_margin_k = base_res.get("dev_margin", 0)  # $k

        return {
            "ok": True,
            "base_ppa": round(base_ppa, 2),
            "target_irr_pct": round(target_irr_pct, 2),
            "sensitivity": sensitivity,
            "iterations": iterations,
            "solution": solution,
            "status": status,
            "dev_margin_k": round(dev_margin_k, 0),
            "tolerance_pct": tol * 100,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Break-even calculation error: {str(e)}")


@router.get("/valuation/calculate/defaults")
def get_calc_defaults(mode: str = 'prediction', user=Depends(get_current_user)):
    """Return default input values for the calculator
    
    mode='prediction' (기본): 신규 프로젝트 표준 PF 가정
    mode='calibration': Neptune Case 2 재현용 파라미터
    """
    # ═══ 공통 defaults (모든 모드) ═══
    common = {
        "pv_mwac": 199, "dc_ac_ratio": 1.348,
        "pv_mwdc": 268.3,
        "bess_mw": 199, "bess_mwh": 796,
        "cf_pct": 21.24, "life": 35,
        "module_cwp": 31.5, "bos_cwp": 42.88,
        "ess_per_kwh": 234.5, "bess_bos_per_kwh": 130.0,
        "epc_cont_pct": 8.0, "owner_pct": 3.0, "softcost_pct": 5.0,
        "intercon_m": 22.5, "dev_cost_m": 20.0,
        # Dev Margin: c/Wac × (PV + BESS) MW (HWR 표준 공식, 모든 모드 공통)
        "dev_margin_cwac": 10.0, "epc_margin_pct": 7.95,
        "ppa_price": 68.82, "ppa_term": 25, "ppa_esc": 0,
        "bess_toll": 14.5, "bess_toll_term": 20,
        "merchant_ppa": 61.0, "merchant_esc": 2.0,
        "degradation": 0.0064,
        "pv_om": 4.5, "bess_om": 8.64, "insurance_pv": 10.57,
        "insurance_bess": 5.05, "asset_mgmt": 210,
        "prop_tax_yr1": 3162, "land_rent_yr1": 437, "opex_esc": 2.0,
        "aug_price": 150, "aug_mwh_pct": 18.8, "aug_y1": 4, "aug_y2": 8,
        "debt_ratio": 47.6, "int_rate": 5.5,
        "credit_mode": "ITC",
        "pv_itc_rate": 0, "bess_itc_rate": 30,
        "ptc_rate_per_kwh": 0.027,
        "itc_elig": 97,
        "flip_term": 7, "flip_yield": 8.0,
    }
    
    if mode == 'calibration':
        # ═══ Calibration (Neptune Case 2 재현) ═══
        return {**common,
            "calibration_mode": "calibration",
            "aug_y3": 14,  # Neptune: Y14
            "loan_term": 28,
            "availability_yr1": 1.0, "availability_yr2": 1.0,  # CF%에 내재
            "bess_months_per_yr": 12.72,
            "opex_etc": 0.56,
            # Capital Stack (Neptune 실측)
            "capex_total_override": 836.7,
            "te_ratio_override": 32.52,
            # Y0 Cash Flow (Neptune 실측)
            "construction_cost_m": 639.855,
            "txn_costs_m": 10.6,
            "cap_interest_m": 14.3,
            "debt_drawdown_ratio": 0.775,
            "te_proceeds_ratio": 0.935,
            # Partnership Flip Waterfall (Neptune 실측)
            "pre_flip_cash_te": 25.5,
            "post_flip_cash_te": 7,
            "depr_share": 0.7721,
            "use_nol_offset": True,
            "use_sculpted_debt": True,
            "flip_event_cf": 0,
            "flip_yield": 8.75,  # Neptune 실측
        }
    else:
        # ═══ Prediction (신규 프로젝트 표준) ═══
        return {**common,
            "calibration_mode": "prediction",
            "aug_y3": 12,  # 표준 Y12
            "loan_term": 18,
            "availability_yr1": 0.977, "availability_yr2": 0.982,
            "bess_months_per_yr": 12.0,
            "opex_etc": 0,
            # Capital Stack — 사용자 프로젝트 맞게 입력
            # (override 없이 ITC 기반 자동 계산)
            # Y0 Cash Flow — 100% drawdown (표준)
            "debt_drawdown_ratio": 1.0,
            "te_proceeds_ratio": 1.0,
            # Partnership Flip (표준 99/5)
            "pre_flip_cash_te": 99,
            "post_flip_cash_te": 5,
            "depr_share_pre": 0.01,
            "depr_share_post": 0.95,
            "use_nol_offset": False,
            "use_sculpted_debt": False,
            "flip_event_cf": 0,
        }


@router.post("/valuation/integrity-check")
async def integrity_check_upload(
    file: UploadFile = File(...),
    lang: str = 'ko',
    user=Depends(get_current_user)
):
    """PF 엑셀 모델 정합성 체크 (5개 카테고리 × HIGH/MEDIUM/LOW). lang=ko|en"""
    if not (file.filename.endswith(".xlsb") or file.filename.endswith(".xlsx")):
        raise HTTPException(400, "xlsb 또는 xlsx 파일만 가능합니다.")
    
    suffix = ".xlsb" if file.filename.endswith(".xlsb") else ".xlsx"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name
    
    try:
        result = _integrity_check_pf_model(tmp_path, lang=lang)
        result['filename'] = file.filename
        return {"ok": True, **result}
    except Exception as e:
        raise HTTPException(500, f"정합성 체크 오류: {str(e)}")
    finally:
        try:
            import os
            os.unlink(tmp_path)
            # xlsb 변환본도 삭제
            if tmp_path.endswith('.xlsb') and os.path.exists(tmp_path.replace('.xlsb', '.xlsx')):
                os.unlink(tmp_path.replace('.xlsb', '.xlsx'))
        except Exception:
            pass


@router.post("/valuation/upload")
async def upload_valuation(
    project_id: str = Form(...),
    scenario:   str = Form(default=""),
    reason:     str = Form(default=""),
    approver:   str = Form(default=""),
    file: UploadFile = File(...),
    user=Depends(get_current_user)
):
    """PF 재무모델(xlsb/xlsx) 업로드 → AI 파싱 → Firebase 저장"""
    if not (file.filename.endswith(".xlsb") or file.filename.endswith(".xlsx")):
        raise HTTPException(400, "xlsb 또는 xlsx 파일만 업로드 가능합니다.")

    suffix = ".xlsb" if file.filename.endswith(".xlsb") else ".xlsx"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        parsed = parse_pf_model(tmp_path)
    except Exception as e:
        raise HTTPException(500, f"모델 파싱 실패: {str(e)}")
    finally:
        os.unlink(tmp_path)

    ts      = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    safe_id = project_id.replace("/", "_").replace(".", "_")

    payload = {
        "uploaded_at": datetime.datetime.now().isoformat(),
        "uploaded_by": user["email"],
        "filename":    file.filename,
        "scenario":    scenario,
        "reason":      reason,
        "approver":    approver,
        "assumptions": parsed["assumptions"],
        "outputs":     parsed["outputs"],
    }

    fb_put(f"valuation/{safe_id}/versions/{ts}", payload)
    fb_put(f"valuation/{safe_id}/latest", payload)

    return {
        "ok":         True,
        "project_id": safe_id,
        "timestamp":  ts,
        "parsed":     parsed,
    }


@router.get("/valuation")
def get_all_valuations(user=Depends(get_current_user)):
    """전체 프로젝트 latest 비교 (Valuation 탭용)"""
    all_data = fb_read("valuation") or {}
    result = {}
    for pid, pdata in all_data.items():
        if isinstance(pdata, dict) and "latest" in pdata:
            result[pid] = pdata["latest"]
    return result


@router.get("/valuation/{project_id}")
def get_valuation(project_id: str, user=Depends(get_current_user)):
    safe_id = project_id.replace("/", "_").replace(".", "_")
    return fb_read(f"valuation/{safe_id}")


@router.get("/valuation/{project_id}/latest")
def get_valuation_latest(project_id: str, user=Depends(get_current_user)):
    safe_id = project_id.replace("/", "_").replace(".", "_")
    return fb_read(f"valuation/{safe_id}/latest")


@router.get("/valuation/{project_id}/versions")
def get_valuation_versions(project_id: str, user=Depends(get_current_user)):
    safe_id = project_id.replace("/", "_").replace(".", "_")
    return fb_read(f"valuation/{safe_id}/versions")


@router.post("/valuation/generate-ic-summary")
async def generate_ic_summary(payload: dict, user=Depends(get_current_user)):
    """Claude API로 IC Summary 전문 보고서 생성"""
    if not ANTHROPIC_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY 환경변수 미설정")

    proj    = payload.get("project_name", "Project")
    metrics = payload.get("metrics", {})
    scenarios = payload.get("scenarios", [])
    assumptions = payload.get("assumptions", {})
    history = payload.get("history", [])
    today   = payload.get("date", "")

    scen_text = ""
    if scenarios:
        scen_text = "\n\nScenario Analysis:\n"
        for s in scenarios:
            scen_text += f"  {s.get('name','')}: IRR {s.get('irr','—')}, Dev Margin {s.get('margin','—')}\n"

    hist_text = ""
    if history:
        hist_text = "\n\nVersion History (recent):\n"
        for h in history[:3]:
            hist_text += f"  {h.get('date','')} — {h.get('reason','')}\n"

    prompt = (
        "You are a senior investment analyst at a US renewable energy developer. "
        "Write a concise, professional Investment Committee (IC) Summary in Korean (with key financial metrics in English). "
        "Use formal Korean business writing style. Structure it with clear sections.\n\n"
        f"Project: {proj}\n"
        f"Date: {today}\n\n"
        "Financial Metrics:\n"
        f"  Sponsor IRR: {metrics.get('sirr','—')}\n"
        f"  Dev Margin: {metrics.get('dev_margin','—')}\n"
        f"  Levered IRR: {metrics.get('lirr','—')}\n"
        f"  Unlevered IRR: {metrics.get('uirr','—')}\n"
        f"  EBITDA Yield: {metrics.get('ebitda_yield','—')}\n"
        f"  Total CAPEX: {metrics.get('capex','—')}\n"
        f"  Debt: {metrics.get('debt','—')} ({metrics.get('debt_pct','—')})\n"
        f"  Tax Equity: {metrics.get('te','—')}\n"
        f"  Sponsor Equity: {metrics.get('eq','—')}\n"
        f"  PPA: {metrics.get('ppa','—')}\n"
        f"  BESS Toll: {metrics.get('toll','—')}\n"
        f"  ITC/PTC: {metrics.get('credit','—')}\n"
        f"  Flip Yield: {metrics.get('flip','—')}\n"
        f"{scen_text}{hist_text}\n\n"
        "Write the IC Summary with these sections:\n"
        "1. 프로젝트 개요 (2-3 sentences)\n"
        "2. 핵심 재무 지표 (bullet points with brief commentary)\n"
        "3. Deal Structure 특징 (TE flip structure, debt terms 등)\n"
        "4. 리스크 요인 (2-3 key risks)\n"
        "5. 투자 의견 (1 paragraph recommendation)\n\n"
        "Keep it concise — suitable for a 1-page print. "
        "Return ONLY valid JSON: "
        '{{"sections":[{{"title":"섹션제목","content":"내용"}}]}}'
    )

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": prompt}]
        },
        timeout=40
    )
    if resp.status_code != 200:
        raise HTTPException(500, f"Claude API 오류: {resp.text[:200]}")

    data = resp.json()
    text = "".join(b.get("text","") for b in data.get("content",[]))
    clean = text.replace("```json","").replace("```","").strip()
    return {"ok": True, "result": clean}


@router.post("/valuation/export-pdf")
async def export_ic_pdf(payload: dict, user=Depends(get_current_user)):
    """IC Summary PDF 생성 (WeasyPrint, world-class formatting)."""
    import traceback
    import sys

    # Step 1: WeasyPrint import
    try:
        from weasyprint import HTML
        print(f"[export-pdf] WeasyPrint import OK", flush=True)
    except Exception as e:
        print(f"[export-pdf] WeasyPrint import FAILED: {e}", flush=True)
        traceback.print_exc(file=sys.stdout)
        raise HTTPException(500, f"WeasyPrint import 실패: {str(e)[:300]}")

    # Step 2: HTML 문자열 생성
    try:
        html_str = _build_ic_pdf_html(payload)
        print(f"[export-pdf] HTML built, length={len(html_str)}", flush=True)
    except Exception as e:
        print(f"[export-pdf] HTML build FAILED: {e}", flush=True)
        traceback.print_exc(file=sys.stdout)
        raise HTTPException(500, f"HTML 생성 오류: {str(e)[:300]}")

    # Step 3: PDF 렌더링
    try:
        pdf_bytes = HTML(string=html_str).write_pdf()
        print(f"[export-pdf] PDF rendered, size={len(pdf_bytes)} bytes", flush=True)
    except Exception as e:
        print(f"[export-pdf] PDF render FAILED: {e}", flush=True)
        traceback.print_exc(file=sys.stdout)
        raise HTTPException(500, f"PDF 렌더링 오류: {str(e)[:300]}")

    proj_name = payload.get("project_name", "IC_Summary").replace(" ", "_")
    date_str = payload.get("date", datetime.date.today().isoformat()).replace("-", "")
    filename = f"IC_Summary_{proj_name}_{date_str}.pdf"

    return _Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# ── WeasyPrint 진단용 미니멀 테스트 엔드포인트 ───────
@router.get("/valuation/export-pdf-test")
async def export_pdf_test(user=Depends(get_current_user)):
    """WeasyPrint가 살아있는지 간단히 테스트."""
    import traceback, sys
    try:
        from weasyprint import HTML
        simple_html = "<html><body><h1>Test</h1><p>안녕하세요, WeasyPrint 테스트</p></body></html>"
        pdf = HTML(string=simple_html).write_pdf()
        return _Response(
            content=pdf,
            media_type="application/pdf",
            headers={"Content-Disposition": 'attachment; filename="weasyprint_test.pdf"'}
        )
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        raise HTTPException(500, f"테스트 실패: {str(e)[:500]}")


@router.post("/valuation/analyze-cf")
async def analyze_cf(payload: dict, user=Depends(get_current_user)):
    """CF 데이터를 Claude API로 분석하여 인사이트 반환"""
    if not ANTHROPIC_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY 환경변수 미설정")

    cf_text   = payload.get("cf_text", "")
    proj_name = payload.get("project_name", "프로젝트")

    context        = payload.get("context", "")
    proj_context   = payload.get("project_context", "")  # PPV 탭 프로젝트 메타데이터
    lang           = payload.get("lang", "en")
    mode           = payload.get("mode", "full")
    proj_meta      = payload.get("project_meta", {})
    stage    = proj_meta.get("stage", "")
    iso      = proj_meta.get("iso", "")
    proj_type= proj_meta.get("type", "")
    ntp_date = proj_meta.get("ntp", "")
    cod_date = proj_meta.get("cod", "")
    risk_pct = proj_meta.get("risk_factor", "")
    if risk_pct != "": risk_pct = f"{float(risk_pct)*100:.0f}%"
    itc_risk = proj_meta.get("itc_expiry_risk", "")
    proj_ctx = proj_meta.get("proj_ctx", "")
    thresholds     = payload.get("thresholds", {})
    current_metrics= payload.get("current_metrics", {})

    irr_thr    = thresholds.get("sponsor_irr_pct", 9.0)
    margin_thr = thresholds.get("dev_margin_cwp", 10.0)
    itc_thr    = thresholds.get("itc_min_pct", 30.0)

    curr_irr    = current_metrics.get("sponsor_irr_pct", "?")
    curr_irr_basis = current_metrics.get("sponsor_irr_basis", "After-TE-Flip, Full Life")
    curr_margin = current_metrics.get("dev_margin_cwp", "?")
    curr_itc    = current_metrics.get("itc_rate_pct", "?")
    sponsor_npv_m = current_metrics.get("sponsor_npv_m")  # $M (optional)
    project_npv_m = current_metrics.get("project_npv_m")  # $M (optional)
    ppa_term    = current_metrics.get("ppa_term", "?")
    toll_term   = current_metrics.get("toll_term", "?")
    pv_mwac     = current_metrics.get("pv_mwac", "?")

    if mode == "interp":
        prompt = (
            "미국 태양광+BESS PF 전문가로서 아래 연도별 Sponsor CF 패턴을 분석해줘.\n"
            f"프로젝트: {proj_name}\n"
            f"CF: {cf_text}\n\n"
            "3~4개 핵심 인사이트를 JSON으로 반환 (다른 텍스트 없이):\n"
            '{"insights":[{"title":"제목","detail":"설명(80자이내)"}]}'
        )
    else:
        # 동적 날짜 계산
        _today = datetime.date.today()
        _current_year = _today.year
        _current_quarter = f"{_current_year}-Q{(_today.month - 1) // 3 + 1}"
        _prev_q_month = _today.month - 3
        _prev_q_year = _current_year
        if _prev_q_month <= 0:
            _prev_q_month += 12
            _prev_q_year -= 1
        _prev_quarter = f"{_prev_q_year}-Q{(_prev_q_month - 1) // 3 + 1}"

        # 시장 데이터 컨텍스트 (payload에서 주입)
        market_context = payload.get("market_context", {}) or {}
        rates_txt = market_context.get("rates_summary", "")
        levelten_txt = market_context.get("levelten_summary", "")
        # BESS 소스 priority: LevelTen Storage (official) 1순위, AI Research (fallback) 2순위
        levelten_storage_txt = market_context.get("levelten_storage_summary", "")  # 공식 ISO-level
        bess_tolling_txt = market_context.get("bess_tolling_summary", "")          # AI Research duration별
        our_bess_duration = market_context.get("our_bess_duration", 4)
        # LevelTen 커버 여부 + 지역 해석 (WECC sub-region, SERC 등)
        lt_covered = market_context.get("levelten_covered", True)  # 기본 True (기존 프로젝트 호환)
        region_display = market_context.get("region_display", "")  # "WECC Rocky Mountain (UT)" 등
        sub_region = market_context.get("sub_region", "")          # WECC_RM, WECC_DSW, etc.
        continental_avg_txt = market_context.get("continental_avg_summary", "")  # Market-Averaged Continental (대용 비교용)

        market_block = ""
        if rates_txt or levelten_txt or levelten_storage_txt or bess_tolling_txt or continental_avg_txt:
            market_block = "=== CURRENT MARKET DATA (most recent; use this INSTEAD of training knowledge) ===\n"
            if rates_txt:
                market_block += f"  Interest Rates: {rates_txt}\n"
            # 지역 해석 명시
            if region_display:
                market_block += f"  Project Region: {region_display}"
                if lt_covered:
                    market_block += " [LevelTen 직접 커버 ISO]\n"
                else:
                    market_block += f" [LevelTen 미커버 — 대용 비교 필요]\n"
            if levelten_txt:
                market_block += f"  LevelTen PPA Benchmark (Solar): {levelten_txt}\n"
                if lt_covered:
                    market_block += "  → Our ISO가 LevelTen에 있음. USE IT to compare against project PPA directly.\n"
                    market_block += "    Reference: 'PPA $X.XX vs LevelTen P25 $Y.YY in {ISO}'.\n"
                else:
                    market_block += "  → Our region is NOT in LevelTen. Use as market context reference only.\n"
            # 대용 비교 (WECC/SERC 등 LevelTen 미커버 지역)
            if not lt_covered and continental_avg_txt:
                market_block += f"  Market-Averaged Continental Index (대용 비교용): {continental_avg_txt}\n"
                market_block += f"  → Use this as PRIMARY benchmark since {sub_region or 'project region'} has NO direct LevelTen coverage.\n"
                market_block += "  → Cite as 'LevelTen Market-Averaged Continental (전 대륙 ISO 평균, 대용치)'.\n"
                market_block += "  → Explicitly note '해당 지역 공식 P25 데이터 없음 → 대륙 평균 대비 비교' in risk commentary.\n"
            # Priority 1: LevelTen Storage (official, ISO-level)
            if levelten_storage_txt:
                market_block += f"  LevelTen Storage Index (OFFICIAL tolling offers, Q4 2025): {levelten_storage_txt}\n"
                market_block += f"  → Project BESS duration: {our_bess_duration}h (ISO-level price applies broadly, consider duration fit)\n"
                market_block += "  → USE THIS OFFICIAL DATA as primary BESS benchmark. Cite as 'LevelTen 공식 Storage Index'.\n"
                market_block += "  → If project toll EXCEEDS ISO P75 → risk 'BESS Toll 시장 상단 초과' (severity: Critical if >20% over, Watch if slight).\n"
                market_block += "  → If project toll is BELOW ISO P25 → positive flag '보수적 산정'.\n"
            # Priority 2: AI Research (fallback for non-LevelTen ISOs: ISO-NE/NYISO/WECC_*/SERC, or duration-level detail)
            if bess_tolling_txt:
                if levelten_storage_txt:
                    market_block += f"  AI Research Duration Detail (supplementary — LevelTen only provides ISO-level): {bess_tolling_txt}\n"
                    market_block += f"  → Use ONLY to add duration-specific nuance ({our_bess_duration}h). LevelTen ISO-level is primary.\n"
                    market_block += "  → Caveat: 'duration 세부는 AI 추정치'.\n"
                else:
                    # LevelTen 없는 지역 (WECC_*, ISO-NE, NYISO, SERC)
                    market_block += f"  BESS Tolling Estimate (AI Research — {sub_region or 'non-LevelTen region'}): {bess_tolling_txt}\n"
                    market_block += f"  → Project BESS duration: {our_bess_duration}h\n"
                    market_block += "  → CAVEAT: '시장 추정치, 공식 index 아님' when citing.\n"
                    if sub_region and sub_region.startswith("WECC"):
                        market_block += f"  → For {sub_region}: reference relevant utility RFPs (PacifiCorp IRP, URC, APS, Xcel Colorado, etc.) if AI Research provided commentary.\n"
            market_block += "\n"

        # 경제성 지표 추출 (Unlevered vs WACC 비교용)
        unlev_irr = current_metrics.get("unlevered_irr_pct")
        wacc_val  = current_metrics.get("wacc_pct")
        wacc_block = ""
        if unlev_irr is not None and wacc_val is not None:
            wacc_block = (
                f"  Unlevered Pre-Tax IRR : {unlev_irr}% (project-level)\n"
                f"  WACC                  : {wacc_val}% (hurdle)\n"
                f"  Value Creation        : Unlev - WACC = "
                f"{'POSITIVE' if float(unlev_irr)>float(wacc_val) else 'NEGATIVE'}\n"
            )

        prompt = (
        "You are the head of Investment Committee at Hanwha Energy USA (HEUH), "
        "a renewable energy developer whose sole business model is: develop → sell at NTP (before COD). "
        "The IC decision: should we continue spending development capital on this project?\n\n"

        f"TODAY'S DATE: {_today.isoformat()} (current quarter: {_current_quarter}, prior: {_prev_quarter}).\n\n"

        "═══ KNOWN REGULATORY & OPERATIONAL FACTS (treat as GIVEN; do not second-guess) ═══\n"
        "1. ITC Section 48E — Solar PV:\n"
        "   - 'Beginning of Construction' (BOC) is a LEGAL construct, not physical construction start.\n"
        "   - BOC deadline: July 4, 2026 — established via Physical Work Test (on-site or off-site binding work).\n"
        "   - Continuity Safe Harbor preserved: if BOC is established, project has until Dec 31, 2030 (4 years) to reach PIS.\n"
        "   - Projects missing BOC by July 4, 2026 must be Placed-in-Service by Dec 31, 2027.\n"
        "2. ITC Section 48E — BESS (SEPARATE TRACK from PV):\n"
        "   - Begin Construction by Dec 31, 2033 → 100% ITC\n"
        "   - 2034 → 75%, 2035 → 50%, 2036 → expires\n"
        "   - BESS is NOT subject to the 2026 solar cliff. Do NOT flag BESS ITC as imminent risk.\n"
        "3. HEUH Business Model & BOC Status:\n"
        "   - HEUH develops → sells at NTP (pre-COD). Post-COD execution risk does NOT affect IC decision.\n"
        "   - HEUH has established BOC for its project pool via Physical Work Test, managed by its compliance team.\n"
        "   - Individual project matching to BOC pool is operational matter — do NOT flag as financial risk.\n"
        "   - Post-BOC physical construction schedule is flexible within 4-year Continuity Safe Harbor.\n"
        "4. FEOC (Foreign Entity of Concern): compliance checklist item — do NOT use as verdict driver.\n\n"

        f"PROJECT: {proj_name} | Size: {pv_mwac} MWac\n"
        f"FINANCIAL SUMMARY: {context}\n"
        f"PROJECT METADATA: {proj_ctx}\n"
        f"ANNUAL SPONSOR CF (Y1-Y10): {cf_text}\n\n"

        + market_block +

        "=== INVESTMENT THRESHOLDS (firm hurdles) ===\n"
        f"  Primary   · Sponsor IRR ≥ {irr_thr}% (After-TE-Flip, Full Life) — 매수자 요구 수익률\n"
        f"  Secondary · Dev Margin  ≥ {margin_thr} c/Wp — HEUH 내부 마진 기준\n"
        "  Both must PASS for IC approval.\n\n"

        "=== CURRENT PROJECT METRICS ===\n"
        f"  Sponsor IRR : {curr_irr}% ({curr_irr_basis})\n"
        f"  Dev Margin  : {curr_margin} c/Wp\n"
        + (f"  Sponsor NPV : ${sponsor_npv_m}M (discounted at {irr_thr}% hurdle)\n" if sponsor_npv_m is not None else "")
        + (f"  Project NPV : ${project_npv_m}M (discounted at WACC)\n" if project_npv_m is not None else "")
        + wacc_block +
        f"  ITC Rate    : {curr_itc}%\n"
        f"  PPA Term    : {ppa_term} yrs | Toll Term: {toll_term} yrs\n\n"

        "═══ VERDICT FRAMEWORK (PURE ECONOMICS ONLY) ═══\n"
        "The verdict is determined ONLY by economic criteria. Development risks are monitoring items and do NOT affect verdict.\n\n"
        "Economic criteria:\n"
        "  1. Dev Margin vs threshold (primary: HEUH's exit value)\n"
        "  2. Sponsor IRR (After-TE-Flip, Full Life) vs threshold (market-clearing for buyer)\n"
        "  3. Unlevered IRR vs WACC (true value creation — leverage-independent)\n\n"
        "VERDICT RULES:\n"
        "  PROCEED:\n"
        "    - Dev Margin ≥ threshold AND Sponsor IRR ≥ threshold AND Unlev IRR > WACC\n"
        "    - Express threshold headroom explicitly if positive (e.g., '+1.5%p 여유')\n"
        "  RECUT:\n"
        "    - 1~2 criteria near miss (gap < 1.5%p from threshold) AND recoverable via negotiation\n"
        "    - Typical levers: PPA price revision, CAPEX reduction, TE/debt terms\n"
        "  STOP:\n"
        "    - Multiple criteria missed OR Unlev IRR < WACC (value destruction)\n"
        "    - Unrecoverable: gap too wide to close via normal levers\n\n"

        "═══ RISK ANALYSIS (monitoring only — NOT verdict driver) ═══\n"
        "Identify project-specific risks AI can assess:\n"
        "  - EPC price adequacy: $/Wdc vs current market (use supplied MARKET DATA if provided)\n"
        "  - ISO / interconnection queue risk based on ISO and state\n"
        "  - PPA market competitiveness vs supplied LevelTen P25 data (if given)\n"
        "  - Construction timeline vs PIS deadlines (Solar PV: 4-year continuity → Dec 31, 2030 PIS if BOC established; BESS: flexible to 2033)\n"
        "  - BESS replacement CAPEX / augmentation assumption sanity\n"
        "DO NOT generate risks for:\n"
        "  - Safe Harbor matching or BOC status (handled separately as fixed checklist item)\n"
        "  - FEOC compliance (handled separately as fixed checklist item)\n"
        "  - BESS ITC expiry (not imminent — 2033+ horizon)\n"
        "  - 'Must begin physical construction by 2026' — INCORRECT; BOC is a legal construct already managed via HEUH's Physical Work Test completion\n"
        "  - Generic 'market uncertainty' or 'policy risk' without specifics\n\n"

        "═══ LANGUAGE ═══\n"
        + ("ALL text fields in KOREAN (한국어).\n"
           "\n"
           "CRITICAL — Korean ENDING STYLE (IC memo convention, formal & concise):\n"
           "Use short nominal/verbal endings, NOT 존대체 (하다/한다) nor 합쇼체 (합니다).\n"
           "Required endings:\n"
           "  - 명사형 종결: '~ 충족', '~ 확인', '~ 권고', '~ 필요', '~ 가능', '~ 부족'\n"
           "  - 축약 서술: '~됨', '~함', '~임', '~없음', '~확보됨'\n"
           "Examples (GOOD):\n"
           "  ✓ '개발 마진 20.0 c/Wp로 기준 대비 +10.0%p 여유 확보'\n"
           "  ✓ '가중평균자본비용 대비 +0.88%p 상회로 가치 창출 확인'\n"
           "  ✓ 'PPA 재협상 또는 CAPEX 3% 절감 필요'\n"
           "  ✓ '경제성 기준 3개 모두 충족, 개발자본 투입 계속 권고'\n"
           "Examples (BAD — do NOT use):\n"
           "  ✗ '~제공한다' (→ '~제공')\n"
           "  ✗ '~확인된다' (→ '~확인됨')\n"
           "  ✗ '~충족한다' (→ '~충족')\n"
           "  ✗ '~하도록 한다' (→ '~권고')\n"
           "  ✗ '~해야 합니다' / '~할 수 있습니다' (too formal/verbose)\n"
           "Maintain consistency — ALL sentences end in the nominal/concise style.\n"
           "\n"
           "CRITICAL — INDUSTRY TERMINOLOGY:\n"
           "  ✗ '증설' (WRONG — means 'capacity expansion')\n"
           "  ✓ 'Augmentation' (English preferred, industry standard)\n"
           "  ✓ '배터리 교체 (용량 유지)' or '배터리 보강 (성능 유지)' (if Korean needed)\n"
           "  Augmentation = replacing/adding cells to MAINTAIN capacity over degradation,\n"
           "  NOT adding new capacity. Never call it '증설'.\n"
           "\n"
           "Only 'verdict' (PROCEED/RECUT/STOP) and 'verdict_color' (green/amber/red) stay English.\n"
           "Financial numbers with units can stay English-style (e.g., '10.38%', '$68.82/MWh').\n"
           "DO NOT mix languages within a single field.\n"
           if payload.get("lang","en")=="kr" else
           "ALL text fields in ENGLISH only. Formal institutional investor tone.\n")
        + "\n"
        "Be direct. Cite specific numbers. No hedging.\n\n"

        "Respond ONLY with valid JSON (no markdown, no code blocks).\n"
        "Required keys:\n"
        "  verdict: \"PROCEED\" | \"RECUT\" | \"STOP\"\n"
        "  verdict_color: \"green\" | \"amber\" | \"red\"\n"
        "  threshold_status: {\n"
        "    margin_ok: bool, margin_gap: str,\n"
        "    irr_ok: bool, irr_gap: str,\n"
        "    wacc_spread_ok: bool, wacc_spread: str  (e.g., '+0.88%p' or '-1.20%p')\n"
        "  }\n"
        "  metrics: ONE compact line, under 120 chars, pipe-delimited.\n"
        "    Example: '199 MWac | 10.4% IRR | $39.8M Margin | $68.8 PPA | $836M CAPEX | 30% ITC'\n"
        "  sensitivity_en: dev margin upside/downside in English with c/Wp numbers\n"
        "  sensitivity_kr: same in Korean (nominal ending style)\n"
        "  thesis: 3-4 sentence economic rationale (경제성 수치 기반 근거)\n"
        "  risks: array of {title, severity: Critical|Watch|OK, detail}\n"
        "    (project-specific only; Safe Harbor/FEOC/BESS ITC are handled separately)\n"
        "  rec: 2-3 sentence actionable recommendation (경제성 관점)\n"
        "All strings double-quoted. No trailing commas. No extra text outside JSON.\n"
        "NOTE: Do NOT include 'dev_ic' field — it has been removed from the schema."
    )

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 4000,
            "messages": [{"role": "user", "content": prompt}]
        },
        timeout=45
    )
    if resp.status_code != 200:
        raise HTTPException(500, f"Claude API 오류: {resp.text[:200]}")

    data = resp.json()
    text = "".join(b.get("text","") for b in data.get("content",[]))

    # JSON 정제 — 코드블록, 줄바꿈, 특수문자 처리
    import re as _re
    clean = text.strip()
    clean = _re.sub(r"```(?:json)?\s*", "", clean).strip()
    clean = clean.strip("`")
    # { ... } 범위만 추출
    start = clean.find("{")
    end   = clean.rfind("}") + 1
    if start >= 0 and end > start:
        clean = clean[start:end]

    # ── 고정 규정 준수 체크리스트 2개 항목을 응답에 주입 ───────
    # AI가 판단하는 risks와 완전히 분리된, 모든 프로젝트 공통 체크리스트
    is_kr = payload.get("lang", "en") == "kr"

    if is_kr:
        compliance_checklist = [
            {
                "title": "ITC BOC(Beginning of Construction) 매칭 확인",
                "severity": "Watch",
                "detail": (
                    "HEUH는 Physical Work Test 방식으로 BOC 요건을 확보하여 관리 중. "
                    "본 프로젝트가 기확보된 BOC pool과 매칭되는지 NTP 전 확인 권고. "
                    "매칭 확보 시 Continuity Safe Harbor에 따라 2030년 말까지 PIS 여유."
                )
            },
            {
                "title": "FEOC 공급망 적격성 검토",
                "severity": "Watch",
                "detail": (
                    "OBBBA에 따라 2026년 착공 프로젝트는 비PFE(중국/러시아/이란/북한 외) "
                    "부품 비중 요건 적용: PV ≥40%, BESS ≥55% (매년 5%p 상향). "
                    "EPC 계약 체결 전 배터리 셀·PV 모듈 원산지 증빙 확보 필요."
                )
            }
        ]
    else:
        compliance_checklist = [
            {
                "title": "ITC BOC Matching Verification",
                "severity": "Watch",
                "detail": (
                    "HEUH has established BOC (Beginning of Construction) for its project pool "
                    "via Physical Work Test, managed by its compliance team. Verify this project "
                    "is matched to the secured BOC pool before NTP. Once matched, Continuity Safe "
                    "Harbor extends PIS to Dec 31, 2030."
                )
            },
            {
                "title": "FEOC Supply Chain Compliance Review",
                "severity": "Watch",
                "detail": (
                    "Under OBBBA, 2026-start projects face non-PFE (China/Russia/Iran/DPRK excluded) "
                    "content thresholds: PV ≥40%, BESS ≥55% (ramping +5%p annually). Verify battery "
                    "cell and PV module country-of-origin documentation before EPC contract."
                )
            }
        ]

    # JSON 파싱해서 risks 배열 앞에 삽입
    try:
        import json as _json
        parsed = _json.loads(clean)
        ai_risks = parsed.get("risks", []) or []
        # 컴플라이언스 체크리스트를 최상단, AI 리스크를 그 뒤에
        parsed["risks"] = compliance_checklist + ai_risks
        # 구분 위해 플래그 추가 (프론트엔드에서 활용 가능)
        parsed["compliance_count"] = len(compliance_checklist)
        clean = _json.dumps(parsed, ensure_ascii=False)
    except Exception as _e:
        # 파싱 실패 시 원본 그대로 반환 (프론트가 처리)
        print(f"[analyze-cf] JSON merge failed: {_e}", flush=True)

    return {"ok": True, "result": clean}


@router.post("/valuation/{project_id}/save")
async def save_valuation_version(
    project_id: str,
    payload: dict,
    user=Depends(get_current_user)
):
    """버전 저장 → 즉시 저장 (승인 flow 제거). 100개 한도."""
    safe_id = project_id.replace("/", "_").replace(".", "_")
    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    payload["uploaded_by"] = user["email"]
    payload["uploaded_at"] = datetime.datetime.now().isoformat()
    # 승인 flow 제거 → 즉시 "saved" 상태
    payload["status"] = "saved"
    payload["requested_by"] = user["email"]
    # "approver" 필드 레거시 호환: 존재하면 "shared_with"로 마이그레이트
    if "approver" in payload and payload["approver"] and "shared_with" not in payload:
        payload["shared_with"] = payload["approver"]

    fb_put(f"valuation/{safe_id}/versions/{ts}", payload)
    fb_put(f"valuation/{safe_id}/latest", payload)

    # 100개 한도 — 초과 시 가장 오래된 것 삭제
    versions = fb_read(f"valuation/{safe_id}/versions") or {}
    keys = sorted(versions.keys())
    if len(keys) > 100:
        for old_key in keys[:len(keys)-100]:
            try:
                requests.delete(
                    f"{FB_URL}/valuation/{safe_id}/versions/{old_key}.json",
                    params=fb_auth_param(),
                    timeout=5
                )
            except Exception:
                pass

    return {"ok": True, "timestamp": ts}


# 레거시 승인/반려 엔드포인트 — 하위호환 유지하되 no-op화 (존재하는 pending 버전 정리용)
@router.post("/valuation/{project_id}/versions/{ts}/approve")
def approve_version(project_id: str, ts: str, user=Depends(require_admin)):
    """[Deprecated] 승인 flow 제거됨. 하위호환용: pending을 saved로 마이그레이트."""
    safe_id = project_id.replace("/", "_").replace(".", "_")
    fb_patch(f"valuation/{safe_id}/versions/{ts}", {
        "status": "saved",
        "approved_by": user["email"],
        "approved_at": datetime.datetime.now().isoformat()
    })
    return {"ok": True}


@router.post("/valuation/{project_id}/versions/{ts}/reject")
def reject_version(project_id: str, ts: str, body: dict = {}, user=Depends(require_admin)):
    """[Deprecated] 승인 flow 제거됨. 하위호환용: 버전 삭제."""
    safe_id = project_id.replace("/", "_").replace(".", "_")
    fb_patch(f"valuation/{safe_id}/versions/{ts}", {
        "status": "rejected",
        "rejected_by": user["email"],
        "rejected_at": datetime.datetime.now().isoformat(),
        "reject_reason": body.get("reason", "")
    })
    return {"ok": True}
