"""
pdf_report.py — HWR IC Opinion PDF 생성

Phase 4 Step 4C refactoring에서 main.py에서 분리됨.

Contents:
- _esc_html: HTML escape helper
- _fmt_pct: 퍼센트 포맷 (12.34%)
- _fmt_usd_m: USD M 포맷 ($123.4M)
- _build_ic_pdf_html: IC Opinion HTML 생성 (WeasyPrint 입력)

HTML 문자열 구성 전용. 실제 PDF 렌더링은 main.py의 export_ic_pdf
엔드포인트가 WeasyPrint로 수행.

Generated: Apr 20, 2026
"""

import base64
import datetime

def _esc_html(s):
    """HTML escape helper."""
    if s is None:
        return ""
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))

def _fmt_pct(v, decimals=2):
    """숫자 → 퍼센트 문자열. 이미 문자열이면 그대로."""
    if v is None or v == "—":
        return "—"
    if isinstance(v, str):
        return v
    try:
        return f"{float(v)*100:.{decimals}f}%"
    except Exception:
        return "—"

def _fmt_usd_m(v):
    """$M 포맷 (input in thousands)."""
    if v is None or v == "—":
        return "—"
    if isinstance(v, str):
        return v
    try:
        return f"${float(v)/1000:.1f}M"
    except Exception:
        return "—"

def _build_ic_pdf_html(data: dict) -> str:
    """IC Summary HTML (WeasyPrint용). World-class IB/PE 수준 포맷."""
    proj_name = data.get("project_name", "Project")
    today = data.get("date", datetime.date.today().isoformat())
    verdict = (data.get("verdict") or "").upper() or "—"
    verdict_color = data.get("verdict_color", "amber")

    # 색상 매핑
    color_map = {
        "green": "#059669",  # emerald-600
        "amber": "#D97706",  # amber-600
        "red":   "#DC2626",  # red-600
    }
    v_color = color_map.get(verdict_color, "#6B7280")

    # 지표
    outputs = data.get("outputs", {}) or {}
    assumptions = data.get("assumptions", {}) or {}
    pv_mwac = assumptions.get("pv_mwac") or outputs.get("pv_mwac") or "—"
    bess_mw = assumptions.get("bess_mw") or "—"
    cod = assumptions.get("cod") or "—"
    ntp = assumptions.get("ntp") or "—"
    state = data.get("state") or assumptions.get("state") or "—"
    iso = data.get("iso") or assumptions.get("iso") or "—"

    # 5 IRR 지표
    irr_lev_pre  = _fmt_pct(outputs.get("sponsor_irr_levered_pretax") or outputs.get("sponsor_irr"))
    irr_at_before = _fmt_pct(outputs.get("sponsor_irr_aftertax_before_nol"))
    irr_at_after  = _fmt_pct(outputs.get("sponsor_irr_aftertax_after_nol"))
    irr_unlev    = _fmt_pct(outputs.get("sponsor_irr_unlevered_pretax") or outputs.get("unlevered_irr"))
    wacc_val     = _fmt_pct(outputs.get("wacc"))

    # 재무 요약
    capex = _fmt_usd_m(outputs.get("capex_total"))
    debt  = _fmt_usd_m(outputs.get("debt"))
    te    = _fmt_usd_m(outputs.get("tax_equity"))
    eq    = _fmt_usd_m(outputs.get("sponsor_equity"))
    dev_margin = _fmt_usd_m(outputs.get("dev_margin"))
    margin_cwp = outputs.get("margin_cwp")
    margin_cwp_str = f"{margin_cwp:.2f} c/Wp" if isinstance(margin_cwp, (int, float)) else "—"
    ppa_price = outputs.get("ppa_price") or assumptions.get("ppa_price") or "—"
    ppa_term  = outputs.get("ppa_term") or assumptions.get("ppa_term") or "—"
    bess_toll = outputs.get("bess_toll") or assumptions.get("bess_toll") or "—"
    ebitda_y  = outputs.get("ebitda_yield")
    ebitda_y_str = f"{ebitda_y:.2f}%" if isinstance(ebitda_y, (int, float)) else "—"

    # AI 분석 결과 (IC Opinion 에서 생성된 것)
    ic_analysis = data.get("ic_analysis", {}) or {}
    thesis = ic_analysis.get("thesis", "")
    rec    = ic_analysis.get("rec", "")
    risks  = ic_analysis.get("risks", []) or []
    threshold_status = ic_analysis.get("threshold_status", {}) or {}
    dev_ic = ic_analysis.get("dev_ic", {}) or {}

    # Sensitivity (프론트에서 계산된 값)
    scenarios = data.get("scenarios", []) or []

    # Threshold 메타
    thresholds = data.get("thresholds", {}) or {}
    thr_irr = thresholds.get("sponsor_irr_pct", 9.0)
    thr_margin = thresholds.get("dev_margin_cwp", 10.0)

    # 리스크 분리: compliance_count만큼 앞쪽은 고정 체크리스트, 뒤는 AI 생성
    compliance_count = int(ic_analysis.get("compliance_count", 0) or 0)
    compliance_items = risks[:compliance_count] if compliance_count else []
    ai_risks = risks[compliance_count:] if compliance_count else risks

    # 1) 컴플라이언스 체크리스트 HTML
    compliance_html = ""
    for r in compliance_items:
        title = _esc_html(r.get("title", ""))
        detail = _esc_html(r.get("detail", ""))
        sev = r.get("severity", "Watch")
        compliance_html += f"""
        <div class="compliance-item">
          <div class="compliance-box"></div>
          <div class="compliance-body">
            <div class="compliance-head">
              <span class="compliance-title">{title}</span>
              <span class="compliance-sev">{sev}</span>
            </div>
            <div class="compliance-detail">{detail}</div>
          </div>
        </div>
        """

    # 2) AI 프로젝트별 리스크 HTML
    risks_html = ""
    sev_color = {"Critical": "#DC2626", "Watch": "#D97706", "OK": "#059669"}
    for i, r in enumerate(ai_risks[:8]):
        sev = r.get("severity", "OK")
        c = sev_color.get(sev, "#6B7280")
        title = _esc_html(r.get("title", ""))
        detail = _esc_html(r.get("detail", ""))
        risks_html += f"""
        <div class="risk-item">
          <div class="risk-header">
            <span class="risk-num">{i+1:02d}</span>
            <span class="risk-title">{title}</span>
            <span class="risk-sev" style="background:{c}">{sev}</span>
          </div>
          <div class="risk-detail">{detail}</div>
        </div>
        """

    # Scenario 테이블
    scen_rows = ""
    for s in scenarios:
        scen_rows += f"""
        <tr>
          <td class="scen-name">{_esc_html(s.get('name','—'))}</td>
          <td class="scen-val">{_esc_html(s.get('irr','—'))}</td>
          <td class="scen-val">{_esc_html(s.get('margin','—'))}</td>
        </tr>
        """

    # Threshold 체크
    def _chk(ok):
        return ('<span style="color:#059669;font-weight:700">✓ PASS</span>' if ok
                else '<span style="color:#DC2626;font-weight:700">✗ FAIL</span>')
    thr_irr_ok = threshold_status.get("irr_ok", False)
    thr_margin_ok = threshold_status.get("margin_ok", False)
    thr_irr_gap = _esc_html(threshold_status.get("irr_gap", ""))
    thr_margin_gap = _esc_html(threshold_status.get("margin_gap", ""))

    # HTML 조립
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>IC Summary - {_esc_html(proj_name)}</title>
<style>
@page {{
  size: A4 portrait;
  margin: 18mm 16mm 18mm 16mm;
  @bottom-center {{
    content: counter(page) " / " counter(pages);
    font-family: 'Noto Sans KR', 'Helvetica', sans-serif;
    font-size: 8pt;
    color: #6B7280;
  }}
  @bottom-left {{
    content: "Hanwha Energy USA Holdings · Internal IC Memo";
    font-family: 'Noto Sans KR', 'Helvetica', sans-serif;
    font-size: 7pt;
    color: #9CA3AF;
  }}
  @bottom-right {{
    content: "{_esc_html(today)}";
    font-family: 'Noto Sans KR', 'Helvetica', sans-serif;
    font-size: 7pt;
    color: #9CA3AF;
  }}
}}
@page :first {{
  @bottom-center {{ content: none; }}
  @bottom-left {{ content: none; }}
  @bottom-right {{ content: none; }}
}}
* {{ box-sizing: border-box; }}
body {{
  font-family: 'Noto Sans KR', 'Helvetica Neue', Helvetica, sans-serif;
  font-size: 10pt;
  line-height: 1.55;
  color: #111827;
  margin: 0;
  padding: 0;
  -webkit-font-smoothing: antialiased;
}}

/* ── Cover ────────────────────────────────────── */
.cover {{
  height: 260mm;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  padding: 20mm 8mm 8mm 8mm;
}}
.cover-header {{
  font-size: 8pt;
  font-weight: 600;
  color: #6B7280;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  border-bottom: 1px solid #E5E7EB;
  padding-bottom: 12pt;
}}
.cover-main {{
  margin-top: 40mm;
}}
.cover-tag {{
  font-size: 9pt;
  font-weight: 600;
  color: {v_color};
  letter-spacing: 0.16em;
  text-transform: uppercase;
  margin-bottom: 10pt;
}}
.cover-title {{
  font-size: 36pt;
  font-weight: 800;
  color: #111827;
  letter-spacing: -1.2pt;
  line-height: 1.05;
  margin-bottom: 14pt;
}}
.cover-sub {{
  font-size: 12pt;
  color: #4B5563;
  font-weight: 400;
  margin-bottom: 40pt;
}}
.cover-verdict {{
  display: inline-block;
  padding: 10pt 22pt;
  border: 2pt solid {v_color};
  border-radius: 2pt;
  font-size: 22pt;
  font-weight: 800;
  color: {v_color};
  letter-spacing: 4pt;
}}
.cover-stats {{
  display: flex;
  gap: 20pt;
  margin-top: 24pt;
}}
.cover-stat {{
  flex: 1;
  border-left: 2pt solid #E5E7EB;
  padding-left: 10pt;
}}
.cover-stat-label {{
  font-size: 7pt;
  font-weight: 700;
  color: #6B7280;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  margin-bottom: 3pt;
}}
.cover-stat-value {{
  font-size: 16pt;
  font-weight: 700;
  color: #111827;
  font-variant-numeric: tabular-nums;
}}
.cover-footer {{
  margin-top: auto;
  padding-top: 20pt;
  border-top: 1px solid #E5E7EB;
  display: flex;
  justify-content: space-between;
  font-size: 8pt;
  color: #6B7280;
}}

/* ── Content Pages ────────────────────────────── */
.page-break {{ page-break-before: always; }}

h1 {{
  font-size: 14pt;
  font-weight: 800;
  color: #111827;
  margin: 0 0 3pt 0;
  letter-spacing: -0.3pt;
}}
.section-sub {{
  font-size: 8pt;
  color: #6B7280;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  margin-bottom: 12pt;
  border-bottom: 1pt solid #E5E7EB;
  padding-bottom: 6pt;
}}
h2 {{
  font-size: 10pt;
  font-weight: 700;
  color: #111827;
  margin: 14pt 0 6pt 0;
  letter-spacing: 0;
}}
p {{ margin: 4pt 0; color: #1F2937; }}

/* Metrics Grid */
.metrics-grid {{
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 4pt;
  margin-bottom: 14pt;
}}
.metric-card {{
  padding: 7pt 8pt;
  border: 0.5pt solid #D1D5DB;
  border-radius: 2pt;
  overflow: hidden;
}}
.metric-card-primary {{
  border-left: 2.5pt solid #059669;
}}
.metric-card-secondary {{
  border-left: 2.5pt solid #D97706;
}}
.metric-card-wacc {{
  border-left: 2.5pt solid #2563EB;
}}
.metric-label {{
  font-size: 6.5pt;
  font-weight: 700;
  color: #6B7280;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  margin-bottom: 2pt;
  white-space: nowrap;
}}
.metric-value {{
  font-size: 14pt;
  font-weight: 700;
  color: #111827;
  font-variant-numeric: tabular-nums;
  line-height: 1.1;
  white-space: nowrap;
}}
.metric-sub {{
  font-size: 6.5pt;
  color: #6B7280;
  margin-top: 2pt;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}}

/* Financial table */
.fin-table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 9pt;
  margin: 8pt 0 14pt 0;
}}
.fin-table th {{
  text-align: left;
  padding: 6pt 8pt;
  border-bottom: 1pt solid #111827;
  font-size: 7pt;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #374151;
}}
.fin-table td {{
  padding: 5pt 8pt;
  border-bottom: 0.3pt solid #E5E7EB;
  font-variant-numeric: tabular-nums;
}}
.fin-table td.val {{ text-align: right; font-weight: 600; }}
.fin-table tr.subtotal td {{
  background: #F9FAFB;
  font-weight: 700;
  border-top: 0.5pt solid #6B7280;
}}

/* Threshold check */
.thr-box {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10pt;
  margin: 10pt 0;
}}
.thr-item {{
  padding: 10pt 12pt;
  border: 1pt solid #E5E7EB;
  border-radius: 2pt;
  background: #F9FAFB;
}}
.thr-label {{
  font-size: 8pt;
  font-weight: 600;
  color: #6B7280;
  margin-bottom: 4pt;
}}
.thr-status {{ font-size: 11pt; margin-bottom: 3pt; }}
.thr-gap {{ font-size: 9pt; color: #374151; }}

/* Thesis / Recommendation boxes */
.thesis-box {{
  padding: 12pt 14pt;
  background: #F9FAFB;
  border-left: 3pt solid #2563EB;
  border-radius: 0 2pt 2pt 0;
  margin: 10pt 0;
  font-size: 10pt;
  line-height: 1.7;
  color: #1F2937;
}}
.rec-box {{
  padding: 14pt 16pt;
  background: #FEF3C7;
  border-left: 3pt solid #D97706;
  border-radius: 0 2pt 2pt 0;
  margin: 10pt 0;
  font-size: 10pt;
  line-height: 1.7;
  color: #78350F;
  font-weight: 500;
}}

/* Risk items */
.risk-item {{
  padding: 10pt 0;
  border-bottom: 0.5pt solid #E5E7EB;
}}
.risk-item:last-child {{ border-bottom: none; }}
.risk-header {{
  display: flex;
  align-items: center;
  gap: 8pt;
  margin-bottom: 4pt;
}}
.risk-num {{
  font-size: 8pt;
  font-weight: 700;
  color: #9CA3AF;
  font-variant-numeric: tabular-nums;
  min-width: 18pt;
}}
.risk-title {{
  font-size: 10pt;
  font-weight: 700;
  color: #111827;
  flex: 1;
}}
.risk-sev {{
  color: #fff;
  font-size: 7pt;
  font-weight: 700;
  padding: 2pt 7pt;
  border-radius: 2pt;
  letter-spacing: 0.05em;
}}
.risk-detail {{
  font-size: 9pt;
  color: #4B5563;
  line-height: 1.6;
  margin-left: 26pt;
}}

/* Compliance Checklist items */
.compliance-note {{
  font-size: 8pt;
  color: #6B7280;
  font-style: italic;
  margin-bottom: 8pt;
}}
.compliance-item {{
  display: flex;
  gap: 10pt;
  padding: 9pt 12pt;
  background: #FFFBEB;
  border: 0.5pt solid #FCD34D;
  border-left: 3pt solid #D97706;
  border-radius: 2pt;
  margin-bottom: 6pt;
}}
.compliance-box {{
  flex-shrink: 0;
  width: 12pt;
  height: 12pt;
  border: 1pt solid #9CA3AF;
  border-radius: 2pt;
  margin-top: 2pt;
}}
.compliance-body {{ flex: 1; }}
.compliance-head {{
  display: flex;
  align-items: center;
  gap: 8pt;
  margin-bottom: 3pt;
}}
.compliance-title {{
  font-size: 10pt;
  font-weight: 700;
  color: #111827;
  flex: 1;
}}
.compliance-sev {{
  font-size: 7pt;
  font-weight: 700;
  color: #D97706;
  border: 0.5pt solid #D97706;
  padding: 1pt 6pt;
  border-radius: 8pt;
  letter-spacing: 0.05em;
}}
.compliance-detail {{
  font-size: 9pt;
  color: #78350F;
  line-height: 1.6;
}}

/* Scenario table */
.scen-table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 9pt;
  margin: 8pt 0;
}}
.scen-table th {{
  text-align: left;
  padding: 7pt 10pt;
  background: #111827;
  color: #fff;
  font-size: 7.5pt;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}}
.scen-table td {{
  padding: 7pt 10pt;
  border-bottom: 0.5pt solid #E5E7EB;
}}
.scen-name {{ font-weight: 700; color: #111827; }}
.scen-val {{ font-variant-numeric: tabular-nums; text-align: right; }}

/* Dev IC Grid */
.devic-grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10pt;
  margin: 10pt 0;
}}
.devic-item {{
  padding: 9pt 12pt;
  border: 0.5pt solid #E5E7EB;
  border-radius: 2pt;
  background: #FEFEFE;
}}
.devic-label {{
  font-size: 7pt;
  font-weight: 700;
  color: #6B7280;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  margin-bottom: 3pt;
}}
.devic-value {{
  font-size: 9pt;
  color: #1F2937;
  line-height: 1.5;
}}

/* Footer note */
.confidential-note {{
  margin-top: 20pt;
  padding-top: 10pt;
  border-top: 0.3pt solid #E5E7EB;
  font-size: 7pt;
  color: #9CA3AF;
  font-style: italic;
  text-align: center;
}}
</style>
</head>
<body>

<!-- ══ COVER PAGE ══ -->
<div class="cover">
  <div>
    <div class="cover-header">Hanwha Energy USA Holdings · Investment Committee Memo</div>
  </div>

  <div class="cover-main">
    <div class="cover-tag">Confidential · Internal Use Only</div>
    <div class="cover-title">{_esc_html(proj_name)}</div>
    <div class="cover-sub">{_esc_html(pv_mwac)} MWac Solar + BESS · {_esc_html(state)} ({_esc_html(iso)}) · COD {_esc_html(cod)}</div>

    <div class="cover-verdict">{_esc_html(verdict)}</div>

    <div class="cover-stats">
      <div class="cover-stat">
        <div class="cover-stat-label">Sponsor IRR</div>
        <div class="cover-stat-value">{irr_lev_pre}</div>
      </div>
      <div class="cover-stat">
        <div class="cover-stat-label">Dev Margin</div>
        <div class="cover-stat-value">{dev_margin}</div>
      </div>
      <div class="cover-stat">
        <div class="cover-stat-label">Total CAPEX</div>
        <div class="cover-stat-value">{capex}</div>
      </div>
      <div class="cover-stat">
        <div class="cover-stat-label">WACC</div>
        <div class="cover-stat-value">{wacc_val}</div>
      </div>
    </div>
  </div>

  <div class="cover-footer">
    <span>Prepared: {_esc_html(today)}</span>
    <span>{_esc_html(data.get("prepared_by",""))}</span>
  </div>
</div>

<!-- ══ PAGE 2 — EXECUTIVE SUMMARY ══ -->
<div class="page-break">
  <h1>Executive Summary</h1>
  <div class="section-sub">투자 의견 · 핵심 논거</div>

  <h2>투자 근거 (Investment Rationale)</h2>
  <div class="thesis-box">{_esc_html(thesis) if thesis else "(AI 분석 미완료 — IC Opinion 탭에서 Run AI Analysis 실행 후 재생성)"}</div>

  <h2>Recommendation</h2>
  <div class="rec-box">{_esc_html(rec) if rec else "(AI 분석 미완료)"}</div>
</div>

<!-- ══ PAGE 3 — FINANCIAL SUMMARY ══ -->
<div class="page-break">
  <h1>Financial Summary</h1>
  <div class="section-sub">재무 지표 · 자본 구조 · 계약 조건</div>

  <h2>Returns Detail</h2>
  <div class="metrics-grid">
    <div class="metric-card metric-card-primary">
      <div class="metric-label">Sponsor IRR</div>
      <div class="metric-value">{irr_lev_pre}</div>
      <div class="metric-sub">Lev · Pre-Tax</div>
    </div>
    <div class="metric-card">
      <div class="metric-label">Sponsor IRR</div>
      <div class="metric-value">{irr_at_before}</div>
      <div class="metric-sub">A-Tax · Pre-NOL</div>
    </div>
    <div class="metric-card metric-card-secondary">
      <div class="metric-label">Sponsor IRR</div>
      <div class="metric-value">{irr_at_after}</div>
      <div class="metric-sub">A-Tax · Post-NOL</div>
    </div>
    <div class="metric-card">
      <div class="metric-label">Project IRR</div>
      <div class="metric-value">{irr_unlev}</div>
      <div class="metric-sub">Unlev · Pre-Tax</div>
    </div>
    <div class="metric-card metric-card-wacc">
      <div class="metric-label">WACC</div>
      <div class="metric-value">{wacc_val}</div>
      <div class="metric-sub">Capital Cost</div>
    </div>
  </div>

  <h2>Investment Thresholds (기준 달성)</h2>
  <div class="thr-box">
    <div class="thr-item">
      <div class="thr-label">Sponsor IRR (After-TE-Flip, Full Life · min {thr_irr}%)</div>
      <div class="thr-status">{_chk(thr_irr_ok)}</div>
      <div class="thr-gap">{thr_irr_gap}</div>
    </div>
    <div class="thr-item">
      <div class="thr-label">Dev Margin (min {thr_margin} c/Wp)</div>
      <div class="thr-status">{_chk(thr_margin_ok)}</div>
      <div class="thr-gap">{thr_margin_gap}</div>
    </div>
  </div>

  <h2>Capital Structure & Deal Terms</h2>
  <table class="fin-table">
    <thead><tr><th>Item</th><th style="text-align:right">Value</th></tr></thead>
    <tbody>
      <tr><td>Total CAPEX</td><td class="val">{capex}</td></tr>
      <tr><td>Senior Debt</td><td class="val">{debt}</td></tr>
      <tr><td>Tax Equity</td><td class="val">{te}</td></tr>
      <tr><td>Sponsor Equity</td><td class="val">{eq}</td></tr>
      <tr class="subtotal"><td>Dev Margin</td><td class="val">{dev_margin} ({margin_cwp_str})</td></tr>
      <tr><td>EBITDA Yield (Y1)</td><td class="val">{ebitda_y_str}</td></tr>
      <tr><td>PPA Price × Term</td><td class="val">${_esc_html(ppa_price)}/MWh × {_esc_html(ppa_term)}yr</td></tr>
      <tr><td>BESS Toll</td><td class="val">${_esc_html(bess_toll)}/kW-mo</td></tr>
    </tbody>
  </table>

  <h2>Scenario Analysis</h2>
  <table class="scen-table">
    <thead><tr><th>Scenario</th><th style="text-align:right">Sponsor IRR</th><th style="text-align:right">Dev Margin</th></tr></thead>
    <tbody>{scen_rows if scen_rows else '<tr><td colspan="3" style="color:#9CA3AF;text-align:center">시나리오 미실행</td></tr>'}</tbody>
  </table>
</div>

<!-- ══ PAGE 4 — RISK ASSESSMENT ══ -->
<div class="page-break">
  <h1>Risk Assessment</h1>
  <div class="section-sub">규정 준수 체크 · 프로젝트별 리스크 (의사결정에 반영되지 않음)</div>

  <h2 style="margin-top:10pt">📋 규정 준수 체크리스트 (IC 승인 전 확인 필수)</h2>
  <div class="compliance-note">고정 체크리스트 · 모든 프로젝트 공통 적용</div>
  {compliance_html if compliance_html else '<p style="color:#9CA3AF;font-size:9pt">체크리스트 없음</p>'}

  <h2 style="margin-top:18pt">🔍 프로젝트별 리스크 (AI 모니터링)</h2>
  <div class="compliance-note">정보 제공 · 경제성 판정에 영향 없음</div>
  {risks_html if risks_html else '<p style="color:#9CA3AF;font-size:9pt">AI 분석 미완료 — IC Opinion 탭에서 Run AI Analysis 실행 후 재생성</p>'}

  <div class="confidential-note">
    본 문서는 Hanwha Energy USA Holdings 내부 투자심의 목적으로만 작성되었으며, 외부 유출을 금합니다.<br>
    수치 및 가정은 {_esc_html(today)} 기준 엑셀 재무모델 및 시장 데이터를 근거로 하며, 시장 변동에 따라 달라질 수 있습니다.<br>
    경제성 판정(PROCEED/RECUT/STOP)은 Dev Margin · Sponsor IRR · Unlev IRR vs WACC 기준의 순수 경제 분석 결과이며, 규정 준수 체크리스트와 개별 리스크는 별도 관리 대상입니다.
  </div>
</div>

</body>
</html>"""
    return html

