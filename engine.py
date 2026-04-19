"""
engine.py — HWR Valuation Engine (core calculations)

Extracted from main.py in Phase 4 Step 4A refactoring.

Contains:
- _irr_robust: Newton-Raphson IRR solver with overflow protection
- _CALIB_STRUCTURAL: Neptune 재현용 보정 상수
- _apply_calibration_defaults: calibration 모드 파라미터 적용
- _calc_engine: 핵심 PF 모델 계산 엔진 (Partnership Flip + MACRS + Debt)
- _decompose_irr_difference: IRR 차이 분해 (Calibration vs Prediction)

Generated: Apr 19, 2026
"""

import numpy as np
import numpy_financial as npf

# ═══════════════════════════════════════════════════════════════
# 상수 (Constants)
# ═══════════════════════════════════════════════════════════════

# MACRS 5-year depreciation schedule (Solar PV + BESS 표준)
# 5년 MACRS: 20%, 32%, 19.2%, 11.52%, 11.52%, 5.76%
MACRS_5YR = [0.20, 0.32, 0.192, 0.1152, 0.1152, 0.0576]


def _irr_robust(cfs, guess=0.08):
    """여러 초기값으로 Newton 반복 → 양수 수렴값 반환.
    Overflow 방지: r은 [-0.99, 10.0] 범위로 클램핑."""
    import numpy as np
    def newton(g):
        r = g
        for _ in range(2000):
            # r이 너무 극단값이면 cf/(1+r)**t overflow — 안전 범위 클램핑
            if r <= -0.99: r = -0.99
            elif r >= 10.0: r = 10.0
            try:
                npv  = sum(cf/(1+r)**t for t,cf in enumerate(cfs))
                dnpv = sum(-t*cf/(1+r)**(t+1) for t,cf in enumerate(cfs))
            except (OverflowError, ZeroDivisionError):
                return None  # 발산 — 실패 신호
            if abs(dnpv) < 1e-12: break
            r_new = r - npv/dnpv
            # 한 step 이동 제한 (안정화)
            if r_new <= -0.99: r_new = -0.99
            elif r_new >= 10.0: r_new = 10.0
            if abs(r_new - r) < 1e-8: return r_new
            r = r_new
        return r
    for g in [guess, 0.01, 0.03, 0.05, 0.10, 0.15, 0.20, -0.05]:
        r = newton(g)
        if r is None: continue  # 발산 스킵
        if -0.5 < r < 5.0:  # 합리적 IRR 범위
            try:
                chk = sum(cf/(1+r)**t for t,cf in enumerate(cfs))
                if abs(chk) < 500:  # $500K 오차 허용
                    return r
            except (OverflowError, ZeroDivisionError):
                continue
    # numpy_financial fallback
    try:
        import numpy_financial as npf
        r0 = float(npf.irr(cfs))
        if not np.isnan(r0) and -0.5 < r0 < 5.0:
            return r0
    except Exception:
        pass
    return None  # 진짜 해 없음 (caller가 처리)

def _calc_engine(inputs: dict) -> dict:
    # ═══════════════════════════════════════════════════════
    # MODE 분기: Neptune Calibration vs 일반 Prediction
    # ═══════════════════════════════════════════════════════
    # 
    # 'prediction' (기본): 업계 표준 PF 가정
    #   - 99/5 Partnership Flip
    #   - 균등 Debt amortization
    #   - MACRS tax benefit 정상 Sponsor 귀속
    #   - CAPEX 전액 Y0 현금 지출
    #   - Debt/TE 100% Y0 drawdown
    # 
    # 'calibration' (Neptune 실측): Neptune 모델 재현 목적
    #   - Sculpted Debt (DSCR 기반 비율)
    #   - NOL 상쇄 (Y1-Y9 tax benefit 0)
    #   - Construction Cost 별도 (FMV와 다름)
    #   - 실측 draw ratio
    #   - 25.5/7 Partnership Flip
    #
    # 신규 프로젝트 분석 시 → 'prediction' 권장
    # Neptune 검증/재현 시 → 'calibration'
    mode = inputs.get('calibration_mode', 'prediction')
    is_calibration = (mode == 'calibration')

    pv_mwac   = inputs.get('pv_mwac', 199)
    pv_mwdc   = inputs.get('pv_mwdc') or pv_mwac * inputs.get('dc_ac_ratio', 1.34)
    bess_mw   = inputs.get('bess_mw', 199)
    bess_mwh  = inputs.get('bess_mwh', 796)
    life      = int(inputs.get('life', 35))

    # CAPEX 구성 ───────────────────────────────────────────────
    module_cwp   = inputs.get('module_cwp', 31.5)        # c/Wdc
    pv_bos_cwp   = inputs.get('bos_cwp', 42.88)          # c/Wdc (PV BOS+Construction)
    ess_per_kwh  = inputs.get('ess_per_kwh', 234.5)      # $/kWh (BESS Equipment)
    bess_bos_per_kwh = inputs.get('bess_bos_per_kwh', 130.0)  # $/kWh (BESS BOS — NEW)
    epc_cont_pct = inputs.get('epc_cont_pct', 8.0)       # %
    owner_pct    = inputs.get('owner_pct', 3.0)          # %
    softcost_pct = inputs.get('softcost_pct', 5.0)
    intercon_m   = inputs.get('intercon_m', 22.5)        # $M (Sub+Gentie+GSU+Trans avg Neptune)
    dev_cost_m   = inputs.get('dev_cost_m', 20.0)        # $M
    capex_etc    = inputs.get('capex_etc', 0)

    # 하드웨어/BOS 비용
    pv_module    = pv_mwdc*1000*module_cwp/100           # $K
    pv_bos       = pv_mwdc*1000*pv_bos_cwp/100           # $K
    ess_equip    = bess_mwh*ess_per_kwh                  # $K
    bess_bos     = bess_mwh*bess_bos_per_kwh             # $K (NEW)
    epc_base     = pv_module + pv_bos + ess_equip + bess_bos
    epc_total    = epc_base * (1 + epc_cont_pct/100)
    pre_capex    = (epc_total*(1+owner_pct/100+softcost_pct/100)
                    + intercon_m*1000 + dev_cost_m*1000 + capex_etc*1000)
    int_rate     = inputs.get('int_rate', 5.5) / 100
    debt_ratio   = inputs.get('debt_ratio', 47.6) / 100
    base_capex   = pre_capex * (1 + debt_ratio*int_rate*0.75 + 0.012)
    total_capex  = float(inputs['capex_total_override'])*1000 if inputs.get('capex_total_override') else base_capex

    # Dev Margin: c/Wac × (PV + BESS) MW × 10 (Neptune 표준 공식)
    dev_margin_cwac_v = inputs.get('dev_margin_cwac', 10.0)
    dev_margin   = dev_margin_cwac_v * (pv_mwac + bess_mw) * 10  # $K
    epc_margin   = epc_base * inputs.get('epc_margin_pct', 7.95)/100
    total_margin = dev_margin + epc_margin

    loan_term  = int(inputs.get('loan_term', 28 if is_calibration else 18))
    debt       = total_capex * debt_ratio
    ann_ds     = float(npf.pmt(int_rate, loan_term, -debt)) if debt > 0 else 0

    # Credit System ─────────────────────────────────────────────
    # Mode: ITC (capital credit) vs PTC (production credit)
    # ITC는 PV와 BESS를 분리해서 적용 가능 (Neptune: PV 0%, BESS 30%)
    # PTC는 PV generation에만 적용 (BESS는 ITC만 가능)
    credit_mode  = inputs.get('credit_mode', 'ITC').upper()  # 'ITC' or 'PTC'
    itc_elig     = inputs.get('itc_elig', 97) / 100

    # PV/BESS ITC 분리 (inputs 없으면 레거시 단일 credit_val 사용)
    pv_itc_rate  = inputs.get('pv_itc_rate')
    bess_itc_rate = inputs.get('bess_itc_rate')
    if pv_itc_rate is None and bess_itc_rate is None:
        # 레거시: credit_val을 전체에 적용
        legacy = inputs.get('itc_rate') or inputs.get('credit_val', 30)
        pv_itc_rate  = legacy
        bess_itc_rate = legacy
    pv_itc_rate  = (pv_itc_rate or 0) / 100
    bess_itc_rate = (bess_itc_rate or 0) / 100

    # PTC Rate ($/kWh) — production-based
    ptc_rate_per_kwh = inputs.get('ptc_rate_per_kwh') or inputs.get('credit_val')
    if credit_mode == 'PTC':
        # credit_val이 30 같이 크면 잘못 입력된 것 — 0.03으로 자동 보정
        if ptc_rate_per_kwh and ptc_rate_per_kwh > 1:
            ptc_rate_per_kwh = ptc_rate_per_kwh / 100.0
    ptc_rate_per_kwh = ptc_rate_per_kwh or 0.0

    # PV CAPEX과 BESS CAPEX 분리 (ITC basis용)
    pv_capex_share   = (pv_module + pv_bos) / epc_base if epc_base > 0 else 0.5
    bess_capex_share = (ess_equip + bess_bos) / epc_base if epc_base > 0 else 0.5

    if credit_mode == 'ITC':
        # 가중 평균 ITC rate (CAPEX 비중 기준)
        effective_itc_rate = pv_itc_rate * pv_capex_share + bess_itc_rate * bess_capex_share
    else:
        # PTC 모드: ITC 적용 안 함 (BESS도 ITC 받을 수 있지만 단순화 위해 일단 미적용)
        # BESS ITC는 PTC와 병행 가능하므로 bess_itc_rate 살아있으면 그대로
        effective_itc_rate = bess_itc_rate * bess_capex_share  # BESS만 ITC

    # TE Flip
    _fy_raw = inputs.get('flip_yield', 8.75)
    if _fy_raw > 50: _fy_raw = _fy_raw / 100
    flip_yield = _fy_raw / 100
    flip_term  = int(inputs.get('flip_term', 7))
    te_mult    = inputs.get('te_mult', 1.115)
    yield_adj  = 1 / (1 + (flip_yield - 0.0875) * 8)

    # ── TE Invest 산정 + Sponsor Equity 최소선 확보 ──────────────────
    te_theoretical = total_capex * itc_elig * effective_itc_rate * te_mult * yield_adj

    min_sponsor_eq_pct = inputs.get('min_sponsor_eq_pct', 10.0) / 100
    max_te_invest = total_capex - debt - total_capex * min_sponsor_eq_pct

    te_invest = max(0, min(te_theoretical, max_te_invest))

    # Capital Stack Override: te_ratio / sponsor_eq_ratio 명시하면 해당 비율 사용
    # (예: Neptune은 Debt 47.6%, TE 32.5%, Eq 19.8% — ITC 기반 공식으로 못 맞춤)
    te_ratio_override = inputs.get('te_ratio_override')
    if te_ratio_override is not None:
        te_invest = total_capex * te_ratio_override / 100
        te_invest = max(0, min(te_invest, max_te_invest))

    sponsor_eq = total_capex - debt - te_invest
    effective_eq = sponsor_eq * (1 - int_rate * 0.75)

    # ═══ Sponsor Y0 Cash Outflow ═══
    # 
    # Prediction mode (기본, 신규 프로젝트):
    #   Sponsor Y0 = Sponsor Equity (effective_eq) 전액 Y0 지출
    #   Debt/TE는 CAPEX 전액 Y0 drawdown
    #
    # Calibration mode (Neptune 재현):
    #   Sponsor Y0 = Construction Cost + Txn + CapInt - Debt Drawdown - TE Proceeds
    #   FMV와 구분된 실제 Y0 현금 흐름 추적
    
    # ═══ Sponsor Y0 Cash Outflow ═══
    # 
    # 모든 사업 공통 구조:
    #   CAPEX (FMV) = Construction Cost + Dev Margin + EPC Margin
    #   Debt / TE sizing은 FMV 기준 (시장 관행)
    #   Sponsor Y0 현금 = Construction + Txn + CapInt - Debt draw - TE proceeds
    #
    # Dev Margin = dev_margin_cwac(c/Wac) × (PV MWac + BESS MW) × 10
    #   Neptune 표준: 10 c/Wac
    #   모든 HWR 프로젝트에 공통 적용
    
    # Dev Margin & EPC Margin
    dev_margin_cwac = inputs.get('dev_margin_cwac', 10.0)  # c/Wac 기본 10
    total_mw_ac = pv_mwac + bess_mw
    dev_margin_k = dev_margin_cwac * total_mw_ac * 10  # $K
    
    # EPC Margin: CAPEX 대비 % (Neptune 기본 7.95%)
    epc_margin_pct_calc = inputs.get('epc_margin_pct', 7.95) / 100
    epc_margin_k = total_capex * epc_margin_pct_calc
    
    # Construction Cost = FMV CAPEX - Dev Margin - EPC Margin
    # (모든 모드 공통 로직)
    construction_cost_override = inputs.get('construction_cost_m')
    if construction_cost_override:
        construction_cost = construction_cost_override * 1000  # $M → $K (수동 override)
    else:
        construction_cost = total_capex - dev_margin_k - epc_margin_k
    
    # Transaction costs & Capitalized Interest
    # 기본값: CAPEX 대비 작은 비율 (Neptune: Txn 1.27%, CapInt 1.71%)
    txn_costs = inputs.get('txn_costs_m')
    if txn_costs is not None:
        txn_costs = txn_costs * 1000  # $M → $K
    else:
        txn_costs = total_capex * 0.0127  # Neptune 비율
    
    cap_interest = inputs.get('cap_interest_m')
    if cap_interest is not None:
        cap_interest = cap_interest * 1000
    else:
        cap_interest = total_capex * 0.0171  # Neptune 비율
    
    # Debt / TE 현금 drawdown 비율
    # Calibration (Neptune): 77.5% / 93.5%
    # Prediction (표준): 100% / 100% (Y0 전액 drawdown)
    if is_calibration:
        debt_drawdown_ratio = inputs.get('debt_drawdown_ratio', 0.775)
        te_proceeds_ratio = inputs.get('te_proceeds_ratio', 0.935)
    else:
        debt_drawdown_ratio = inputs.get('debt_drawdown_ratio', 1.0)
        te_proceeds_ratio = inputs.get('te_proceeds_ratio', 1.0)
    
    debt_drawdown = debt * debt_drawdown_ratio
    te_proceeds = te_invest * te_proceeds_ratio
    
    # Sponsor Y0 실제 현금 outflow
    sponsor_y0_cash = construction_cost + txn_costs + cap_interest - debt_drawdown - te_proceeds
    
    # Flip year event cash (Neptune Y10 pattern, 일반 프로젝트는 0)
    flip_event_cf = inputs.get('flip_event_cf', 0)

    # 레거시 호환
    itc_rate = effective_itc_rate
    net_sponsor_y0 = sponsor_y0_cash
    
    # Dev Margin 최종 값 저장 (output에 노출)
    computed_dev_margin_k = dev_margin_k

    # MACRS depreciation
    tax_rate    = inputs.get('tax_rate', 21) / 100
    macrs_basis = total_capex * itc_elig * (1 - itc_rate/2)
    depr_sched  = {i+1: macrs_basis*r for i,r in enumerate(MACRS_5YR)}
    
    # Depreciation share — Partnership Flip 구조에 따라 다름
    # Prediction mode 기본값:
    #   Pre-flip: TE가 depr 99% → Sponsor depr_share = 1%
    #   Post-flip: Sponsor 95% → Sponsor depr_share = 95%
    # Calibration mode (Neptune): 0.7721 (실측)
    # 사용자 override 가능
    if is_calibration:
        depr_share_pre = inputs.get('depr_share', 0.7721)
        depr_share_post = inputs.get('depr_share_post', depr_share_pre)
    else:
        depr_share_pre = inputs.get('depr_share_pre', 0.01)  # TE 99%
        depr_share_post = inputs.get('depr_share_post', 0.95)  # Sponsor 95%
    # 레거시 호환: depr_share만 override 주면 pre/post 모두 동일
    if 'depr_share' in inputs and not is_calibration:
        depr_share_pre = depr_share_post = inputs.get('depr_share')
    depr_share = depr_share_pre  # backward compat

    # Cash allocation
    # Prediction mode (기본): 표준 Partnership Flip 99%/5%
    # Calibration mode (Neptune): 실측 25.5%/7%
    if is_calibration:
        default_pre_flip = 25.5
        default_post_flip = 7
    else:
        default_pre_flip = 99
        default_post_flip = 5
    pre_flip_cash_te  = inputs.get('pre_flip_cash_te', default_pre_flip) / 100
    post_flip_cash_te = inputs.get('post_flip_cash_te', default_post_flip) / 100

    # Revenue
    cf_pct       = inputs.get('cf_pct', 21.24)
    net_prod_yr1 = inputs.get('net_prod_yr1', None)
    ann_prod_yr1 = float(net_prod_yr1) if net_prod_yr1 else pv_mwac*cf_pct/100*8760
    ppa_price   = inputs.get('ppa_price', 68.82)
    ppa_term    = int(inputs.get('ppa_term', 25))
    ppa_esc     = inputs.get('ppa_esc', 0) / 100
    # bess_toll: CF_Annual Y1 실제값 우선, 없으면 Summary 파싱값
    bess_toll   = inputs.get('bess_toll_y1_effective') or inputs.get('bess_toll', 14.50)
    bess_toll_t = int(inputs.get('bess_toll_term', 20))
    bess_toll_esc = inputs.get('bess_toll_esc', 0) / 100  # Toll escalation (%)
    # BESS Toll 월 적용: 표준 12개월, Neptune calibration은 12.72 (pro-rated)
    bess_months_per_yr = inputs.get('bess_months_per_yr', 12.72 if is_calibration else 12.0)
    merch_ppa   = inputs.get('merchant_ppa', 45.0)
    merch_esc   = inputs.get('merchant_esc', 3.0) / 100
    degradation = inputs.get('degradation', 0.0064)
    # Neptune의 CF%는 availability 내재 → Calibration 모드에서 1.0
    # 일반 프로젝트는 별도 availability factor 적용
    avail_1     = inputs.get('availability_yr1', 1.0 if is_calibration else 0.977)
    avail_2     = inputs.get('availability_yr2', 1.0 if is_calibration else 0.982)

    # OPEX
    pv_om=inputs.get('pv_om',4.5); pv_om_nc=inputs.get('pv_om_nc',1.0)
    pv_aux=inputs.get('pv_aux',1.56); bess_om=inputs.get('bess_om',8.64)
    bess_om_nc=inputs.get('bess_om_nc',1.0); bess_aux=inputs.get('bess_aux',3.84)
    ins_pv=inputs.get('insurance_pv',10.57); ins_bess=inputs.get('insurance_bess',5.05)
    asset_mgmt=inputs.get('asset_mgmt',210); prop_tax=inputs.get('prop_tax_yr1',3162)
    land_rent=inputs.get('land_rent_yr1',437); opex_etc=inputs.get('opex_etc', 0.56 if is_calibration else 0)
    opex_esc=inputs.get('opex_esc',2.0)/100

    # Augmentation (Neptune: Y4, Y8, Y14 × $22.5M / 표준: Y4, Y8, Y12)
    default_aug_y3 = 14 if is_calibration else 12
    aug_price=inputs.get('aug_price',150); aug_mwh_pct=inputs.get('aug_mwh_pct',18.8)
    aug_mwh_ea=bess_mwh*aug_mwh_pct/100
    aug_years=[int(y) for y in [inputs.get('aug_y1',4),inputs.get('aug_y2',8),inputs.get('aug_y3',default_aug_y3)] if y and int(y)>0]
    aug_cost_ea=aug_mwh_ea*aug_price

    # Full 35-year CF schedule
    # Sponsor Y0 = 실제 현금 outflow (Neptune R25 방식)
    # Unlev Y0 = -CAPEX + ITC tax credit Y0 benefit
    #   Neptune R51 Y0 = -385,453 → CAPEX -639,855에서 +254,402 TE proceeds 반영 + 추가 조정
    #   단순화: Unlev는 project 전체 관점이므로 full CAPEX
    effective_itc_value = total_capex * itc_elig * effective_itc_rate
    # Unlev Y0 (Neptune Row 26 방식): -Construction + TE proceeds
    # Neptune Row 26 Y0 = -385,453 ≈ -639,855 + 254,405 (Construction - TE proceeds)
    # txn + cap_interest는 Partnership 관점에서 Y0 cash flow 이전의 financing 비용이라
    # Unlevered IRR 계산에는 포함 안 함 (엑셀 실측 일치)
    unlev_y0 = -construction_cost + te_proceeds

    cashflows=[-effective_eq]; unlev_cfs=[unlev_y0]
    sponsor_cfs=[-sponsor_y0_cash]; pretax_cfs=[-sponsor_y0_cash]
    debt_bal=debt; detail=[]; ebitda_yr1=None

    for yr in range(1, life+1):
        avail = avail_1 if yr==1 else avail_2
        prod  = ann_prod_yr1 * avail * ((1-degradation)**(yr-1))

        # CF_Annual parsed schedule 우선 사용 (실제 Neptune 모델값)
        pv_sched   = inputs.get('pv_rev_schedule', [])
        bess_sched = inputs.get('bess_rev_schedule', [])
        merch_sched= inputs.get('merch_rev_schedule', [])

        if pv_sched and yr-1 < len(pv_sched):
            pv_rev = pv_sched[yr-1]
        elif yr <= ppa_term:
            pv_rev = prod*ppa_price*((1+ppa_esc)**(yr-1))/1000
        else:
            # Merchant 기간: escalation 기산점은 merchant 시작 연도
            # Neptune: Y26 $61/MWh, Y35 $73/MWh → merchant_esc가 Y26부터 적용됨
            merch_yr = yr - ppa_term  # merchant 경과년수 (Y26 → 1)
            pv_rev = prod*merch_ppa*((1+merch_esc)**(merch_yr-1))/1000

        if bess_sched and yr-1 < len(bess_sched):
            bess_rev = bess_sched[yr-1]
        else:
            bess_rev = bess_mw*1000*bess_toll*((1+bess_toll_esc)**(yr-1))*bess_months_per_yr/1000 if yr<=bess_toll_t else 0

        if merch_sched and yr-1 < len(merch_sched) and merch_sched[yr-1] > 0:
            pv_rev = merch_sched[yr-1]  # merchant 기간은 merch_sched 우선

        total_rev = pv_rev + bess_rev

        esc=(1+opex_esc)**(yr-1); prop_esc=max(0.35,1-0.025*(yr-1))
        opex=(pv_mwdc*1000*pv_om/1000*esc + pv_mwac*1000*pv_om_nc/1000*esc +
              pv_mwac*1000*pv_aux/1000*esc + bess_mw*1000*bess_om/1000*esc +
              bess_mw*1000*bess_om_nc/1000*esc + bess_mw*1000*bess_aux/1000*esc +
              pv_mwac*1000*ins_pv/1000*esc + bess_mw*1000*ins_bess/1000*esc +
              asset_mgmt*esc + prop_tax*prop_esc + land_rent*esc + opex_etc*1000*esc)

        ebitda = total_rev - opex
        if yr==1: ebitda_yr1=ebitda
        aug_c = aug_cost_ea if yr in aug_years else 0

        # Partnership CF = EBITDA - Aug (Neptune R19 방식)
        partnership_cf = ebitda - aug_c

        # Debt Service: Neptune sculpted schedule (DSCR 기반)
        # Calibration mode 기본 True, Prediction mode 기본 False (균등 amortization)
        use_sculpted = inputs.get('use_sculpted_debt', is_calibration)
        if use_sculpted and yr <= 28:
            # Neptune Case 2 Debt/Partnership 비율 (실측)
            neptune_debt_ratios = {
                1: 0.602, 2: 0.604, 3: 0.606, 4: 0.570, 5: 0.800,
                6: 0.663, 7: 0.663, 8: 0.677, 9: 0.663, 10: 0.263,
                11: 0.607, 12: 0.700, 13: 0.701, 14: 0.711, 15: 0.701,
                16: 0.701, 17: 0.701, 18: 0.701, 19: 0.702, 20: 0.700,
                21: 0.905, 22: 0.914, 23: 0.923, 24: 0.933, 25: 0.938,
                26: 0.504, 27: 0.504, 28: 0.503,
            }
            ratio = neptune_debt_ratios.get(yr, 0)
            ds = partnership_cf * ratio
            debt_bal = max(0, debt_bal - max(0, ds - debt_bal * int_rate))
        elif yr <= loan_term and debt_bal > 0:
            # 기존 amortization (non-Neptune)
            int_p = debt_bal * int_rate
            prin = ann_ds - int_p
            ds = ann_ds
            debt_bal = max(0, debt_bal - prin)
        else:
            ds = 0

        # MACRS depreciation tax benefit
        # Neptune NOL 이월 효과로 Y1-Y9 tax benefit이 Partnership tax와 상쇄 (R42 + R33 ≈ 0)
        # 따라서 Sponsor aftertax CF ≈ Sponsor pretax CF (MACRS 따로 더하지 않음)
        # ITC는 이미 Y0 Sponsor cash outflow에 반영됨 (Construction Cost 기반)
        depr = depr_sched.get(yr, 0)
        # NOL 상쇄 로직
        # Calibration mode 기본 True (Neptune처럼 Y1-Y9 tax benefit 상쇄)
        # Prediction mode 기본 False (MACRS tax benefit 정상 반영)
        use_nol_offset = inputs.get('use_nol_offset', is_calibration)
        # flip year 기반 Sponsor depr share (pre/post flip)
        current_depr_share = depr_share_pre if yr <= flip_term else depr_share_post
        if use_nol_offset:
            s_tax = 0  # NOL로 상쇄
        else:
            s_tax = depr * tax_rate * current_depr_share

        # PTC (Production Tax Credit) — PV만, COD 후 10년간
        ptc_benefit = 0
        if credit_mode == 'PTC' and yr <= 10:
            ptc_benefit = prod * ptc_rate_per_kwh  # MWh × $/kWh → $K
            # PTC도 flip 기반 배분
            s_tax += ptc_benefit * current_depr_share

        op_cf = ebitda - ds - aug_c
        # ── Sponsor CF: Neptune Returns Row 25 방식 ──
        # Row 25 = Row 19 (Partnership) - Row 21 (Debt net) - Row 22 (TE dist) + Row 23 (Pay-Go)
        # TE distribution은 Partnership CF에 비례 (Debt와 독립)
        # 이전 엔진은 op_cf × (1-TE%) 로 계산했으나, 이는 Debt 영향을 TE 배분에 섞음 → 교정
        te_cash_pct = pre_flip_cash_te if yr <= flip_term else post_flip_cash_te
        te_dist_cash = partnership_cf * te_cash_pct
        s_cf = partnership_cf - ds - te_dist_cash + s_tax

        # Flip Year event: TE buyout 직후 Sponsor 일시 대금 수령
        if yr == flip_term + 1 and flip_event_cf > 0:
            s_cf += flip_event_cf

        s_cf_pretax = partnership_cf - ds - te_dist_cash

        # Unlevered aftertax CF (Neptune R51 구조):
        # = Partnership CF - TE distribution (Debt 제외한 Sponsor+TE 관점)
        # TE dist 비율은 위 Sponsor CF와 동일한 te_cash_pct 사용 (일관성)
        te_dist_unlev = partnership_cf * te_cash_pct
        unlev_aftertax_cf = partnership_cf - te_dist_unlev + (ptc_benefit if credit_mode == 'PTC' and yr <= 10 else 0)

        cashflows.append(op_cf); unlev_cfs.append(unlev_aftertax_cf); sponsor_cfs.append(s_cf); pretax_cfs.append(s_cf_pretax)
        if yr<=10:
            detail.append({'yr':yr,'rev':round(total_rev,0),'opex':round(opex,0),
                'ebitda':round(ebitda,0),'ds':round(ds,0),'aug':round(aug_c,0),
                'depr':round(depr,0),'s_cf':round(s_cf,0),'ptc':round(ptc_benefit,0)})

    lirr = _irr_robust(pretax_cfs, guess=0.10)   # Sponsor pretax levered (Neptune Row 26 ~10%)
    uirr = _irr_robust(unlev_cfs, guess=0.05)    # Asset-level unlevered (Neptune Row 27 ~8%)
    sirr = _irr_robust(sponsor_cfs, guess=0.10)  # Sponsor after-tax w/ MACRS (Full Life)
    try:
        sirr_c = float(npf.irr(sponsor_cfs[:ppa_term+1]))
        if np.isnan(sirr_c): sirr_c = None
    except Exception:
        sirr_c = None
    ebitda_yield = ebitda_yr1/total_capex*100 if total_capex else 0

    # ── NPV 계산 (Hurdle 기준 할인) ────────────────────────────────
    # Sponsor NPV: Hurdle IRR(예: 10%)로 할인 — 매수자 관점 가치
    # Project NPV: WACC로 할인 — 프로젝트 자체 가치
    hurdle_sponsor = inputs.get('hurdle_sponsor_irr', 9.0) / 100  # Default 9%
    # WACC 계산 (approximation): tax-adjusted weighted cost
    wacc_debt_cost = int_rate * (1 - tax_rate)  # after-tax
    wacc_te_cost = 0.07   # TE 조달 비용 (typical)
    wacc_eq_cost = 0.11   # Sponsor eq 비용 (typical)
    debt_w = debt / total_capex if total_capex else 0
    te_w = te_invest / total_capex if total_capex else 0
    eq_w = sponsor_eq / total_capex if total_capex else 0
    wacc = (debt_w * wacc_debt_cost) + (te_w * wacc_te_cost) + (eq_w * wacc_eq_cost)
    if wacc <= 0 or wacc > 0.5: wacc = 0.072  # fallback

    def _npv(cfs, rate):
        try:
            return float(npf.npv(rate, cfs))
        except Exception:
            return None

    sponsor_npv = _npv(sponsor_cfs, hurdle_sponsor)
    project_npv = _npv(unlev_cfs, wacc)
    # ───────────────────────────────────────────────────────────────

    return {
        'capex_total':   round(total_capex,0),
        'epc_base':      round(epc_base,0),
        'pv_module':     round(pv_module,0),
        'pv_bos':        round(pv_bos,0),
        'bess_equip':    round(ess_equip,0),
        'bess_bos':      round(bess_bos,0),
        'debt':          round(debt,0),
        'equity':        round(sponsor_eq+te_invest,0),
        'te_invest':     round(te_invest,0),
        'sponsor_equity':round(sponsor_eq,0),
        'dev_margin':    round(dev_margin,0),
        'epc_margin':    round(epc_margin,0),
        'total_margin':  round(total_margin,0),
        'levered_irr':   round(lirr,6) if (lirr is not None and not np.isnan(lirr)) else None,
        'unlevered_irr': round(uirr,6) if (uirr is not None and not np.isnan(uirr)) else None,
        'sponsor_irr':   round(sirr,6) if (sirr is not None and not np.isnan(sirr)) else None,
        'sponsor_irr_contract': round(sirr_c,6) if (sirr_c is not None and not np.isnan(sirr_c)) else None,
        'sponsor_npv':   round(sponsor_npv,0) if sponsor_npv is not None else None,
        'project_npv':   round(project_npv,0) if project_npv is not None else None,
        'wacc':          round(wacc,6),
        'hurdle_sponsor_irr_used': round(hurdle_sponsor,6),
        'ebitda_yield':  round(ebitda_yield,2),
        'aug_cost_ea':   round(aug_cost_ea,0),
        'life_yrs':      life,
        'credit_mode':   credit_mode,
        'pv_itc_rate':   round(pv_itc_rate*100, 2),
        'bess_itc_rate': round(bess_itc_rate*100, 2),
        'ptc_rate':      round(ptc_rate_per_kwh, 4),
        'annual_detail': detail,
        'cashflows':     [round(x,0) for x in cashflows[:36]],
    }

# 구조적 Neptune 파라미터 — calibration 모드에서 항상 이 값 사용
# (사이드바의 prediction default를 덮어쓴다; 예: loan_term 18 → 28)
_CALIB_STRUCTURAL = {
    'loan_term': 28,
    'aug_y3': 14,
    'bess_months_per_yr': 12.72,
    'opex_etc': 0.56,
    'construction_cost_m': 639.855,
    'txn_costs_m': 10.6,
    'cap_interest_m': 14.3,
    'debt_drawdown_ratio': 0.775,
    'te_proceeds_ratio': 0.935,
    # Neptune Returns 시트 Row 22 실측: TE dist ≈ 9.2% of Partnership CF (Y1-9)
    # Y10에서 5%로 내려감 (Flip effective Y10) → flip_term = 9
    'pre_flip_cash_te': 9.2,
    'post_flip_cash_te': 5.0,
    'flip_term': 9,
    'depr_share': 0.7721,
    'use_nol_offset': True,
    'use_sculpted_debt': True,
    'flip_event_cf': 0,
}
# 사이드바에서 오기도 하는 파라미터 — 없을 때만 Neptune 값으로 채움
_CALIB_FILL_IF_MISSING = {
    'availability_yr1': 1.0,
    'availability_yr2': 1.0,
    'capex_total_override': 836.7,
    'te_ratio_override': 32.52,
    'flip_yield': 8.75,
}

def _apply_calibration_defaults(inputs: dict) -> dict:
    """calibration_mode='calibration'일 때 Neptune 구조적 파라미터 자동 주입.
    
    주의: _calc_engine에 직접 넣지 않는다 — _decompose_irr_difference가
    step별로 실험적 param 변경 (예: pre_flip_cash_te 25.5→99) 할 때
    auto-merge가 덮어쓰면 decompose가 깨진다.
    따라서 endpoint 레벨에서만 호출한다.
    """
    if inputs.get('calibration_mode') != 'calibration':
        return inputs
    merged = dict(inputs)
    # 구조적 파라미터는 Neptune 값 강제
    for k, v in _CALIB_STRUCTURAL.items():
        merged[k] = v
    # 나머지는 fill-if-missing
    for k, v in _CALIB_FILL_IF_MISSING.items():
        if k not in merged or merged[k] is None:
            merged[k] = v
    return merged

# ══════════════════════════════════════════════════════════════
# IRR 차이 분해 (Calibration vs Prediction)
# ══════════════════════════════════════════════════════════════
def _decompose_irr_difference(inputs_base: dict) -> dict:
    """Calibration → Prediction 전환 시 각 요인이 IRR에 미치는 기여도 분해
    
    방법: 순차적 ON/OFF
      1) Full Calibration IRR (starting point)
      2) 각 Neptune-specific 파라미터를 하나씩 '해제' (Prediction 값으로)
      3) 각 단계의 IRR 변화량 = 해당 요인의 기여도
      4) 최종 = Prediction IRR
    
    4개 주요 요인:
      - NOL 상쇄 (use_nol_offset)
      - Sculpted Debt (use_sculpted_debt)
      - Partnership Flip 25.5/7 vs 99/5
      - Y0 현금 구조 (Construction < FMV)
    
    Returns:
        {
            'calib_irr': 11.14,
            'predict_irr': 6.17,
            'total_delta': 4.97,
            'factors': [
                {'name': 'NOL 상쇄', 'delta_pp': 3.2, 'from': '...', 'to': '...'},
                ...
            ]
        }
    """
    def _get_irr(inp):
        """Full Life Sponsor IRR (%)"""
        try:
            r = _calc_engine(inp)
            v = r.get('sponsor_irr')
            return (v * 100) if v is not None else 0.0
        except:
            return 0.0
    
    # Start: full calibration
    base = dict(inputs_base)
    base['calibration_mode'] = 'calibration'
    calib_irr = _get_irr(base)
    
    # End point: full prediction
    predict = dict(inputs_base)
    predict['calibration_mode'] = 'prediction'
    # prediction에서는 calibration 전용 파라미터 제거
    for k in ['construction_cost_m', 'txn_costs_m', 'cap_interest_m',
              'debt_drawdown_ratio', 'te_proceeds_ratio',
              'pre_flip_cash_te', 'post_flip_cash_te',
              'depr_share', 'use_nol_offset', 'use_sculpted_debt']:
        predict.pop(k, None)
    predict_irr = _get_irr(predict)
    
    total_delta = predict_irr - calib_irr  # 보통 음수 (Prediction이 낮음)
    
    factors = []
    current = dict(base)  # Calibration 상태에서 시작
    current_irr = calib_irr
    
    # 순서 중요: 영향 큰 구조적 요인부터 해제 (현실적 기여도 계산)
    # 1. Partnership Flip (가장 큰 구조 차이)
    # 2. Y0 현금 구조
    # 3. Sculpted Debt
    # 4. NOL 상쇄 (마지막, 세금 효과)
    
    # ─── Factor 1: Partnership Flip 25.5/7 → 99/5 ───
    step1 = dict(current)
    step1['pre_flip_cash_te'] = 99
    step1['post_flip_cash_te'] = 5
    step1['depr_share_pre'] = 0.01
    step1['depr_share_post'] = 0.95
    step1.pop('depr_share', None)
    irr1 = _get_irr(step1)
    delta1 = irr1 - current_irr
    factors.append({
        'name_ko': 'Partnership Flip 구조 (25/7 → 99/5)',
        'name_en': 'Partnership Flip Structure (25/7 → 99/5)',
        'delta_pp': round(delta1, 2),
        'from_calib': '25.5/7 (Neptune Pay-Go 추정)',
        'to_predict': '99/5 (표준 Yield-Based Flip)',
        'explain_ko': 'Neptune은 pre-flip cash를 TE 25.5% / Sponsor 74.5%로 배분 (Pay-Go 또는 hybrid 구조 추정). Prediction은 표준 99/5 flip으로 pre-flip Sponsor 현금이 1%로 줄어듦.',
        'explain_en': 'Neptune allocates pre-flip cash as TE 25.5% / Sponsor 74.5% (likely Pay-Go or hybrid). Prediction uses standard 99/5 flip, reducing pre-flip Sponsor cash to just 1%.',
        'excel_hint_ko': 'Excel Partnership Flip 탭에서 pre-flip cash split 확인. 표준 99/1 아니면 Pay-Go 구조인지 또는 별도 hybrid 로직인지 문서화 필요.',
        'excel_hint_en': 'Check Partnership Flip tab for pre-flip cash split. If not standard 99/1, document whether Pay-Go or hybrid.',
    })
    current = step1
    current_irr = irr1
    
    # ─── Factor 2: Y0 현금 구조 (Construction ≠ FMV) ───
    step2 = dict(current)
    step2['calibration_mode'] = 'prediction'
    for k in ['construction_cost_m', 'txn_costs_m', 'cap_interest_m',
              'debt_drawdown_ratio', 'te_proceeds_ratio']:
        step2.pop(k, None)
    # 단, NOL과 Debt는 유지 (뒤에서 순차 해제)
    step2['use_nol_offset'] = current.get('use_nol_offset', True)
    step2['use_sculpted_debt'] = current.get('use_sculpted_debt', True)
    irr2 = _get_irr(step2)
    delta2 = irr2 - current_irr
    factors.append({
        'name_ko': 'Y0 현금 구조 (Construction ≠ FMV)',
        'name_en': 'Y0 Cash Structure (Construction ≠ FMV)',
        'delta_pp': round(delta2, 2),
        'from_calib': 'Construction + Txn + CapInt - Debt draw - TE proceeds',
        'to_predict': 'Sponsor Equity 전액 Y0 지출',
        'explain_ko': 'Neptune은 Y0에 Construction Cost $640M (FMV $837M 아님) + Txn Cost $10.6M + Cap Interest $14.3M 지출, Debt 77.5% / TE 93.5%만 drawdown. 나머지는 후속 기간에 drawdown. Prediction은 전액 Y0.',
        'explain_en': 'Neptune Y0 uses Construction Cost $640M (not FMV $837M) + Txn $10.6M + Cap Interest $14.3M, with Debt 77.5% / TE 93.5% drawn. Rest drawn later. Prediction uses full drawdown at Y0.',
        'excel_hint_ko': 'Excel Sources & Uses 탭에서 Y0 Debt/TE drawdown 비율 확인. 전체 Debt 대비 construction 기간 drawdown 비율이 77.5%인지 검증.',
        'excel_hint_en': 'Check Sources & Uses tab for Y0 Debt/TE drawdown ratios. Verify if construction-period drawdown is 77.5% of total Debt.',
    })
    current = step2
    current_irr = irr2
    
    # ─── Factor 3: Sculpted Debt 해제 ───
    step3 = dict(current)
    step3['use_sculpted_debt'] = False
    irr3 = _get_irr(step3)
    delta3 = irr3 - current_irr
    factors.append({
        'name_ko': 'Sculpted Debt (DSCR 기반)',
        'name_en': 'Sculpted Debt (DSCR-based)',
        'delta_pp': round(delta3, 2),
        'from_calib': 'DSCR 1.30 맞춤형 상환',
        'to_predict': '균등 amortization',
        'explain_ko': 'Neptune은 각 연도 DSCR 1.30 맞추기 위해 상환 금액을 동적으로 조정 (Sculpted). Prediction은 균등 상환으로 초기 DSCR이 낮고 후반 높음.',
        'explain_en': 'Neptune dynamically adjusts principal to maintain DSCR 1.30 (Sculpted). Prediction uses level amortization—lower DSCR upfront, higher later.',
        'excel_hint_ko': 'Excel Debt 탭 상환 스케줄이 DSCR 기반 sculpted인지 확인. R161~R180 부근의 IF(DSCR...) 수식.',
        'excel_hint_en': 'Verify Debt tab amortization schedule is DSCR-based sculpted. Check IF(DSCR...) formulas near R161-R180.',
    })
    current = step3
    current_irr = irr3
    
    # ─── Factor 4: NOL 상쇄 해제 ───
    step4 = dict(current)
    step4['use_nol_offset'] = False
    irr4 = _get_irr(step4)
    delta4 = irr4 - current_irr
    factors.append({
        'name_ko': 'NOL 상쇄 (Y1~Y9 Tax 상쇄)',
        'name_en': 'NOL Offset (Y1~Y9 Tax offset)',
        'delta_pp': round(delta4, 2),
        'from_calib': 'Y1-Y9 Sponsor tax = $0',
        'to_predict': 'Sponsor tax = MACRS × share',
        'explain_ko': 'Neptune은 NOL 이월로 Y1~Y9 Partnership tax를 상쇄. Prediction은 MACRS tax benefit이 정상적으로 Sponsor에게 귀속.',
        'explain_en': 'Neptune offsets Y1~Y9 Partnership tax via NOL carryforward. Prediction allocates MACRS tax benefit normally to Sponsor.',
        'excel_hint_ko': 'Excel에서 NOL carryforward 로직이 MACRS benefit을 과도하게 소진하는지 확인. IRS 80% 규칙 적용 여부.',
        'excel_hint_en': 'Verify NOL carryforward in Excel is not over-consuming MACRS benefit. Check IRS 80% limitation.',
    })
    
    return {
        'calib_irr': round(calib_irr, 2),
        'predict_irr': round(predict_irr, 2),
        'total_delta': round(total_delta, 2),
        'factors': factors,
        'note_ko': '각 요인은 Calibration 상태에서 순차적으로 해제한 기여도. 순서에 따라 값이 조금씩 달라질 수 있음 (상호작용 효과).',
        'note_en': 'Each factor is measured by sequentially disabling from Calibration state. Values may vary slightly by order (interaction effects).',
    }

