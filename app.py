
import os
from datetime import datetime, timedelta, date
from io import BytesIO

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
from flask import Flask, render_template, request, redirect, send_file

ACCOUNTS_FILE = 'fd_accounts.xlsx'
RATES_FILE = 'fd_rates.xlsx'

app = Flask(__name__)

ACCOUNT_COLUMNS = [
    'FDID','CustomerName','Segment','Principal','TenureDays','StartDate','PayoutType','RateBps',
    'Compounding','MaturityInstruction','Status','AccruedInterest','YTDInterest','PANAvailable',
    'DeclarationForm','ResidencyClass','LastAccrualDate','MaturityDate'
]

RATE_COLUMNS = [
    'RatePlanId','CustomerSegment','TenureMinDays','TenureMaxDays','AmountMin','AmountMax','PayoutType',
    'AnnualRateBps','EffectiveFrom','EffectiveTo','Priority','Notes'
]

# ---------------- Storage helpers ----------------

def init_storage():
    if not os.path.exists(RATES_FILE):
        sample = pd.DataFrame([
            ['RP_STD','RETAIL',7,45,0,1000000,'PAYOUT',350,'2026-01-01','2026-12-31',10,'Standard short-term'],
            ['RP_STD','RETAIL',46,179,0,1000000,'PAYOUT',600,'2026-01-01','2026-12-31',10,'Standard mid-term'],
            ['RP_STD','RETAIL',180,365,0,1000000,'CUMULATIVE',725,'2026-01-01','2026-12-31',10,'Compounded quarterly (demo rate)'],
            ['RP_SENIOR','SENIOR',180,365,0,1000000,'CUMULATIVE',775,'2026-01-01','2026-12-31',20,'Senior uplift +50bps'],
            ['RP_STAFF','STAFF',7,365,0,2000000,'PAYOUT',800,'2026-01-01','2026-12-31',30,'Staff special']
        ], columns=RATE_COLUMNS)
        with pd.ExcelWriter(RATES_FILE, engine='openpyxl') as w:
            sample.to_excel(w, index=False, sheet_name='rates')

    if not os.path.exists(ACCOUNTS_FILE):
        empty = pd.DataFrame(columns=ACCOUNT_COLUMNS)
        with pd.ExcelWriter(ACCOUNTS_FILE, engine='openpyxl') as w:
            empty.to_excel(w, index=False, sheet_name='accounts')


def load_accounts() -> pd.DataFrame:
    df = pd.read_excel(ACCOUNTS_FILE, engine='openpyxl', sheet_name='accounts')
    # ensure types
    for c in ['Principal','AccruedInterest','YTDInterest']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
    return df


def save_accounts(df: pd.DataFrame):
    df = df[ACCOUNT_COLUMNS]
    with pd.ExcelWriter(ACCOUNTS_FILE, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='accounts')


def load_rates() -> pd.DataFrame:
    df = pd.read_excel(RATES_FILE, engine='openpyxl', sheet_name='rates')
    return df

# ---------------- Domain helpers ----------------

def next_id(df: pd.DataFrame) -> str:
    if df.empty:
        return 'FD0001'
    max_num = 0
    for v in df['FDID'].astype(str):
        try:
            max_num = max(max_num, int(v.replace('FD','')))
        except Exception:
            continue
    return f"FD{max_num+1:04d}"


def parse_date(s: str) -> date:
    return datetime.strptime(str(s), '%Y-%m-%d').date()


def format_date(d: date) -> str:
    return d.strftime('%Y-%m-%d')


def select_rate(segment: str, tenure_days: int, amount: float, payout_type: str, today: date) -> int:
    rates = load_rates()
    rates['EffectiveFrom'] = pd.to_datetime(rates['EffectiveFrom']).dt.date
    rates['EffectiveTo'] = pd.to_datetime(rates['EffectiveTo']).dt.date
    candidates = rates[
        (rates['CustomerSegment'] == segment)
        & (rates['TenureMinDays'] <= tenure_days)
        & (tenure_days <= rates['TenureMaxDays'])
        & (rates['AmountMin'] <= amount)
        & (amount <= rates['AmountMax'])
        & (rates['PayoutType'] == payout_type)
        & (rates['EffectiveFrom'] <= today)
        & (today <= rates['EffectiveTo'])
    ]
    if candidates.empty:
        # Fallback: pick any matching segment ignoring payout type
        candidates = rates[
            (rates['CustomerSegment'] == segment)
            & (rates['TenureMinDays'] <= tenure_days)
            & (tenure_days <= rates['TenureMaxDays'])
        ]
    if candidates.empty:
        return 600  # 6.00% default fallback
    candidates = candidates.sort_values('Priority', ascending=False)
    return int(candidates.iloc[0]['AnnualRateBps'])


def compute_maturity_date(start: date, tenure_days: int) -> date:
    return start + timedelta(days=int(tenure_days))


def accrual_for_period(principal: float, annual_rate_bps: int, days: int) -> float:
    annual_rate = annual_rate_bps / 10000.0
    daily = principal * (annual_rate / 365.0)
    return round(daily * max(days, 0), 2)


def accrue_all():
    df = load_accounts()
    if df.empty:
        return
    today = date.today()
    for i, row in df.iterrows():
        if row['Status'] != 'OPEN':
            continue
        start = parse_date(row['StartDate']) if isinstance(row['StartDate'], str) else row['StartDate'].date()
        last_acc_str = row['LastAccrualDate'] if pd.notna(row['LastAccrualDate']) else None
        last_acc = parse_date(last_acc_str) if isinstance(last_acc_str, str) else (last_acc_str.date() if pd.notna(last_acc_str) else start)
        days = (today - last_acc).days
        if days <= 0:
            continue
        add_int = accrual_for_period(row['Principal'], int(row['RateBps']), days)
        df.at[i, 'AccruedInterest'] = float(row['AccruedInterest']) + add_int
        df.at[i, 'YTDInterest'] = float(row['YTDInterest']) + add_int
        df.at[i, 'LastAccrualDate'] = format_date(today)
        # mark matured if crossed maturity date
        maturity = parse_date(row['MaturityDate']) if isinstance(row['MaturityDate'], str) else (row['MaturityDate'].date() if pd.notna(row['MaturityDate']) else compute_maturity_date(start, int(row['TenureDays'])))
        if today >= maturity and row['Status'] == 'OPEN':
            df.at[i, 'Status'] = 'MATURED'
    save_accounts(df)


def compute_premature_preview(row: pd.Series):
    today = date.today()
    start = parse_date(row['StartDate']) if isinstance(row['StartDate'], str) else row['StartDate'].date()
    days_elapsed = (today - start).days
    if days_elapsed < 7:
        return {'disallowed': True, 'days_elapsed': days_elapsed}
    # penalty: reduce effective rate by 100 bps; seniors 50 bps
    penalty_bps = 100 if row['Segment'] != 'SENIOR' else 50
    eff_rate_bps = max(int(row['RateBps']) - penalty_bps, 0)
    interest = accrual_for_period(float(row['Principal']), eff_rate_bps, days_elapsed)
    # Simple placeholder TDS logic
    pan = str(row['PANAvailable']).lower() == 'true'
    dec = str(row['DeclarationForm']).upper()
    residency = str(row['ResidencyClass']).upper()
    if not pan and residency == 'RESIDENT':
        tds_rate = 0.20
    elif dec in ('15G','15H') and residency == 'RESIDENT':
        tds_rate = 0.0
    else:
        tds_rate = 0.10
    tds_amount = round(interest * tds_rate, 2)
    net_payout = float(row['Principal']) + interest - tds_amount
    return {
        'disallowed': False,
        'days_elapsed': days_elapsed,
        'effective_rate_pct': eff_rate_bps / 100.0,
        'interest_earned': interest,
        'tds_amount': tds_amount,
        'net_payout': net_payout
    }

# ---------------- Routes ----------------

@app.route('/')
def dashboard():
    df = load_accounts()
    open_df = df[df['Status'] == 'OPEN'] if not df.empty else pd.DataFrame(columns=df.columns)
    open_count = len(open_df)
    total_principal = round(open_df['Principal'].sum(), 2) if not open_df.empty else 0.0
    total_ytd_interest = round(df['YTDInterest'].sum(), 2) if not df.empty else 0.0

    # maturity buckets
    today = date.today()
    m30 = m60 = m90 = 0
    recent = []
    if not df.empty:
        df['MaturityDate'] = pd.to_datetime(df['MaturityDate']).dt.date
        m30 = int(((df['MaturityDate'] - today).dt.days.between(0, 30)).sum())
        m60 = int(((df['MaturityDate'] - today).dt.days.between(31, 60)).sum())
        m90 = int(((df['MaturityDate'] - today).dt.days.between(61, 90)).sum())
        recent = df.sort_values('StartDate', ascending=False).head(5).to_dict('records')
    return render_template('index.html', open_count=open_count, total_principal=f"{total_principal:,.2f}",
                           total_ytd_interest=f"{total_ytd_interest:,.2f}", m30=m30, m60=m60, m90=m90, recent=recent)


@app.route('/open', methods=['GET','POST'])
def open_fd():
    if request.method == 'POST':
        form = request.form
        name = form.get('CustomerName')
        segment = form.get('Segment')
        principal = float(form.get('Principal'))
        tenure = int(form.get('TenureDays'))
        start = datetime.strptime(form.get('StartDate'), '%Y-%m-%d').date()
        payout = form.get('PayoutType')
        comp = form.get('Compounding')
        instr = form.get('MaturityInstruction')
        pan = form.get('PANAvailable')
        decl = form.get('DeclarationForm')
        res = form.get('ResidencyClass')
        rate_bps = select_rate(segment, tenure, principal, payout, start)

        df = load_accounts()
        fdid = next_id(df)
        maturity = compute_maturity_date(start, tenure)
        new_row = {
            'FDID': fdid,
            'CustomerName': name,
            'Segment': segment,
            'Principal': principal,
            'TenureDays': tenure,
            'StartDate': start.strftime('%Y-%m-%d'),
            'PayoutType': payout,
            'RateBps': rate_bps,
            'Compounding': comp,
            'MaturityInstruction': instr,
            'Status': 'OPEN',
            'AccruedInterest': 0.0,
            'YTDInterest': 0.0,
            'PANAvailable': str(pan),
            'DeclarationForm': decl,
            'ResidencyClass': res,
            'LastAccrualDate': start.strftime('%Y-%m-%d'),
            'MaturityDate': maturity.strftime('%Y-%m-%d')
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        save_accounts(df)
        return render_template('open.html', rate_info=rate_bps, today=date.today().strftime('%Y-%m-%d'))
    return render_template('open.html', rate_info=None, today=date.today().strftime('%Y-%m-%d'))


@app.route('/accounts')
def accounts():
    df = load_accounts()
    rows = df.to_dict('records') if not df.empty else []
    return render_template('accounts.html', rows=rows)


@app.route('/accrual/run', methods=['POST'])
def run_accrual():
    accrue_all()
    return redirect(request.referrer or '/')


@app.route('/accounts/<fdid>/preview-closure', methods=['GET','POST'])
def preview_closure(fdid):
    df = load_accounts()
    if df.empty or fdid not in set(df['FDID']):
        return redirect('/accounts')
    row = df[df['FDID'] == fdid].iloc[0]
    if request.method == 'POST':
        # Execute closure if allowed
        details = compute_premature_preview(row)
        if details.get('disallowed'):
            return redirect('/accounts')
        # Update record
        idx = df[df['FDID'] == fdid].index[0]
        df.at[idx, 'Status'] = 'CLOSED'
        df.at[idx, 'LastAccrualDate'] = date.today().strftime('%Y-%m-%d')
        save_accounts(df)
        return redirect('/accounts')
    else:
        details = compute_premature_preview(row)
        return render_template('preview_closure.html', acc=row.to_dict(), details=details)


@app.route('/chart/maturity')
def chart_maturity():
    df = load_accounts()
    fig = plt.figure(figsize=(6, 3))
    if df.empty:
        plt.text(0.5, 0.5, 'No data', ha='center', va='center')
    else:
        df['MaturityDate'] = pd.to_datetime(df['MaturityDate'])
        today_dt = pd.to_datetime(date.today())
        buckets = {
            '0-30': 0,
            '31-60': 0,
            '61-90': 0,
            '>90': 0
        }
        for d in df['MaturityDate']:
            days = (d - today_dt).days
            if days < 0:
                continue
            if days <= 30:
                buckets['0-30'] += 1
            elif days <= 60:
                buckets['31-60'] += 1
            elif days <= 90:
                buckets['61-90'] += 1
            else:
                buckets['>90'] += 1
        plt.bar(list(buckets.keys()), list(buckets.values()), color=['#60a5fa','#34d399','#fbbf24','#f472b6'])
        plt.title('Maturity Ladder (counts)')
        plt.xlabel('Days to Maturity')
        plt.ylabel('Number of FDs')
        plt.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=120, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return send_file(buf, mimetype='image/png')


if __name__ == '__main__':
    init_storage()
    # Helpful on first run: pre-create files and start server
    app.run(host='127.0.0.1', port=5000, debug=True)
