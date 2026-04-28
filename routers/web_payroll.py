"""
Enterprise Payroll API.
Handles company setup, employee roster, and batch salary disbursements.
All payments are pass-through via Yellow Card / Breet — Qreek never holds funds.
"""
import asyncio, uuid
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func
from pydantic import BaseModel
from typing import Optional, List

from database.session import get_db
from database.models import Company, Employee, PayrollRun, PayrollEntry, AuditLog, User
from core.web_jwt import decode_token
from core.banks import resolve_bank, BANKS
from services.security_service import verify_pin
from core.payout import best_payout

router = APIRouter(prefix="/api/v1/payroll", tags=["payroll"])

FEE_PCT = 0.003   # 0.3% per payment


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class CompanyIn(BaseModel):
    name:       str
    industry:   Optional[str] = None
    rc_number:  Optional[str] = None
    email:      Optional[str] = None
    address:    Optional[str] = None


class EmployeeIn(BaseModel):
    name:        str
    email:       Optional[str] = None
    phone:       Optional[str] = None
    bank_account: str
    bank_code:   str
    department:  Optional[str] = None
    job_title:   Optional[str] = None
    salary:      float


class EmployeeUpdate(BaseModel):
    name:        Optional[str] = None
    email:       Optional[str] = None
    phone:       Optional[str] = None
    bank_account: Optional[str] = None
    bank_code:   Optional[str] = None
    department:  Optional[str] = None
    job_title:   Optional[str] = None
    salary:      Optional[float] = None
    is_active:   Optional[bool] = None


class BulkEmployeeIn(BaseModel):
    employees: List[EmployeeIn]


class PayrollRunIn(BaseModel):
    period_label:  str                     # "April 2026"
    note:          Optional[str] = None
    employee_ids:  Optional[List[str]] = None   # None = all active employees
    overrides:     Optional[dict] = None         # {employee_id: amount} for one-off adjustments


class ExecuteRunIn(BaseModel):
    pin: str


# ── Helpers ───────────────────────────────────────────────────────────────────

def _co_dict(c: Company) -> dict:
    return {
        "id": c.id, "name": c.name, "industry": c.industry,
        "rc_number": c.rc_number, "email": c.email, "address": c.address,
        "total_paid_ngn": c.total_paid_ngn, "employee_count": c.employee_count,
        "is_verified": c.is_verified, "created_at": c.created_at.isoformat() if c.created_at else None,
    }


def _emp_dict(e: Employee) -> dict:
    return {
        "id": e.id, "company_id": e.company_id, "name": e.name,
        "email": e.email, "phone": e.phone,
        "bank_account": "****" + e.bank_account[-4:] if e.bank_account else None,
        "bank_account_full": e.bank_account,
        "bank_code": e.bank_code, "bank_name": e.bank_name,
        "department": e.department, "job_title": e.job_title,
        "salary": e.salary, "is_active": e.is_active,
        "created_at": e.created_at.isoformat() if e.created_at else None,
    }


def _run_dict(r: PayrollRun) -> dict:
    return {
        "id": r.id, "company_id": r.company_id, "period_label": r.period_label,
        "total_gross": r.total_gross, "total_fee": r.total_fee, "total_net": r.total_net,
        "entry_count": r.entry_count, "paid_count": r.paid_count, "failed_count": r.failed_count,
        "status": r.status, "note": r.note,
        "scheduled_at": r.scheduled_at.isoformat() if r.scheduled_at else None,
        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


def _entry_dict(e: PayrollEntry) -> dict:
    return {
        "id": e.id, "employee_name": e.employee_name,
        "bank_account": "****" + e.bank_account[-4:] if e.bank_account else None,
        "bank_name": e.bank_name,
        "gross_amount": e.gross_amount, "fee": e.fee, "net_amount": e.net_amount,
        "status": e.status, "reference": e.reference, "provider": e.provider,
        "error_msg": e.error_msg,
        "paid_at": e.paid_at.isoformat() if e.paid_at else None,
    }


async def _get_company(db: AsyncSession, phone: str) -> Company:
    r = await db.execute(select(Company).where(Company.owner_phone == phone))
    co = r.scalar_one_or_none()
    if not co:
        raise HTTPException(status_code=404, detail="No company registered. Set up your company first.")
    return co


async def _log(db: AsyncSession, phone: str, action: str, entity_type: str = None,
               entity_id: str = None, amount: float = None, request: Request = None, meta: dict = None):
    ip = request.client.host if request else None
    log = AuditLog(
        actor_phone=phone, action=action, entity_type=entity_type,
        entity_id=entity_id, amount=amount, ip_address=ip, metadata=meta,
    )
    db.add(log)


# ── Company ───────────────────────────────────────────────────────────────────

@router.get("/company")
async def get_company(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    r     = await db.execute(select(Company).where(Company.owner_phone == phone))
    co    = r.scalar_one_or_none()
    if not co:
        return {"company": None}
    return {"company": _co_dict(co)}


@router.post("/company")
async def create_company(
    body: CompanyIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    exists = await db.execute(select(Company).where(Company.owner_phone == phone))
    if exists.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Company already registered for this account.")

    co = Company(owner_phone=phone, **body.model_dump(exclude_none=True))
    db.add(co)
    await db.commit()
    await db.refresh(co)
    return {"company": _co_dict(co), "message": "Company created successfully."}


@router.put("/company")
async def update_company(
    body: CompanyIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    co    = await _get_company(db, phone)
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(co, k, v)
    await db.commit()
    return {"company": _co_dict(co)}


# ── Employees ─────────────────────────────────────────────────────────────────

@router.get("/employees")
async def list_employees(
    department: str = None,
    active_only: bool = True,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    co    = await _get_company(db, phone)

    q = select(Employee).where(Employee.company_id == co.id)
    if active_only:
        q = q.where(Employee.is_active == True)
    if department:
        q = q.where(Employee.department == department)
    q = q.order_by(Employee.name)

    r   = await db.execute(q)
    emp = r.scalars().all()
    return {"employees": [_emp_dict(e) for e in emp], "total": len(emp)}


@router.post("/employees")
async def add_employee(
    body: EmployeeIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    co    = await _get_company(db, phone)

    if body.salary <= 0:
        raise HTTPException(status_code=400, detail="Salary must be greater than 0.")

    bank = resolve_bank(body.bank_code)
    if not bank:
        raise HTTPException(status_code=400, detail=f"Invalid bank code: {body.bank_code}")

    emp = Employee(
        company_id=co.id,
        bank_name=bank["name"],
        **body.model_dump(exclude_none=True),
    )
    db.add(emp)
    co.employee_count = (co.employee_count or 0) + 1
    await db.commit()
    await db.refresh(emp)
    return {"employee": _emp_dict(emp)}


@router.post("/employees/bulk")
async def bulk_add_employees(
    body: BulkEmployeeIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    co    = await _get_company(db, phone)

    if not body.employees:
        raise HTTPException(status_code=400, detail="No employees provided.")
    if len(body.employees) > 500:
        raise HTTPException(status_code=400, detail="Max 500 employees per bulk import.")

    added, errors = [], []
    for i, emp_data in enumerate(body.employees):
        bank = resolve_bank(emp_data.bank_code)
        if not bank:
            errors.append({"row": i + 1, "name": emp_data.name, "error": f"Invalid bank code: {emp_data.bank_code}"})
            continue
        if emp_data.salary <= 0:
            errors.append({"row": i + 1, "name": emp_data.name, "error": "Salary must be > 0"})
            continue
        emp = Employee(company_id=co.id, bank_name=bank["name"], **emp_data.model_dump(exclude_none=True))
        db.add(emp)
        added.append(emp_data.name)

    co.employee_count = (co.employee_count or 0) + len(added)
    await db.commit()
    return {
        "added": len(added),
        "errors": errors,
        "message": f"{len(added)} employee(s) imported successfully." + (f" {len(errors)} failed." if errors else ""),
    }


@router.put("/employees/{employee_id}")
async def update_employee(
    employee_id: str,
    body: EmployeeUpdate,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    co    = await _get_company(db, phone)

    r   = await db.execute(select(Employee).where(Employee.id == employee_id, Employee.company_id == co.id))
    emp = r.scalar_one_or_none()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found.")

    updates = body.model_dump(exclude_none=True)
    if "bank_code" in updates:
        bank = resolve_bank(updates["bank_code"])
        if not bank:
            raise HTTPException(status_code=400, detail=f"Invalid bank code: {updates['bank_code']}")
        updates["bank_name"] = bank["name"]

    for k, v in updates.items():
        setattr(emp, k, v)

    # Sync employee count when deactivating
    if "is_active" in updates:
        active_r = await db.execute(
            select(func.count()).where(Employee.company_id == co.id, Employee.is_active == True)
        )
        co.employee_count = active_r.scalar() or 0

    await db.commit()
    return {"employee": _emp_dict(emp)}


@router.delete("/employees/{employee_id}")
async def deactivate_employee(
    employee_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    co    = await _get_company(db, phone)

    r   = await db.execute(select(Employee).where(Employee.id == employee_id, Employee.company_id == co.id))
    emp = r.scalar_one_or_none()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found.")

    emp.is_active     = False
    co.employee_count = max(0, (co.employee_count or 1) - 1)
    await db.commit()
    return {"message": f"{emp.name} removed from payroll."}


@router.get("/departments")
async def list_departments(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    co    = await _get_company(db, phone)
    r     = await db.execute(
        select(Employee.department).where(Employee.company_id == co.id, Employee.department != None).distinct()
    )
    return {"departments": [row[0] for row in r.all() if row[0]]}


# ── Payroll Runs ──────────────────────────────────────────────────────────────

@router.get("/runs")
async def list_runs(
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    co    = await _get_company(db, phone)
    r     = await db.execute(
        select(PayrollRun).where(PayrollRun.company_id == co.id).order_by(desc(PayrollRun.created_at)).limit(50)
    )
    runs  = r.scalars().all()
    return {"runs": [_run_dict(run) for run in runs]}


@router.post("/runs")
async def create_run(
    body: PayrollRunIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Preview a payroll run. Returns calculated totals without executing.
    Call POST /runs/{id}/execute with PIN to actually fire the payments.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone)

    # Fetch employees
    q = select(Employee).where(Employee.company_id == co.id, Employee.is_active == True)
    if body.employee_ids:
        q = q.where(Employee.id.in_(body.employee_ids))
    r     = await db.execute(q)
    emps  = r.scalars().all()

    if not emps:
        raise HTTPException(status_code=400, detail="No active employees found for this payroll run.")

    # Calculate totals
    overrides    = body.overrides or {}
    total_gross  = 0.0
    entry_data   = []

    for emp in emps:
        gross = float(overrides.get(emp.id, emp.salary))
        fee   = round(gross * FEE_PCT, 2)
        net   = round(gross - fee, 2)
        total_gross += gross
        entry_data.append({
            "employee_id": emp.id, "employee_name": emp.name,
            "bank_account": emp.bank_account, "bank_code": emp.bank_code, "bank_name": emp.bank_name,
            "gross_amount": gross, "fee": fee, "net_amount": net,
        })

    total_fee = round(total_gross * FEE_PCT, 2)
    total_net = round(total_gross - total_fee, 2)

    # Create the run in PENDING state
    run = PayrollRun(
        company_id=co.id,
        initiated_by=phone,
        period_label=body.period_label,
        total_gross=total_gross,
        total_fee=total_fee,
        total_net=total_net,
        entry_count=len(entry_data),
        note=body.note,
        status="pending",
    )
    db.add(run)
    await db.flush()

    for ed in entry_data:
        db.add(PayrollEntry(run_id=run.id, **ed))

    await db.commit()
    await db.refresh(run)

    return {
        "run": _run_dict(run),
        "preview": entry_data,
        "summary": {
            "employees": len(entry_data),
            "total_gross": total_gross,
            "total_fee": total_fee,
            "fee_pct": FEE_PCT * 100,
            "total_net": total_net,
            "message": f"Review the payroll. Call /runs/{run.id}/execute with your PIN to disburse.",
        },
    }


@router.get("/runs/{run_id}")
async def get_run(
    run_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    co    = await _get_company(db, phone)

    r   = await db.execute(select(PayrollRun).where(PayrollRun.id == run_id, PayrollRun.company_id == co.id))
    run = r.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Payroll run not found.")

    er      = await db.execute(select(PayrollEntry).where(PayrollEntry.run_id == run_id).order_by(PayrollEntry.employee_name))
    entries = er.scalars().all()

    return {"run": _run_dict(run), "entries": [_entry_dict(e) for e in entries]}


@router.post("/runs/{run_id}/execute")
async def execute_run(
    run_id: str,
    body: ExecuteRunIn,
    request: Request,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    """PIN-confirmed execution. Fires all payouts asynchronously."""
    phone = claims["phone"]
    co    = await _get_company(db, phone)

    r   = await db.execute(select(PayrollRun).where(PayrollRun.id == run_id, PayrollRun.company_id == co.id))
    run = r.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Payroll run not found.")
    if run.status not in ("pending",):
        raise HTTPException(status_code=400, detail=f"Run is already {run.status}. Cannot re-execute.")

    # Verify PIN
    ok = await verify_pin(db, phone, body.pin)
    if not ok:
        raise HTTPException(status_code=401, detail="Incorrect PIN.")

    # Mark as processing
    run.status = "processing"
    await db.commit()

    # Fetch all pending entries
    er      = await db.execute(select(PayrollEntry).where(PayrollEntry.run_id == run_id, PayrollEntry.status == "pending"))
    entries = er.scalars().all()

    # Audit log
    await _log(db, phone, "payroll_run_executed", "payroll_run", run.id, run.total_gross, request,
               {"company": co.name, "period": run.period_label, "count": len(entries)})
    await db.commit()

    # Fire payouts asynchronously — non-blocking
    async def _fire_all():
        async with __import__("database.session", fromlist=["AsyncSessionLocal"]).AsyncSessionLocal() as sess:
            for entry in entries:
                bank = {"account_number": entry.bank_account, "bank_code": entry.bank_code}
                ref  = "QRK_PR_" + uuid.uuid4().hex[:10].upper()
                try:
                    result = await best_payout(phone, entry.net_amount, bank, ref)
                    entry.status   = "completed"
                    entry.provider = result.get("provider")
                    entry.reference = ref
                    entry.paid_at  = datetime.utcnow()
                    run.paid_count = (run.paid_count or 0) + 1
                except Exception as e:
                    entry.status    = "failed"
                    entry.error_msg = str(e)[:200]
                    run.failed_count = (run.failed_count or 0) + 1

                sess.add(entry)
                await sess.flush()

            if run.failed_count and run.paid_count:
                run.status = "partial"
            elif run.failed_count == run.entry_count:
                run.status = "failed"
            else:
                run.status = "completed"
            run.completed_at = datetime.utcnow()

            co_r = await sess.execute(select(Company).where(Company.id == co.id))
            co2  = co_r.scalar_one_or_none()
            if co2:
                co2.total_paid_ngn = (co2.total_paid_ngn or 0) + run.total_net

            sess.add(run)
            await sess.commit()

    asyncio.create_task(_fire_all())

    return {
        "message": f"Payroll run for {run.period_label} is now processing. {len(entries)} payments fired.",
        "run_id": run.id,
        "status": "processing",
    }


@router.delete("/runs/{run_id}")
async def cancel_run(
    run_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    phone = claims["phone"]
    co    = await _get_company(db, phone)

    r   = await db.execute(select(PayrollRun).where(PayrollRun.id == run_id, PayrollRun.company_id == co.id))
    run = r.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Payroll run not found.")
    if run.status != "pending":
        raise HTTPException(status_code=400, detail="Can only cancel pending runs.")

    run.status = "failed"
    await db.commit()
    return {"message": "Payroll run cancelled."}


# ── Analytics ─────────────────────────────────────────────────────────────────

@router.get("/analytics")
async def get_analytics(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    phone = claims["phone"]
    co    = await _get_company(db, phone)

    # Total paid per month (last 6 runs)
    runs_r = await db.execute(
        select(PayrollRun)
        .where(PayrollRun.company_id == co.id, PayrollRun.status.in_(["completed", "partial"]))
        .order_by(desc(PayrollRun.created_at))
        .limit(12)
    )
    runs = runs_r.scalars().all()

    # Department breakdown
    dept_r = await db.execute(
        select(Employee.department, func.sum(Employee.salary), func.count())
        .where(Employee.company_id == co.id, Employee.is_active == True)
        .group_by(Employee.department)
    )
    departments = [
        {"department": row[0] or "Unassigned", "total_salary": row[1], "count": row[2]}
        for row in dept_r.all()
    ]

    return {
        "total_paid_ngn": co.total_paid_ngn,
        "employee_count": co.employee_count,
        "runs_history": [
            {"period": r.period_label, "total_net": r.total_net, "count": r.paid_count, "status": r.status}
            for r in reversed(runs)
        ],
        "department_breakdown": departments,
    }


# ── Banks list ────────────────────────────────────────────────────────────────

@router.get("/banks")
async def list_banks():
    return {"banks": [{"code": b["code"], "name": b["name"]} for b in BANKS]}
