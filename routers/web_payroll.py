"""
@file web_payroll.py
@description Enterprise Payroll API — handles batch salary disbursements and organization management.
Enables organizations to manage employee rosters, preview payroll runs, and execute 
secure parallel payouts.

Flow:
1. Organization Setup: Users register a company profile to unlock enterprise features.
2. Roster Management: Admins add/import employees via CSV and manage departments/salaries.
3. Execution Lifecycle:
   a. Initialization: Create a "Payroll Run" (preview) with optional salary overrides.
   b. Validation: Admins review the preview, fees, and total disbursement amount.
   c. Execution: Requires PIN verification. Debits the owner's NGN balance and 
      triggers asynchronous, non-blocking payouts to all employee banks.
4. Transparency: Every payroll run generates an audit log and real-time status 
   updates (pending -> processing -> completed/failed).
5. Analytics: Provides organization-wide spending insights and department-level breakdowns.
"""
import asyncio, csv, io, uuid, re
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Request, Header
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func
from pydantic import BaseModel
from typing import Optional, List

from database.session import get_db
from database.models import Company, Employee, PayrollRun, PayrollEntry, AuditLog, User, Transaction
from core.web_jwt import decode_token
from core.banks import resolve_bank, BANKS
from services.security_service import is_frozen, pin_attempts_remaining, verify_transaction_pin
from services.payment_service import debit_ngn_or_reject, refund_ngn, debit_company_wallet_or_reject, refund_company_wallet
from core.payout import best_payout, settle_fee
from services.flutterwave_service import resolve_account

router = APIRouter(prefix="/api/v1/payroll", tags=["payroll"])

FEE_PCT = 0.002    # 0.2% per direct payroll payment


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class CompanyIn(BaseModel):
    name:       str
    industry:   Optional[str] = None
    rc_number:  Optional[str] = None
    email:      Optional[str] = None
    address:    str


class EmployeeIn(BaseModel):
    name:        str
    email:       Optional[str] = None
    phone:       Optional[str] = None
    bank_account: Optional[str] = None
    bank_code:   Optional[str] = None
    department:  Optional[str] = None
    job_title:   Optional[str] = None
    salary:      float


class EmployeeSelfServiceUpdate(BaseModel):
    name:        Optional[str] = None
    email:       Optional[str] = None
    phone:       Optional[str] = None
    bank_account: Optional[str] = None
    bank_code:   Optional[str] = None
    department:  Optional[str] = None
    job_title:   Optional[str] = None
    salary:      Optional[float] = None


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
    """
    Converts a Company model instance into a dictionary for JSON response.
    """
    slug = __import__("re").sub(r'[^a-z0-9]+', '-', c.name.lower()).strip('-') if c.name else 'general'
    link = f"https://qreekfinance.org/invite/{slug}/{c.invite_token}" if c.invite_token else None
    return {
        "id": c.id, "name": c.name, "industry": c.industry,
        "rc_number": c.rc_number, "email": c.email, "address": c.address,
        "total_paid_ngn": c.total_paid_ngn, "wallet_balance_ngn": c.wallet_balance_ngn or 0, "employee_count": c.employee_count,
        "is_verified": c.is_verified, "invite_link": link, "created_at": c.created_at.isoformat() if c.created_at else None,
    }


def _emp_dict(e: Employee) -> dict:
    """
    Converts an Employee model instance into a dictionary, masking sensitive bank account details.
    """
    return {
        "id": e.id, "company_id": e.company_id, "name": e.name or "",
        "email": e.email, "phone": e.phone,
        "bank_account": "****" + e.bank_account[-4:] if e.bank_account else None,
        "bank_account_full": e.bank_account,
        "bank_code": e.bank_code, "bank_name": e.bank_name,
        "department": e.department, "job_title": e.job_title,
        "salary": e.salary, "is_active": e.is_active,
        "has_details": bool(e.name and e.bank_account and e.salary),
        "created_at": e.created_at.isoformat() if e.created_at else None,
    }


def _run_dict(r: PayrollRun) -> dict:
    """
    Converts a PayrollRun model instance into a dictionary for tracking batch payments.
    """
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
    """
    Converts a PayrollEntry model instance into a dictionary representing a single employee's payment.
    """
    return {
        "id": e.id, "employee_name": e.employee_name,
        "bank_account": "****" + e.bank_account[-4:] if e.bank_account else None,
        "bank_name": e.bank_name,
        "gross_amount": e.gross_amount, "fee": e.fee, "net_amount": e.net_amount,
        "status": e.status, "reference": e.reference, "provider": e.provider,
        "error_msg": e.error_msg,
        "paid_at": e.paid_at.isoformat() if e.paid_at else None,
    }


async def _get_company(db: AsyncSession, phone: str, company_id: str = None) -> Company:
    """
    Helper to fetch the company associated with a user's phone number.
    If a company_id is provided but not found (e.g., stale localStorage), falls
    back to the user's most recent company. Raises 404 only if no company exists at all.
    """
    if company_id:
        # Try fetching the specific company first
        q = select(Company).where(Company.owner_phone == phone, Company.id == company_id)
        r = await db.execute(q)
        co = r.first()
        if co:
            return co[0]
        # company_id was stale/invalid – fall through to pick any company

    # Fallback: return the user's most recent company
    q = select(Company).where(Company.owner_phone == phone).order_by(Company.created_at.desc())
    r = await db.execute(q)
    co = r.first()
    if not co:
        raise HTTPException(status_code=404, detail="No company registered or found.")
    return co[0]


async def _log(db: AsyncSession, phone: str, action: str, entity_type: str = None,
               entity_id: str = None, amount: float = None, request: Request = None, meta: dict = None):
    """
    Helper to record significant payroll events in the audit log.
    """
    ip = request.client.host if request else None
    log = AuditLog(
        actor_phone=phone, action=action, entity_type=entity_type,
        entity_id=entity_id, amount=amount, ip_address=ip, event_metadata=meta,
    )
    db.add(log)


# ── Company ───────────────────────────────────────────────────────────────────

@router.get("/company")
async def get_company(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    """
    Retrieves the company profiles for the authenticated owner.
    """
    phone = claims["phone"]
    r     = await db.execute(select(Company).where(Company.owner_phone == phone).order_by(Company.created_at.desc()))
    cos   = r.scalars().all()
    if not cos:
        return {"company": None, "companies": []}
    return {"company": _co_dict(cos[0]), "companies": [_co_dict(c) for c in cos]}


@router.post("/company")
async def create_company(
    body: CompanyIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Registers a new company profile for an authenticated user.
    """
    phone = claims["phone"]

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
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Updates the existing company profile details.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)
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
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Lists all employees in the user's company, with optional filtering by department and active status.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

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
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Adds a new employee to the company roster.
    Validates bank details and increments the company's employee count.
    """
    phone = claims["phone"]
    co = await _get_company(db, phone, company_id)

    if body.salary <= 0:
        raise HTTPException(status_code=400, detail="Salary must be greater than 0.")

    emp_data = body.model_dump(exclude_none=True)
    if body.bank_code:
        bank = resolve_bank(body.bank_code)
        if not bank:
            raise HTTPException(status_code=400, detail=f"Invalid bank code: {body.bank_code}")
        emp_data["bank_name"] = bank["name"]
    else:
        emp_data["bank_name"] = ""
    if not body.bank_account:
        emp_data["bank_account"] = ""

    emp = Employee(company_id=co.id, **emp_data)
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
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Imports multiple employees at once.
    Validates each record and returns a summary of successes and failures.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

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
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Updates an employee's details (salary, department, job title, bank info, or active status).
    Synchronizes the company's employee count if the active status changes.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

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
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Deactivates an employee (soft delete) and decrements the company's employee count.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

    r   = await db.execute(select(Employee).where(Employee.id == employee_id, Employee.company_id == co.id))
    emp = r.scalar_one_or_none()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found.")

    emp.is_active     = False
    co.employee_count = max(0, (co.employee_count or 1) - 1)
    await db.commit()
    return {"message": f"{emp.name} removed from payroll."}


@router.post("/employees/{employee_id}/generate-link")
async def generate_employee_edit_link(
    employee_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Generates a unique self-service edit link for an employee.
    The link can be shared with the employee so they can update their own details.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

    r   = await db.execute(select(Employee).where(Employee.id == employee_id, Employee.company_id == co.id))
    emp = r.scalar_one_or_none()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found.")

    token = uuid.uuid4().hex[:16]
    emp.edit_token = token
    await db.commit()

    slug = re.sub(r'[^a-z0-9]+', '-', co.name.lower()).strip('-') if co.name else 'general'
    link = f"https://qreekfinance.org/invite/{slug}/{token}"
    return {"token": token, "link": link, "employee_name": emp.name or ""}


@router.post("/employees/generate-invite")
async def generate_employee_invite(
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Creates or generates a single company-wide invite link that can be shared
    with all employees. Each employee who opens the link can submit their own
    details, creating a new employee record under this company.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

    if not co.invite_token:
        co.invite_token = uuid.uuid4().hex[:16]
        await db.commit()

    slug = re.sub(r'[^a-z0-9]+', '-', co.name.lower()).strip('-') if co.name else 'general'
    link = f"https://qreekfinance.org/invite/{slug}/{co.invite_token}"
    return {"token": co.invite_token, "link": link, "company_name": co.name}


# ── Employee Self-Service (public, token-authenticated) ──────────────────────


@router.get("/employee-self-service/{token}")
async def get_employee_by_token(
    token: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Public endpoint. Supports two modes:
    1. Company invite: token matches Company.invite_token → returns company info, no employee
    2. Employee edit: token matches Employee.edit_token → returns employee data for pre-fill
    """
    co_r = await db.execute(select(Company).where(Company.invite_token == token))
    company = co_r.scalar_one_or_none()
    if company:
        return {
            "mode": "invite",
            "company_name": company.name,
        }

    emp_r = await db.execute(select(Employee).where(Employee.edit_token == token))
    emp = emp_r.scalar_one_or_none()
    if not emp:
        raise HTTPException(status_code=404, detail="Invalid or expired link.")

    return {
        "mode": "edit",
        "employee": {
            "id": emp.id,
            "name": emp.name,
            "email": emp.email,
            "phone": emp.phone,
            "department": emp.department,
            "job_title": emp.job_title,
            "salary": emp.salary,
            "bank_account_masked": "****" + emp.bank_account[-4:] if emp.bank_account else None,
            "bank_code": emp.bank_code,
            "bank_name": emp.bank_name,
            "has_details": bool(emp.name and emp.bank_account and emp.salary),
        }
    }


@router.put("/employee-self-service/{token}")
async def update_employee_by_token(
    token: str,
    body: EmployeeSelfServiceUpdate,
    db: AsyncSession = Depends(get_db),
):
    """
    Public endpoint. Supports two modes:
    1. Company invite: token matches Company.invite_token → creates a NEW employee record
    2. Employee edit: token matches Employee.edit_token → updates existing employee
    Verifies bank account via Flutterwave before saving in both cases.
    """
    co_r = await db.execute(select(Company).where(Company.invite_token == token))
    company = co_r.scalar_one_or_none()

    if company:
        # Mode 1: Company invite — create new employee
        if not body.name or not body.name.strip():
            raise HTTPException(status_code=400, detail="Full name is required.")
        if not body.salary or body.salary <= 0:
            raise HTTPException(status_code=400, detail="A valid salary is required.")
        if not body.bank_account or not body.bank_code:
            raise HTTPException(status_code=400, detail="Bank account and bank code are required.")

        try:
            result = await resolve_account(body.bank_account, body.bank_code)
            account_name = None
            if isinstance(result, dict):
                data = result.get("data") or result
                account_name = data.get("account_name") or data.get("accountNumberName")
            if not account_name:
                raise HTTPException(status_code=400, detail="Could not verify bank account. Check the details.")
        except Exception as exc:
            if isinstance(exc, HTTPException):
                raise
            raise HTTPException(status_code=400, detail=f"Bank verification failed: {str(exc)[:200]}")

        bank = resolve_bank(body.bank_code)
        bank_name = bank["name"] if bank else ""

        emp = Employee(
            company_id=company.id,
            name=body.name.strip(),
            email=body.email,
            phone=body.phone,
            bank_account=body.bank_account,
            bank_code=body.bank_code,
            bank_name=bank_name,
            department=body.department,
            job_title=body.job_title,
            salary=body.salary,
        )
        db.add(emp)
        company.employee_count = (company.employee_count or 0) + 1
        await db.commit()
        await db.refresh(emp)

        return {
            "mode": "invite",
            "message": "Your details have been submitted successfully.",
            "employee": {
                "name": emp.name,
                "email": emp.email,
                "phone": emp.phone,
                "department": emp.department,
                "job_title": emp.job_title,
                "salary": emp.salary,
                "bank_name": emp.bank_name,
            }
        }

    # Mode 2: Employee edit — update existing
    emp_r = await db.execute(select(Employee).where(Employee.edit_token == token))
    emp = emp_r.scalar_one_or_none()
    if not emp:
        raise HTTPException(status_code=404, detail="Invalid or expired link.")

    updates = body.model_dump(exclude_none=True)

    if "bank_account" in updates or "bank_code" in updates:
        bank_account = updates.get("bank_account", emp.bank_account)
        bank_code    = updates.get("bank_code", emp.bank_code)
        try:
            result = await resolve_account(bank_account, bank_code)
            account_name = None
            if isinstance(result, dict):
                data = result.get("data") or result
                account_name = data.get("account_name") or data.get("accountNumberName")
            if not account_name:
                raise HTTPException(status_code=400, detail="Could not verify bank account. Check the details.")
        except Exception as exc:
            if isinstance(exc, HTTPException):
                raise
            raise HTTPException(status_code=400, detail=f"Bank verification failed: {str(exc)[:200]}")

    if "bank_code" in updates:
        bank = resolve_bank(updates["bank_code"])
        if not bank:
            raise HTTPException(status_code=400, detail=f"Invalid bank code: {updates['bank_code']}")
        updates["bank_name"] = bank["name"]

    for k, v in updates.items():
        setattr(emp, k, v)

    await db.commit()
    await db.refresh(emp)
    return {
        "mode": "edit",
        "message": "Your details have been updated successfully.",
        "employee": {
            "name": emp.name,
            "email": emp.email,
            "phone": emp.phone,
            "department": emp.department,
            "job_title": emp.job_title,
            "salary": emp.salary,
            "bank_name": emp.bank_name,
        }
    }


@router.get("/departments")
async def list_departments(company_id: Optional[str] = Header(None, alias="x-company-id"),claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db)):
    """
    Returns a distinct list of all departments existing in the company's roster.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)
    r     = await db.execute(
        select(Employee.department).where(Employee.company_id == co.id, Employee.department != None).distinct()
    )
    return {"departments": [row[0] for row in r.all() if row[0]]}


# ── Payroll Runs ──────────────────────────────────────────────────────────────

@router.get("/runs")
async def list_runs(
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Lists the history of payroll runs for the company.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)
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
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Preview a payroll run. Returns calculated totals without executing.
    Call POST /runs/{id}/execute with PIN to actually fire the payments.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

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

    total_fee = round(sum(ed["fee"] for ed in entry_data), 2)
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
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Retrieves the details of a specific payroll run, including all individual employee entries.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

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
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """PIN-confirmed execution. Fires all payouts asynchronously."""
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

    r   = await db.execute(select(PayrollRun).where(PayrollRun.id == run_id, PayrollRun.company_id == co.id))
    run = r.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Payroll run not found.")
    if run.status not in ("pending",):
        raise HTTPException(status_code=400, detail=f"Run is already {run.status}. Cannot re-execute.")

    # Verify PIN
    if await is_frozen(db, phone):
        raise HTTPException(status_code=403, detail="Account frozen after too many failed PIN attempts. Contact support.")

    ok = await verify_transaction_pin(db, phone, body.pin)
    if not ok:
        remaining = await pin_attempts_remaining(db, phone)
        if remaining <= 0:
            raise HTTPException(status_code=403, detail="Account frozen after 5 failed PIN attempts.")
        raise HTTPException(status_code=401, detail=f"Incorrect PIN. {remaining} attempts remaining.")

    await debit_company_wallet_or_reject(db, co.id, run.total_gross)

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
                    await settle_fee(phone, entry.fee, ref)
                    entry.status   = "completed"
                    entry.provider = result.get("provider")
                    entry.reference = ref
                    entry.paid_at  = datetime.utcnow()
                    entry.qreek_fee    = entry.fee
                    entry.provider_fee = 0.0
                    run.paid_count = (run.paid_count or 0) + 1
                except Exception as e:
                    entry.status    = "failed"
                    entry.error_msg = str(e)[:200]
                    entry.qreek_fee    = 0.0
                    entry.provider_fee = 0.0
                    run.failed_count = (run.failed_count or 0) + 1
                    await refund_company_wallet(sess, co.id, entry.gross_amount)

                # Record transaction
                tx = Transaction(
                    user_phone=phone, tx_type="payroll",
                    currency="NGN", amount=entry.net_amount,
                    ngn_amount=entry.net_amount, gross_amount=entry.gross_amount,
                    qreek_fee=entry.qreek_fee, provider_fee=entry.provider_fee,
                    net_amount=entry.net_amount, status=entry.status,
                    provider=entry.provider, reference=entry.reference,
                    payment_description=f"Payroll {run.period_label} — {entry.employee_name}",
                )
                sess.add(tx)
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
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Cancels a pending payroll run, preventing it from being executed.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

    r   = await db.execute(select(PayrollRun).where(PayrollRun.id == run_id, PayrollRun.company_id == co.id))
    run = r.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Payroll run not found.")
    if run.status != "pending":
        raise HTTPException(status_code=400, detail="Can only cancel pending runs.")

    run.status = "failed"
    await db.commit()
    return {"message": "Payroll run cancelled."}


@router.post("/runs/{run_id}/entries/{entry_id}/retry")
async def retry_entry(
    run_id: str,
    entry_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """Retry a single failed payroll entry. Re-debits the company wallet and re-fires the payout."""
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

    r   = await db.execute(select(PayrollRun).where(PayrollRun.id == run_id, PayrollRun.company_id == co.id))
    run = r.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")
    if run.status not in ("partial", "failed"):
        raise HTTPException(status_code=400, detail="Can only retry entries from runs with failures.")

    er   = await db.execute(select(PayrollEntry).where(PayrollEntry.id == entry_id, PayrollEntry.run_id == run_id))
    entry = er.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found.")
    if entry.status != "failed":
        raise HTTPException(status_code=400, detail="Entry is not in failed status.")

    # Re-debit gross (refund_company_wallet would have returned it on failure)
    await debit_company_wallet_or_reject(db, co.id, entry.gross_amount)

    bank = {"account_number": entry.bank_account, "bank_code": entry.bank_code}
    ref  = "QRK_PR_RETRY_" + uuid.uuid4().hex[:10].upper()
    try:
        result = await best_payout(phone, entry.net_amount, bank, ref)
        await settle_fee(phone, entry.fee, ref)
        entry.status    = "completed"
        entry.provider  = result.get("provider")
        entry.reference = ref
        entry.error_msg = None
        entry.paid_at   = datetime.utcnow()
        entry.qreek_fee    = entry.fee
        entry.provider_fee = 0.0
        run.paid_count  = (run.paid_count or 0) + 1
        run.failed_count = max(0, (run.failed_count or 1) - 1)
    except Exception as e:
        entry.status    = "failed"
        entry.error_msg = str(e)[:200]
        await refund_company_wallet(db, co.id, entry.gross_amount)
        await db.commit()
        raise HTTPException(status_code=502, detail=f"Retry failed: {e}")

    # Record transaction
    tx = Transaction(
        user_phone=phone, tx_type="payroll_retry",
        currency="NGN", amount=entry.net_amount,
        ngn_amount=entry.net_amount, gross_amount=entry.gross_amount,
        qreek_fee=entry.qreek_fee, provider_fee=entry.provider_fee,
        net_amount=entry.net_amount, status=entry.status,
        provider=entry.provider, reference=entry.reference,
        payment_description=f"Payroll retry {run.period_label} — {entry.employee_name}",
    )
    db.add(tx)

    # Recalculate run status
    if run.failed_count == 0:
        run.status = "completed"
    elif run.failed_count > 0 and run.paid_count > 0:
        run.status = "partial"
    else:
        run.status = "failed"

    await db.commit()
    return {"message": f"Entry for {entry.employee_name} retried successfully.", "entry": _entry_dict(entry)}


@router.post("/runs/{run_id}/retry-failed")
async def retry_all_failed(
    run_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """Retry all failed entries in a run."""
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

    r   = await db.execute(select(PayrollRun).where(PayrollRun.id == run_id, PayrollRun.company_id == co.id))
    run = r.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")
    if run.status not in ("partial", "failed"):
        raise HTTPException(status_code=400, detail="No failed entries to retry.")

    er      = await db.execute(select(PayrollEntry).where(PayrollEntry.run_id == run_id, PayrollEntry.status == "failed"))
    failed  = er.scalars().all()
    if not failed:
        raise HTTPException(status_code=400, detail="No failed entries to retry.")

    results = {"success": 0, "failed": 0, "details": []}
    for entry in failed:
        try:
            await debit_company_wallet_or_reject(db, co.id, entry.gross_amount)
            bank = {"account_number": entry.bank_account, "bank_code": entry.bank_code}
            ref  = "QRK_PR_RETRY_" + uuid.uuid4().hex[:10].upper()
            result = await best_payout(phone, entry.net_amount, bank, ref)
            await settle_fee(phone, entry.fee, ref)
            entry.status    = "completed"
            entry.provider  = result.get("provider")
            entry.reference = ref
            entry.error_msg = None
            entry.paid_at   = datetime.utcnow()
            entry.qreek_fee    = entry.fee
            entry.provider_fee = 0.0
            run.paid_count  = (run.paid_count or 0) + 1
            run.failed_count = max(0, (run.failed_count or 1) - 1)
            tx = Transaction(
                user_phone=phone, tx_type="payroll_retry",
                currency="NGN", amount=entry.net_amount,
                ngn_amount=entry.net_amount, gross_amount=entry.gross_amount,
                qreek_fee=entry.qreek_fee, provider_fee=entry.provider_fee,
                net_amount=entry.net_amount, status=entry.status,
                provider=entry.provider, reference=entry.reference,
                payment_description=f"Payroll retry {run.period_label} — {entry.employee_name}",
            )
            db.add(tx)
            results["success"] += 1
            results["details"].append({"employee": entry.employee_name, "status": "completed"})
        except Exception as e:
            results["failed"] += 1
            entry.error_msg = str(e)[:200]
            try:
                await refund_company_wallet(db, co.id, entry.gross_amount)
            except Exception:
                pass
            results["details"].append({"employee": entry.employee_name, "status": "failed", "error": str(e)[:100]})
        db.add(entry)

    if run.failed_count == 0:
        run.status = "completed"
    elif run.paid_count > 0:
        run.status = "partial"
    else:
        run.status = "failed"

    await db.commit()
    return {"message": f"Retried {len(failed)} entries: {results['success']} succeeded, {results['failed']} failed.", **results}


# ── Company Wallet ────────────────────────────────────────────────────────────

class WalletDepositIn(BaseModel):
    amount: float


@router.post("/wallet/deposit")
async def deposit_to_company_wallet(
    body: WalletDepositIn,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """Create a Flutterwave checkout to fund the company wallet."""
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

    if body.amount <= 0:
        raise HTTPException(status_code=400, detail="Deposit amount must be greater than zero.")
    if body.amount > 10_000_000:
        raise HTTPException(status_code=400, detail="Maximum deposit is ₦10,000,000 per transaction.")

    ref = "QRK_WAL_" + uuid.uuid4().hex[:10].upper()
    tx  = Transaction(
        user_phone=phone, tx_type="wallet_deposit",
        currency="NGN", amount=body.amount, ngn_amount=body.amount,
        gross_amount=body.amount, qreek_fee=0.0, provider_fee=0.0,
        net_amount=body.amount, status="pending", reference=ref,
        payment_description=f"Company wallet deposit — {co.name}",
    )
    tx.event_metadata = {"company_id": co.id}
    db.add(tx)
    await db.flush()

    from services.flutterwave_service import initialize_checkout

    checkout = await initialize_checkout(
        tx_ref=ref, amount=body.amount,
        customer_name=co.name, customer_phone=phone,
        redirect_url=None,
        title=f"Fund {co.name} wallet",
        description=f"Deposit ₦{body.amount:,.2f} to {co.name} company wallet",
        metadata={"company_id": co.id, "tx_ref": ref},
    )

    tx.provider_checkout_url = checkout.get("data", {}).get("link")
    await db.commit()

    return {
        "checkout_url": tx.provider_checkout_url,
        "reference": ref,
        "message": "Proceed to Flutterwave checkout to fund your company wallet.",
    }


@router.get("/wallet/balance")
async def get_wallet_balance(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db), company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """Get the company wallet balance."""
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)
    return {"wallet_balance_ngn": co.wallet_balance_ngn or 0}


# ── CSV Export ────────────────────────────────────────────────────────────────

@router.get("/runs/{run_id}/export")
async def export_run_csv(
    run_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """Export payroll run entries as a downloadable CSV file."""
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

    r   = await db.execute(select(PayrollRun).where(PayrollRun.id == run_id, PayrollRun.company_id == co.id))
    run = r.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    er      = await db.execute(select(PayrollEntry).where(PayrollEntry.run_id == run_id).order_by(PayrollEntry.employee_name))
    entries = er.scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Employee", "Bank", "Account", "Gross", "Fee", "Net", "Status", "Reference", "Paid At", "Error"])
    for e in entries:
        writer.writerow([
            e.employee_name, e.bank_name, e.bank_account,
            e.gross_amount, e.fee, e.net_amount,
            e.status, e.reference or "",
            e.paid_at.isoformat() if e.paid_at else "",
            e.error_msg or "",
        ])

    output.seek(0)
    safe_name = run.period_label.replace(" ", "_")
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="payroll_{safe_name}_{run.id[:8]}.csv"'},
    )


# ── Payslip ───────────────────────────────────────────────────────────────────

@router.get("/runs/{run_id}/entries/{entry_id}/payslip")
async def get_payslip(
    run_id: str,
    entry_id: str,
    claims: dict = Depends(decode_token),
    db: AsyncSession = Depends(get_db),
    company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """Generate a payslip for a single payroll entry."""
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

    r   = await db.execute(select(PayrollRun).where(PayrollRun.id == run_id, PayrollRun.company_id == co.id))
    run = r.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")

    er   = await db.execute(select(PayrollEntry).where(PayrollEntry.id == entry_id, PayrollEntry.run_id == run_id))
    entry = er.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found.")

    return {
        "company": co.name,
        "company_rc": co.rc_number,
        "period": run.period_label,
        "run_id": run.id,
        "employee": {
            "name": entry.employee_name,
            "bank_name": entry.bank_name,
            "bank_account": "****" + entry.bank_account[-4:] if entry.bank_account else None,
        },
        "earnings": {
            "gross": entry.gross_amount,
            "fee": entry.fee,
            "net": entry.net_amount,
        },
        "status": entry.status,
        "reference": entry.reference,
        "paid_at": entry.paid_at.isoformat() if entry.paid_at else None,
    }


# ── Analytics ─────────────────────────────────────────────────────────────────

@router.get("/analytics")
async def get_analytics(claims: dict = Depends(decode_token), db: AsyncSession = Depends(get_db), company_id: Optional[str] = Header(None, alias="x-company-id"),
):
    """
    Retrieves high-level payroll analytics, including total disbursements, run history, and department spending.
    """
    phone = claims["phone"]
    co    = await _get_company(db, phone, company_id)

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


# ── Bank account verification ──────────────────────────────────────────────────

@router.get("/employees/verify-account")
async def verify_employee_account(
    account_number: str,
    bank_code: str,
    claims: dict = Depends(decode_token),
):
    """
    Resolves the account name for a given bank account number and bank code
    using Flutterwave's account resolution API. Returns the account name
    so the user can confirm it before saving the employee.
    """
    try:
        result = await resolve_account(account_number, bank_code)
        account_name = None
        if isinstance(result, dict):
            data = result.get("data") or result
            account_name = data.get("account_name") or data.get("accountNumberName") or None
        if not account_name:
            raise HTTPException(status_code=400, detail="Could not resolve account name. Verify the bank details.")
        return {"account_name": account_name}
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(status_code=400, detail=f"Account verification failed: {str(exc)[:200]}")


# ── Banks list ────────────────────────────────────────────────────────────────

@router.get("/banks")
async def list_banks():
    """
    Returns a list of supported banks for payroll disbursements.
    """
    return {"banks": [{"code": b["code"], "name": b["name"]} for b in BANKS]}
