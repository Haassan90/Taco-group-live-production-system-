# =====================================================
# 🔒 erpnext_sync.py – FINAL LOCKED
# ERPNext Production Integration (Locked, Safe, Async Ready)
# =====================================================

import os
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

from typing import List, Dict
from database import SessionLocal
from models import Machine, ERPNextMetadata

# =====================================================
# Logging Configuration
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# =====================================================
# Load Environment Variables
# =====================================================
load_dotenv()
ERP_URL = os.getenv("ERP_URL")
API_KEY = os.getenv("ERP_API_KEY")
API_SECRET = os.getenv("ERP_API_SECRET")
TIMEOUT = int(os.getenv("ERP_TIMEOUT", 20))

HEADERS = {
    "Authorization": f"token {API_KEY}:{API_SECRET}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# =====================================================
# Fetch Active Work Orders from ERPNext
# =====================================================
def get_work_orders() -> List[Dict]:
    if not ERP_URL or not API_KEY or not API_SECRET:
        logging.error("ERPNext credentials missing")
        return []

    url = f"{ERP_URL}/api/resource/Work Order"
    params = {
        "fields": (
            '["name","qty","produced_qty","status",'
            '"custom_machine_id","custom_pipe_size","custom_location"]'
        ),
        "filters": '[["status","in",["Not Started","In Process"]]]'
    }

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json().get("data", []) or []
        logging.info(f"Fetched {len(data)} work orders from ERPNext")
        return data
    except Exception as e:
        logging.error(f"ERP fetch error: {e}")
        return []

# =====================================================
# Update ERP Work Order Status
# =====================================================
def update_work_order_status(erp_work_order_id: str, status: str):
    if not erp_work_order_id:
        return
    try:
        url = f"{ERP_URL}/api/resource/Work Order/{erp_work_order_id}"
        requests.put(
            url,
            json={"status": status},
            headers=HEADERS,
            timeout=TIMEOUT
        ).raise_for_status()
        logging.info(f"ERP Work Order {erp_work_order_id} → {status}")
    except Exception as e:
        logging.error(f"ERP status update failed: {e}")

# =====================================================
# Auto-Assign ERP Work Orders to Machines
# =====================================================
def auto_assign_work_orders() -> None:
    db = SessionLocal()
    try:
        work_orders = get_work_orders()
        if not work_orders:
            return

        for wo in work_orders:
            wo_name = wo.get("name")
            wo_status = wo.get("status")
            if wo_status == "In Process":
                continue
            if wo.get("custom_machine_id"):
                continue

            existing = db.query(Machine).filter(Machine.erpnext_work_order_id == wo_name).first()
            if existing:
                continue

            location = wo.get("custom_location")
            pipe_size = wo.get("custom_pipe_size")
            qty = wo.get("qty", 0)
            produced = wo.get("produced_qty", 0)

            free_machines = db.query(Machine).filter(
                Machine.location == location,
                Machine.is_locked == False,
                Machine.status.in_(["free", "paused", "stopped"])
            ).all()

            if not free_machines:
                continue

            selected_machine = next((m for m in free_machines if m.pipe_size == pipe_size), None)
            if not selected_machine:
                selected_machine = free_machines[0]

            # Assign work order
            selected_machine.erpnext_work_order_id = wo_name
            selected_machine.work_order = wo_name
            selected_machine.pipe_size = pipe_size
            selected_machine.target_qty = qty
            selected_machine.produced_qty = produced
            selected_machine.status = "paused"
            selected_machine.is_locked = True

            meta = db.query(ERPNextMetadata).filter(ERPNextMetadata.work_order == wo_name).first()
            if not meta:
                meta = ERPNextMetadata(
                    machine_id=selected_machine.id,
                    work_order=wo_name,
                    erp_status="Assigned",
                    last_synced=datetime.now()
                )
                db.add(meta)
            else:
                meta.machine_id = selected_machine.id
                meta.erp_status = "Assigned"
                meta.last_synced = datetime.now()

            db.commit()
            logging.info(f"Assigned ERP WO {wo_name} → Machine {selected_machine.name}")

    except SQLAlchemyError as e:
        db.rollback()
        logging.error(f"DB error: {e}")
    except Exception as e:
        db.rollback()
        logging.error(f"Auto-assign error: {e}")
    finally:
        db.close()
