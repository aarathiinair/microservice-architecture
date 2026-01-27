from datetime import datetime
from sqlalchemy.orm import Session
from app.db_functions.db_schema2 import Maintenance, MaintenanceStatus
from app.db_functions.db_schema2 import SessionLocal   # your existing sessionLocal

def create_maintenance_entry(
    server_group: str,
    server_name: str | None,
    other_server: str | None,
    comments: str | None,
    start_dt: datetime,
    end_dt: datetime,
    status: MaintenanceStatus = MaintenanceStatus.SCHEDULED
):
    db: Session = SessionLocal()
    try:
        entry = Maintenance(
            server_group=server_group,
            server_name=server_name,
            other_server=other_server,
            comments=comments,
            start_datetime=start_dt,
            end_datetime=end_dt,
            status=status
        )

        db.add(entry)
        db.commit()
        db.refresh(entry)

        return entry
    
    except Exception as e:
        db.rollback()
        raise e
    
    finally:
        db.close()

from datetime import datetime, timedelta
# from models import MaintenanceStatus

entry = create_maintenance_entry(
    server_group="OI-RDA",
    server_name="DKSGD16710",
    other_server=None,
    comments="Routine patching",
    start_dt=datetime.utcnow() - timedelta(hours = 1),
    end_dt=datetime.utcnow() + timedelta(hours=2),
    status=MaintenanceStatus.COMPLETED   # or SCHEDULED
)

print("Created ID:", entry.id)

