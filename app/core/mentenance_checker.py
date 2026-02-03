from app.db_functions.db_schema2 import get_db, SessionLocal,MachineHierarchy,MaintenanceAlert
from sqlalchemy.orm import Session,sessionmaker
from typing import Dict, Optional, Generator, Tuple
from sqlalchemy import create_engine, Column, Integer, String, DateTime, select



db = next(get_db())
class MaintenanceChecker:
    """
    A class to encapsulate all logic for checking machine maintenance status 
    and hierarchy directly against the database.
    """
    
    def __init__(self):
        # Database connection is handled by SessionLocal
        self.blocking_statuses = ['ONGOING']

    def get_db_session(self) -> Generator[Session, None, None]:
        """Generator to provide and safely close a database session."""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def _get_parent_id(self, db: Session, machine_id: str) -> Optional[str]:
        """QUERIES THE DATABASE for the immediate parent ID."""
        if not machine_id:
            return None
            
        # SQL: SELECT parent_id FROM machine_hierarchy WHERE child_id = :machine_id
        stmt = select(MachineHierarchy.parent_id).where(
            MachineHierarchy.child_id == machine_id
        )
        
        # Executes the query and returns the parent ID string or None
        return db.execute(stmt).scalar_one_or_none()

    def _is_machine_under_maintenance(self, db: Session, machine_id: str) -> bool:
        """
        Checks the MaintenanceAlerts table for a given machine_id with an 'ONGOING' status.
        """
        if not machine_id:
            return False
            
        # SQL: SELECT * FROM maintenance_alerts WHERE machine_id = :id AND status = 'ONGOING'
        stmt = select(MaintenanceAlert).where(
            MaintenanceAlert.machine_id == machine_id,
            MaintenanceAlert.status.in_(self.blocking_statuses)
        )
        
        # If a row is found, maintenance is ongoing.
        return db.execute(stmt).scalar_one_or_none() is not None

    def check_maintenance_status(self, machine_id: str) -> Tuple[bool, str]:
        """
        Performs the required two-tier check: Child -> Parent.
        
        Returns: (is_blocked, blocking_entity_id)
        """
        if not machine_id:
            return True, "INVALID_INPUT"
            
        # Use the context manager to open and automatically close the session
        db: Session = next(self.get_db_session())
        
        try:
            # 1. Check the immediate machine (Child)
            if self._is_machine_under_maintenance(db, machine_id):
                print(f"🛑 BLOCKED: Machine {machine_id} has an ONGOING maintenance alert.")
                return True, machine_id
                
            # 2. Get the Parent ID (Executing a DB Query here for hierarchy)
            parent_id = self._get_parent_id(db, machine_id) 
            
            # 3. Check the Parent's Status (If parent exists)
            if parent_id:
                if self._is_machine_under_maintenance(db, parent_id):
                    print(f"🛑 BLOCKED: Parent {parent_id} of machine {machine_id} has an ONGOING maintenance alert.")
                    return True, parent_id
            
            # 4. Unblocked
            print(f"✅ UNBLOCKED: Neither {machine_id} nor its parent is currently blocked.")
            return False, ""
            
        except Exception as e:
            print(f"ERROR: Failed to check maintenance status for {machine_id} (Queries Failed). {e}")
            # Return False to avoid system disruption on DB error
            return False, "DB_ERROR"