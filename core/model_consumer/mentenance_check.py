import httpx
from datetime import datetime
from sqlalchemy.orm import Session
from app.db_functions.db_schema2 import Maintenance as maintenance_windows, ParentChildRelationship as parent_child_relationships, SessionLocal  # your models
# from app.db_functions.db_schema2 import SessionLocal               # your sessionLocal
from app.config import settings                      # your config
from app.logging.logging_config import model_logger
from app.logging.logging_decorator import log_function_call

class MaintenanceWindowService:
    def __init__(self):
        pass
    


    
    # ------------ LOGIC FUNCTION -----------------
    @log_function_call(model_logger)
    def is_in_maintenance(self, machine_name: str) -> bool:
        """
        Returns True if machine OR its parent has an 'ongoing' maintenance window table.
        Returns False otherwise.
        """

        db = SessionLocal()
        try:
            machines_to_check = set()

            # 1. Always check the machine itself
            machines_to_check.add(machine_name)

            # 2. Check if machine has a parent
            parent = (
                db.query(parent_child_relationships.parent)
                .filter(parent_child_relationships.child== machine_name)
                .first()
            )

            if parent:
                parent_name = parent[0]
                machines_to_check.add(parent_name)
            print("machine to check ", machines_to_check)

            # 3. Query maintenance windows for all machines
            mw_rows = (
                db.query(maintenance_windows)
                .filter(maintenance_windows.server_name.in_(machines_to_check))
                .filter(maintenance_windows.status=='ONGOING')
                .all()
            )

            if len(mw_rows)!=0:
                return True
            else:
                return False

            # 4. If any machine has status "ongoing" → True
            # for row in mw_rows:
            #     print("rows ",row)
            #     print("row",row.status)
            #     if row.status.lower() == "ongoing":
            #         print("row.status.lower()",row.status.lower())
            #         return True

            # return False

        finally:
            db.close()
            print("closing the db connection in mentenance_check file")

# ---------------------------
# ✔ MAIN FUNCTION FOR TESTING
# ---------------------------
def main():
    service = MaintenanceWindowService()

    # Ask for machine from command line
    machine = input("Enter machine name: ").strip()
    db = SessionLocal()
    in_mw = service.is_in_maintenance(machine)
    t = db.query(maintenance_windows).all()
    print("t the rows",t)


    db.close()


    if in_mw:
        print(f"⚠ Machine '{machine}' IS in maintenance window.")
    else:
        print(f"✅ Machine '{machine}' is NOT in maintenance window.")


# Only runs when executed directly: python maintenance_service.py
if __name__ == "__main__":
    main()
    
