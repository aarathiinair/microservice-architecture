import time
import logging
from datetime import datetime, timedelta, date
import pytz
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import create_engine, Column, Integer, String, Date, DateTime, ForeignKey, Enum
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.exc import SQLAlchemyError

# Mock imports for context (Replace with your actual imports)
from app.core.certificate_watcher.certificate_jira_integration import CertificateJiraIntegration
from app.core.certificate_watcher.certificate_teams_integration import CertificateTeamsIntegration

# ==========================================
# CONFIGURATION & SETUP
# ==========================================
DATABASE_URL = "postgresql://postgres:Admin@localhost:5432/email_processor_db"
GERMANY_TZ = pytz.timezone('Europe/Berlin')

# Setup Logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

Base = declarative_base()

# ==========================================
# DATABASE MODELS
# ==========================================
class Certificates(Base):
    __tablename__ = 'certificates'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    certificate_name = Column(String, unique=True, nullable=False)
    description = Column(String, unique=False, nullable=True)
    usage = Column(String, unique=False, nullable=True)
    expiration_date = Column(DateTime, nullable=False) 
    created_at=Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)
    updated_at=Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)
    effected_users=Column(String, unique=False, nullable=True)
    responsible_group = Column(String, nullable=False)
    teams_channel = Column(String, nullable=False)
    calculated_status = Column(String) # 'ACTIVE', 'EXPIRING SOON', 'EXPIRED'

class JiraState(Base):
    __tablename__ = 'jira_state'
    
    # CHANGE 1: Primary Key is now the Jira Ticket ID
    jira_ticket_id = Column(String, primary_key=True) 
    
    # Foreign key link (optional, but good practice)
    certificate_name = Column(String, nullable=False) # ForeignKey('Certificates.certificate_name') need to be added in bug free way
    
    # CHANGE 2: Added Expiration Date to track specific renewal cycles
    expiration_date = Column(DateTime, nullable=False)
    
    ticket_created_on = Column(DateTime, default=datetime.utcnow)

# Initialize DB
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

# ==========================================
# CORE LOGIC
# ==========================================

def update_certificate_status(cert, days_diff):
    """Updates the status field in the DB based on days remaining."""
    new_status = "ACTIVE"
    if days_diff < 0:
        new_status = "EXPIRED"
    elif days_diff <= 14:
        new_status = "EXPIRING_SOON"
    
    if cert.calculated_status != new_status:
        cert.calculated_status = new_status
        return True 
    return False

async def check_certificates():
    session = SessionLocal()
    
    try:
        # 1. Standardize Time (Germany/Berlin)
        now_germany = datetime.now(GERMANY_TZ)
        today_date = now_germany.date()
        
        logger.info(f"--- Starting Daily Run: {today_date} ---")

        # ---------------------------------------------------------
        # CLEANUP PHASE
        # ---------------------------------------------------------
        # Remove JiraState entries if the certificate has been renewed.
        # Logic: If we have a ticket, but the certificate's CURRENT expiration date 
        # is significantly in the future (>16 days), that ticket is for an old cert.
        

        # ---------------------------------------------------------
        # MAIN PROCESSING LOOP
        # ---------------------------------------------------------
        certificates = session.query(Certificates).all()
        
        for cert in certificates:
            try:
                # FIX 2: Date Calculation Crash
                # Convert DB DateTime to Python Date before subtraction
                expiry_date_obj = cert.expiration_date.date()
                days_diff = (expiry_date_obj - today_date).days
                
                logger.info(f"Checking '{cert.certificate_name}': Expires {expiry_date_obj} ({days_diff} days left)")

                # Update Status
                update_certificate_status(cert, days_diff)

                # Initialize ticket_id as None for this iteration
                ticket_id = None
                
                # --- LOGIC RULE 1: JIRA TICKET MANAGEMENT ---
                if  days_diff < 14:
                    # FIX 3: Robust Ticket Retrieval
                    # Check if a ticket exists for THIS specific certificate AND THIS specific expiration date
                    existing_state = session.query(JiraState).filter_by(
                        certificate_name=cert.certificate_name,
                        expiration_date=cert.expiration_date # Matches specific cycle
                    ).first()
                    
                    if existing_state:
                        # CASE A: Ticket already exists. Retrieve ID for later use.
                        ticket_id = existing_state.jira_ticket_id
                        logger.info(f"  -> Found active ticket: {ticket_id}")
                    else:
                        # CASE B: No ticket exists for this expiration cycle. Create one.
                        try:
                            # External Calls
                            certificate_details = {
                                "certificate_name": cert.certificate_name,
                                "description": cert.description if cert.description is not None else "N/A",
                                "status": cert.calculated_status,
                                "responsible_group": cert.responsible_group,
                                "team_channel": cert.teams_channel,
                                "expiration_timestamp": cert.expiration_date
                            }

                            jira = CertificateJiraIntegration()
                            ticket_id = jira.create_ticket_sync(certificate_details)
                            
                            # Persist State to DB (New Schema)
                            new_state = JiraState(
                                jira_ticket_id=ticket_id, # PK
                                certificate_name=cert.certificate_name,
                                expiration_date=cert.expiration_date, # Tracking the cycle
                                ticket_created_on=datetime.utcnow()
                            )
                            session.add(new_state)
                            session.commit() # Commit immediately so ID is saved
                            logger.info(f"  -> Created new Jira Ticket {ticket_id}")
                        except Exception as e:
                            logger.error(f"  [ERROR] Failed to create Jira ticket: {e}")
                            ticket_id = None # Ensure it's None if creation failed

                # --- LOGIC RULE 2: TEAMS NOTIFICATIONS ---
                notification_days = [14, 7, 3, 2, 1]
                
                # Only proceed if it is a notification day OR it is expired
                if days_diff in notification_days or days_diff < 1:
                    
                    # Logic to craft the message
                    if days_diff < 0:
                        msg_type = "URGENT (EXPIRED)"
                    elif days_diff == 0:
                        msg_type = "URGENT (TODAY)"
                    else:
                        msg_type = "WARNING"
                    
                    logger.info(f"  -> Preparing Teams Alert: {msg_type}")

                    try:
                        teams = CertificateTeamsIntegration()
                        # FIX 4: Safe variable usage
                        # We pass ticket_id. If it's None (creation failed or logic skipped), the integration handles it (or we pass "N/A")
                        safe_ticket_id = ticket_id if ticket_id else "N/A"
                        
                        # Re-pack details if needed
                        details = {
                            "certificate_name": cert.certificate_name,
                            "expiration_timestamp": cert.expiration_date,
                            "description": cert.description  if cert.description is not None  else "N/A" ,
                            "status": cert.calculated_status,
                            "responsible_group": cert.responsible_group,
                            "team_channel": cert.teams_channel,
                        }
                        
                        await teams.send_notification(details, safe_ticket_id)
                        logger.info(f"  -> Sent Teams notification (Ticket: {safe_ticket_id})")
                    except Exception as e:
                        logger.error(f"  [ERROR] Failed to send Teams alert: {e}")

            except Exception as inner_e:
                logger.error(f"Error processing specific certificate {cert.certificate_name}: {inner_e}")
                continue 

        session.commit()
        logger.info("--- Daily Run Complete ---")

    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
        session.rollback()
    finally:
        session.close()

# ==========================================
# SCHEDULER (Unchanged)
# ==========================================
async def certificate_main():
    scheduler = AsyncIOScheduler(timezone=GERMANY_TZ)
    scheduler.add_job(check_certificates, 'date', id='immediate_run', run_date=datetime.now(GERMANY_TZ) + timedelta(seconds=5))
    scheduler.add_job(check_certificates, 'cron', hour=8, minute=0)
    scheduler.start()
    print("Scheduler started...")
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        pass
