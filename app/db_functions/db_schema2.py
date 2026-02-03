from sqlalchemy import (
    Column,
    String,
    Text,
    Boolean,
    Integer,
    ForeignKey,
    TIMESTAMP,
    UniqueConstraint,
    DateTime,
    Enum as SQLEnum, 
    event,
    BigInteger
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship,sessionmaker, Mapped
from sqlalchemy import create_engine
from datetime import datetime , timezone
from app.config import settings
import uuid
from sqlalchemy.orm import Session

import hashlib
from typing import Optional, List
 



 
import enum
# Base = declarative_base()

engine = create_engine(settings.DATABASE_URL,pool_size=50,       # The number of connections to keep open permanently
    max_overflow=10,    # How many extra connections can be created during spikes
    pool_timeout=30,    # Seconds to wait for a connection before failing
    pool_recycle=1800)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create base class for models
Base = declarative_base()

# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_utc_now_no_micro():
    return datetime.utcnow().replace(microsecond=0)

# -------------------
# TABLES
# -------------------

def generate_email_id(subject: str, received_time: datetime) -> str:
    """
    Generates a unique, deterministic ID based on the email's subject and 
    the received timestamp (to the microsecond).
    
    The hash ensures that the same email received multiple times will have 
    the same unique key.
    """
    # Ensure datetime is timezone-aware and normalized
    if received_time.tzinfo is None:
        received_time = received_time.replace(tzinfo=timezone.utc)
    
    # Format time with microseconds for better uniqueness
    timestamp_str = received_time.isoformat()
    # print("timestamp_str",timestamp_str)
    
    # Combine components and encode to bytes
    hash_input = f"{subject}|{timestamp_str}".encode('utf-8')
    
    # Return the SHA-256 hash (64 characters)
    return hashlib.sha256(hash_input).hexdigest()

class RawEmail(Base):
    __tablename__ = 'raw_emails'
    
    # Primary Key, generated from hash, CHAR(64) is efficient for SHA-256
    email_id = Column(String(64), primary_key=True, unique=True, index=True) 
    sender = Column(String, nullable=False)
    subject = Column(String, nullable=False)
    body = Column(Text, nullable=False)
    email_path = Column(String, nullable=False, comment="Local path to the saved .msg file")
    
    # datetime objects should always be timezone-aware (using UTC)
    received_at = Column(DateTime(timezone=False), nullable=False)
    
    # Tracking for when the record was inserted
    inserted_at = Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)
    status = Column(Boolean, default=True, nullable=False, comment="Indicates if the email is sent to the queue or not")
    
    # Relationships back to the processing tables (Lazy loading is default)
    segregation = relationship("SegregatedEmail", back_populates="raw_email", uselist=False, cascade="all, delete-orphan")
    summary = relationship("SummaryTable", back_populates="raw_email", uselist=False, cascade="all, delete-orphan")
    jira_entry = relationship("JiraEntry", back_populates="raw_email", uselist=False, cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<RawEmail(id='{self.email_id[:10]}...', subject='{self.subject[:30]}')>"

# --- 2. SegregatedEmail Table ---
class SegregatedEmail(Base):
    __tablename__ = 'segregated_email'
    
    # Foreign Key linking to RawEmails
    email_id = Column(String(64), ForeignKey('raw_emails.email_id', ondelete='CASCADE'), primary_key=True)
    priority = Column(String(50), nullable=True) # e.g., 'High', 'Medium', 'Low'
    type = Column(String(50), nullable=True)     # e.g., 'Alert', 'Notification', 'Info'
    resource_name = Column(String(255), nullable=True)
    trigger_name = Column(String(255), nullable=True)

    inserted_at = Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)
    status = Column(Boolean, default=True, nullable=False, comment="Indicates if segregation was successful and sent to the queue")
    
    # Relationship to the parent RawEmail
    raw_email = relationship("RawEmail", back_populates="segregation")
    
    def __repr__(self):
        return f"<SegregatedEmail(id='{self.email_id[:10]}...', type='{self.type}')>"

# --- 3. SummaryTable ---
class SummaryTable(Base):
    __tablename__ = 'summary_table'
    
    # Foreign Key linking to RawEmails
    email_id = Column(String(64), ForeignKey('raw_emails.email_id', ondelete='CASCADE'), primary_key=True)
    summary = Column(Text, nullable=False, comment="Text Blob for the AI-generated summary")
    
    inserted_at = Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)
    status = Column(Boolean, default=True, nullable=False, comment="Indicates if summarization was successful and sent to the queue")
    
    # Relationship to the parent RawEmail
    raw_email = relationship("RawEmail", back_populates="summary")
    
    def __repr__(self):
        return f"<SummaryTable(id='{self.email_id[:10]}...', summary_len={len(self.summary)})>"

# --- 4. JiraEntry Table ---
class JiraEntry(Base):
    __tablename__ = 'jira_table'
    
    # Auto-incrementing Primary Key
    jira_id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Foreign Key linking to RawEmails
    email_id = Column(String(64), ForeignKey('raw_emails.email_id', ondelete='CASCADE'), nullable=False, index=True)
    
    jiraticket_id = Column(String(50), unique=True, nullable=False) # e.g., 'PROJ-1234'
    assigned_to = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=False), nullable=False, comment="JIRA creation time")
    teams_flag = Column(String(10), nullable=True, default='false', comment="Teams notification sent status")
    teams_channel = Column(String(100), nullable=True, comment="Teams channel the notification was sent to")
    inserted_at = Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)
    
    # Relationship to the parent RawEmail
    raw_email = relationship("RawEmail", back_populates="jira_entry")
    
    def __repr__(self):
        return f"<JiraEntry(id='{self.jira_id}', ticket='{self.jiraticket_id}')>"

# --- 5. JobTable (Standalone) ---
class JobTable(Base):
    __tablename__ = 'job_table'
    
    # Auto-incrementing Primary Key
    job_id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    job_start_time = Column(DateTime(timezone=False), nullable=True)
    job_end_time = Column(DateTime(timezone=False), nullable=True)
    last_run_time = Column(DateTime(timezone=False), nullable=True)
    frequency = Column(String(50), nullable=False, comment="e.g., 'hourly', 'daily', 'every 5 minutes'")
    
    # Added optional name for clarity
    job_name = Column(String(100), unique=True, nullable=True) 

    inserted_at = Column(DateTime(timezone=False), default=lambda: datetime.now(), nullable=False)
    
    def __repr__(self):
        return f"<JobTable(id='{self.job_id}', name='{self.job_name}')>"

class DuplicateEmail(Base):
    __tablename__ = "duplicate_emails"

    email_id = Column(String ,nullable=False)
    duplicate_email_id= Column(String, primary_key=True) # hash instead of UUID
    subject = Column(String)
    body = Column(Text)
    sender = Column(String)
    received_at = Column(TIMESTAMP(timezone=False))
    inserted_at = Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)


class Emails(Base):
    __tablename__ = "emails"

    email_id = Column(String, primary_key=True)  # hash
    subject = Column(String)
    body = Column(Text)
    sender = Column(String)
    received_at = Column(TIMESTAMP(timezone=True))
    inserted_at = Column(TIMESTAMP(timezone=True))
    status = Column(String)

    processing = relationship("EmailProcessing", back_populates="email", cascade="all, delete-orphan")


class EmailProcessing(Base):
    __tablename__ = "email_processing"

    process_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email_id = Column(String, ForeignKey("emails.email_id", ondelete="CASCADE"))
    classification_result = Column(String)
    processed_at = Column(TIMESTAMP(timezone=True))
    machine_details = Column(String)
    extracted_details = Column(String)
    category = Column(String)
    priority = Column(String)
    triggername = Column(String)

    email = relationship("Emails", back_populates="processing")


class ErrorCodeMapping(Base):
    __tablename__ = "error_code_mapping"

    error_code = Column(String, primary_key=True)
    machine = Column(String)
    description = Column(String)


class JiraTickets(Base):
    __tablename__ = "jira_tickets"

    jira_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    jira_ticket_id = Column(String)
    machine = Column(String)
    created_at = Column(TIMESTAMP(timezone=True))
    priority = Column(String)


class TriggerList(Base):
    __tablename__ = "trigger_list"

    triggername = Column(String, primary_key=True)
    category = Column(String)
    actionable = Column(Boolean, default=False)
    priority = Column(String)
    enabled = Column(Boolean, default=True)

class Configuration(Base):
    __tablename__ = "configuration"
 
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True, nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id"))
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    job_frequency = Column(Integer, nullable=False)
    outlook_email = Column(String, nullable=False)
    jira_base_url = Column(String, nullable=False)
    jira_api_token = Column(String, nullable=False)
    #teams_webhook = Column(String, nullable=False)
 
    # relationship
    user = relationship("User", back_populates="configs")
class User(Base):
    __tablename__ = "users"
 
    user_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True, nullable=False)
    username = Column(String, nullable=False, unique=True)
    email_id = Column(String, nullable=False, unique=True)
    password = Column(String, nullable=False)
    role = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)
    created_by = Column(String, nullable=False)
 
    # relationships
    configs = relationship("Configuration", back_populates="user")
    notifications = relationship("Notification", back_populates="user")

class Notification(Base):
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True, unique=True, nullable=False)
    text = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.now)
    read = Column(Boolean, nullable=False, default=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.user_id'))

    user = relationship("User", back_populates="notifications")




class Config(Base): 
    """SQLAlchemy model for the config table."""
    __tablename__ = "config"
    
    job_name = Column(String, primary_key=True, index=True)
    interval_unit = Column(String, nullable=False)
    interval_value = Column(Integer, nullable=False)


class JobExecution(Base):
    """SQLAlchemy model for the job_execution table."""
    __tablename__ = "job_execution"
    
    job_id = Column(Integer, primary_key=True, autoincrement=True)
    job_interval = Column(String)
    period_from = Column(DateTime)
    period_to = Column(DateTime)
    execution_end_time = Column(DateTime)


# --- 1. Machine Hierarchy Table ---
class MachineHierarchy(Base):
    __tablename__ = "machine_hierarchy"
    
    parent_id = Column(String, primary_key=True)
    child_id = Column(String, primary_key=True)
    
# --- 2. Maintenance Alert Log Table ---
class MaintenanceAlert(Base):
    __tablename__ = "maintenance_alerts"
    
    alert_id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(String, unique=True, index=True, nullable=False)
    
    # Store times as timezone-aware
    start_time_utc = Column(DateTime(timezone=True), nullable=False)
    end_time_utc = Column(DateTime(timezone=True), nullable=False)
    updated_at_utc = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    status = Column(String)
    
    source = Column(String)

class Server(Base):
    """
    Database model for storing server/computer information, grouped by function.
    """
    __tablename__ = "servers"

    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    computername: Mapped[str] = Column(String, index=True, nullable=False)
    group: Mapped[str] = Column(String, index=True, nullable=False)
    description_function: Mapped[str] = Column(String, nullable=True)
    responsible_person: Mapped[str] = Column(String, nullable=True)

    def __repr__(self):
        return f"<Server(computername='{self.computername}', group='{self.group}')>"
    

class MaintenanceStatus(enum.Enum):
    SCHEDULED = "Scheduled"
    ONGOING = "Ongoing"
    COMPLETED = "Completed"

class ParentChildRelationship(Base):
    __tablename__ = "parent_child_relationships"
 
    # SQLAlchemy MUST have a PK → use composite PK (not unique constraint!)
    parent: Mapped[str] = Column(String, primary_key=True)
    child: Mapped[str] = Column(String, primary_key=True)
 
class Maintenance(Base):
    __tablename__ = "maintenance_windows"
    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    server_group: Mapped[str] = Column(String, nullable=False)
    server_name: Mapped[Optional[str]] = Column(String, nullable=True)
    other_server: Mapped[Optional[str]] = Column(String, nullable=True)
   
    comments: Mapped[Optional[str]] = Column(String, nullable=True)
   
    start_datetime: Mapped[datetime] = Column(DateTime, nullable=False)
    end_datetime: Mapped[datetime] = Column(DateTime, nullable=False)
   
    status: Mapped[str] = Column(SQLEnum(MaintenanceStatus),
                                     default=MaintenanceStatus.SCHEDULED,
                                     nullable=False)
   
    created_at: Mapped[datetime] = Column(DateTime, default=get_utc_now_no_micro, nullable=False)
    updated_at: Mapped[datetime] = Column(DateTime, default=get_utc_now_no_micro, onupdate=get_utc_now_no_micro, nullable=False)
 
    def __repr__(self):
        return f"<Maintenance(id={self.id}, group='{self.server_group}', server='{self.server_name}', status='{self.status}')>"
   
    # children = relationship(
    #     "ParentChildRelationship",
    #     primaryjoin="Maintenance.server_name == ParentChildRelationship.parent",
    #     viewonly=True,
    # )
 
    # # Servers for which this server is a child
    # parents = relationship(
    #     "ParentChildRelationship",
    #     primaryjoin="Maintenance.server_name == ParentChildRelationship.child",
    #     viewonly=True,
    # )
 
@event.listens_for(Maintenance, 'load')
def update_maintenance_status(target, context):
    now = datetime.utcnow()
   
    if target.status == MaintenanceStatus.COMPLETED.value:
        return
       
    if target.start_datetime <= now and target.end_datetime >= now:
        target.status = MaintenanceStatus.ONGOING.value
    elif target.end_datetime < now:
        target.status = MaintenanceStatus.COMPLETED.value
    elif target.start_datetime > now:
        target.status = MaintenanceStatus.SCHEDULED.value



 
    # Relationships to Maintenance table
    # parent_maintenance = relationship(
    #     "Maintenance",
    #     primaryjoin="ParentChildRelationship.parent == Maintenance.server_name",
    #     viewonly=True,
    # )
 
    # child_maintenance = relationship(
    #     "Maintenance",
    #     primaryjoin="ParentChildRelationship.child == Maintenance.server_name",
    #     viewonly=True,
    # )
 
    def __repr__(self):
        return f"<ParentChild(parent='{self.parent}', child='{self.child}')>"


# ==============================================================================
# NEW: TriggerMapping Table for Trigger-Based Channel Routing
# ==============================================================================
class TriggerMapping(Base):
    """
    Maps trigger names from ControlUp alerts to Teams channels.
    Loaded from Excel spreadsheet (ControlUp Trigger Details.xlsx).
    """
    __tablename__ = "trigger_mappings"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    trigger_name = Column(String(500), nullable=False, index=True)
    category = Column(String(100), nullable=True)
    priority = Column(String(50), nullable=True)
    actionable = Column(String(50), nullable=True)
    recommended_action = Column(Text, nullable=True)
    team = Column(String(100), nullable=False, index=True)  # Maps to Teams channel
    department = Column(String(100), nullable=True)
    responsible_persons = Column(String(255), nullable=True)
    
    
    
    def __repr__(self):
        return f"<TriggerMapping(trigger='{self.trigger_name[:40]}...', team='{self.team}')>"


def populate_trigger_mappings_from_excel(excel_path: str, db: Session = None) -> int:
    """
    Load trigger mappings from Excel file into the trigger_mappings table.
    
    Expected Excel columns:
    - TriggerName (or Trigger Name)
    - Category
    - Priority
    - Actionable
    - Recommended Action
    - Team
    - Department
    
    Args:
        excel_path: Path to the Excel file
        db: Optional database session (will create one if not provided)
    
    Returns:
        Number of rows inserted
    """
    try:
        import pandas as pd
    except ImportError:
        print("ERROR: pandas is required. Install with: pip install pandas openpyxl")
        return 0
    
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    
    try:
        # Read Excel file
        print(f"📖 Reading Excel file: {excel_path}")
        df = pd.read_excel(excel_path)
        
        # Normalize column names (handle variations)
        df.columns = df.columns.str.strip()
        column_mapping = {
            'TriggerName': 'trigger_name',
            'Category': 'category',
            'Priority': 'priority',
            'Informational/Actionable': 'actionable',
            'Recommended Actions': 'recommended_action',
            'Team': 'team',
            'Department': 'department',
            'Responsible Person': 'responsible_persons',
        }
        
        df = df.rename(columns=column_mapping)
        
        # Ensure required columns exist
        if 'trigger_name' not in df.columns:
            print("ERROR: Excel must have 'TriggerName' or 'Trigger Name' column")
            return 0
        if 'team' not in df.columns:
            print("ERROR: Excel must have 'Team' column")
            return 0
        
        # Clean data

        df['trigger_name'] = df['trigger_name'].astype(str).str.strip()
        df['team'] = df['team'].astype(str).str.strip()
        
        # Clear existing mappings
        deleted = db.query(TriggerMapping).delete()
        print(f"🗑️  Cleared {deleted} existing trigger mappings")
        
        # Insert new mappings
        count = 0
        for _, row in df.iterrows():
            mapping = TriggerMapping(
                trigger_name=row['trigger_name'],
                category=str(row.get('category', '')).strip() if pd.notna(row.get('category')) else None,
                priority=str(row.get('priority', '')).strip() if pd.notna(row.get('priority')) else None,
                actionable=str(row.get('actionable', '')).strip() if pd.notna(row.get('actionable')) else None,
                recommended_action=str(row.get('recommended_action', '')).strip() if pd.notna(row.get('recommended_action')) else None,
                team=row['team'],
                department=str(row.get('department', '')).strip() if pd.notna(row.get('department')) else None,
                responsible_persons=str(row.get('responsible_persons', '')).strip().lower() if pd.notna(row.get('responsible_persons')) else None,
            )
            db.add(mapping)
            count += 1
        
        db.commit()
        print(f"✅ Loaded {count} trigger mappings from Excel")
        
        # Print summary by team
        team_counts = df['team'].value_counts()
        print("\n📊 Mappings by Team:")
        for team, cnt in team_counts.items():
            print(f"   {team}: {cnt}")
        
        return count
        
    except Exception as e:
        print(f"ERROR loading Excel: {e}")
        db.rollback()
        return 0
    finally:
        if close_db:
            db.close()


def get_all_trigger_mappings(db: Session = None) -> List[TriggerMapping]:
    """Get all trigger mappings from database."""
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    
    try:
        return db.query(TriggerMapping).all()
    finally:
        if close_db:
            db.close()


def create_tables(engine):
    print("Initializing database with SQLAlchemy...")
    Base.metadata.create_all(bind=engine)
    
    # Use a session to add the default config
    db: Session = SessionLocal()
    # db = next(get_db())
    try:
        # Check if default config already exists
        default_job = db.query(Config).filter(Config.job_name == 'hello_job').first()
        
        if not default_job:
            print("Adding default config: ('hello_job', 'minutes', 10)")
            default_config = Config(
                job_name='hello_job',
                interval_unit='minutes',
                interval_value=10
            )
            db.add(default_config)
            db.commit()
        else:
            print("Default config already exists.")
            
    except Exception as e:
        print(f"Error during DB initialization: {e}")
        db.rollback()
    finally:
        db.close()
        
    print("Database initialized successfully.")



create_tables(engine)


# ==============================================================================
# CLI Support: python db_schema2.py --setup "path/to/excel.xlsx"
# ==============================================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Database schema management")
    parser.add_argument("--setup", metavar="EXCEL_PATH", 
                        help="Path to Excel file to populate trigger_mappings table")
    
    args = parser.parse_args()
    
    if args.setup:
        print("=" * 60)
        print("🚀 Setting up Trigger Mappings from Excel")
        print("=" * 60)
        populate_trigger_mappings_from_excel(args.setup)
    else:
        print("Database tables created/verified.")
        print("Usage: python db_schema2.py --setup 'ControlUp Trigger Details.xlsx'")
from sqlalchemy import select
from sqlalchemy.orm import Session

# Assuming 'Server' is the class defined in your snippet

def get_server_by_name(session: Session, target_name: str) -> Server | None:
    """
    Retrieves a Server row by computername. 
    Returns the Server object if found, otherwise returns None.
    """
    # 1. Construct the select statement
    statement = select(Server).where(Server.computername == target_name)
    
    # 2. Execute the query
    result = session.execute(statement)
    
    # 3. Return the single scalar result or None if empty
    return result.scalar_one_or_none()