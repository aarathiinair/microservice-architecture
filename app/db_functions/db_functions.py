from datetime import datetime
from sqlalchemy.orm import Session
from app.db_functions.db_schema2 import get_db,Emails , EmailProcessing, JiraTickets, ErrorCodeMapping, TriggerList, User, Config, DuplicateEmail,JobExecution
from typing import Dict, Any, Optional
from sqlalchemy import select, update
from app.db_functions.db_schema2 import RawEmail, SegregatedEmail, SummaryTable, JiraEntry, JobTable, generate_email_id, Configuration
from datetime import datetime, timedelta
from sqlalchemy import and_
from app.config import settings

# ---------------- Emails ----------------
def insert_email(db: Session, subject: str, body: str, sender: str,
                 status: str = None, category: str = None,
                 priority: str = None, trigger: str = None):
    email = Emails(
        subject=subject,
        body=body,
        sender=sender,
        received_at=datetime.utcnow(),
        saved_at=datetime.utcnow(),
        status=status,
        category=category,
        priority=priority,
        trigger=trigger
    )
    db.add(email)
    db.commit()
    db.refresh(email)
    return email


# ---------------- EmailProcessing ----------------
def insert_email_processing(db: Session, email_id, classification_result: str,
                            jira_ticket_id: str = None,
                            machine_details: str = None,
                            extracted_details: str = None):
    processing = EmailProcessing(
        email_id=email_id,
        classification_result=classification_result,
        processed_at=datetime.utcnow(),
        jira_ticket_id=jira_ticket_id,
        machine_details=machine_details,
        extracted_details=extracted_details
    )
    db.add(processing)
    db.commit()
    db.refresh(processing)
    return processing


# ---------------- JiraTickets ----------------
def insert_jira_ticket(db: Session, email_id, jira_ticket_id: str,
                       machine: str, priority: str):
    ticket = JiraTickets(
        email_id=email_id,
        jira_ticket_id=jira_ticket_id,
        machine=machine,
        created_at=datetime.utcnow().isoformat(),
        priority=priority
    )
    db.add(ticket)
    db.commit()
    db.refresh(ticket)
    return ticket


# ---------------- ErrorCodeMapping ----------------
def insert_error_code_mapping(db: Session, error_code_mapping: str,
                              machine_info: str,
                              jira_ticket_id: str = None,
                              description: str = None):
    mapping = ErrorCodeMapping(
        error_code_mapping=error_code_mapping,
        machine_info=machine_info,
        jira_ticket_id=jira_ticket_id,
        description=description
    )
    db.add(mapping)
    db.commit()
    db.refresh(mapping)
    return mapping


# ---------------- TriggerList ----------------
def insert_trigger(db: Session, trigger_name: str, category: str,
                   type_: bool, priority: str = None, enabled: bool = True):
    trigger = TriggerList(
        trigger_name=trigger_name,
        category=category,
        type=type_,
        priority=priority,
        enabled=enabled
    )
    db.add(trigger)
    db.commit()
    db.refresh(trigger)
    return trigger


# ---------------- User ----------------
def insert_user(db: Session, username: str, email_id: str,
                password_hash: str, role: str, created_by: str = "system"):
    user = User(
        username=username,
        email_id=email_id,
        password_hash=password_hash,
        role=role,
        created_at=datetime.now(),
        created_by=created_by
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


# ---------------- Config ----------------
def model_to_dict(model_instance):
    """Converts a SQLAlchemy model instance to a dictionary."""
    if model_instance is None:
        return None
    return {c.name: getattr(model_instance, c.name) for c in model_instance.__table__.columns}

async def get_config() -> tuple:
    """Fetches the current job interval config from the database."""
    db :Session=next(get_db())
    try:
        config = db.query(Config).filter(Config.job_name == 'hello_job').first()
        if config:
            db.close()
            return (config.interval_unit, config.interval_value)
        else:
            db.close()
            # Fallback if row is missing
            return ('minutes', 10)
        
    except Exception as e:
        print(f"Error setting config: {e}")
        db.rollback()
        db.close()

async def set_config(unit: str, value: int):
    """Updates the job interval in the config table."""
    db :Session=next(get_db())
    try:
        
        config = db.query(Config).filter(Config.job_name == 'hello_job').first()
        if config:
            config.interval_unit = unit
            config.interval_value = value
            db.commit()
            db.close()
            print(f"Database config updated to: {value} {unit}")
        else:
            db.close()
            print("Error: Config row 'hello_job' not found.")
    except Exception as e:
        print(f"Error setting config: {e}")
        db.rollback()
        db.close()
    

async def add_job_execution(job_interval: str, period_from: datetime, period_to: datetime, execution_end_time: datetime):
    """Adds a new record to the job_execution log table."""
    
    try:
        db : Session =next(get_db())
        new_execution = JobExecution(
            job_interval=job_interval,
            period_from=period_from,
            period_to=period_to,
            execution_end_time=execution_end_time
        )
        db.add(new_execution)
        db.commit()
        print("Job execution logged to database via SQLAlchemy.")
    except Exception as e:
        print(f"Error adding job execution: {e}")
        db.rollback()
    

def get_job_history(limit: int = 20) -> list:
    """Fetches the N most recent job execution logs."""
    db: Session =  next(get_db())
    try:
        rows = db.query(JobExecution).order_by(JobExecution.job_id.desc()).limit(limit).all()
        # Convert model objects to dicts for FastAPI/JSON serialization
        return [model_to_dict(row) for row in rows]
    except Exception as e:
        print(e)


# --- I. RawEmail Operations ---

def insert_raw_email(db: Session, data: Dict[str, Any]) -> RawEmail:
    """
    Inserts a new RawEmail record. Calculates the email_id hash first.
   
    data must contain: sender, subject, body, email_path, received_at (datetime).
    """
    # email_id = generate_email_id(data['subject'], data['received_at'])
    row= db.query(RawEmail).filter(RawEmail.email_id==data["email_id"]).first()
    if row is not None:
        return row
   
    new_email = RawEmail(
        email_id=data["email_id"],
        sender=data['sender'],
        subject=data['subject'],
        body=data['body'],
        email_path=data['email_path'],
        received_at=data['received_at'],
        # inserted_at defaults automatically
        status=data.get('status', False)
    )
   
    db.add(new_email)
    db.commit()
    db.refresh(new_email)
    return new_email

def update_raw_email_status(db: Session, email_id: str, new_status: bool) -> Optional[RawEmail]:
    """Updates the status field of a RawEmail record."""
    
    stmt = update(RawEmail).where(RawEmail.email_id == email_id).values(status=new_status).returning(RawEmail)
    result = db.scalars(stmt).first()
    
    if result:
        db.commit()
        return result
    return None
def insert_duplicate_email(db: Session, data: Dict[str, Any]) -> RawEmail:
    """
    Inserts a new RawEmail record. Calculates the email_id hash first.
   
    data must contain: sender, subject, body, email_path, received_at (datetime).
    """
    # email_id = generate_email_id(data['subject'], data['received_at'])
    
   
    new_email = DuplicateEmail(
        email_id=data["email_id"],
        duplicate_email_id=data["duplicate_email_id"],
        sender=data['sender'],
        subject=data['subject'],
        body=data['body'],
        #email_path=data['email_path'],
        received_at=data['received_at'],
        # inserted_at defaults automatically
    )
   
    db.add(new_email)
    db.commit()
    db.refresh(new_email)
    return new_email

# --- II. SegregatedEmail Operations ---

def insert_or_update_segregation(db: Session, email_id: str, data: Dict[str, Any]) -> SegregatedEmail:
    """
    Inserts a new SegregatedEmail record or updates it if the email_id exists.
    
    data must contain: priority, type, resource_name, trigger_name.
    """
    # Check if a record already exists
    existing_segregation = db.get(SegregatedEmail, email_id)
    
    if existing_segregation:
        # Update existing record
        for key, value in data.items():
            setattr(existing_segregation, key, value)
        existing_segregation.status = data.get('status', existing_segregation.status)
        # inserted_at is not updated on purpose, as it marks the first insert time.
        db.commit()
        db.refresh(existing_segregation)
        return existing_segregation
    else:
        # Insert new record
        new_segregation = SegregatedEmail(
            email_id=email_id,
            priority=data['priority'],
            type=data['type'],
            resource_name=data['resource_name'],
            trigger_name=data['trigger_name'],
            status= False
        )
        db.add(new_segregation)
        db.commit()
        db.refresh(new_segregation)
        return new_segregation

# --- III. SummaryTable Operations ---

def insert_or_update_summary(db: Session, email_id: str, summary_text: str, status: bool = False) -> SummaryTable:
    """Inserts or updates the summary record for a given email_id."""
    
    existing_summary = db.get(SummaryTable, email_id)
    
    if existing_summary:
        # Update existing record
        existing_summary.summary = summary_text
        existing_summary.status = True
        db.commit()
        db.refresh(existing_summary)
        return existing_summary
    else:
        # Insert new record
        new_summary = SummaryTable(
            email_id=email_id,
            summary=summary_text,
            status=status
        )
        db.add(new_summary)
        db.commit()
        db.refresh(new_summary)
        return new_summary

# --- IV. JiraEntry Operations ---

def insert_jira_entry(db: Session, email_id: str, data: Dict[str, Any]) -> JiraEntry:
    """
    Inserts a new JiraEntry record.
    data must contain: jiraticket_id, assigned_to, created_at (datetime).
    Optional: teams_flag ('true'/'false')
    """
    new_jira = JiraEntry(
        email_id=email_id,
        jiraticket_id=data['jiraticket_id'],
        assigned_to=data.get('assigned_to'),
        created_at=data['created_at'],
        teams_flag=data.get('teams_flag', 'false'),
        teams_channel=data.get('teams_channel')
    )
    db.add(new_jira)
    db.commit()
    db.refresh(new_jira)
    return new_jira

def update_jira_assignment(db: Session, jiraticket_id: str, new_assigned_to: str) -> Optional[JiraEntry]:
    """Updates the assigned_to field for a specific JIRA ticket."""
    
    stmt = update(JiraEntry).where(JiraEntry.jiraticket_id == jiraticket_id).values(assigned_to=new_assigned_to).returning(JiraEntry)
    result = db.scalars(stmt).first()
    
    if result:
        db.commit()
        return result
    return None

# --- V. JobTable Operations ---

def insert_job_info(db: Session, data: Dict[str, Any]) -> JobTable:
    """
    Inserts a new JobTable record.
    
    data must contain: frequency, job_name (optional), job_start_time (optional).
    """
    new_job = JobTable(
        job_name=data.get('job_name'),
        frequency=data['frequency'],
        job_start_time=data.get('job_start_time'),
        last_run_time=data.get('last_run_time'),
        job_end_time = datetime.now()
    )
    db.add(new_job)
    db.commit()
    db.refresh(new_job)
    return new_job

def update_job_completion(db: Session, job_id: int, job_end_time: datetime) -> Optional[JobTable]:
    """Updates job end time and sets the current end time as the last run time."""
    
    stmt = update(JobTable).where(JobTable.job_id == job_id).values(
        job_end_time=job_end_time,
        last_run_time=job_end_time # Update last_run_time to the completion time
    ).returning(JobTable)
    
    result = db.scalars(stmt).first()
    
    if result:
        db.commit()
        return result
    return None
def get_email_id_within_hour(db: Session, target_trigger: str,target_resource:str,delete_email:str):
    window=int(settings.WINDOW)
    target_timestamp=datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f')
    one_hour_ago = target_timestamp - timedelta(hours=window) if window else target_timestamp - timedelta(hours=1)

    result = db.query(SegregatedEmail.email_id).filter(
        and_(
            SegregatedEmail.trigger_name == target_trigger,
            SegregatedEmail.resource_name == target_resource,
            SegregatedEmail.inserted_at <= target_timestamp,
            SegregatedEmail.inserted_at >= one_hour_ago,
            SegregatedEmail.priority!= "informational"

        )
    ).first()
    result=result.email_id if result else None
    if result:
        raw=db.query(RawEmail).filter(RawEmail.email_id == delete_email).first()
        if raw:
            db.delete(raw)
            db.commit()
 
    # Return the email_id (result is a Row object or None)
    return result
