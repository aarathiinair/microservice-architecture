from datetime import datetime
from typing import Dict, Any, Optional

from sqlalchemy import select, update
from sqlalchemy.orm import Session
from models import RawEmail, SegregatedEmail, SummaryTable, JiraEntry, JobTable, generate_email_id, get_db_engine

# --- I. RawEmail Operations ---

def insert_raw_email(db: Session, data: Dict[str, Any]) -> RawEmail:
    """
    Inserts a new RawEmail record. Calculates the email_id hash first.
    
    data must contain: sender, subject, body, email_path, recieved_at (datetime).
    """
    email_id = generate_email_id(data['subject'], data['recieved_at'])
    
    new_email = RawEmail(
        email_id=email_id,
        sender=data['sender'],
        subject=data['subject'],
        body=data['body'],
        email_path=data['email_path'],
        recieved_at=data['recieved_at'],
        # inserted_at defaults automatically
        status=data.get('status', True)
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
            status=data.get('status', True)
        )
        db.add(new_segregation)
        db.commit()
        db.refresh(new_segregation)
        return new_segregation

# --- III. SummaryTable Operations ---

def insert_or_update_summary(db: Session, email_id: str, summary_text: str, status: bool = True) -> SummaryTable:
    """Inserts or updates the summary record for a given email_id."""
    
    existing_summary = db.get(SummaryTable, email_id)
    
    if existing_summary:
        # Update existing record
        existing_summary.summary = summary_text
        existing_summary.status = status
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
    Inserts a new JiraEntry record. Assumes the email\_id and created\_at are present.
    
    data must contain: jiraticket_id, assigned_to, created_at (datetime).
    """
    new_jira = JiraEntry(
        email_id=email_id,
        jiraticket_id=data['jiraticket_id'],
        assigned_to=data.get('assigned_to'),
        created_at=data['created_at']
        # inserted_at defaults automatically
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
        last_run_time=data.get('last_run_time')
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