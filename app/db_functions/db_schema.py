from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, Float, ForeignKey, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
import uuid
from datetime import datetime
from app.config import settings

Base = declarative_base()

# Create database engine
engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# ---------------- Emails ----------------
class Emails(Base):
    __tablename__ = "emails"

    email_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    subject = Column(Text, nullable=False)
    body = Column(Text, nullable=False)
    sender = Column(String, nullable=False)
    received_at = Column(TIMESTAMP, nullable=False)
    saved_at = Column(TIMESTAMP, nullable=False)
    status = Column(String)
    category = Column(String)
    priority = Column(String)
    trigger = Column(String)

    # Relationships
    processing = relationship("EmailProcessing", back_populates="email", cascade="all, delete-orphan")
    jira_tickets = relationship("JiraTickets", back_populates="email", cascade="all, delete-orphan")


# ---------------- Email_Processing ----------------
class EmailProcessing(Base):
    __tablename__ = "email_processing"

    process_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    email_id = Column(UUID(as_uuid=True), ForeignKey("emails.email_id", ondelete="CASCADE"), nullable=False)
    classification_result = Column(String, nullable=False)
    processed_at = Column(TIMESTAMP, nullable=False)
    jira_ticket_id = Column(String)
    machine_details = Column(String)
    extracted_details = Column(String)

    # Relationships
    email = relationship("Emails", back_populates="processing")


# ---------------- Jira_tickets ----------------
class JiraTickets(Base):
    __tablename__ = "jira_tickets"

    jira_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    email_id = Column(UUID(as_uuid=True), ForeignKey("emails.email_id", ondelete="CASCADE"))
    jira_ticket_id = Column(String)
    machine = Column(String, nullable=False)
    created_at = Column(String, nullable=False)
    priority = Column(String, nullable=False)

    # Relationships
    email = relationship("Emails", back_populates="jira_tickets")


# ---------------- Error_code_mapping ----------------
class ErrorCodeMapping(Base):
    __tablename__ = "error_code_mapping"

    error_code_mapping = Column(String, primary_key=True, nullable=False)
    machine_info = Column(String, nullable=False)
    jira_ticket_id = Column(String)
    description = Column(Text)


# ---------------- Trigger_list ----------------
class TriggerList(Base):
    __tablename__ = "trigger_list"

    trigger_name = Column(String, primary_key=True, nullable=False)
    category = Column(String, nullable=False)
    type = Column(Boolean, nullable=False)
    priority = Column(String)
    enabled = Column(Boolean, nullable=False)


# ---------------- User ----------------
class User(Base):
    __tablename__ = "user"

    user_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    username = Column(String, nullable=False)
    email_id = Column(String, nullable=False)
    password_hash = Column(String, nullable=False)
    role = Column(String, nullable=False)
    created_at = Column(TIMESTAMP, nullable=False)
    created_by = Column(String)

    # Relationships
    configs = relationship("Config", back_populates="user", cascade="all, delete-orphan")


# ---------------- Config ----------------
class Config(Base):
    __tablename__ = "config"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey("user.user_id", ondelete="CASCADE"), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False)

    # Relationships
    user = relationship("User", back_populates="configs")


# Create tables
def create_tables():
    Base.metadata.create_all(bind=engine)


