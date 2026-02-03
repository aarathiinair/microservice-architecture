import pytz
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

# --- SETUP ---
DATABASE_URL = "postgresql://postgres:Admin@localhost:5432/email_processor_db" # UPDATE THIS (Currently set to local sqlite for testing)
GERMANY_TZ = pytz.timezone('Europe/Berlin')

Base = declarative_base()

class Certificates(Base):
    __tablename__ = 'certificates'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    certificate_name = Column(String, unique=True, nullable=False)
    description = Column(String, unique=False, nullable=True)
    usage = Column(String, unique=False, nullable=True) # Added
    expiration_date = Column(DateTime, nullable=False) 
    # Note: Using your specific default lambda logic here
    created_at = Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)
    updated_at = Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)
    effected_users = Column(String, unique=False, nullable=True) # Added
    responsible_group = Column(String, nullable=False)
    teams_channel = Column(String, nullable=False)
    calculated_status = Column(String) # Renamed from 'status'

engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

# --- DATE CALCULATION HELPER ---
now_germany = datetime.now(GERMANY_TZ)

def get_expiry(days_offset):
    """
    Returns a datetime object set to days_offset from now, at 23:59:59.
    converted to naive datetime if your DB expects timezone=False
    """
    future_date = now_germany + timedelta(days=days_offset)
    future_date = future_date.replace(hour=23, minute=59, second=59, microsecond=0)
    # Remove timezone info to match DateTime(timezone=False) column type usually
    return future_date.replace(tzinfo=None) 

# --- TEST DATA GENERATION ---
# Note: created_at and updated_at are handled by the default=lambda in the Class definition
test_data = [
    # CASE 1: Active Certificate (> 14 days)
    Certificates(
        certificate_name="Global Load Balancer",
        description="Main entry point cert",
        usage="Production HTTPS Traffic",
        effected_users="External Customers",
        expiration_date=get_expiry(30),  # +30 Days
        responsible_group="General",
        teams_channel="NetOps-Alerts",
        calculated_status="ACTIVE"
    ),

    # CASE 2: Exactly 14 Days (Boundary Condition)
    Certificates(
        certificate_name="Payment Gateway Cert",
        description="Stripe integration",
        usage="Payment Processing",
        effected_users="Finance Dept & Customers",
        expiration_date=get_expiry(14),  # +14 Days
        responsible_group="FinTech Team",
        teams_channel="Finance-Ops",
        calculated_status="ACTIVE"
    ),

    # CASE 3: Milestone Days (7, 3, 2, 1)
    Certificates(
        certificate_name="Internal Wiki",
        description="Confluence SSL",
        usage="Internal Documentation",
        effected_users="All Employees",
        expiration_date=get_expiry(7),   # +7 Days
        responsible_group="IT Support",
        teams_channel="IT-Helpdesk",
        calculated_status="EXPIRING SOON"
    ),
    Certificates(
        certificate_name="VPN Gateway East",
        description="Remote access",
        usage="Remote Connectivity",
        effected_users="Remote Workers",
        expiration_date=get_expiry(3),   # +3 Days
        responsible_group="SecOps",
        teams_channel="Security-Alerts",
        calculated_status="EXPIRING SOON"
    ),
    Certificates(
        certificate_name="Legacy API",
        description="Old SOAP Service",
        usage="Legacy Integration",
        effected_users="B2B Partners",
        expiration_date=get_expiry(2),   # +2 Days
        responsible_group="Backend Team",
        teams_channel="Backend-Devs",
        calculated_status="EXPIRING SOON"
    ),
    Certificates(
        certificate_name="Mail Server",
        description="SMTP Exchange",
        usage="Email Services",
        effected_users="All Staff",
        expiration_date=get_expiry(1),   # +1 Day (Critical)
        responsible_group="SysAdmin",
        teams_channel="SysAdmin-L1",
        calculated_status="EXPIRING SOON"
    ),

    # CASE 4: The 12-Day Case
    Certificates(
        certificate_name="Customer Portal DB",
        description="Postgres TLS",
        usage="Database Encryption",
        effected_users="DB Admins",
        expiration_date=get_expiry(12),  # +12 Days
        responsible_group="DBA Team",
        teams_channel="DBA-Alerts",
        calculated_status="ACTIVE" 
    )
]

# --- INSERT ---
try:
    # Optional: Clear table to ensure clean test state
    # session.query(Certificates).delete()
    
    session.add_all(test_data)
    session.commit()
    print(f"Successfully inserted {len(test_data)} test certificates into 'Certificates'.")
except Exception as e:
    session.rollback()
    print(f"Error inserting data: {e}")
finally:
    session.close()