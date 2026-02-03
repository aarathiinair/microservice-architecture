import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# --- CONFIGURATION ---
CSV_FILE_PATH = 'Certificates .xlsx - Sheet1.csv'  # Ensure this matches your file name
DATABASE_URL = "sqlite:///./certificates.db"       # Changing to your actual DB URL

Base = declarative_base()

# --- SCHEMA DEFINITION ---
class Certificates(Base):
    __tablename__ = 'certificates'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    certificate_name = Column(String, unique=True, nullable=False)
    description = Column(String, unique=False, nullable=True)
    usage = Column(String, unique=False, nullable=True)
    expiration_date = Column(DateTime, nullable=False) 
    
    # Auto-generated timestamps using your specific lambda logic
    created_at = Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)
    updated_at = Column(DateTime(timezone=False), default=lambda: datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f'), nullable=False)
    
    effected_users = Column(String, unique=False, nullable=True)
    responsible_group = Column(String, nullable=False)
    teams_channel = Column(String, nullable=False)
    calculated_status = Column(String) # 'ACTIVE', 'EXPIRING SOON', 'EXPIRED'

# --- HELPER FUNCTIONS ---

def parse_expiration_date(date_str):
    """
    Parses the expiration string like '6/3/2026;2026-06-02T22:00:00Z'.
    Logic: Split by ';', find the part with '-' (hyphens), and convert to naive datetime.
    """
    if not isinstance(date_str, str):
        return None

    parts = date_str.split(';')
    
    iso_part = None
    for part in parts:
        if '-' in part:
            iso_part = part.strip()
            break
    
    if iso_part:
        try:
            # Parse ISO format (e.g., 2026-06-02T22:00:00Z)
            # We replace 'Z' with +00:00 for strict ISO parsing if needed, 
            # or rely on fromisoformat depending on python version.
            # Then we make it naive (tz=None) to match the DB schema requirement.
            dt = datetime.fromisoformat(iso_part.replace('Z', '+00:00'))
            return dt.replace(tzinfo=None)
        except ValueError as e:
            print(f"Error parsing date '{iso_part}': {e}")
            return None
    return None

def calculate_status_logic(expiry_date):
    """Simple logic to determine status based on expiration date."""
    if not expiry_date:
        return 'UNKNOWN'
        
    now = datetime.now()
    delta = expiry_date - now

    if delta.days < 0:
        return 'EXPIRED'
    elif delta.days <= 30: # Example threshold for 'Expiring Soon'
        return 'EXPIRING SOON'
    else:
        return 'ACTIVE'

# --- MAIN ETL PROCESS ---

def main():
    # 1. Setup Database
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    # 2. Read CSV
    try:
        # Using pandas to handle CSV parsing (it handles quotes/commas in descriptions better)
        df = pd.read_csv(CSV_FILE_PATH)
        
        # Strip whitespace from headers just in case
        df.columns = df.columns.str.strip()
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # 3. Iterate and Transform
    count = 0
    skipped = 0

    for index, row in df.iterrows():
        try:
            # -- Extract and Map Data --
            
            # Map: Title -> certificate_name
            cert_name = row.get('Title')
            if pd.isna(cert_name): continue # Skip empty rows

            # Map: Expiration -> expiration_date (Custom Parsing)
            raw_expiry = row.get('Expiration')
            expiry_dt = parse_expiration_date(raw_expiry)
            
            if not expiry_dt:
                print(f"Skipping '{cert_name}': Could not parse expiration date '{raw_expiry}'")
                skipped += 1
                continue

            # Map: Description -> description
            desc = row.get('Description')
            desc = desc if not pd.isna(desc) else None

            # Map: Usage -> usage
            usage_val = row.get('Usage')
            usage_val = usage_val if not pd.isna(usage_val) else None

            # Map: Effected Users -> effected_users
            users = row.get('Effected Users')
            users = users if not pd.isna(users) else None

            # Map: Jira Team -> responsible_group AND teams_channel
            jira_team = row.get('Jira Team')
            if pd.isna(jira_team):
                # Fallback if Jira Team is empty, though schema says nullable=False
                # You might want to handle this differently (e.g., set default)
                jira_team = "Unknown Group"
            
            resp_group = jira_team
            channel = jira_team

            # Calculate Status
            status = calculate_status_logic(expiry_dt)

            # -- Create Object --
            cert_entry = Certificates(
                certificate_name=cert_name,
                description=desc,
                usage=usage_val,
                expiration_date=expiry_dt,
                effected_users=users,
                responsible_group=resp_group,
                teams_channel=channel,
                calculated_status=status
                # created_at and updated_at are handled by default lambda
            )

            # -- Upsert Logic (Optional: avoid duplicates based on unique constraint) --
            # Check if exists
            existing = session.query(Certificates).filter_by(certificate_name=cert_name).first()
            if existing:
                print(f"Certificate '{cert_name}' already exists. Skipping.")
                skipped += 1
            else:
                session.add(cert_entry)
                count += 1

        except Exception as e:
            print(f"Error processing row {index}: {e}")
            skipped += 1

    # 4. Commit
    try:
        session.commit()
        print(f"\n--- Import Summary ---")
        print(f"Successfully added: {count}")
        print(f"Skipped/Error: {skipped}")
    except Exception as e:
        session.rollback()
        print(f"Database commit failed: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()