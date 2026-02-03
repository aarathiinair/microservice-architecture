"""
Database Schema for Trigger-to-Channel Mapping
Run: python db_schema2.py --setup "path/to/ControlUp Trigger Details.xlsx"
"""

from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Mapped
from config import settings

engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class Server(Base):
    """Existing: Machine-to-infrastructure mapping"""
    __tablename__ = "servers"
    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    computername: Mapped[str] = Column(String, index=True, nullable=False)
    group: Mapped[str] = Column(String, index=True, nullable=False)
    description_function: Mapped[str] = Column(String, nullable=True)
    responsible_person: Mapped[str] = Column(String, nullable=True)


class TriggerMapping(Base):
    """Trigger name → Teams channel mapping"""
    __tablename__ = "trigger_mappings"
    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    trigger_name: Mapped[str] = Column(String, index=True, nullable=False)
    category: Mapped[str] = Column(String, nullable=True)
    priority: Mapped[str] = Column(String, nullable=True)
    actionable: Mapped[str] = Column(String, nullable=True)
    recommended_action: Mapped[str] = Column(String, nullable=True)
    team: Mapped[str] = Column(String, index=True, nullable=False)
    department: Mapped[str] = Column(String, nullable=True)


def create_tables():
    Base.metadata.create_all(bind=engine)
    print("✅ Tables created")


def populate_from_excel(excel_path: str):
    """
    Load trigger mappings from Excel.
    Columns: TriggerName, Category, Priority, Informational/Actionable, 
             Recommended Actions, Team, Department
    """
    import pandas as pd
    
    df = pd.read_excel(excel_path, engine='openpyxl')
    print(f"📄 Read {len(df)} rows from Excel")
    
    # Map columns
    col_map = {
        'TriggerName': 'trigger_name',
        'Category': 'category', 
        'Priority': 'priority',
        'Informational/Actionable': 'actionable',
        'Recommended Actions': 'recommended_action',
        'Team': 'team',
        'Department': 'department'
    }
    df = df.rename(columns=col_map)
    
    # Keep only needed columns, drop rows without trigger_name or team
    cols = ['trigger_name', 'category', 'priority', 'actionable', 
            'recommended_action', 'team', 'department']
    df = df[[c for c in cols if c in df.columns]]
    df = df.dropna(subset=['trigger_name', 'team'])
    
    db = next(get_db())
    try:
        db.query(TriggerMapping).delete()
        count = 0
        for _, row in df.iterrows():
            m = TriggerMapping(
                trigger_name=str(row['trigger_name']).strip(),
                category=str(row.get('category', '')).strip() or None,
                priority=str(row.get('priority', '')).strip() or None,
                actionable=str(row.get('actionable', '')).strip() or None,
                recommended_action=str(row.get('recommended_action', '')).strip() or None,
                team=str(row['team']).strip(),
                department=str(row.get('department', '')).strip() or None
            )
            db.add(m)
            count += 1
        db.commit()
        print(f"✅ Loaded {count} trigger mappings into database")
    finally:
        db.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) >= 3 and sys.argv[1] == "--setup":
        excel_path = sys.argv[2]
        print(f"\n🔧 Setting up database from: {excel_path}")
        create_tables()
        populate_from_excel(excel_path)
        print("\n✅ Setup complete!")
    else:
        print("Usage: python db_schema2.py --setup \"path/to/ControlUp Trigger Details.xlsx\"")
        print("\nOr create tables only:")
        create_tables()