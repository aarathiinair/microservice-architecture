import os
from dotenv import load_dotenv
from typing import Optional, Dict

# Load environment variables
load_dotenv()

class Settings:
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:Admin@localhost:5432/postgres")
    
    # Email Configuration
    OUTLOOK_EMAIL: Optional[str] = os.getenv("OUTLOOK_EMAIL")
    OUTLOOK_PASSWORD: Optional[str] = os.getenv("OUTLOOK_PASSWORD")
    OUTLOOK_SERVER: str = os.getenv("OUTLOOK_SERVER", "outlook.office365.com")
    
    GMAIL_CLIENT_ID: Optional[str] = os.getenv("GMAIL_CLIENT_ID")
    GMAIL_CLIENT_SECRET: Optional[str] = os.getenv("GMAIL_CLIENT_SECRET")
    GMAIL_REFRESH_TOKEN: Optional[str] = os.getenv("GMAIL_REFRESH_TOKEN")
    
    # Scheduler
    SCHEDULER_INTERVAL_MINUTES: int = int(os.getenv("SCHEDULER_INTERVAL_MINUTES", "30"))
    SAMPLE_EMAILS_COUNT: int = int(os.getenv("SAMPLE_EMAILS_COUNT", "10"))
    USE_SAMPLE_EMAILS_ONLY: bool = os.getenv("USE_SAMPLE_EMAILS_ONLY", "True").lower() == "true"
    
    # ML Model
    MODEL_PATH: str = os.getenv("MODEL_PATH", "./models")
    HUGGINGFACE_MODEL_NAME: str = os.getenv("HUGGINGFACE_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
    
    # API
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    DEBUG: bool = os.getenv("DEBUG", "True").lower() == "true"

    #LLM path
    LLM_PATH : str= os.getenv("LLM_PATH", r'C:/Users/E00868/Downloads/Qwen_fine_tuned')
    EMBEDDING_MODEL_PATH : str= os.getenv("EMBEDDING_MODEL_PATH", r'C:/Users/E00868/Downloads/Qwen_fine_tuned')
    CSV_FILE_PATH: str= os.getenv("CSV_FILE_PATH", r"./ControlUp Trigger Details.xlsx")
    VECTOR_STORE_PATH: str= os.getenv("VECTOR_STORE_PATH", "./faiss_index_manual")
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "./logs/app.log")
    #Time Window
    WINDOW: float = os.getenv("WINDOW",1)
    # Backups
    BACKUP_DIR: str = os.getenv("BACKUP_DIR", "./emails_backup")

    # Jira Integration
    JIRA_BASE_URL: Optional[str] = os.getenv("JIRA_BASE_URL")
    JIRA_EMAIL: Optional[str] = os.getenv("JIRA_EMAIL")
    JIRA_API_TOKEN: Optional[str] = os.getenv("JIRA_API_TOKEN")
    JIRA_PROJECT_KEY: Optional[str] = os.getenv("JIRA_PROJECT_KEY")
    JIRA_ISSUE_TYPE: str = os.getenv("JIRA_ISSUE_TYPE", "Task")

    #Rabbit MQ Configuration
    RABBITMQ_URL:Optional[str] = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")  # <-- replace with your RabbitMQ URL
    CLASS_QUEUE_NAME:Optional[str] = os.getenv("CLASS_QUEUE", "my_class_queue")  # <-- replace with your queue name
    SUMM_QUEUE_NAME:Optional[str] = os.getenv("SUMM_QUEUE", "my_summ_queue") 
    JIRA_QUEUE_NAME:Optional[str] = os.getenv("JIRA_QUEUE", "my_jira_queue") 
    CLASS_DLQ_NAME:Optional[str] = os.getenv("DLQ_QUEUE_CLASS", "dlq_queue_class") 
    SUMM_DLQ_NAME:Optional[str] = os.getenv("DLQ_QUEUE_SUMM", "dlq_queue_summ") 
    JIRA_DLQ_NAME:Optional[str] = os.getenv("DLQ_QUEUE_JIRA", "dlq_queue_jira") 


    # MS Teams - General Settings
    MS_TEAMS_ENABLED: bool = os.getenv("MS_TEAMS_ENABLED", "True").lower() == "true"
    
    # Legacy default webhook (fallback)
    MS_TEAMS_WEBHOOK_URL: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_URL")

    # ===========================================================================
    # MS Teams Webhook URLs by Infrastructure Channel (EXISTING - Machine-based)
    # ===========================================================================
    MS_TEAMS_WEBHOOK_ACC: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_ACC")
    MS_TEAMS_WEBHOOK_CITRIX: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_CITRIX")
    MS_TEAMS_WEBHOOK_DKSGD: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_DKSGD")
    MS_TEAMS_WEBHOOK_ITVIC: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_ITVIC")
    MS_TEAMS_WEBHOOK_OI_BA: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_OI_BA")
    MS_TEAMS_WEBHOOK_OI_IBS: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_OI_IBS")
    MS_TEAMS_WEBHOOK_OI_RDA: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_OI_RDA")
    MS_TEAMS_WEBHOOK_OI_TC: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_OI_TC")
    MS_TEAMS_WEBHOOK_TRIGONOVA: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TRIGONOVA")
    MS_TEAMS_WEBHOOK_GENERAL: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_GENERAL")

    # ===========================================================================
    # MS Teams Webhook URLs by Team (NEW - Trigger-based routing)
    # These map to the "Team" column in ControlUp Trigger Details.xlsx
    # ===========================================================================
    # IBS Teams
    MS_TEAMS_WEBHOOK_TEAM_CITRIX: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_CITRIX")
    MS_TEAMS_WEBHOOK_TEAM_VIRTUAL_SERVER: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_VIRTUAL_SERVER")
    MS_TEAMS_WEBHOOK_TEAM_MAIL_SERVICE: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_MAIL_SERVICE")
    MS_TEAMS_WEBHOOK_TEAM_BACKUP: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_BACKUP")
    MS_TEAMS_WEBHOOK_TEAM_ROT: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_ROT")
    
    # SAP Teams
    MS_TEAMS_WEBHOOK_TEAM_SAP_BASIS: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_SAP_BASIS")
    MS_TEAMS_WEBHOOK_TEAM_SAP_SALES: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_SAP_SALES")
    MS_TEAMS_WEBHOOK_TEAM_SAP_OPERATIONS: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_SAP_OPERATIONS")
    MS_TEAMS_WEBHOOK_TEAM_SAP_DEVELOPMENT: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_SAP_DEVELOPMENT")
    
    # OI Teams
    MS_TEAMS_WEBHOOK_TEAM_DB_DEVELOPMENT: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_DB_DEVELOPMENT")
    MS_TEAMS_WEBHOOK_TEAM_DB_ADMINISTRATION: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_DB_ADMINISTRATION")
    MS_TEAMS_WEBHOOK_TEAM_RDA: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_RDA")
    MS_TEAMS_WEBHOOK_TEAM_TELECOMMUNICATIONS: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_TELECOMMUNICATIONS")
    
    # Other Teams
    MS_TEAMS_WEBHOOK_TEAM_ADC: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_ADC")
    MS_TEAMS_WEBHOOK_TEAM_CONTROLUP: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_CONTROLUP")
    MS_TEAMS_WEBHOOK_TEAM_SONSTIGE: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_TEAM_SONSTIGE")

    def get_teams_webhook_map(self) -> Dict[str, str]:
        """Returns mapping of infrastructure group to webhook URL (legacy machine-based)"""
        return {
            "ACC Technical": self.MS_TEAMS_WEBHOOK_ACC,
            "Citrix Infrastructure": self.MS_TEAMS_WEBHOOK_CITRIX,
            "DKSGD Infrastructure": self.MS_TEAMS_WEBHOOK_DKSGD,
            "ITVIC Infrastructure": self.MS_TEAMS_WEBHOOK_ITVIC,
            "OI-BA Infrastructure": self.MS_TEAMS_WEBHOOK_OI_BA,
            "OI-IBS Infrastructure": self.MS_TEAMS_WEBHOOK_OI_IBS,
            "OI-RDA Infrastructure": self.MS_TEAMS_WEBHOOK_OI_RDA,
            "OI-TC Infrastructure": self.MS_TEAMS_WEBHOOK_OI_TC,
            "Trigonova Infrastructure": self.MS_TEAMS_WEBHOOK_TRIGONOVA,
            "General": self.MS_TEAMS_WEBHOOK_GENERAL,
        }

    def get_webhook_for_infrastructure(self, infrastructure: str) -> Optional[str]:
        """Get webhook URL for a specific infrastructure group (legacy machine-based)"""
        webhook_map = self.get_teams_webhook_map()
        # Try exact match first
        if infrastructure in webhook_map:
            return webhook_map[infrastructure]
        # Try partial match (case-insensitive)
        infra_lower = infrastructure.lower()
        for key, url in webhook_map.items():
            if key.lower() in infra_lower or infra_lower in key.lower():
                return url
        # Fallback to general
        return self.MS_TEAMS_WEBHOOK_GENERAL or self.MS_TEAMS_WEBHOOK_URL

    def get_team_webhook_map(self) -> Dict[str, Optional[str]]:
        """
        Returns mapping of Team names (from Excel) to webhook URLs.
        Used for trigger-based routing.
        """
        return {
            # IBS Teams
            "CITRIX": self.MS_TEAMS_WEBHOOK_TEAM_CITRIX or self.MS_TEAMS_WEBHOOK_CITRIX,
            "Citrix": self.MS_TEAMS_WEBHOOK_TEAM_CITRIX or self.MS_TEAMS_WEBHOOK_CITRIX,
            "Virtual Server Infrastructure": self.MS_TEAMS_WEBHOOK_TEAM_VIRTUAL_SERVER,
            "Virtual Server": self.MS_TEAMS_WEBHOOK_TEAM_VIRTUAL_SERVER,
            "Mail Service": self.MS_TEAMS_WEBHOOK_TEAM_MAIL_SERVICE,
            "Backup": self.MS_TEAMS_WEBHOOK_TEAM_BACKUP,
            "ROT": self.MS_TEAMS_WEBHOOK_TEAM_ROT,
            
            # SAP Teams
            "Basis": self.MS_TEAMS_WEBHOOK_TEAM_SAP_BASIS,
            "SAP Basis": self.MS_TEAMS_WEBHOOK_TEAM_SAP_BASIS,
            "Sales": self.MS_TEAMS_WEBHOOK_TEAM_SAP_SALES,
            "SAP Sales": self.MS_TEAMS_WEBHOOK_TEAM_SAP_SALES,
            "Operations": self.MS_TEAMS_WEBHOOK_TEAM_SAP_OPERATIONS,
            "SAP Operations": self.MS_TEAMS_WEBHOOK_TEAM_SAP_OPERATIONS,
            "Development": self.MS_TEAMS_WEBHOOK_TEAM_SAP_DEVELOPMENT,
            "SAP Development": self.MS_TEAMS_WEBHOOK_TEAM_SAP_DEVELOPMENT,
            
            # OI Teams
            "DB Development": self.MS_TEAMS_WEBHOOK_TEAM_DB_DEVELOPMENT,
            "DB Administration": self.MS_TEAMS_WEBHOOK_TEAM_DB_ADMINISTRATION,
            "RDA": self.MS_TEAMS_WEBHOOK_TEAM_RDA,
            "Telecommunications": self.MS_TEAMS_WEBHOOK_TEAM_TELECOMMUNICATIONS,
            "OI - IBS": self.MS_TEAMS_WEBHOOK_OI_IBS,
            
            # Other Teams
            "ADC": self.MS_TEAMS_WEBHOOK_TEAM_ADC,
            "ControlUp": self.MS_TEAMS_WEBHOOK_TEAM_CONTROLUP,
            "Sonstige": self.MS_TEAMS_WEBHOOK_TEAM_SONSTIGE,
            
            # Fallback
            "General": self.MS_TEAMS_WEBHOOK_GENERAL,
        }

    def get_webhook_for_team(self, team: str) -> Optional[str]:
        """
        Get webhook URL for a specific Team (trigger-based routing).
        
        Args:
            team: Team name from trigger_mappings table (from Excel "Team" column)
            
        Returns:
            Webhook URL or None if not found
        """
        if not team:
            return self.MS_TEAMS_WEBHOOK_GENERAL or self.MS_TEAMS_WEBHOOK_URL
        
        webhook_map = self.get_team_webhook_map()
        
        # Try exact match first
        if team in webhook_map and webhook_map[team]:
            return webhook_map[team]
        
        # Try case-insensitive match
        team_lower = team.lower().strip()
        for key, url in webhook_map.items():
            if key.lower() == team_lower and url:
                return url
        
        # Try partial match (e.g., "CITRIX" in "CITRIX Team")
        for key, url in webhook_map.items():
            if url and (key.lower() in team_lower or team_lower in key.lower()):
                return url
        
        # Fallback to infrastructure-based lookup (backward compatibility)
        infra_url = self.get_webhook_for_infrastructure(team)
        if infra_url:
            return infra_url
        
        # Final fallback to General
        return self.MS_TEAMS_WEBHOOK_GENERAL or self.MS_TEAMS_WEBHOOK_URL


settings = Settings()