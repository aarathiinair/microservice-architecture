import os
from dotenv import load_dotenv
from typing import Optional, Dict

load_dotenv()

class Settings:
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:Admin@localhost:5432/email_processor_db")
    
    # Email Configuration
    OUTLOOK_EMAIL: Optional[str] = os.getenv("OUTLOOK_EMAIL")
    OUTLOOK_PASSWORD: Optional[str] = os.getenv("OUTLOOK_PASSWORD")
    OUTLOOK_SERVER: str = os.getenv("OUTLOOK_SERVER", "outlook.office365.com")
    
    # Jira Integration
    JIRA_BASE_URL: Optional[str] = os.getenv("JIRA_BASE_URL")
    JIRA_EMAIL: Optional[str] = os.getenv("JIRA_EMAIL")
    JIRA_API_TOKEN: Optional[str] = os.getenv("JIRA_API_TOKEN")
    JIRA_PROJECT_KEY: Optional[str] = os.getenv("JIRA_PROJECT_KEY")
    JIRA_ISSUE_TYPE: str = os.getenv("JIRA_ISSUE_TYPE", "Task")

    # RabbitMQ
    RABBITMQ_URL: Optional[str] = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    QUEUE_NAME: Optional[str] = os.getenv("QUEUE_NAME", "my_async_queue")

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "./logs/app.log")

    # MS Teams - Enable/Disable
    MS_TEAMS_ENABLED: bool = os.getenv("MS_TEAMS_ENABLED", "True").lower() == "true"
    
    # ============================================================
    # MS Teams Webhooks - NEW CHANNEL MAPPING (from spreadsheet)
    # ============================================================
    MS_TEAMS_WEBHOOK_IBS_CITRIX: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_IBS_CITRIX")
    MS_TEAMS_WEBHOOK_IBS_VIRTUAL: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_IBS_VIRTUAL")
    MS_TEAMS_WEBHOOK_IBS_MAIL: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_IBS_MAIL")
    MS_TEAMS_WEBHOOK_IBS_BACKUP: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_IBS_BACKUP")
    MS_TEAMS_WEBHOOK_IBS_ROT: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_IBS_ROT")
    MS_TEAMS_WEBHOOK_SAP_BASIS: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_SAP_BASIS")
    MS_TEAMS_WEBHOOK_SAP_SALES: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_SAP_SALES")
    MS_TEAMS_WEBHOOK_SAP_OPS: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_SAP_OPS")
    MS_TEAMS_WEBHOOK_SAP_DEV: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_SAP_DEV")
    MS_TEAMS_WEBHOOK_OI_DB_DEV: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_OI_DB_DEV")
    MS_TEAMS_WEBHOOK_OI_DB_ADMIN: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_OI_DB_ADMIN")
    MS_TEAMS_WEBHOOK_OI_RDA: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_OI_RDA")
    MS_TEAMS_WEBHOOK_OI_TC: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_OI_TC")
    MS_TEAMS_WEBHOOK_ADC: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_ADC")
    MS_TEAMS_WEBHOOK_CONTROLUP: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_CONTROLUP")
    MS_TEAMS_WEBHOOK_SONSTIGE: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_SONSTIGE")
    MS_TEAMS_WEBHOOK_GENERAL: Optional[str] = os.getenv("MS_TEAMS_WEBHOOK_GENERAL")

    def get_teams_webhook_map(self) -> Dict[str, str]:
        """Maps Team column values from spreadsheet to webhook URLs"""
        return {
            "IBS - CITRIX": self.MS_TEAMS_WEBHOOK_IBS_CITRIX,
            "IBS - Virtual Server Infrastructure": self.MS_TEAMS_WEBHOOK_IBS_VIRTUAL,
            "IBS - Mail Service": self.MS_TEAMS_WEBHOOK_IBS_MAIL,
            "IBS - Backup": self.MS_TEAMS_WEBHOOK_IBS_BACKUP,
            "IBS - ROT": self.MS_TEAMS_WEBHOOK_IBS_ROT,
            "SAP Basis": self.MS_TEAMS_WEBHOOK_SAP_BASIS,
            "SAP Sales": self.MS_TEAMS_WEBHOOK_SAP_SALES,
            "SAP Operations": self.MS_TEAMS_WEBHOOK_SAP_OPS,
            "SAP Development": self.MS_TEAMS_WEBHOOK_SAP_DEV,
            "OI - DB Development": self.MS_TEAMS_WEBHOOK_OI_DB_DEV,
            "OI - DB Administration": self.MS_TEAMS_WEBHOOK_OI_DB_ADMIN,
            "OI - RDA": self.MS_TEAMS_WEBHOOK_OI_RDA,
            "OI - Telecommunications": self.MS_TEAMS_WEBHOOK_OI_TC,
            "ADC": self.MS_TEAMS_WEBHOOK_ADC,
            "ControlUp": self.MS_TEAMS_WEBHOOK_CONTROLUP,
            "Sonstige": self.MS_TEAMS_WEBHOOK_SONSTIGE,
            "General": self.MS_TEAMS_WEBHOOK_GENERAL,
        }

    def get_webhook_for_team(self, team: str) -> Optional[str]:
        """Get webhook URL for a specific team/channel"""
        webhook_map = self.get_teams_webhook_map()
        if team in webhook_map and webhook_map[team]:
            return webhook_map[team]
        # Partial match fallback
        team_lower = team.lower()
        for key, url in webhook_map.items():
            if url and (key.lower() in team_lower or team_lower in key.lower()):
                return url
        return self.MS_TEAMS_WEBHOOK_GENERAL


settings = Settings()