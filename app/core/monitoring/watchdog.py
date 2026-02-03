import asyncio
import logging
import subprocess
import json
from datetime import datetime
from typing import List
from fastapi import WebSocket
from sqlalchemy import text
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.db_functions.db_schema2 import get_db
from app.config import settings
import aio_pika

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [WATCHDOG] %(message)s")
logger = logging.getLogger(__name__)

# --- ADMIN CREDENTIALS ---
ADMIN_USER = "Administrator"      
ADMIN_PASS = "YourPassword123"    
# -------------------------

# --- WEBSOCKET MANAGER ---
class ConnectionManager:
    """
    Manages active WebSocket connections and broadcasts messages.
    """
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """Sends the status JSON to all connected clients."""
        if not self.active_connections:
            return
            
        # Convert to JSON string
        json_msg = json.dumps(message, default=str)
        
        # Iterate over a copy to safely handle disconnects during iteration
        for connection in self.active_connections[:]:
            try:
                await connection.send_text(json_msg)
            except Exception:
                self.disconnect(connection)
# -------------------------

class SystemWatchdog:
    def __init__(self, app_executor, model, tokenizer, email_scheduler):
        self.app_executor = app_executor
        self.model = model
        self.tokenizer = tokenizer
        self.email_scheduler = email_scheduler
        
        self.scheduler = AsyncIOScheduler()
        self.consumer_task_class = None
        self.consumer_task_noti = None
        self._paused = False
        
        # New: WebSocket Manager & Status Storage
        self.ws_manager = ConnectionManager()
        self.latest_report = {"status": "Initializing...", "timestamp": datetime.now()}

    async def start(self, consumer_task_class, consumer_task_noti):
        if self.scheduler.running:
            logger.warning("Watchdog is already running.")
            return

        self.consumer_task_class = consumer_task_class
        self.consumer_task_noti = consumer_task_noti
        self._paused = False
        
        # Startup Wait
        logger.info("Watchdog: Validating consumer startup status...")
        await asyncio.sleep(5) 
        
        startup_failed = False
        if self.consumer_task_class.done():
            logger.error("Watchdog: Classification Consumer failed to start!")
            startup_failed = True
        if self.consumer_task_noti.done():
            logger.error("Watchdog: Notification Consumer failed to start!")
            startup_failed = True

        if startup_failed:
            logger.warning("Watchdog: Detected immediate startup failure. Triggering recovery...")
            await self._check_consumers()
        else:
            logger.info("Watchdog: Consumers appear to be UP.")

        if not self.scheduler.get_job('system_health_check'):
            self.scheduler.add_job(
                self._monitor_job, 
                'interval', 
                seconds=60, 
                id='system_health_check',
                replace_existing=True
            )
        
        self.scheduler.start()
        logger.info("System Watchdog scheduler started (Interval: 60s).")

    def stop(self):
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        logger.info("System Watchdog stopped.")

    def pause(self):
        self._paused = True
        self.latest_report["status"] = "PAUSED (Maintenance)"
        # Broadcast pause state immediately
        asyncio.create_task(self.ws_manager.broadcast(self.latest_report))
        logger.info("Watchdog PAUSED.")

    def resume(self):
        self._paused = False
        logger.info("Watchdog RESUMED.")

    def get_tasks(self):
        return self.consumer_task_class, self.consumer_task_noti

    async def _monitor_job(self):
        """
        Runs every 60s: Checks health, builds report, broadcasts via WebSocket.
        """
        try:
            if self._paused:
                return

            # 1. Initialize Report
            report = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": "HEALTHY",
                "checks": {}
            }

            # 2. Run Checks and Populate Report
            report["checks"]["postgres"] = await self._check_postgres()
            report["checks"]["rabbitmq"] = await self._check_rabbitmq()
            report["checks"]["scheduler"] = await self._check_scheduler()
            
            # Consumers
            cons_res = await self._check_consumers()
            report["checks"].update(cons_res)

            # 3. Determine Overall Status
            if any(v != "UP" for v in report["checks"].values()):
                report["status"] = "DEGRADED (Self-Healing Active)"

            # 4. Save and Broadcast
            self.latest_report = report
            logger.info(f"Watchdog Check Complete: {report['status']}")
            await self.ws_manager.broadcast(report)
            
        except Exception as e:
            logger.error(f"Watchdog: Unexpected error in monitor job: {e}")

    # --- Updated Check Methods to Return Status Strings ---

    async def _check_postgres(self):
        try:
            db = next(get_db())
            try:
                db.execute(text("SELECT 1"))
                return "UP"
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Postgres Check Failed: {e}")
            return f"DOWN: {str(e)}"

    async def _check_rabbitmq(self):
        try:
            connection = await aio_pika.connect_robust(settings.RABBITMQ_URL, timeout=5)
            await connection.close()
            return "UP"
        except Exception:
            # Trigger Restart
            logger.warning("RabbitMQ is DOWN. Restarting...")
            await self._restart_rabbitmq()
            return "RESTARTING"

    async def _restart_rabbitmq(self):
        # ... (Same Restart Logic as before) ...
        logger.info("Watchdog: RabbitMQ is down. Attempting direct restart...")
        loop = asyncio.get_running_loop()
        
        # Kill old
        await loop.run_in_executor(
            self.app_executor, 
            lambda: subprocess.run("taskkill /F /IM erl.exe", shell=True, capture_output=True)
        )
        await asyncio.sleep(2)

        # Start new
        ps_script = f"""
        $secpasswd = ConvertTo-SecureString '{ADMIN_PASS}' -AsPlainText -Force
        $mycreds = New-Object System.Management.Automation.PSCredential ('{ADMIN_USER}', $secpasswd)
        Start-Process -FilePath "rabbitmq-server" -ArgumentList "-detached" -Credential $mycreds -WindowStyle Hidden
        """
        try:
            await loop.run_in_executor(
                self.app_executor,
                lambda: subprocess.run(["powershell", "-Command", ps_script], capture_output=True)
            )
        except Exception as e:
            logger.error(f"RabbitMQ Restart Failed: {e}")

    async def _check_scheduler(self):
        try:
            if not self.email_scheduler.scheduler.running:
                await self.email_scheduler.start()
                return "RESTARTING"
            return "UP"
        except Exception as e:
            return f"ERROR: {str(e)}"

    async def _check_consumers(self):
        results = {}
        results["consumer_classification"] = await self._ensure_task_running("consumer_task_class", "consumer_main", "Class Consumer")
        results["consumer_notification"] = await self._ensure_task_running("consumer_task_noti", "consumer_notification", "Noti Consumer")
        return results

    async def _ensure_task_running(self, task_ref_name, task_func_identifier, task_name):
        # Lazy imports...
        from app.core.model_consumer.consumer import consumer_main
        from app.core.notification_consumer.consumer_notification import consumer_main as consumer_notification
        
        if task_func_identifier == "consumer_main": task_func = consumer_main
        elif task_func_identifier == "consumer_notification": task_func = consumer_notification
        else: return "UNKNOWN"

        current_task = getattr(self, task_ref_name)
        status = "UP"
        
        if current_task is None or current_task.done():
            status = "RESTARTING"
            logger.info(f"Watchdog: Restarting {task_name}...")
            try:
                new_task = asyncio.create_task(
                    task_func(self.app_executor, self.model, self.tokenizer)
                )
                setattr(self, task_ref_name, new_task)
            except Exception as e:
                status = f"FAILED: {str(e)}"

        return status