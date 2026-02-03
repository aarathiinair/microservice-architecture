from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
from fastapi.concurrency import asynccontextmanager
from pydantic import BaseModel, field_validator
from typing import Literal 
from app.config import settings
from app.db_functions.db_functions import set_config ,get_job_history
from app.db_functions.db_schema2 import get_db, Emails, EmailProcessing, create_tables , engine
from app.models.model_pydantic import EmailResponse, EmailListResponse, HealthCheck,IntervalConfig
from app.core.scheduler.scheduler import cron_runner as email_scheduler
from app.core.model_consumer.consumer import consumer_main
from app.core.notification_consumer.consumer_notification import consumer_main as consumer_notification
from app.core.summerization_consumer.consumer_summarization import consumer_main as consumer_summarization
from app.core.model_consumer.model import load_model
from app.core.monitoring.watchdog import SystemWatchdog
import asyncio
from concurrent.futures import ThreadPoolExecutor
from app.core.certificate_watcher.certificatewatcher import certificate_main
from fastapi import FastAPI, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
import json

#from app.core.consumer import consumer_main
# from app.email_service import EmailService
# from app.ml_classifier import EmailClassifier
# from app.jira_service import JiraService

# Create FastAPI app
app = FastAPI(
    title="Email Processor API",
    description="FastAPI server for processing and classifying emails using ML",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
# email_service = EmailService()
# classifier = EmailClassifier()
# jira_service = JiraService()
create_tables(engine)
consumer_task = None
app_executor=None
system_watchdog = None
 
from app.core.monitoring.watchdog import SystemWatchdog
 
# Global variables
system_watchdog = None
consumer_task_class = None
consumer_task_noti = None
app_executor = None
certificate_task=None
@asynccontextmanager
async def lifespan(app: FastAPI):
    # On application startup:
    global app_executor
    global system_watchdog
    global consumer_task_class 
    global consumer_task_noti
    global certificate_task
    app_executor = ThreadPoolExecutor(max_workers=3)
    print("Application starting up...")
    # 1. Start Scheduler
    await email_scheduler.start()
    # 2. Load Model
    model, tokenizer = await asyncio.to_thread(load_model)
    certificate_task = asyncio.create_task(certificate_main())
    # 3. Start Consumers
    consumer_task_class = asyncio.create_task(consumer_main(app_executor, model, tokenizer))
    consumer_task_noti = asyncio.create_task(consumer_notification(app_executor, model, tokenizer))
    # 4. Initialize & Start Watchdog (WITH AWAIT)
    system_watchdog = SystemWatchdog(app_executor, model, tokenizer, email_scheduler)
    # !!! CHANGED: Added 'await' because start() now performs an async startup check !!!
    await system_watchdog.start(consumer_task_class, consumer_task_noti)
 
    yield
    # On application shutdown:
    print("Application shutting down...")
    # Stop Watchdog first
    if system_watchdog:
        system_watchdog.stop()
        # Retrieve latest task references
        consumer_task_class, consumer_task_noti = system_watchdog.get_tasks()
 
    await email_scheduler.stop()
    app_executor.shutdown(wait=True)
    # Cancel tasks
    if consumer_task_class:
        consumer_task_class.cancel()
    if consumer_task_noti:
        consumer_task_noti.cancel()
    try:
        if consumer_task_class: await consumer_task_class
        if consumer_task_noti: await consumer_task_noti
    except asyncio.CancelledError:
        print("Consumer tasks cancelled successfully.")

# --- FastAPI App ---
app = FastAPI(lifespan=lifespan, title="Async Scheduler API")


@app.get("/", response_model=dict)
async def root():
    """Root endpoint"""
    return {
        "message": "Email Processor API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health", response_model=HealthCheck)
async def health_check():
    """Health check endpoint"""
    try:
        # Check database connection
        db = next(get_db())
        from sqlalchemy import text
        db.execute(text("SELECT 1"))
        db_status = "healthy"
        db.close()
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
    
    # Check scheduler status
    scheduler_status = "healthy" if email_scheduler.is_running else "unhealthy"
    
    return HealthCheck(
        status="healthy" if db_status == "healthy" and scheduler_status == "healthy" else "unhealthy",
        timestamp=datetime.utcnow(),
        database=db_status,
        scheduler=scheduler_status
    )

@app.get("/scheduler/status")
async def get_scheduler_status():
    """Get scheduler status and information"""
    return email_scheduler.get_status()

@app.post("/scheduler/start")
async def start_scheduler():
    """Start the email scheduler"""
    try:
        await email_scheduler.start()
        return {"message": "Scheduler started successfully", "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error starting scheduler: {str(e)}")

@app.post("/scheduler/stop")
async def stop_scheduler():
    """Stop the email scheduler"""
    try:
        await email_scheduler.stop()
        return {"message": "Scheduler stopped successfully", "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error stopping scheduler: {str(e)}")

@app.get("/emails", response_model=EmailListResponse)
async def list_emails(
    skip: int = 0,
    limit: int = Query(10, le=100),
    db: Session = Depends(get_db)
):
    """List processed emails with pagination"""
    emails = db.query(Email).offset(skip).limit(limit).all()
    total = db.query(Email).count()
    return EmailListResponse(total=total, emails=emails)
# --- API Endpoints ---
@app.put("/config/interval", tags=["Scheduler Configuration"])
async def update_interval(config: IntervalConfig):
    """
    Updates the job interval asynchronously.
    """
    try:
        # 1. Get info about the old job
        old_next_run = email_scheduler.get_next_run_time()
        old_run_msg = (
            f"Previous job was scheduled to run at: {old_next_run}"
            if old_next_run
            else "Previous job was not running."
        )
 
        # 2. Update config in DB
        await set_config(config.unit, config.value)
        new_interval_msg = f"New interval set to: {config.value} {config.unit}"
 
        # --- WATCHDOG: PAUSE ---
        if system_watchdog:
            system_watchdog.pause()
        # -----------------------
 
        # 3. Restart the scheduler
        restart_details = await email_scheduler.restart()
        # --- WATCHDOG: RESUME ---
        if system_watchdog:
            system_watchdog.resume()
        # ------------------------
 
        return {
            "status": "success",
            "message": "Scheduler restarted with new interval.",
            "old_schedule_info": old_run_msg,
            "new_schedule_info": new_interval_msg,
            "restart_details": restart_details
        }
    except Exception as e:
        # Ensure we resume even if restart fails
        if system_watchdog:
            system_watchdog.resume()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/config/next_run", tags=["Scheduler Configuration"])
async def get_next_run():
    """Gets the next scheduled run time."""
    next_run = email_scheduler.get_next_run_time()
    if next_run:
        return {"next_run_time": next_run}
    return {"next_run_time": None, "message": "Scheduler is not running or job not found."}



@app.get("/jobs/history", tags=["Job History"])
async def get_job_history():
    """
    Retrieves the 20 most recent job execution logs from the database.
    """
    try:
        history = get_job_history(limit=20)
        return history
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch history: {str(e)}")

@app.get("/scheduler/status", tags=["Scheduler Configuration"])
async def get_scheduler_status():
    """
    Checks the current status of the scheduler and its next run time.
    """
    is_running = email_scheduler.scheduler.running
    next_run = email_scheduler.get_next_run_time()
    
    return {
        "is_running": is_running,
        "next_run_time": next_run if next_run else "N/A"
    }
# @app.websocket("/ws/monitoring")
# async def websocket_monitoring(websocket: WebSocket):
#     """
#     Live WebSocket for Watchdog Status.
#     Connect here to receive JSON updates every 60 seconds.
#     """
#     if not system_watchdog:
#         await websocket.close(reason="Watchdog not initialized")
#         return

#     # 1. Connect the client
#     await system_watchdog.ws_manager.connect(websocket)
    
#     # 2. Send the *current* report immediately upon connection
#     # So the UI doesn't have to wait 60s for the first update
#     if system_watchdog.latest_report:
#         await websocket.send_json(system_watchdog.latest_report)

#     try:
#         # 3. Keep connection open (listen mode)
#         while True:
#             # We just wait for client messages (heartbeats) or disconnects
#             # The broadcast happens in the background via watchdog.py
#             await websocket.receive_text()
#     except WebSocketDisconnect:
#         system_watchdog.ws_manager.disconnect(websocket)
#     except Exception as e:
#         print(f"WebSocket Error: {e}")
#         system_watchdog.ws_manager.disconnect(websocket)

@app.websocket("/ws/monitoring")
async def websocket_monitoring(websocket: WebSocket):
    if not system_watchdog:
        await websocket.close(reason="Watchdog not initialized")
        return

    await system_watchdog.ws_manager.connect(websocket)
    
    # FIX: Use a serializer that handles datetimes if they exist
    if system_watchdog.latest_report:
        # Instead of await websocket.send_json(system_watchdog.latest_report)
        # Use json.dumps with default=str to be 100% safe
        safe_report = json.dumps(system_watchdog.latest_report, default=str)
        await websocket.send_text(safe_report)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        system_watchdog.ws_manager.disconnect(websocket)

@app.get("/monitoring/status")
async def get_monitoring_status():
    """
    Standard HTTP GET endpoint for current system health.
    """
    if not system_watchdog:
        return {"status": "Watchdog not active"}
    return system_watchdog.latest_report

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.api:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG
    )
