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
import asyncio
from concurrent.futures import ThreadPoolExecutor
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
@asynccontextmanager
async def lifespan(app: FastAPI):
    # On application startup:
    global app_executor
    app_executor = ThreadPoolExecutor(max_workers=3)
    print("Application starting up...")
    await email_scheduler.start()

    model,tokenizer = await asyncio.to_thread(load_model)

    global consumer_task_class 
    consumer_task_class = asyncio.create_task(consumer_main(app_executor,model,tokenizer))
    
    #global consumer_task_summ
    #consumer_task_summ = asyncio.create_task(consumer_summarization(app_executor,model,tokenizer))

    global consumer_task_noti
    consumer_task_noti = asyncio.create_task(consumer_notification(app_executor,model,tokenizer))
    # await asyncio.run(consumer_main())
    yield
    # On application shutdown:
    print("Application shutting down...")
    await email_scheduler.stop()
    app_executor.shutdown(wait=True)
    if consumer_task_class:
        consumer_task_class.cancel()
        consumer_task_noti.cancel()
        try:
            await consumer_task_class
            await consumer_task_noti
        except asyncio.CancelledError:
            print("Consumer task cancelled successfully.")
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
        email_scheduler.start()
        return {"message": "Scheduler started successfully", "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error starting scheduler: {str(e)}")

@app.post("/scheduler/stop")
async def stop_scheduler():
    """Stop the email scheduler"""
    try:
        email_scheduler.stop()
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
    This will:
    1. Get the next run time of the *old* job.
    2. Asynchronously update the interval in the database.
    3. Asynchronously restart the scheduler.
    """
    try:
        # 1. Get info about the old job (sync, fine)
        old_next_run = email_scheduler.get_next_run_time()
        old_run_msg = (
            f"Previous job was scheduled to run at: {old_next_run}"
            if old_next_run
            else "Previous job was not running or had no next run time."
        )
        print(old_run_msg)

        # 2. Update the config in the database (asynchronously)
        await set_config(config.unit, config.value) # CRITICAL CHANGE
        new_interval_msg = f"New interval set to: {config.value} {config.unit}"
        print(new_interval_msg)

        # 3. Restart the scheduler (asynchronously)
        restart_details = await email_scheduler.restart() # CRITICAL CHANGE
        
        return {
            "status": "success",
            "message": "Scheduler restarted with new interval.",
            "old_schedule_info": old_run_msg,
            "new_schedule_info": new_interval_msg,
            "restart_details": restart_details
        }
    except Exception as e:
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.api:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG
    )
