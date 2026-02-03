from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import logger
from sqlalchemy import desc
# from datetime import datetime
from app.db_functions.db_schema2 import get_db, Emails, DuplicateEmail, RawEmail,JobTable
from app.db_functions.db_functions import *
from app.core.scheduler.producer import send_data_to_queue
from app.logging.logging_config import scheduler_logger
from app.logging.logging_decorator import log_function_call
import win32com.client
import pythoncom
import hashlib
from datetime import datetime,timezone
from pathlib import Path
import re
from datetime import timedelta 
LAST_RUN_FILE = "last_run.txt"
import re
import time
from app.core.deduplication import dedup_main
from app.config import settings
import logging
from app.core.scheduler.producer import send_data_to_queue
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.config import settings
from app.db_functions.db_schema2 import Configuration
import aio_pika

RABBITMQ_URL = settings.RABBITMQ_URL
QUEUE_NAME = settings.CLASS_QUEUE_NAME


class CronJobScheduler:
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        print("Async Scheduler manager initialized.")
    @log_function_call(scheduler_logger)
    async def start(self):
        """
        Starts the scheduler (asynchronously).
        - Fetches the interval from the config table.
        - Adds one immediate job.
        - Adds the recurring interval job.
        """
        if self.scheduler.running:
            return {"status": "error", "message": "Scheduler is already running."}
        
        try:
            # 1. Fetch interval from DB (asynchronously)
            unit, value = await get_config()
            interval_kwargs = {unit: value} # e.g., {'minutes': 10}
            
            # 2. Add the immediate job (APScheduler knows to await the async job)
            self.scheduler.add_job(
                self.hello_world_job,
                'date', # 'date' trigger means run once, now
                id='hello_job_immediate',
                name="Immediate Startup Job",
                replace_existing=True
            )
            
            # 3. Add the recurring interval job
            self.scheduler.add_job(
                self.hello_world_job,
                'interval',
                id='hello_job_recurring',
                name="Recurring Hello Job",
                **interval_kwargs,
                max_instances=3)
            
            # 4. Start the scheduler (this is non-blocking)
            self.scheduler.start()
            
            msg = f"Scheduler started. Interval: {value} {unit}. First job runs now."
            print(msg)
            return {"status": "success", "message": msg}

        except Exception as e:
            print(f"Error starting scheduler: {e}")
            return {"status": "error", "message": str(e)}
    @log_function_call(scheduler_logger)
    async def stop(self):
        """
        Stops the scheduler gracefully (asynchronously).
        """
        if not self.scheduler.running:
            return {"status": "info", "message": "Scheduler was not running."}
            
        try:
            # Await shutdown to let running jobs finish
            self.scheduler.shutdown() 
            self.scheduler = AsyncIOScheduler() # Re-init
            print("Scheduler stopped.")
            return {"status": "success", "message": "Scheduler stopped."}
        except Exception as e:
            print(f"Error stopping scheduler: {e}")
            return {"status": "error", "message": str(e)}
        
    @log_function_call(scheduler_logger)
    async def restart(self):
        """Stops and then starts the scheduler to apply new settings."""
        print("Scheduler restarting...")
        stop_response = await self.stop()
        start_response = await self.start()
        print("Scheduler restart complete.")
        return {
            "status": "success",
            "stop_details": stop_response,
            "start_details": start_response
        }
    @log_function_call(scheduler_logger)
    def get_next_run_time(self) -> (datetime | None):
        """Helper to get the next run time (this is still synchronous)."""
        if not self.scheduler.running:
            return None
            
        job = self.scheduler.get_job('hello_job_recurring')
        if job:
            return job.next_run_time
        return None

    @log_function_call(scheduler_logger)
    async def hello_world_job(self):
        """
        The main ASYNC job function.
        1. Prints "Hello, World!".
        2. Asynchronously logs its execution to the job_execution table.
        """
        print(f"\n--- [ASYNC JOB EXECUTING at {datetime.now()}] ---")
        print("Hello, World!")

        # Get current time as the "from" period
        exec_start_time = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:23], '%Y-%m-%d %H:%M:%S.%f')

        # Get the interval from the DB (asynchronously)
        # This no longer blocks the event loop
        unit, value = await get_config()
        job_interval_str = f"{value} {unit}"

        if unit == 'minutes':
            delta = timedelta(minutes=value)
        elif unit == 'seconds':
            delta = timedelta(seconds=value)
        else:
            # Fallback for invalid config
            delta = timedelta(minutes=10)
            job_interval_str = "10 minutes (fallback)"
            
        # period_from  = exec_start_time - delta
                # The job "work" is done, get end timestamp
        RABBITMQ_URL = settings.RABBITMQ_URL
        QUEUE_NAME = settings.CLASS_QUEUE_NAME
        items,end_time = await asyncio.to_thread(self.outlook_job)
        try:
        # 1. Connect ONCE, outside the loop
        # connect_robust will handle retries if the connection drops
            logging.info("Connecting to RabbitMQ...")
            connection = await aio_pika.connect_robust(RABBITMQ_URL,heartbeat=600,timeout=6000)
            
            logging.info("Connection successful.")

            # 2. Use the connection context manager
            async with connection:
                # 3. Create ONE channel
                channel = await connection.channel()
                logging.info("Channel created.")
                
                # 4. Declare the queue ONCE
                # This is idempotent, so it's safe to call.
                # It ensures the queue exists before we publish.
                await channel.declare_queue(QUEUE_NAME, durable=True)
                logging.info(f"Queue '{QUEUE_NAME}' declared.")

                # 5. This is your "for loop"
                logging.info(f"Starting to publish {len(items)} messages...")
                db=next(get_db())
                for item in items:
            
                    # await send_data_to_queue(RABBITMQ_URL,QUEUE_NAME,message_data={"message":"hello world"})

                    await send_data_to_queue(channel,QUEUE_NAME,message_data=item)
                    
                    
                    result = update_raw_email_status(db,item['email_id'],True)
                    print("saved in db",result)

                logging.info("All messages have been published.")
                db.close()


        except aio_pika.exceptions.AMQPConnectionError as e:
            logging.error(f"FATAL: Connection to RabbitMQ failed: {e}")
        except Exception as e:
            logging.exception("An unhandled error occurred in main_publisher.")


        # Apply a dlq 
        
        # exec_end_time = datetime.datetime.now()


        # Log to database (asynchronously)
        # await add_job_execution(
        #     job_interval=job_interval_str,
        #     period_from=period_from,
        #     period_to=exec_start_time,
        #     execution_end_time=exec_end_time
        # )
        db = next(get_db())
        insert_job_info(next(get_db()),{'frequency':delta,'job_start_time':exec_start_time,'last_run_time':end_time})
        db.close()
        print("--- [ASYNC JOB FINISHED] ---")
 
    @log_function_call(scheduler_logger)
    def extract_current_body(self,body_text):
    # Common reply markers
        markers = [
            r"^From:.*",  # Outlook reply format
            r"^On .* wrote:",  # Gmail-style reply
            r"^-----Original Message-----",  # Outlook classic
            r"^Sent:.*",  # Sent timestamp
        ]
       
        # Combine markers into one regex
        pattern = re.compile("|".join(markers), re.MULTILINE)
       
        # Split at first marker
        split_body = pattern.split(body_text)
       
        # Return the top part (current message)
        return split_body[0].strip() if split_body else body_text.strip()

    @log_function_call(scheduler_logger)
    def generate_email_id(subject: str, received_time: datetime) -> str:
        """
        Generates a unique, deterministic ID based on the email's subject and 
        the received timestamp (to the microsecond).
        
        The hash ensures that the same email received multiple times will have 
        the same unique key.
        """
        # Ensure datetime is timezone-aware and normalized
        if received_time.tzinfo is None:
            received_time = received_time.replace(tzinfo=timezone.utc)
        
        # Format time with microseconds for better uniqueness
        timestamp_str = received_time.isoformat()
        
        # Combine components and encode to bytes
        hash_input = f"{subject}|{timestamp_str}".encode('utf-8')
        
        # Return the SHA-256 hash (64 characters)
        return hashlib.sha256(hash_input).hexdigest()

    @log_function_call(scheduler_logger)
    def outlook_job(self):
       
        print(f"[{datetime.now()}] Cron job executed.")
        items = []
        pythoncom.CoInitialize()
 
        try:
            outlook = win32com.client.Dispatch("Outlook.Application")
            namespace = outlook.GetNamespace("MAPI")
            print("Outlook COM object available")
        except pythoncom.com_error as e:
            print("COM error:", e)
        # namespace = self.namespace_sp
       
       
       
        print("Namespace:", namespace)
        inbox = namespace.GetDefaultFolder(6)
        # "6" refers to the inbox
        messages2 = inbox.Items
        db = next(get_db())
        try:
            #Write function from the db
           
            last_job = db.query(JobTable).order_by(desc(JobTable.job_id)).first()
            print("in the try",last_job)
           
            # with open(LAST_RUN_FILE, "r") as f:
            #     last_run = datetime.datetime.fromisoformat(f.read().strip())
 
            last_run = datetime.fromisoformat(str(last_job.last_run_time))
            print("last_run in try block ",last_run)
            last_run =  datetime.now() - timedelta(hours=7)
            
        except Exception as e:
            print("exception in except ", {e})
            last_run = datetime.now() - timedelta(hours = 8)
 
 
        db.close()
        end_time = last_run
   
        restriction = f"[ReceivedTime] > '{last_run.strftime('%d/%m/%Y %H:%M')}'"
        messages = inbox.Items.Restrict(restriction)
        print("Message:",messages.count)
        messages.Sort("[ReceivedTime]", False)
        print("Last run:",last_run.strftime('%m/%d/%Y %H:%M:%S'))
        folder_path = Path("email_msg_files")
        folder_path.mkdir(parents=True, exist_ok=True)
 
 
        def sanitize_filename(name):
            # Remove characters invalid in Windows file names: \ / : * ? " < > |
            return re.sub(r'[\\/:*?"<>|]', '_', name)
 
       
        for msg in messages:
            try:
                # print("MSG:", msg)
                # Get SMTP email using PropertyAccess
                #print("msg.Receivedtime Pulkit0", msg.ReceivedTime)
                prop_accessor = msg.PropertyAccessor
                # print("Prop Accessor:", prop_accessor)
                smtp_address = prop_accessor.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x5D01001F")
                db = next(get_db())
                latest_config = (
                    db.query(Configuration)
                    .order_by(Configuration.created_at.desc())
                    .first())
                db.close()

                # ✅ UPDATED: Support multiple comma-separated emails from configuration
                allowed_senders = [email.strip().lower() for email in latest_config.outlook_email.split(',')]
                print(f"\n{'='*60}")
                print(f"📧 Current email sender: {smtp_address}")
                print(f"✅ Allowed senders from config: {allowed_senders}")
                print(f"{'='*60}\n")

                # Case-insensitive comparison
                if smtp_address.lower() in allowed_senders: #== "nairaarathi@bitzerasia.com":
                    print(f"✅ PROCESSING email from: {smtp_address}")
                    #if msg.Subject!="ControlUp alert mail - Advanced Trigger -  Logical Disk: D:\ on Computer: DEROT04406.bitzer.biz.":
                        #continue   
                    
                    
                    #Pulkit 19/11 10:45PM Test for Email Processing Error
                    print("Before sanitize", msg.Subject)
                    subject = sanitize_filename(msg.Subject)
                    print("In between sanitiize", type(subject))
                    email_hash2 = generate_email_id(subject,msg.ReceivedTime)
                    # file_name=subject+".msg"
                    file_name = str(email_hash2)+".msg"
                    print("After sanitize", subject, type(subject))
                    #End
                    # file_name=msg.Subject+".msg"
                    # print("File name:", file_name)
                    file_path=folder_path / file_name
                    # print("File path:", file_path)
                   
                    # print(str(file_path.resolve()),3)
                    # print("smtp_address",smtp_address)
                   
                    #create hash function  using subject and recieved_time
                    msg.SaveAs(str(file_path.resolve()),3)
 
                    # check here for the db primary key
 
                    #send email hash in the items
                    db = next(get_db())
                    rt = msg.ReceivedTime.strftime("%Y-%m-%d %H:%M:%S.%f")[0:23]
 
                    # Convert the string to a datetime object
                    datetime_object = datetime.strptime(rt, '%Y-%m-%d %H:%M:%S.%f')
 
                    # Convert the datetime object to a Unix timestamp (float)
                    timestamp = datetime_object.timestamp()
                    # print("msg.Receivedtime Pulkit1",datetime_object)
                    end_time = max(end_time,datetime_object)

                    email = insert_raw_email(db,{"email_id":email_hash2,
                                        "sender":smtp_address,
                                        "body":msg.Body,
                                        "subject":msg.Subject,
                                        "received_at":datetime_object,
                                        "email_path":str(file_path.resolve())
                                        }) 
                    db.close()
                    print("msg.Receivedtime Pulkit2",msg.ReceivedTime)
                    print("email status ",email.status)
                    if email.status == True:
                        print("printing because its already in the table")
                        continue
 
                    items.append({"email_id":email.email_id,
                                  "sender_address":smtp_address,
                                    "content":msg.Body,
                                    "subject":msg.Subject,
                                    "received_time":msg.ReceivedTime.isoformat(),
                                    "msg_path":str(file_path.resolve())
                                    })
                   
                    # end_time = max(end_time,datetime_object)
                    print("msg.Receivedtime",msg.ReceivedTime)
 
                # if smtp_address=="pundareeks@kpmg.com" or smtp_address == "tnarchana@kpmg.com":
                   
                #     current_body = self.extract_current_body(msg.Body)
                #     print("Current Message Body:\n", current_body,msg.Subject)
                   
 
                # print("-" * 50)
           
            except Exception as e:
                print("Error processing message:", e)
       
 
        # remove writing to LAST_RUNF_FILE from here and write it into the queue producer try block
        # with open(LAST_RUN_FILE, "w") as f:
        #     # f.write(datetime.datetime.now().isoformat())
        #     f.write(end_time)
        pythoncom.CoUninitialize()
        print("length of items in outlook job ",len(items))
        if end_time:
            print("end_time ", end_time)
            return items,end_time
        print("last run ",last_run)
        return items,last_run


    @log_function_call(scheduler_logger)
    async def job_task(self):
       
        output= self.outlook_job()
        deduplicated,duplicated = dedup_main(output)
        # saving to database table emails and duplicate_emails will come here.
        db = next(get_db())
        for email in deduplicated:
            queue_response = await send_data_to_queue(email)
            allparsedfields = email.get("all_parsed_fields", {})

            email = Emails(
                email_id=email.get("signature"),
                subject=allparsedfields.get("subject", ""),
                body=allparsedfields.get("content", ""),
                sender=allparsedfields.get("sender_address", ""),
                received_at=email.get("received_time"),
                inserted_at=datetime.datetime.utcnow(),
                status = None,
            )
            
            # db = get_db()
            db.add(email)
        for duplicat in duplicated:
            allparsedfields = duplicat.get("all_parsed_fields", {})
            duplicate_email = DuplicateEmail(
                email_id=duplicat.get("signature"),
                subject=allparsedfields.get("subject", ""),
                body=allparsedfields.get("content", ""),
                sender=allparsedfields.get("sender_address", ""),
                received_at=duplicat.get("received_time"),
                inserted_at=datetime.datetime.utcnow(),
            )
            db.add(duplicate_email)
        db.commit()

        


        

    @log_function_call(scheduler_logger)
    def get_status(self) -> dict:
        """Get scheduler status"""
        return {
            "is_running": self.scheduler.running,
            "next_run": None if not self.scheduler.running else str(self.scheduler.get_job('hello_job_recurring').next_run_time),
            "interval_minutes": settings.SCHEDULER_INTERVAL_MINUTES,
            "job_count": len(self.scheduler.get_jobs())
        }
    
    def trigger_manual_run(self):
        """Manually trigger email processing"""
        logger.info("Manual email processing triggered")
        self.job_task()
        return {"message":"Success"}
     
        
 

cron_runner = CronJobScheduler()
 
# if __name__ == "__main__":
#     cron_runner = CronJobScheduler()
#     cron_runner.start()
