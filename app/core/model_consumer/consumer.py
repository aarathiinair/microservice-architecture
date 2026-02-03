import asyncio
import json_repair
import logging
import json
import re
import aio_pika
import sqlite3
from app.core.model_consumer.model_processing import process_trigger
from datetime import datetime
from typing import Dict
from app.core.model_consumer.model_processing import ModelProcessing
#from app.config import settings
from app.core.model_consumer.model import load_model 
#from app.core.teams_integration import TeamsIntegration,EmailData
# Setup basic logging
from app.core.model_consumer.mentenance_check import MaintenanceWindowService
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
from app.core.notification_consumer.jira_integration import JiraIntegration
from app.config import settings
from app.core.model_consumer.producer import send_data_to_queue
from app.db_functions.db_schema2 import *
from app.db_functions.db_functions import *
from app.logging.logging_config import model_logger
from app.logging.logging_decorator import log_function_call
#from app.api.v1.endpoints.api import app_executor
# Global RabbitMQ URL reference (defined in main but needed here for publishing retry)
# We must define a placeholder here, it will be populated by main in a real application
RABBITMQ_URL_GLOBAL ="amqp://guest:guest@localhost/" #settings.RABBITMQ_URL

RABBITMQ_URL = settings.RABBITMQ_URL
CLASS_QUEUE_NAME = settings.CLASS_QUEUE_NAME
SUMM_QUEUE_NAME = settings.JIRA_QUEUE_NAME
CLASS_DLQ_NAME = settings.CLASS_DLQ_NAME


# ==============================
# Dedicated Publish Function for Retry (Indentation Fixed)
# ==============================
@log_function_call(model_logger)
async def publish_retry_message(
    body: bytes, routing_key: str, headers: Dict, url: str = RABBITMQ_URL_GLOBAL
):
    """Establishes an isolated connection/channel to republish the message."""
    try:
        connection = await aio_pika.connect_robust(url)
        async with connection:
            channel = await connection.channel()

            # --- FIX: Instantiate the Default Exchange object directly (name="") ---
            # This avoids the network conflict caused by trying to DECLARE the built-in exchange.
            default_exchange = aio_pika.Exchange(
                channel=channel,
                name="",
                type=aio_pika.ExchangeType.DIRECT,
                durable=True
            )
            # --- END FIX ---

            message_to_send = aio_pika.Message(
                body=body,
                headers=headers,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )

            await default_exchange.publish(
                message_to_send,
                routing_key=routing_key,
            )
            print(f"Successfully republished message for retry to '{routing_key}'")

    except Exception as e:
        logging.error(f"FATAL: Failed to republish message for retry: {e}")
        # If publishing fails here, the message is permanently lost, hence the importance of robust publish


# ==============================
# Consumer with DLQ (Indentation Fixed)
# ==============================
MAX_RETRIES = 5

@log_function_call(model_logger)
async def process_message(message: aio_pika.IncomingMessage, dlx_exchange: aio_pika.Exchange,model,tokenizer,app_executor):
    # This context manager manages the lifecycle of the message on the consumption channel

    try:
        loop = asyncio.get_running_loop()
        body = json.loads(message.body.decode())
        
        db = next(get_db())
        query = db.query(SegregatedEmail).filter(SegregatedEmail.email_id==body.get('email_id')).first()
        #status = query.status if query else False
        db.close()
        #print("status in consumer",status)
        
        if not query:
            

        #check for the hash if it exist then do the processing else not
            mp = ModelProcessing(model,tokenizer)
            
            print("in consumer in model consumer  printing", body)
            file_path = "./segregationprompt1.txt"
            output1 = await loop.run_in_executor(app_executor, mp.process,body,file_path)
            print("output",output1)
            file_path = "./segregationprompt.txt"
            output1=json_repair.loads(output1)
            output1["trigger_name"]=output1["trigger_name"].strip()
            output1["resource_name"]=output1["resource_name"].strip()
            output1["subject"]=body["subject"]

            graceful_pattern = re.compile(r'machine\s+shut\s*down\s+gracefully', re.IGNORECASE)
            combined_text = f"{body.get('subject', '')} {body.get('content', '')}"
            if graceful_pattern.search(combined_text):
                output1['priority'] = 'informational'
                output1['type'] = 'informational'
                output1['recommended_action'] = 'N/A'
                
                db = next(get_db())
                row = insert_or_update_segregation(db,body['email_id'],output1)
                db.close()

                db=next(get_db())
                row = insert_or_update_segregation(db,body['email_id'],{"status":True})
                db.close()
                print("not found in the table segregationEmail and inserted in that",row)
                await message.ack()
                return
            db=next(get_db())
            #row=get_email_id_within_hour(db,target_trigger=output1["trigger_name"],target_resource=output1["resource_name"],delete_email=body["email_id"])
            #print("\n Results after checking for a match",body["subject"],row)
                    
            #if row!=None:
            #    print("printing because there is a email within an hour")
                #duplicateemail=insert_duplicate_email(db,{"email_id":row,
                                    #"duplicate_email_id":body["email_id"],
                                    #"sender":body["sender_address"],
                                    #"body":body["content"],
                                    #"subject":body["subject"],
                                    #"received_at":body["received_time"],
                                    #"email_path":body["msg_path"]
                                       # })
                #db.close()
                #await message.ack()
                #return
            db.close()


            output2= await loop.run_in_executor(app_executor, process_trigger,output1["trigger_name"],model,tokenizer)
            print("output",output2)
        
            
            if isinstance(output2, str):
              output2 = json_repair.loads(output2)
              db=next(get_db())
              server_obj = get_server_by_name(db, output1["resource_name"])
              if server_obj:
                output1["generated_summary"]=f"Found description: {server_obj.description_function} \n" +"generated summary:" +output1["generated_summary"]
                print(f"Found description: {server_obj.description_function}")
              else:
                print("Server not found.")
            output=output2|output1
            

            # write into the email_processing table
            db = next(get_db())
            row = insert_or_update_segregation(db,body['email_id'],output)
            db.close()
        
            

        else:
            output = {'priority':query.priority, "trigger_name": query.trigger_name, "resource_name": query.resource_name, "type": query.type, "status":query.status} 
            if output["status"]:
                await message.ack()
                return
            db = next(get_db())
            query = db.query(SummaryTable).filter(SummaryTable.email_id==body.get('email_id')).first()
            summary1=query.summary
            l=summary1.split("\n Recommended Actions:") if len(summary1.split("\n Recommended Actions:"))>1 else list(summary1)
            output['generated_summary'] = l[0] 
            output['recommended_action']=l[1] if len(l)>1 else None
            db.close()
            
            
        mainten = MaintenanceWindowService()

            # Maintenance check 
        if mainten.is_in_maintenance(output["resource_name"]): 
            await message.ack()     #     
            print(f"Not creating JIRA ticket due to machine {output['resource_name']} being in maintenance" )
            
        
        else:
            #Send it to the que if it is in p1 and p2 
            if output["priority"] in ("P1","P2") :
                #send to the queue 
                print("send to queue")

                output['email_id'] = body['email_id']
                output["body"]=body["content"]
                output["subject"]=body["subject"]
                output['sender'] = body["sender_address"]
                output['timestamp'] = body['received_time']
                output['content'] = body['content']
                output['generated_summary']=output['generated_summary'] + "\n Recommended Actions:"+output['recommended_action']
                output["path"]=body["msg_path"]
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
                        await channel.declare_queue(SUMM_QUEUE_NAME, durable=True)
                        logging.info(f"Queue '{SUMM_QUEUE_NAME}' declared.")


                        await send_data_to_queue(channel,SUMM_QUEUE_NAME,message_data=output)
                        await message.ack()
                        db=next(get_db())
                        row = insert_or_update_segregation(db,body['email_id'],{"status":True})
                        db.close()
                        db=next(get_db())
                        row1 = insert_or_update_summary(db,body.get('email_id'),output.get('generated_summary'),status=True)
                        db.close()
                        print("sent to the queue in model_consumer",row)
                        

                        #update into the email_processing table


                except aio_pika.exceptions.AMQPConnectionError as e:
                    logging.error(f"FATAL: Connection to RabbitMQ failed: {e}")
                except Exception as e:
                    
                    logging.exception("An unhandled error occurred in main_publisher.")
            
            else:
                await message.ack()

    except Exception as e:
        # Retry tracking in headers
        headers = message.headers or {}
        retries = headers.get("x-retries", 0)
        print(e)
        if retries < MAX_RETRIES:
            # Log the retry count being attempted
            logging.warning("Retry %s/%s for message %s", retries + 1, MAX_RETRIES, body)
            print(f"retrying and setting header to {retries + 1} time")

            # 1. NACK the current message delivery (DO NOT requeue)
            # This clears the message from the current consumption channel.
            await message.nack(requeue=False)

            # 2. Prepare NEW headers by copying existing ones
            new_headers = message.headers.copy() if message.headers else {}
            new_headers["x-retries"] = retries + 1

            # --- FIX: Delegate publishing to an isolated function/channel ---
            # This prevents the race condition and "channel closed" errors.
            await publish_retry_message(
                message.body,
                message.routing_key,
                new_headers
            )

        else:
            logging.error("Max retries reached. Sending to DLQ & DB...")
            await message.nack(requeue=False)

            # Send to DLQ using the dedicated DLX
            await dlx_exchange.publish(
                aio_pika.Message(
                    body=message.body,
                    headers={"x-error": str(e)},
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="dlq.class",
            )
            print(f"DLQ done.")
            # Log to DB (uncommented for full functionality)
            #log_error_to_db(message.body.decode(), str(e))
        


# ==============================
# Main loop (Indentation Fixed)
# ==============================
@log_function_call(model_logger)
async def consumer_main(app_executor,model,tokenizer):
    global RABBITMQ_URL_GLOBAL # Use global for the URL so the isolated function can access it

    # Setup logging again (redundant but ensures configuration)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    #init_db()

    #RABBITMQ_URL = "amqp://guest:guest@localhost/"
    # RABBITMQ_URL_GLOBAL = "amqp://guest:guest@localhost/" #settings.RABBITMQ_URL # Set the global variable for isolated publishing
    # QUEUE_NAME = "my_async_queue"#settings.QUEUE_NAME
    # DLQ_NAME = "my_async_queue_dlq"
    # CLASS_DLQ_NAME = 'dlq_queue_class'
    #global pipe 
    # model,tokenizer = await asyncio.to_thread(load_model)
    connection = await aio_pika.connect_robust(RABBITMQ_URL,heartbeat=600 ,timeout=6000)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        # Declare normal queue
        queue = await channel.declare_queue(CLASS_QUEUE_NAME, durable=True)

        # Declare Dead Letter Queue & Exchange
        dlx_exchange = await channel.declare_exchange("dlx", aio_pika.ExchangeType.DIRECT, durable=True)
        dlq = await channel.declare_queue(CLASS_DLQ_NAME, durable=True)
        await dlq.bind(dlx_exchange, routing_key="dlq.class")

        logging.info("Consumer started. Waiting for messages in consumer.py file  '%s'...", CLASS_QUEUE_NAME)
        print(f"starting")
        # Consume messages
        await queue.consume(lambda msg: process_message(msg, dlx_exchange,model,tokenizer,app_executor), no_ack=False)

        # Keep running
        await asyncio.Future()
    

# Check if the script is run directly
# if __name__ == "__main__":
#     try:
#         asyncio.run(consumer_main())
#     except KeyboardInterrupt:
#         logging.info("Consumer stopped by user.")