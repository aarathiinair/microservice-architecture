

 # Patch running event loop so asyncio.run works inside Jupyter/Colab

import asyncio
import logging
import json
import os
import signal
from typing import Dict
from app.config import settings


import aio_pika
from app.logging.logging_config import model_logger
from app.logging.logging_decorator import log_function_call


RABBITMQ_URL = settings.RABBITMQ_URL
# QUEUE_NAME = settings.SUMM_MODEL_QUEUE
# ==============================
# Job function
# ==============================
@log_function_call(model_logger)
async def send_data_to_queue(channel: aio_pika.abc.AbstractChannel, queue_name: str, message_data: Dict):
    logging.info("Job triggered: preparing to send message to RabbitMQ...")
    try:
        
        body_bytes = json.dumps(message_data).encode("utf-8")
        message = aio_pika.Message(body=body_bytes, content_type="application/json",delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await channel.default_exchange.publish(message, routing_key=queue_name)
        #print("Message published to queue '%s': %s", queue_name, message_data)
        logging.info("Message published to queue '%s': %s", queue_name,"successfully") #, message_data)
        print("Success")


    except Exception:
        logging.exception("Failed to publish message to RabbitMQ")


  
