import asyncio
import aio_pika

async def peek_dlq_message():
    RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
    DLQ_NAME = "dlq_queue_class"

    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(DLQ_NAME, passive=True)

        for i in range(0, 4):
            message = await queue.get(fail=False)

            if message:
                async with message.process(requeue=False): # This ACKs automatically if no error
                    print(f"\nProcessing Message: {message.message_id}")
                    
                    # 1. Prepare the copy of the message
                    new_message = aio_pika.Message(
                        body=message.body,
                        headers=message.headers,
                        correlation_id=message.correlation_id,
                        delivery_mode=message.delivery_mode,
                        # Copy any other relevant properties here
                    )

                    # 2. Publish back to the same queue (via default exchange)
                    await channel.default_exchange.publish(
                        new_message,
                        routing_key=DLQ_NAME
                    )
                    print(message.body)
                    print("Message moved to the tail of the queue.")
            else:
                print("The DLQ is empty.")
                break

if __name__ == "__main__":
    asyncio.run(peek_dlq_message())