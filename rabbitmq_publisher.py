#!/usr/bin/env python3
"""
RabbitMQ Publisher for Email Processing System

This script reads JSON email files from a folder and publishes them to RabbitMQ
for the processor to consume.

Usage:
    python rabbitmq_publisher.py                 # Publish all emails from individual_emails/
    python rabbitmq_publisher.py --folder path   # Publish from custom folder
    python rabbitmq_publisher.py --delay 2       # Delay 2 seconds between messages
"""

import asyncio
import argparse
import json
import os
from pathlib import Path
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def publish_emails(folder_path: str = "individual_emails", delay_seconds: float = 0.5):
    """
    Read JSON files from folder and publish to RabbitMQ
    
    Args:
        folder_path: Path to folder containing JSON email files
        delay_seconds: Delay between publishing messages (prevent overwhelming)
    """
    
    try:
        import aio_pika
    except ImportError:
        logger.error("❌ aio_pika not installed")
        logger.error("   Run: pip install aio-pika")
        return False
    
    from config import settings
    
    # Validate folder
    folder = Path(folder_path)
    if not folder.exists():
        logger.error(f"❌ Folder not found: {folder_path}")
        return False
    
    # Find JSON files
    json_files = list(folder.glob("*.json"))
    if not json_files:
        logger.error(f"❌ No JSON files found in {folder_path}")
        return False
    
    logger.info(f"📂 Found {len(json_files)} JSON email files")
    
    # Validate RabbitMQ config
    if not settings.RABBITMQ_URL:
        logger.error("❌ RABBITMQ_URL not configured in .env")
        return False
    
    try:
        # Connect to RabbitMQ
        logger.info(f"🔗 Connecting to RabbitMQ: {settings.RABBITMQ_URL}")
        connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        channel = await connection.channel()
        
        # Declare exchange and queue
        exchange = await channel.declare_exchange(
            settings.RABBITMQ_EXCHANGE,
            aio_pika.ExchangeType.DIRECT,
            durable=True
        )
        
        queue = await channel.declare_queue(
            settings.RABBITMQ_QUEUE,
            durable=True
        )
        
        # Bind queue to exchange
        await queue.bind(exchange, settings.RABBITMQ_ROUTING_KEY)
        
        logger.info(f"✅ Connected to RabbitMQ")
        logger.info(f"📨 Publishing to: {settings.RABBITMQ_EXCHANGE} -> {settings.RABBITMQ_QUEUE}")
        
        published_count = 0
        errors = 0
        
        # Publish each email
        for json_file in sorted(json_files):
            try:
                with open(json_file, 'r') as f:
                    email_data = json.load(f)
                
                # Convert to JSON string
                message_body = json.dumps(email_data).encode()
                
                # Create message with persistent delivery
                message = aio_pika.Message(
                    body=message_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type='application/json'
                )
                
                # Publish message
                await exchange.publish(
                    message,
                    routing_key=settings.RABBITMQ_ROUTING_KEY
                )
                
                logger.info(
                    f"✅ [{published_count + 1}] Published: {json_file.name} "
                    f"(Priority: {email_data.get('priority', 'N/A')})"
                )
                
                published_count += 1
                
                # Add delay between messages
                if delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)
                    
            except json.JSONDecodeError as e:
                logger.error(f"❌ Invalid JSON in {json_file.name}: {e}")
                errors += 1
            except Exception as e:
                logger.error(f"❌ Error publishing {json_file.name}: {e}")
                errors += 1
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info("📊 PUBLISHING SUMMARY")
        logger.info("="*70)
        logger.info(f"Total files:      {len(json_files)}")
        logger.info(f"Published:        {published_count}")
        logger.info(f"Errors:           {errors}")
        logger.info("="*70)
        
        if published_count > 0:
            logger.info(f"\n✅ Successfully published {published_count} emails to RabbitMQ")
            logger.info(f"⏳ Processor will consume them from the queue")
            logger.info(f"🔄 Start processor with: python main.py --rabbitmq")
        
        await connection.close()
        return published_count > 0
        
    except Exception as e:
        logger.error(f"❌ RabbitMQ connection error: {e}")
        logger.error("\n   Make sure RabbitMQ is running:")
        logger.error("   Windows: rabbitmq-service start")
        logger.error("   macOS: brew services start rabbitmq")
        logger.error("   Linux: sudo systemctl start rabbitmq-server")
        return False


async def main():
    parser = argparse.ArgumentParser(
        description="Publish JSON emails to RabbitMQ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python rabbitmq_publisher.py                    # Default: individual_emails folder
  python rabbitmq_publisher.py --folder ./emails  # Custom folder
  python rabbitmq_publisher.py --delay 1          # 1 second between messages
        """
    )
    
    parser.add_argument(
        '--folder',
        type=str,
        default='individual_emails',
        help='Folder containing JSON email files (default: individual_emails)'
    )
    
    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Delay between publishing messages in seconds (default: 0.5)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("🐰 RABBITMQ EMAIL PUBLISHER")
    print("="*70)
    
    success = await publish_emails(args.folder, args.delay)
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())