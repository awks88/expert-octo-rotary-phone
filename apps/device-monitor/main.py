import asyncio
import json
import os
import logging
from collections import deque
from datetime import datetime

import aio_pika
from fastapi import FastAPI
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = "sensor.events"

# Хранилище телеметрии — просто список в памяти, ничего сложного
telemetry_store: deque = deque(maxlen=500)


async def consume_events():
    """Читает события из RabbitMQ и складывает в память."""
    # Пытаемся подключиться несколько раз — RabbitMQ стартует медленнее нас
    for attempt in range(10):
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            break
        except Exception as e:
            logger.warning(f"RabbitMQ not ready yet (attempt {attempt + 1}): {e}")
            await asyncio.sleep(3)
    else:
        logger.error("Could not connect to RabbitMQ, telemetry collection disabled")
        return

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        event = json.loads(message.body)
                        event["received_at"] = datetime.utcnow().isoformat()
                        telemetry_store.append(event)
                        logger.info(f"Got event for sensor {event.get('sensor_id')}: {event.get('value')}")
                    except Exception as e:
                        logger.error(f"Failed to process message: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Запускаем консьюмер в фоне при старте
    task = asyncio.create_task(consume_events())
    yield
    task.cancel()


app = FastAPI(title="Device Monitor", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "events_stored": len(telemetry_store)}


@app.get("/telemetry")
def get_all_telemetry(limit: int = 50):
    """Последние события по всем датчикам."""
    events = list(telemetry_store)[-limit:]
    return {"count": len(events), "events": events}


@app.get("/telemetry/{sensor_id}")
def get_sensor_telemetry(sensor_id: int, limit: int = 20):
    """История по конкретному датчику."""
    events = [e for e in telemetry_store if e.get("sensor_id") == sensor_id]
    events = events[-limit:]
    return {"sensor_id": sensor_id, "count": len(events), "events": events}
