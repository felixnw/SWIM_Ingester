import time
import logging
import signal
from threading import Thread, Event
from queue import Queue as ThreadQueue, Full, Empty

import xmltodict
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_batch

from solace.messaging.messaging_service import (
    MessagingService,
    RetryStrategy,
    ReconnectionListener,
    ReconnectionAttemptListener,
)

from solace.messaging.receiver.message_receiver import (
    MessageHandler,
    InboundMessage,
)

from solace.messaging.resources.queue import Queue

# --- PRIVATE CONFIG ---
import config


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# GLOBALS
# ============================================================

RUNNING = Event()
RUNNING.set()

FLIGHT_QUEUE_MAXSIZE = 10000
DB_BATCH_SIZE = 100
DB_BATCH_TIMEOUT = 2

flight_queue = ThreadQueue(maxsize=FLIGHT_QUEUE_MAXSIZE)


# ============================================================
# DATABASE CONNECTION POOL
# ============================================================

try:
    db_pool = ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        **config.db_params,
    )

    logger.info("Database connection pool created.")

except Exception as e:
    logger.exception(f"Failed to create DB pool: {e}")
    raise


def get_db_connection():
    conn = db_pool.getconn()

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '30000';")

    return conn


def release_db_connection(conn):
    try:
        db_pool.putconn(conn)
    except Exception:
        logger.exception("Failed to release DB connection")


# ============================================================
# SOLACE RECONNECTION LISTENERS
# ============================================================

class MyConnectionListener(
    ReconnectionListener,
    ReconnectionAttemptListener,
):

    def on_reconnecting(self, event):
        logger.warning(f"SOLACE reconnecting: {event}")

    def on_reconnected(self, event):
        logger.info(f"SOLACE reconnected: {event}")


# ============================================================
# DATABASE WORKER THREAD
# ============================================================

UPSERT_QUERY = """
INSERT INTO flights (
    gufi,
    callsign,
    operator,
    major,
    origin,
    destination,
    aircraft_type,
    original_eta,
    updated_eta,
    original_etd,
    updated_etd,
    flight_status
)
VALUES (
    %(gufi)s,
    %(callsign)s,
    %(operator)s,
    %(major)s,
    %(origin)s,
    %(destination)s,
    %(aircraft_type)s,
    %(eta)s,
    %(eta)s,
    %(etd)s,
    %(etd)s,
    %(status)s
)
ON CONFLICT (gufi)
DO UPDATE SET
    callsign = EXCLUDED.callsign,
    operator = EXCLUDED.operator,
    major = EXCLUDED.major,
    origin = EXCLUDED.origin,
    destination = EXCLUDED.destination,
    aircraft_type = EXCLUDED.aircraft_type,
    updated_eta = EXCLUDED.updated_eta,
    updated_etd = EXCLUDED.updated_etd,
    flight_status = EXCLUDED.flight_status,
    last_updated = CURRENT_TIMESTAMP;
"""


class DBWorker(Thread):

    def __init__(self, worker_id: int):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.conn = None

    def connect(self):
        while RUNNING.is_set():
            try:
                self.conn = get_db_connection()
                logger.info(f"DBWorker-{self.worker_id}: Connected to DB")
                return

            except Exception:
                logger.exception(
                    f"DBWorker-{self.worker_id}: DB connection failed"
                )
                time.sleep(5)

    def ensure_connection(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            logger.warning(
                f"DBWorker-{self.worker_id}: Reconnecting DB..."
            )

            try:
                self.conn.close()
            except Exception:
                pass

            self.connect()

    def flush_batch(self, batch):
        if not batch:
            return

        self.ensure_connection()

        try:
            with self.conn.cursor() as cur:
                execute_batch(
                    cur,
                    UPSERT_QUERY,
                    batch,
                    page_size=DB_BATCH_SIZE,
                )

            self.conn.commit()

            logger.info(
                f"DBWorker-{self.worker_id}: Upserted {len(batch)} flights"
            )

        except psycopg2.OperationalError:
            logger.exception(
                f"DBWorker-{self.worker_id}: Operational DB failure"
            )

            try:
                self.conn.rollback()
            except Exception:
                pass

            self.connect()

        except Exception:
            logger.exception(
                f"DBWorker-{self.worker_id}: Batch upsert failed"
            )

            try:
                self.conn.rollback()
            except Exception:
                pass

    def run(self):

        self.connect()

        batch = []
        last_flush = time.time()

        while RUNNING.is_set():

            try:
                timeout = max(
                    0.1,
                    DB_BATCH_TIMEOUT - (time.time() - last_flush),
                )

                item = flight_queue.get(timeout=timeout)

                batch.append(item)

                flight_queue.task_done()

                if len(batch) >= DB_BATCH_SIZE:
                    self.flush_batch(batch)
                    batch.clear()
                    last_flush = time.time()

            except Empty:

                if batch:
                    self.flush_batch(batch)
                    batch.clear()
                    last_flush = time.time()

            except Exception:
                logger.exception(
                    f"DBWorker-{self.worker_id}: Unexpected worker error"
                )
                time.sleep(1)

        if batch:
            self.flush_batch(batch)

        if self.conn:
            release_db_connection(self.conn)

        logger.info(f"DBWorker-{self.worker_id}: Stopped")


# ============================================================
# TFM MESSAGE HANDLER
# ============================================================

class TFMMessageHandler(MessageHandler):

    namespaces_to_ignore = {
        'urn:us:gov:dot:faa:atm:tfm:tfmdataservice': None,
        'urn:us:gov:dot:faa:atm:tfm:flightdata': None,
        'urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements': None,
        'urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages': None
    }

    def on_message(self, message: InboundMessage):

        try:
            payload = message.get_payload_as_string()

            data = xmltodict.parse(
                payload,
                process_namespaces=True,
                namespaces=self.namespaces_to_ignore,
            )

            root = data.get("tfmDataService", {})
            output = root.get("fltdOutput", {})
            messages = output.get("fltdMessage", [])

            if isinstance(messages, dict):
                messages = [messages]

            for msg in messages:

                flight_data = self.parse_tfm_fields(msg)

                if not flight_data:
                    continue

                if not flight_data.get("gufi"):
                    continue

                try:
                    flight_queue.put_nowait(flight_data)

                except Full:
                    logger.error(
                        "Flight queue full. Dropping message."
                    )

        except Exception:
            logger.exception("Failed processing TFM message")

        return

    def parse_tfm_fields(self, msg):

        try:

            callsign = msg.get("@acid")
            operator = msg.get("@airline")
            major = msg.get("@major")
            origin = msg.get("@depArpt")
            destination = msg.get("@arrArpt")

            body = (
                msg.get("ncsmFlightModify")
                or msg.get("nccmFlightModify")
                or msg.get("ncsmFlightTimes")
                or msg.get("trackInformation")
                or {}
            )

            qualified_id = body.get(
                "qualifiedAircraftId",
                {}
            )

            gufi = qualified_id.get("gufi")

            etd = (
                qualified_id.get("igtd")
                or body.get("etd", {}).get("@timeValue")
                or body.get("airlineData", {})
                    .get("etd", {})
                    .get("@timeValue")
            )

            eta = (
                body.get("eta", {}).get("@timeValue")
                or body.get("airlineData", {})
                    .get("eta", {})
                    .get("@timeValue")
            )

            status_spec = (
                body.get("flightStatusAndSpec")
                or body.get("airlineData", {})
                    .get("flightStatusAndSpec")
                or {}
            )

            status = status_spec.get("flightStatus")

            spec_raw = status_spec.get(
                "aircraftSpecification"
            )

            spec_text = (
                spec_raw.get("#text")
                if isinstance(spec_raw, dict)
                else spec_raw
            )

            aircraft_type = (
                status_spec.get("aircraftModel")
                or spec_text
            )

            return {
                "gufi": gufi,
                "callsign": callsign,
                "operator": operator,
                "major": major,
                "origin": origin,
                "destination": destination,
                "aircraft_type": aircraft_type,
                "eta": eta,
                "etd": etd,
                "status": status,
            }

        except Exception:
            logger.exception("Failed parsing TFM fields")
            return None


# ============================================================
# SOLACE CONFIG
# ============================================================

tfm_broker_props = {
    "solace.messaging.transport.host":
        "tcps://ems1.swim.faa.gov:55443",

    "solace.messaging.service.vpn-name":
        "TFMS",

    "solace.messaging.authentication.scheme.basic.username":
        config.swim_username,

    "solace.messaging.authentication.scheme.basic.password":
        config.swim_password,

    "solace.messaging.transport.compression-level":
        "1",

    # PRODUCTION:
    # Replace with real cert validation later
    "solace.messaging.tls.cert-validated":
        False,

    "solace.messaging.tls.cert-validate-servername":
        False,
}


# ============================================================
# SHUTDOWN HANDLER
# ============================================================

def shutdown_handler(signum, frame):
    logger.info("Shutdown signal received...")
    RUNNING.clear()


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ============================================================
# START DB WORKERS
# ============================================================

workers = []

for i in range(4):

    worker = DBWorker(worker_id=i + 1)

    worker.start()

    workers.append(worker)


# ============================================================
# CREATE SOLACE SERVICE
# ============================================================

tfm_messaging_service = (
    MessagingService.builder()
    .from_properties(tfm_broker_props)
    .with_reconnection_retry_strategy(
        RetryStrategy.parametrized_retry(
            20,
            3000,
        )
    )
    .build()
)

tfm_messaging_service.connect()

logger.info("Connected to FAA SWIM")


listener = MyConnectionListener()

tfm_messaging_service.add_reconnection_listener(listener)

tfm_messaging_service.add_reconnection_attempt_listener(
    listener
)


# ============================================================
# QUEUE SUBSCRIPTION
# ============================================================

tfm_queue = Queue.durable_exclusive_queue(
    config.tfm_queue_name
)

tfm_receiver = (
    tfm_messaging_service
    .create_persistent_message_receiver_builder()
    .build(tfm_queue)
)

tfm_receiver.start()

tfm_receiver.receive_async(TFMMessageHandler())

logger.info("TFM receiver started")


# ============================================================
# MAIN LOOP
# ============================================================

try:

    while RUNNING.is_set():

        logger.info(
            f"Queue depth: {flight_queue.qsize()}"
        )

        time.sleep(30)

except KeyboardInterrupt:

    logger.info("Keyboard interrupt received")

finally:

    logger.info("Shutting down...")

    RUNNING.clear()

    try:
        tfm_receiver.terminate()
    except Exception:
        logger.exception("Receiver shutdown failed")

    try:
        tfm_messaging_service.disconnect()
    except Exception:
        logger.exception("Messaging disconnect failed")

    for worker in workers:
        worker.join(timeout=10)

    try:
        db_pool.closeall()
    except Exception:
        logger.exception("Failed closing DB pool")

    logger.info("Shutdown complete")