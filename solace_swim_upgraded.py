import logging
import signal
import time
from datetime import timezone
from threading import Event, Thread
from queue import Queue as ThreadQueue, Empty, Full

import psycopg2
import xmltodict

from dateutil.parser import isoparse

from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values
from psycopg2.errors import DeadlockDetected

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

NUM_PARSE_WORKERS = 8
NUM_DB_WORKERS = 4

RAW_QUEUE_MAXSIZE = 50000
DB_QUEUE_MAXSIZE = 10000

DB_BATCH_SIZE = 250
DB_BATCH_TIMEOUT = 2

raw_queue = ThreadQueue(maxsize=RAW_QUEUE_MAXSIZE)

flight_queues = [
    ThreadQueue(maxsize=DB_QUEUE_MAXSIZE)
    for _ in range(NUM_DB_WORKERS)
]


# ============================================================
# DB POOL
# ============================================================

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

logger.info("DB pool created")


def get_db_connection():
    conn = db_pool.getconn()
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '30000';")
    conn.autocommit = False
    return conn


def release_db_connection(conn):
    if conn:
        db_pool.putconn(conn)


# ============================================================
# SOLACE RECONNECT
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
# SQL
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
VALUES %s
ON CONFLICT (gufi)
DO UPDATE SET
    callsign = EXCLUDED.callsign,
    operator = EXCLUDED.operator,
    major = EXCLUDED.major,
    origin = EXCLUDED.origin,
    destination = EXCLUDED.destination,
    aircraft_type = EXCLUDED.aircraft_type,

    updated_eta = COALESCE(EXCLUDED.updated_eta, flights.updated_eta),
    updated_etd = COALESCE(EXCLUDED.updated_etd, flights.updated_etd),

    flight_status = EXCLUDED.flight_status,
    last_updated = CURRENT_TIMESTAMP;
"""


# ============================================================
# TIMESTAMP NORMALIZATION
# ============================================================

def normalize_timestamp(value):
    if not value:
        return None
    try:
        dt = isoparse(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        logger.warning(f"Bad timestamp: {value}")
        return None


# ============================================================
# XML NAMESPACES
# ============================================================

NAMESPACES_TO_IGNORE = {
    'urn:us:gov:dot:faa:atm:tfm:tfmdataservice': None,
    'urn:us:gov:dot:faa:atm:tfm:flightdata': None,
    'urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements': None,
    'urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages': None
}


# ============================================================
# HELPERS
# ============================================================

def extract_payload(msg):
    return msg.get("fdm:ncsmFlightCreate") or \
           msg.get("fdm:ncsmFlightRoute") or {}


def classify_time_type(time_type: str):
    if not time_type:
        return None

    t = time_type.upper()

    if t == "SCHEDULED":
        return "ORIGINAL"

    if t in ("ESTIMATED", "ACTUAL"):
        return "UPDATED"

    return None


def extract_typed_time(node):
    if not isinstance(node, dict):
        return None, None

    value = node.get("timeValue")
    ttype = node.get("etaType") or node.get("etdType")

    return value, classify_time_type(ttype)


# ============================================================
# PARSER
# ============================================================

def parse_tfm_fields(msg):

    try:
        attrs = msg if isinstance(msg, dict) else {}

        gufi = attrs.get("flightRef")
        callsign = attrs.get("acid")
        operator = attrs.get("airline")
        major = attrs.get("major")

        origin = attrs.get("depArpt")
        destination = attrs.get("arrArpt")

        msg_type = attrs.get("msgType")

        payload = extract_payload(msg)

        aircraft_type = None
        fs = payload.get("flightStatusAndSpec", {})
        if isinstance(fs, dict):
            aircraft_type = fs.get("aircraftModel")

        status_map = {
            "trackInformation": "ENROUTE",
            "FlightModify": "AMENDED",
            "FlightTimes": "ACTIVE",
            "arrivalInformation": "ARRIVED",
        }

        status = status_map.get(msg_type, "UNKNOWN")

        # ============================
        # ETA / ETD SEMANTIC MODEL
        # ============================

        eta_node = payload.get("eta", {})
        etd_node = payload.get("etd", {})

        eta_val, eta_class = extract_typed_time(eta_node)
        etd_val, etd_class = extract_typed_time(etd_node)

        igtd = payload.get("igtd")

        original_eta = None
        updated_eta = None

        original_etd = None
        updated_etd = None

        # ETA rules
        if eta_class == "ORIGINAL":
            original_eta = eta_val
            updated_eta = eta_val
        elif eta_class == "UPDATED":
            updated_eta = eta_val

        # ETD rules
        if etd_class == "ORIGINAL":
            original_etd = etd_val
            updated_etd = etd_val
        elif etd_class == "UPDATED":
            updated_etd = etd_val

        # IGTD fallback
        if igtd:
            igtd_val = normalize_timestamp(igtd)

            if not original_etd:
                original_etd = igtd_val
            if not updated_etd:
                updated_etd = igtd_val

        return {
            "gufi": gufi,
            "callsign": callsign,
            "operator": operator,
            "major": major,
            "origin": origin,
            "destination": destination,
            "aircraft_type": aircraft_type,

            "original_eta": normalize_timestamp(original_eta),
            "updated_eta": normalize_timestamp(updated_eta),

            "original_etd": normalize_timestamp(original_etd),
            "updated_etd": normalize_timestamp(updated_etd),

            "flight_status": status,
        }

    except Exception:
        logger.exception("Parse failure")
        return None


# ============================================================
# PARSE WORKER
# ============================================================

class ParseWorker(Thread):

    def __init__(self, worker_id):
        super().__init__(daemon=True)
        self.worker_id = worker_id

    def run(self):

        logger.info(f"ParseWorker-{self.worker_id} started")

        while RUNNING.is_set():

            try:
                payload = raw_queue.get(timeout=1)

                data = xmltodict.parse(
                    payload,
                    process_namespaces=True,
                    namespaces=NAMESPACES_TO_IGNORE,
                )

                root = data.get("tfmDataService", {})
                output = root.get("fltdOutput", {})
                messages = output.get("fltdMessage", [])

                if isinstance(messages, dict):
                    messages = [messages]

                for msg in messages:

                    flight = parse_tfm_fields(msg)

                    if not flight:
                        continue

                    gufi = flight.get("gufi")
                    if not gufi:
                        continue

                    idx = hash(gufi) % NUM_DB_WORKERS

                    try:
                        flight_queues[idx].put_nowait(flight)
                    except Full:
                        logger.error(f"DB queue {idx} full")

                raw_queue.task_done()

            except Empty:
                continue
            except Exception:
                logger.exception("Parse error")
                time.sleep(1)


# ============================================================
# DB WORKER
# ============================================================

class DBWorker(Thread):

    def __init__(self, worker_id):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.queue = flight_queues[worker_id]
        self.conn = None

    def connect(self):
        while RUNNING.is_set():
            try:
                self.conn = get_db_connection()
                logger.info(f"DBWorker-{self.worker_id} connected")
                return
            except Exception:
                logger.exception("DB connect failed")
                time.sleep(5)

    def flush_batch(self, batch):

        if not batch:
            return

        batch.sort(key=lambda x: x["gufi"])

        values = [
            (
                r["gufi"],
                r["callsign"],
                r["operator"],
                r["major"],
                r["origin"],
                r["destination"],
                r["aircraft_type"],
                r["original_eta"],
                r["updated_eta"],
                r["original_etd"],
                r["updated_etd"],
                r["flight_status"],
            )
            for r in batch
        ]

        try:
            with self.conn.cursor() as cur:
                execute_values(cur, UPSERT_QUERY, values)

            self.conn.commit()

            logger.info(f"DBWorker-{self.worker_id} upserted {len(batch)}")

            for _ in batch:
                self.queue.task_done()

        except DeadlockDetected:
            self.conn.rollback()
            time.sleep(1)

        except Exception:
            logger.exception("Batch failure")
            self.conn.rollback()

    def run(self):

        self.connect()

        batch = []
        last_flush = time.time()

        while RUNNING.is_set():

            try:
                timeout = max(0.1, DB_BATCH_TIMEOUT - (time.time() - last_flush))
                item = self.queue.get(timeout=timeout)

                batch.append(item)

                if len(batch) >= DB_BATCH_SIZE:
                    self.flush_batch(batch)
                    batch.clear()
                    last_flush = time.time()

            except Empty:
                if batch:
                    self.flush_batch(batch)
                    batch.clear()
                    last_flush = time.time()

        if self.conn:
            release_db_connection(self.conn)


# ============================================================
# SOLACE HANDLER
# ============================================================

class TFMMessageHandler(MessageHandler):

    def on_message(self, message: InboundMessage):
        try:
            raw_queue.put_nowait(message.get_payload_as_string())
        except Full:
            logger.error("Raw queue full")


# ============================================================
# SHUTDOWN
# ============================================================

def shutdown_handler(sig, frame):
    RUNNING.clear()


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ============================================================
# START WORKERS
# ============================================================

parse_workers = [ParseWorker(i) for i in range(NUM_PARSE_WORKERS)]
db_workers = [DBWorker(i) for i in range(NUM_DB_WORKERS)]

for w in parse_workers + db_workers:
    w.start()


# ============================================================
# SOLACE SETUP
# ============================================================

service = (
    MessagingService.builder()
    .from_properties(config.solace_props)
    .with_reconnection_retry_strategy(
        RetryStrategy.parametrized_retry(20, 3000)
    )
    .build()
)

service.connect()

service.add_reconnection_listener(MyConnectionListener())
service.add_reconnection_attempt_listener(MyConnectionListener())

queue = Queue.durable_exclusive_queue(config.tfm_queue_name)

receiver = (
    service.create_persistent_message_receiver_builder()
    .with_message_auto_acknowledgement()
    .build(queue)
)

receiver.start()
receiver.receive_async(TFMMessageHandler())


# ============================================================
# MAIN LOOP
# ============================================================

try:
    while RUNNING.is_set():
        logger.info(
            f"RAW={raw_queue.qsize()} DB={[q.qsize() for q in flight_queues]}"
        )
        time.sleep(30)

finally:

    RUNNING.clear()

    try:
        receiver.terminate()
    except Exception:
        pass

    service.disconnect()

    for w in parse_workers + db_workers:
        w.join(timeout=10)

    db_pool.closeall()

    logger.info("Shutdown complete")