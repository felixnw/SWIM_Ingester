import logging
import signal
import time
import hashlib

from datetime import timezone
from threading import Event, Thread, Lock
from queue import Queue as ThreadQueue, Empty

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

DB_BATCH_SIZE = 250
DB_BATCH_TIMEOUT = 2

raw_queue = ThreadQueue(maxsize=RAW_QUEUE_MAXSIZE)

# ============================================================
# IN-MEMORY DEDUPE MAPS
# ============================================================

flight_maps = [
    {}
    for _ in range(NUM_DB_WORKERS)
]

flight_map_locks = [
    Lock()
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

    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '30000';")

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

    original_eta = COALESCE(
        flights.original_eta,
        EXCLUDED.original_eta
    ),

    updated_eta = COALESCE(
        EXCLUDED.updated_eta,
        flights.updated_eta
    ),

    original_etd = COALESCE(
        flights.original_etd,
        EXCLUDED.original_etd
    ),

    updated_etd = COALESCE(
        EXCLUDED.updated_etd,
        flights.updated_etd
    ),

    flight_status = COALESCE(
    EXCLUDED.flight_status,
    flights.flight_status
    ),
    
    last_updated = CURRENT_TIMESTAMP;
"""


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

KNOWN_PAYLOAD_KEYS = (
    "ncsmFlightModify",
    "nccmFlightModify",
    "ncsmFlightTimes",
    "trackInformation",
    "arrivalInformation",
    "departureInformation",
    "surfaceInformation",
    "flightLegInformation",
    "flightRouteInformation",
    "flightPlanInformation",
    "ncsmFlightCreate",
    "ncsmFlightRoute",
)


def attr(node, name, default=None):

    if not isinstance(node, dict):
        return default

    return node.get(f"@{name}", default)


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


def classify_time_type(time_type: str):

    if not time_type:
        return None

    t = str(time_type).upper()

    if t == "SCHEDULED":
        return "ORIGINAL"

    if t in ("ESTIMATED", "ACTUAL"):
        return "UPDATED"

    return None


def extract_time(node):

    if not isinstance(node, dict):
        return None, None

    value = (
        node.get("@timeValue")
        or node.get("timeValue")
    )

    time_type = (
        node.get("@etaType")
        or node.get("@etdType")
        or node.get("etaType")
        or node.get("etdType")
    )

    return value, classify_time_type(time_type)


def find_payload(msg):

    for key in KNOWN_PAYLOAD_KEYS:

        payload = msg.get(key)

        if isinstance(payload, dict):
            return key, payload

    for key, value in msg.items():

        if isinstance(value, dict):

            if any(
                k in value
                for k in (
                    "qualifiedAircraftId",
                    "eta",
                    "etd",
                    "flightStatusAndSpec",
                )
            ):
                return key, value

    return None, {}


def stable_worker_index(gufi: str):

    digest = hashlib.md5(
        gufi.encode("utf-8")
    ).hexdigest()

    return int(digest, 16) % NUM_DB_WORKERS


# ============================================================
# PARSER
# ============================================================

def parse_tfm_fields(msg):

    try:

        payload_name, payload = find_payload(msg)

        if not payload:

            logger.warning(
                f"Unsupported payload shape: {list(msg.keys())}"
            )

            return None

        # ====================================================
        # TOP-LEVEL ATTRIBUTES
        # ====================================================

        callsign = attr(msg, "acid")
        operator = attr(msg, "airline")
        major = attr(msg, "major")

        origin = attr(msg, "depArpt")
        destination = attr(msg, "arrArpt")

        msg_type = (
            attr(msg, "msgType")
            or payload_name
        )

        # ====================================================
        # QUALIFIED AIRCRAFT ID
        # ====================================================

        qualified = payload.get(
            "qualifiedAircraftId",
            {}
        )

        gufi = (
            qualified.get("gufi")
        )

        if not gufi:
            return None

        # ====================================================
        # AIRCRAFT TYPE
        # ====================================================

        aircraft_type = None

        status_spec = (
            payload.get("flightStatusAndSpec")
            or payload.get("airlineData", {})
                     .get("flightStatusAndSpec")
            or {}
        )

        if isinstance(status_spec, dict):

            aircraft_type = (
                status_spec.get("aircraftModel")
            )

            if not aircraft_type:

                spec_raw = status_spec.get(
                    "aircraftSpecification"
                )

                if isinstance(spec_raw, dict):
                    aircraft_type = spec_raw.get("#text")
                else:
                    aircraft_type = spec_raw

        # ====================================================
        # FAA STATUS
        # ====================================================

        status = None

        if isinstance(status_spec, dict):

            raw_status = status_spec.get(
                "flightStatus"
            )

            if raw_status:

                status = (
                    str(raw_status)
                    .strip()
                    .upper()
                )

        # ====================================================
        # TIME EXTRACTION
        # ====================================================

        eta_node = (
            payload.get("eta")
            or payload.get("airlineData", {})
                      .get("eta")
            or {}
        )

        etd_node = (
            payload.get("etd")
            or payload.get("airlineData", {})
                      .get("etd")
            or {}
        )

        eta_val, eta_class = extract_time(eta_node)
        etd_val, etd_class = extract_time(etd_node)

        igtd = (
            qualified.get("igtd")
            or payload.get("igtd")
        )

        original_eta = None
        updated_eta = None

        original_etd = None
        updated_etd = None

        # ====================================================
        # ETA LOGIC
        # ====================================================

        if eta_class == "ORIGINAL":

            original_eta = normalize_timestamp(eta_val)
            updated_eta = normalize_timestamp(eta_val)

        elif eta_class == "UPDATED":

            updated_eta = normalize_timestamp(eta_val)

        elif eta_val:

            updated_eta = normalize_timestamp(eta_val)

        # ====================================================
        # ETD LOGIC
        # ====================================================

        if etd_class == "ORIGINAL":

            original_etd = normalize_timestamp(etd_val)
            updated_etd = normalize_timestamp(etd_val)

        elif etd_class == "UPDATED":

            updated_etd = normalize_timestamp(etd_val)

        elif etd_val:

            updated_etd = normalize_timestamp(etd_val)

        # ====================================================
        # IGTD FALLBACK
        # ====================================================

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

            "original_eta": original_eta,
            "updated_eta": updated_eta,

            "original_etd": original_etd,
            "updated_etd": updated_etd,

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

        logger.info(
            f"ParseWorker-{self.worker_id} started"
        )

        while RUNNING.is_set():

            try:

                payload = raw_queue.get(timeout=1)

                try:

                    data = xmltodict.parse(
                        payload,
                        process_namespaces=True,
                        namespaces=NAMESPACES_TO_IGNORE,
                    )

                    root = data.get(
                        "tfmDataService",
                        {}
                    )

                    output = root.get(
                        "fltdOutput",
                        {}
                    )

                    messages = output.get(
                        "fltdMessage",
                        []
                    )

                    if isinstance(messages, dict):
                        messages = [messages]

                    for msg in messages:

                        flight = parse_tfm_fields(msg)

                        if not flight:
                            continue

                        idx = stable_worker_index(
                            flight["gufi"]
                        )

                        lock = flight_map_locks[idx]

                        with lock:

                            existing = flight_maps[idx].get(
                                flight["gufi"]
                            )

                            if existing:

                                for k, v in flight.items():

                                    if v is not None:
                                        existing[k] = v

                            else:

                                flight_maps[idx][
                                    flight["gufi"]
                                ] = flight

                finally:
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

        self.conn = None

    def connect(self):

        while RUNNING.is_set():

            try:

                self.conn = get_db_connection()

                logger.info(
                    f"DBWorker-{self.worker_id} connected"
                )

                return

            except Exception:

                logger.exception("DB connect failed")

                time.sleep(5)

    def ensure_connection(self):

        try:

            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")

        except Exception:

            logger.warning(
                f"DBWorker-{self.worker_id} reconnecting DB"
            )

            try:
                self.conn.close()
            except Exception:
                pass

            self.connect()

    def flush_batch(self, batch):

        if not batch:
            return

        # ====================================================
        # FINAL SAFETY DEDUPE
        # ====================================================

        deduped = {}

        for r in batch:

            existing = deduped.get(
                r["gufi"]
            )

            if existing:

                for k, v in r.items():

                    if v is not None:
                        existing[k] = v

            else:

                deduped[r["gufi"]] = dict(r)

        batch = list(deduped.values())

        self.ensure_connection()

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

                execute_values(
                    cur,
                    UPSERT_QUERY,
                    values,
                    page_size=DB_BATCH_SIZE,
                )

            self.conn.commit()

            logger.info(
                f"DBWorker-{self.worker_id} "
                f"upserted {len(batch)} flights"
            )

        except DeadlockDetected:

            logger.warning(
                f"DBWorker-{self.worker_id} deadlock detected"
            )

            self.conn.rollback()

            time.sleep(1)

        except psycopg2.OperationalError:

            logger.exception(
                f"DBWorker-{self.worker_id} operational DB error"
            )

            try:
                self.conn.rollback()
            except Exception:
                pass

            self.connect()

        except Exception:

            logger.exception(
                f"DBWorker-{self.worker_id} batch failure"
            )

            try:
                self.conn.rollback()
            except Exception:
                pass

    def run(self):

        self.connect()

        while RUNNING.is_set():

            try:

                time.sleep(DB_BATCH_TIMEOUT)

                lock = flight_map_locks[
                    self.worker_id
                ]

                with lock:

                    if not flight_maps[
                        self.worker_id
                    ]:
                        continue

                    batch = list(
                        flight_maps[
                            self.worker_id
                        ].values()
                    )

                    flight_maps[
                        self.worker_id
                    ].clear()

                self.flush_batch(batch)

            except Exception:

                logger.exception(
                    f"DBWorker-{self.worker_id} worker error"
                )

                time.sleep(1)

        try:

            lock = flight_map_locks[
                self.worker_id
            ]

            with lock:

                batch = list(
                    flight_maps[
                        self.worker_id
                    ].values()
                )

                flight_maps[
                    self.worker_id
                ].clear()

            if batch:
                self.flush_batch(batch)

        except Exception:

            logger.exception(
                f"DBWorker-{self.worker_id} final flush failed"
            )

        if self.conn:
            release_db_connection(self.conn)

        logger.info(
            f"DBWorker-{self.worker_id} stopped"
        )


# ============================================================
# SOLACE HANDLER
# ============================================================

class TFMMessageHandler(MessageHandler):

    def on_message(self, message: InboundMessage):

        try:

            raw_queue.put_nowait(
                message.get_payload_as_string()
            )

        except Exception:

            logger.error("Raw queue full")


# ============================================================
# SHUTDOWN
# ============================================================

def shutdown_handler(sig, frame):

    logger.info("Shutdown requested")

    RUNNING.clear()


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ============================================================
# START WORKERS
# ============================================================

parse_workers = [
    ParseWorker(i + 1)
    for i in range(NUM_PARSE_WORKERS)
]

db_workers = [
    DBWorker(i)
    for i in range(NUM_DB_WORKERS)
]

for worker in parse_workers + db_workers:
    worker.start()


# ============================================================
# SOLACE SETUP
# ============================================================

service = (
    MessagingService.builder()
    .from_properties(config.solace_props)
    .with_reconnection_retry_strategy(
        RetryStrategy.parametrized_retry(
            20,
            3000,
        )
    )
    .build()
)

service.connect()

logger.info("Connected to FAA SWIM")

listener = MyConnectionListener()

service.add_reconnection_listener(listener)

service.add_reconnection_attempt_listener(
    listener
)

queue = Queue.durable_exclusive_queue(
    config.tfm_queue_name
)

receiver = (
    service.create_persistent_message_receiver_builder()
    .with_message_auto_acknowledgement()
    .build(queue)
)

receiver.start()

receiver.receive_async(
    TFMMessageHandler()
)

logger.info("TFM receiver started")


# ============================================================
# MAIN LOOP
# ============================================================

try:

    while RUNNING.is_set():

        logger.info(
            f"RAW={raw_queue.qsize()} "
            f"DB={[len(m) for m in flight_maps]}"
        )

        time.sleep(30)

finally:

    logger.info("Shutting down")

    RUNNING.clear()

    try:
        receiver.terminate()
    except Exception:
        logger.exception("Receiver shutdown failed")

    try:
        service.disconnect()
    except Exception:
        logger.exception("Service disconnect failed")

    for worker in parse_workers + db_workers:
        worker.join(timeout=10)

    try:
        db_pool.closeall()
    except Exception:
        logger.exception("DB pool close failure")

    logger.info("Shutdown complete")