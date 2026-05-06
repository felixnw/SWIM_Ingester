import time
import xmltodict
import logging

import psycopg2
from psycopg2.extras import RealDictCursor

from solace.messaging.messaging_service import MessagingService, RetryStrategy, ReconnectionListener, ReconnectionAttemptListener
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.resources.queue import Queue

# --- IMPORT YOUR PRIVATE CONFIG ---
import config

# Configure logging to see errors without stopping the script
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create DB connection
try:
    conn = psycopg2.connect(**config.db_params)
    logger.info("Successfully connected to the database.")
except Exception as e:
    logger.error(f"Database connection failed: {e}")
    exit(1)  # Exit if we can't connect to the database, since it's critical for processing messages

class MyConnectionListener(ReconnectionListener, ReconnectionAttemptListener):
    def on_reconnecting(self, event):
        logging.warning(f"SOLACE: Connection lost. Attempting to reconnect... {event}")
        print("SOLACE: Connection lost. Attempting to reconnect...")

    def on_reconnected(self, event):
        logging.info(f"SOLACE: Reconnection successful! {event}")
        print("SOLACE: Reconnection successful!")

# 1. Define Message Handlers
class TFMMessageHandler(MessageHandler):
    def __init__(self, db_conn):
            self.db_conn = db_conn

    def on_message(self, message: InboundMessage):
        try:
            payload = message.get_payload_as_string()

            # Strips the long FAA URIs so we can use clean keys like 'gufi' instead of 'nxce:gufi'
            namespaces_to_ignore = {
                'urn:us:gov:dot:faa:atm:tfm:tfmdataservice': None,
                'urn:us:gov:dot:faa:atm:tfm:flightdata': None,
                'urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements': None,
                'urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages': None
            }
                    
            data = xmltodict.parse(payload, process_namespaces=True, namespaces=namespaces_to_ignore)
            
            # TFM batches multiple flights in one XML delivery
            root = data.get('tfmDataService', {})
            output = root.get('fltdOutput', {})
            messages = output.get('fltdMessage', [])

            # xmltodict quirk: if there is only 1 flight, it returns a dict. Force it to a list.
            if isinstance(messages, dict):
                messages = [messages]
            
            for msg in messages:
                flight_data = self.parse_tfm_fields(msg)
                
                if flight_data and flight_data.get('gufi'):
                    # Call your DB Upsert logic
                    self.upsert_flight(flight_data)
                    print(f"Updated TFM: {flight_data['callsign']} | {flight_data['operator']} | GUFI: {flight_data['gufi']} | "
      f"Major: {flight_data['major']} | Origin: {flight_data['origin']} | Dest: {flight_data['destination']} | "
      f"Type: {flight_data['aircraft_type']} | ETA: {flight_data['eta']} | ETD: {flight_data['etd']} | Status: {flight_data['status']}")

        except Exception as e:
            logger.error(f"Error processing TFM message: {e}")
            
    def parse_tfm_fields(self, msg):
        """Helper to navigate polymorphic TFM XML structures"""
        try:
            # 1. Top-level attributes (Reliably present in the fltdMessage tag)
            callsign = msg.get('@acid')
            operator = msg.get('@airline')
            major = msg.get('@major')
            origin = msg.get('@depArpt')
            destination = msg.get('@arrArpt')

            # 2. Identify the message body (Polymorphic container)
            # This looks for the data in whatever message type the FAA sent
            body = (msg.get('ncsmFlightModify') or 
                    msg.get('nccmFlightModify') or # Handling potential typos in SWIM feeds
                    msg.get('ncsmFlightTimes') or 
                    msg.get('trackInformation') or {})

            # 3. Drilling into the ID block
            qualified_id = body.get('qualifiedAircraftId', {})
            gufi = qualified_id.get('gufi')
            
            # 4. Extract Times (Coalesce logic)
            # igtd = Initial Gated Departure (Great for 'original_etd')
            etd = (qualified_id.get('igtd') or 
                   body.get('etd', {}).get('@timeValue') or
                   body.get('airlineData', {}).get('etd', {}).get('@timeValue'))

            eta = (body.get('eta', {}).get('@timeValue') or 
                   body.get('airlineData', {}).get('eta', {}).get('@timeValue'))
            
            # 5. Extract Status
            status_spec = (body.get('flightStatusAndSpec') or 
                           body.get('airlineData', {}).get('flightStatusAndSpec') or {})
            status = status_spec.get('flightStatus')

            # 6. Extract Aircraft Type (Model > Spec > Category)
            status_spec = (body.get('flightStatusAndSpec') or 
                           body.get('airlineData', {}).get('flightStatusAndSpec') or {})
            
            spec_raw = status_spec.get('aircraftSpecification')
            spec_text = spec_raw.get('#text') if isinstance(spec_raw, dict) else spec_raw

            aircraft_type = (status_spec.get('aircraftModel') or 
                             spec_text)

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
                "status": status
            }
    
        except Exception as e:
            logger.error(f"Error parsing TFM fields: {e}")
            return None
        
    def upsert_flight(self, data):
        query = """
            INSERT INTO flights (
                gufi, callsign, operator, major, origin, destination, 
                aircraft_type, original_eta, updated_eta, original_etd, updated_etd, flight_status
            ) VALUES (
                %(gufi)s, %(callsign)s, %(operator)s, %(major)s, %(origin)s, %(destination)s, 
                %(aircraft_type)s, %(eta)s, %(eta)s, %(etd)s, %(etd)s, %(status)s
            )
            ON CONFLICT (gufi) DO UPDATE SET
                callsign = EXCLUDED.callsign,
                updated_eta = EXCLUDED.updated_eta,
                updated_etd = EXCLUDED.updated_etd,
                flight_status = EXCLUDED.flight_status,
                last_updated = CURRENT_TIMESTAMP;
        """
        try:
            # You need a cursor to execute the query
            with self.db_conn.cursor() as cur:
                cur.execute(query, data)
                self.db_conn.commit()
        except psycopg2.OperationalError as e:
            print(f"Database connection error during upsert for GUFI {data.get('gufi')}: {e}")
            logging.error(f"DATABASE TIMEOUT/DROP: Connection is dead. Error: {e}")
        except Exception as e:
            self.db_conn.rollback()
            print(f"DB Upsert Failed for GUFI {data.get('gufi')}: {e}")
            logger.error(f"DB Upsert Failed: {e}")

# class SFDPSHandler(MessageHandler):
#     def on_message(self, message: InboundMessage):
#         # SFDPS specific logic (Tactical/GUFI data)
#         payload = message.get_payload_as_string()
#         # Parse and Upsert DB...
#         # print(f"\n--- New SFDPS Message Received ---")
#         # print(payload)
#         # print("---------------------------------")


# 2. Broker Configuration (Use your SCDS credentials)
tfm_broker_props = {
    "solace.messaging.transport.host": "tcps://ems1.swim.faa.gov:55443", 
    "solace.messaging.service.vpn-name": "TFMS",
    "solace.messaging.authentication.scheme.basic.username": config.swim_username,
    "solace.messaging.authentication.scheme.basic.password": config.swim_password,
    # MANDATORY: Enable Compression for FAA SWIM
    "solace.messaging.transport.compression-level": "1", 
    # Disable certificate validation for testing purposes (not recommended for production)
    "solace.messaging.tls.cert-validated": False,
    "solace.messaging.tls.cert-validate-servername": False
}

sfdps_broker_props = {
    "solace.messaging.transport.host": "tcps://ems1.swim.faa.gov:55443", 
    "solace.messaging.service.vpn-name": "FDPS",
    "solace.messaging.authentication.scheme.basic.username": config.swim_username,
    "solace.messaging.authentication.scheme.basic.password": config.swim_password,
    # MANDATORY: Enable Compression for FAA SWIM
    "solace.messaging.transport.compression-level": "1", 
    # Disable certificate validation for testing purposes (not recommended for production)
    "solace.messaging.tls.cert-validated": False,
    "solace.messaging.tls.cert-validate-servername": False
}

# 3. Build and Connect
tfm_messaging_service = MessagingService.builder().from_properties(tfm_broker_props) \
    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3000)) \
    .build()

# sfdps_messaging_service = MessagingService.builder().from_properties(sfdps_broker_props) \
#     .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3000)) \
#     .build()

tfm_messaging_service.connect()
# sfdps_messaging_service.connect()
print("Connected to FAA SWIM SCDS via Solace.")

tfm_messaging_service.add_reconnection_listener(MyConnectionListener())
tfm_messaging_service.add_reconnection_attempt_listener(MyConnectionListener())

# 4. Subscribe to a Topic or Queue
# Convert the string name into a Queue Resource object
tfm_queue_name_string = config.tfm_queue_name
tfm_queue = Queue.durable_exclusive_queue(tfm_queue_name_string)

# sfdps_queue_name_string = sfdps_queue_name
# sfdps_queue = Queue.durable_exclusive_queue(sfdps_queue_name_string)

tfm_receiver = tfm_messaging_service.create_persistent_message_receiver_builder() \
    .build(tfm_queue) # Pass the object, not the string

tfm_receiver.start()
tfm_receiver.receive_async(TFMMessageHandler(db_conn=conn))

# sfdps_receiver = sfdps_messaging_service.create_persistent_message_receiver_builder() \
#     .build(sfdps_queue) # Pass the object, not the string

# sfdps_receiver.start()
# sfdps_receiver.receive_async(SFDPSHandler())

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Disconnecting...")
finally:
    tfm_receiver.terminate()
    # sfdps_receiver.terminate()
    tfm_messaging_service.disconnect()
    # sfdps_messaging_service.disconnect()