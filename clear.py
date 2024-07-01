from mygeotab import API, dates
from dotenv import load_dotenv
import os
import logging
import sqlite3

# Load environment variables from .env file
load_dotenv()

# Get MyGeotab credentials and groups from environment variables
username = os.getenv('GEOTAB_USERNAME')
password = os.getenv('GEOTAB_PASSWORD')
database = os.getenv('GEOTAB_DATABASE')
group_names = os.getenv('GEOTAB_GROUPS', '').split(',')
db_file = 'authlist.db'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the API connection
api = API(username, password, database)

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to SQLite database: {e}")
    return None

def clear_keys(conn, group_id):
    try:
        with conn:
            c = conn.cursor()
            c.execute(f"DELETE FROM keys_{group_id}")
            logging.info(f"All keys removed for group {group_id}")
    except sqlite3.Error as e:
        logging.error(f"Error clearing keys for group {group_id}: {e}")

def clear_vans(conn, group_id):
    try:
        with conn:
            c = conn.cursor()
            c.execute(f"DELETE FROM devices_{group_id}")
            logging.info(f"All vans removed for group {group_id}")
    except sqlite3.Error as e:
        logging.error(f"Error clearing vans for group {group_id}: {e}")

def send_clear_message(api, vehicle_id):
    try:
        api.call('Add', 'TextMessage', {
            "device": {
                "id": vehicle_id
            },
            "isDirectionToVehicle": True,
            "messageContent": {
                "driverKey": "",
                "contentType": "DriverWhiteList",
                "clearWhiteList": True,
                "addToWhiteList": False
            }
        })
        logging.info(f"Clear message sent to vehicle with ID: {vehicle_id}")
    except Exception as e:
        logging.error(f"Error sending clear message to vehicle with ID: {vehicle_id}: {e}")

def clear_group(api, group, db_file):
    group_id = group['id']
    conn = create_connection(db_file)
    if conn:
        clear_keys(conn, group_id)
        clear_vans(conn, group_id)
        
        devices = get_vans_by_group(api, group_id, conn)
        for device in devices:
            send_clear_message(api, device['id'])
        
        conn.close()

def get_vans_by_group(api, group_id, conn):
    try:
        logging.info(f"Fetching devices for group ID: {group_id}")
        devices = api.get('Device', search={'groups': [{'id': group_id}]})
        
        filtered_devices = []
        for device in devices:
            custom_parameters = device.get('customParameters', [])
            for param in custom_parameters:
                if param.get('description') == "Enable Authorised Driver List"
                    filtered_devices.append(device)
                    break
        
        if not filtered_devices:
            logging.warning(f"No devices with 'Enable Authorised Driver List' enabled found in group ID: {group_id}")
        
        return filtered_devices
    except Exception as e:
        logging.error(f"Error fetching devices for group {group_id}: {e}")
        return []

def main():
    try:
        api.authenticate()
        groups = api.get('Group', search=dict(active=True))
        filtered_groups = [group for group in groups if group['name'] in group_names]
        
        for group in filtered_groups:
            clear_group(api, group, db_file)
            
    except Exception as e:
        logging.error(f"Error in main process: {e}")

if __name__ == "__main__":
    main()
