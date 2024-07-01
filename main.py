from mygeotab import API,dates
from dotenv import load_dotenv
import os
import json
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

def get_vans_by_group(api, group_id):
    try:
        logging.info(f"Fetching devices for group ID: {group_id}")
        response = api.get('Device', search={'groups': [{'id': group_id}]})
        return response
        return []
    except Exception as e:
        logging.error(f"Error fetching devices for group {group_id}: {e}")
        return []

###SQLITE Functions (Memory in between runs) ##################################################################################################################################################
# We will be storing all keys in a table labeled keys_group id for comparison later, we need to keep a list of who we've added so we know what to remove in the future. There is a 1000 key limit on the iox device and we cannot retrieve this from the device itself. 
# It also wouldnt make sense to enable this feature and not do this.
# Should refactor to use the storage api eventually.
# Function to create SQLite connection

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to SQLite database: {e}")
    return None

def create_table(conn, group_id):
    try:
        c = conn.cursor()
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS keys_{group_id} (
                serialNumber TEXT PRIMARY KEY
            );
        ''')
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error creating table for group {group_id}: {e}")

def insert_keys(conn, group_id, keys):
    new_keys = []
    try:
        c = conn.cursor()
        for key in keys:
            c.execute(f'''
                INSERT OR IGNORE INTO keys_{group_id} (serialNumber) VALUES (?)
            ''', (key,))
            if c.rowcount > 0:
                new_keys.append(key)
        conn.commit()
        return new_keys
    except sqlite3.Error as e:
        logging.error(f"Error inserting keys for group {group_id}: {e}")
        return []

def remove_unused_keys(conn, group_id, keys):
    try:
        c = conn.cursor()
        query = f'''
            DELETE FROM keys_{group_id} WHERE serialNumber NOT IN ({','.join('?' for _ in keys)})
        '''
        c.execute(query, keys)
        conn.commit()
        return [key for key in keys if key not in c.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Error removing unused keys for group {group_id}: {e}")
        return []
        


####Drivers######################################################################################################################################################################################################################################################
#This section is to get all drivers by group, return their nfc key, compare it with the database, and either add or remove them from the database, and then in turn, the authlist

def get_users_with_nfc_keys(api, group_id, db_file):
    try:
        # Connect to SQLite database
        conn = create_connection(db_file)
        if conn is None:
            return []

        # Create table if not exists for the group_id
        create_table(conn, group_id)

        # Fetch users and their keys
        users = api.get('User', search={'driverGroups': [{'id': group_id}]})
        nfc_keys = []
        for user in users:
            if 'keys' in user:
                for key in user['keys']:
                    nfc_keys.append(key['serialNumber'])

        # Get new keys inserted and removed from the database
        new_keys = insert_keys(conn, group_id, nfc_keys)
        remove_keys = remove_unused_keys(conn, group_id, nfc_keys)

        return new_keys, remove_keys
    except Exception as e:
        logging.error(f"Error fetching users with NFC keys for group {group_id}: {e}")
        return []

def process_group(api, group):
    group_id = group['id']
    new_keys, remove_keys = get_users_with_nfc_keys(api, group_id, db_file)
    devices = get_vans_by_group(api, group_id)
    return new_keys, remove_keys, devices

####Update Vehicles
#Geotab uses text messages to communicate to the iox reader instructions to either remove or add with the addtowhitelist part. 

def send_text_message(api, vehicles_to_update, keys, add=True):
    try:
        api.call('Add', 'TextMessage', {
            "device": {
                "id": vehicles_to_update
            },
            "isDirectionToVehicle": True,
            "messageContent": {
                "driverKey": keys,
                "contentType": "DriverWhiteList",
                "clearWhiteList": False,
                "addToWhiteList": add
            }
        })
        action = "added to" if add else "removed from"
        logging.info(f"Keys {action} vehicle with ID: {vehicles_to_update}")
    except Exception as e:
        logging.error(f"Error sending text message to vehicle with ID: {vehicles_to_update}

####Main Process

def main():
    try:
        api.authenticate()
        groups = api.get('Group', search=dict(active=True))
        filtered_groups = [group for group in groups if group['name'] in group_names]
        for group in filtered_groups:
            group_id = group['id']
            remove_keys, new_keys, devices = process_group(api, group, db_file)
            for device in devices:
                vehicles_to_update = device['id']
                logging.info(f"Device {device['name']} found in group {group['name']} with ID: {device['id']}")
                if new_keys:
                    send_text_message(api, vehicles_to_update, new_keys, add=True)
                if remove_keys:
                    send_text_message(api, vehicles_to_update, remove_keys_add=False)
            
    except Exception as e:
        logging.error(f"Error in main process: {e}")

if __name__ == "__main__":
    main()
