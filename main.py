from mygeotab import API,dates
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

###SQLITE Functions (Memory in between runs) ##################################################################################################################################################
# We will be storing all keys in a table labeled keys_group id for comparison later, we need to keep a list of who we've added so we know what to remove in the future. There is a 1000 key limit on the iox device and we cannot retrieve this from the device itself. 
# It also wouldn't make sense to enable this feature and not do this.
# Should refactor to use the storage api eventually.
# Function to create SQLite connection


#Base Functions
def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to SQLite database: {e}")
    return None

def create_table(conn, table_name, table_schema):
    try:
        with conn:
            conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {table_schema}
                );
            ''')
    except sqlite3.Error as e:
        logging.error(f"Error creating table {table_name}: {e}")

### Driver Key storage
def insert_keys(conn, group_id, keys):
    new_keys = []
    try:
        with conn:
            c = conn.cursor()
            for key in keys:
                c.execute(f'''
                    INSERT OR IGNORE INTO keys_{group_id} (serialNumber) VALUES (?)
                ''', (key,))
                if c.rowcount > 0:
                    new_keys.append(key)
        logging.info(f"Keys inserted for group {group_id}: {new_keys}")
        return new_keys
    except sqlite3.Error as e:
        logging.error(f"Error inserting keys for group {group_id}: {e}")
        return []
#Remove Old Keys
def remove_unused_keys(conn, group_id, keys):
    try:
        c = conn.cursor()
        placeholders = ','.join('?' for _ in keys)
        query = f'''
            DELETE FROM keys_{group_id} WHERE serialNumber NOT IN ({placeholders})
            RETURNING serialNumber
        '''
        c.execute(query, keys)
        removed_keys = c.fetchall()
        conn.commit()
        
        if removed_keys:
            removed_keys_list = [key[0] for key in removed_keys]
            logging.info(f"Keys removed for group {group_id}: {removed_keys_list}")
        
        return removed_keys_list
    except sqlite3.Error as e:
        logging.error(f"Error removing unused keys for group {group_id}: {e}")
        return []

##Vehicle Database portion

def insert_devices(conn, group_id, devices):
    new_devices = []
    all_keys = []
    try:
        with conn:
            c = conn.cursor()
            for device in devices:
                c.execute(f'''
                    INSERT OR IGNORE INTO devices_{group_id} (deviceId, name) VALUES (?, ?)
                ''', (device['id'], device['name']))
                if c.rowcount > 0:
                    new_devices.append(device['id'])
        logging.info(f"Devices inserted for group {group_id}: {new_devices}")
        all_keys = get_all_keys(conn, group_id)
        return new_devices, all_keys
    except sqlite3.Error as e:
        logging.error(f"Error inserting devices for group {group_id}: {e}")
        return [], []

def remove_old_devices(conn, group_id, current_device_ids):
    try:
        c = conn.cursor()
        placeholders = ','.join('?' for _ in current_device_ids)
        query = f'''
            DELETE FROM devices_{group_id} WHERE deviceId NOT IN ({placeholders})
            RETURNING deviceId
        '''
        c.execute(query, current_device_ids)
        removed_devices = c.fetchall()
        conn.commit()
        
        if removed_devices:
            removed_device_list = [device[0] for device in removed_devices]
            logging.info(f"Devices removed for group {group_id}: {removed_device_list}")
        
        return
    except sqlite3.Error as e:
        logging.error(f"Error removing old devices for group {group_id}: {e}")
        return []
###Get Vehicles#############################################################################################################################################################################
#This section is to get all vehicles in group that have the authorized driver list enabled, return their nfc key, compare it with the database, and either add or remove them from the database, and then in turn, the authlist

def get_vans_by_group(api, group_id, conn):
    try:
        create_table(conn, f"devices_{group_id}", "deviceId TEXT PRIMARY KEY, name TEXT")
        logging.info(f"Fetching devices for group ID: {group_id}")
        devices = api.get('Device', search={'groups': [{'id': group_id}]})
        
        filtered_devices = []
        for device in devices:
            custom_parameters = device.get('customParameters', [])
            for param in custom_parameters:
                if param.get('description') == "Enable Authorised Driver List" and param.get('isEnabled'):
                    filtered_devices.append(device)
                    break
        
        if not filtered_devices:
            logging.warning(f"No devices with 'Enableauthorizeddriverslist' enabled found in group ID: {group_id}")
        
        new_devices, all_keys = insert_devices(conn, group_id, filtered_devices)
        removed_devices = remove_old_devices(conn, group_id, [device['id'] for device in filtered_devices])
        
        return filtered_devices, new_devices, removed_devices, all_keys
    except Exception as e:
        logging.error(f"Error fetching devices for group {group_id}: {e}")
        return [], [], [], []

####Get Drivers###########################################################################################################################################################################
#This section is to get all drivers by group, return their nfc key, compare it with the database, and either add or remove them from the database, and then in turn, the authlist

def get_users_with_nfc_keys(api, group_id, conn):
    try:
        # Create table if not exists for the group_id
        create_table(conn, f"keys_{group_id}", "serialNumber TEXT PRIMARY KEY")

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


####We will use below function to ensure all keys are passed along for new vehicles
def get_all_keys(conn, group_id):
    try:
        c = conn.cursor()
        c.execute(f"SELECT serialNumber FROM keys_{group_id}")
        all_keys = [row[0] for row in c.fetchall()]
        logging.info(f"All keys for group {group_id}: {all_keys}")
        return all_keys
    except sqlite3.Error as e:
        logging.error(f"Error fetching all keys for group {group_id}: {e}")
        return []

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
        logging.error(f"Error sending text message to vehicle with ID: {vehicles_to_update}")

## Combined Processes 

def process_group(api, group, db_file):
    group_id = group['id']
    conn = create_connection(db_file)
    if conn:
        new_keys, remove_keys = get_users_with_nfc_keys(api, group_id, conn)
        devices, new_devices, all_keys = get_vans_by_group(api, group_id, conn)
        conn.close()
        return new_keys, remove_keys, new_devices, all_keys
    return [], [], [], [], []
####Main Process

def main():
    try:
        api.authenticate()
        groups = api.get('Group', search=dict(active=True))
        filtered_groups = [group for group in groups if group['name'] in group_names]
        
        for group in filtered_groups:
            new_keys, remove_keys, devices, new_devices, all_keys = process_group(api, group, db_file)
            
            for device in devices:
                vehicles_to_update = device['id']
                logging.info(f"Device {device['name']} found in group {group['name']} with ID: {device['id']}")
                
                if device['id'] in new_devices:
                    logging.info(f"New device {device['name']} (ID: {device['id']}) found. Adding all keys to whitelist.")
                    send_text_message(api, vehicles_to_update, all_keys, add=True)
                else:
                    if new_keys:
                        send_text_message(api, vehicles_to_update, new_keys, add=True)
                    if remove_keys:
                        send_text_message(api, vehicles_to_update, remove_keys, add=False)
            
    except Exception as e:
        logging.error(f"Error in main process: {e}")

if __name__ == "__main__":
    main()
            
