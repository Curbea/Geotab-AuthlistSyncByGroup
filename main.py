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
#def default_serializer(obj):
#    if isinstance(obj, datetime):
#        return obj.isoformat()
#    raise TypeError(f"Type {type(obj)} not serializable")

####Van By Groups




def get_vans_by_group(api, group_id):
    try:
        logging.info(f"Fetching devices for group ID: {group_id}")
        response = api.get('Device', search={'groups': [{'id': group_id}]})
        # Log the raw response for debugging with a custom serializer
#        logging.debug(f"Raw response data: {json.dumps(response, default=default_serializer, indent=2)}")
        return response
#    except json.JSONDecodeError as json_ex:
        # Specific handling for JSON decode errors
        logging.error(f"JSON decode error while fetching devices for group {group_id}: {json_ex}")
        return []
    except Exception as e:
        logging.error(f"Error fetching devices for group {group_id}: {e}")
        return []

###SQLITE
# Function to create SQLite connection
def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to SQLite database: {e}")
    return None

# Function to create table for a specific group ID if it doesn't exist
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

# Function to insert keys into the database and return new keys that were inserted
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

# Function to remove keys that have no match
def remove_unused_keys(conn, group_id, keys):
    try:
        c = conn.cursor()
        c.execute(f'''
            DELETE FROM keys_{group_id} WHERE serialNumber NOT IN ({','.join('?' for _ in keys)})
        ''', keys)
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error removing unused keys for group {group_id}: {e}")


####Drivers

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

        # Insert keys into database and get new keys inserted
        new_keys = insert_keys(conn, group_id, nfc_keys)

        # Remove keys that have no match
        remove_unused_keys(conn, group_id, nfc_keys)

        return new_keys
    except Exception as e:
        logging.error(f"Error fetching users with NFC keys for group {group_id}: {e}")
        return []

def process_group(api, group):
    group_id = group['id']
    nfc_keys = get_users_with_nfc_keys(api, group_id, db_file)
    vans = get_vans_by_group(api, group_id)







def main():
    try:
        api.authenticate()
        groups = api.get('Group', search=dict(active=True))
        filtered_groups = [group for group in groups if group['name'] in group_names]
        for group in filtered_groups:
            group_id = group['id']
            process_group(api, group)
            devices = get_vans_by_group(api, group_id)
            for device in devices:
                logging.info(f"Device {device['name']} found in group {group['name']} with ID: {device['id']}")


    except Exception as e:
        logging.error(f"Error in main process: {e}")

if __name__ == "__main__":
    main()