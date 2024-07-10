from mygeotab import API,MyGeotabException
from dotenv import load_dotenv
import os
import logging
import sqlite3
from time import sleep
from datetime import datetime,timezone
#import json
# Load environment variables from .env file
load_dotenv()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
# Get MyGeotab credentials and groups from environment variables
username = os.getenv('GEOTAB_USERNAME')
password = os.getenv('GEOTAB_PASSWORD')
database = os.getenv('GEOTAB_DATABASE')
group_names = os.getenv('GEOTAB_GROUPS', '').split(',')
db_file = 'authlist.db'

#Base Functions
def authenticate(db_file):
    """This is to authenticate and then grab the session token so all further calls need not reauthenticate; we also establish our sqlite connection"""
    try:
        api = API(username, password, database)
        credentials = api.authenticate()
        conn = create_connection(db_file)
        logging.info("Authenticated successfully.")
        new_api = API.from_credentials(credentials)
        return new_api, conn, credentials
    except Exception as e:
        logging.error(f"Authentication failed: {e}")
        raise





###Database Functions (for memory in between runs) ##################################################################################################################################################
# We will be storing all keys in a table labeled keys_group id for comparison later, we need to keep a list of who we've added so we know what to remove in the future. There is a 1000 key limit on the iox device and we cannot retrieve this from the device itself. 
# It also wouldn't make sense to enable this feature and not do this.
# Function to create SQLite connection

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

### Insert new keys into database
def insert_keys(conn, group_id, keys):
    new_keys = []
    try:
        with conn:
            c = conn.cursor()
            for key in keys:
                c.execute(f'''
                    INSERT OR IGNORE INTO keys_{group_id} (driverKeyType, id, keyId, serialNumber) 
                    VALUES (?, ?, ?, ?)
                ''', (key['driverKeyType'], key['id'], key['keyId'], key['serialNumber']))
                if c.rowcount > 0:
                    new_keys.append(key)
        logging.info(f"Keys inserted for group {group_id}: {new_keys}")
        return new_keys
    except sqlite3.Error as e:
        logging.error(f"Error inserting keys for group {group_id}: {e}")
        return []


#Remove unused keys from storage


def remove_unused_keys(conn, group_id, keys):
    removed_keys_list = []
    try:
        c = conn.cursor()
        placeholders = ','.join('?' for _ in keys)
        query = f'''
            DELETE FROM keys_{group_id} WHERE serialNumber NOT IN ({placeholders})
            RETURNING driverKeyType, id, keyId, serialNumber
        '''
        c.execute(query, [key['serialNumber'] for key in keys])
        removed_keys = c.fetchall()
        conn.commit()
        
        if removed_keys:
            removed_keys_list = [{'driverKeyType': key[0], 'id': key[1], 'keyId': key[2], 'serialNumber': key[3]} for key in removed_keys]
            logging.info(f"Keys removed for group {group_id}: {removed_keys_list}")
        
        return removed_keys_list
    except sqlite3.Error as e:
        logging.error(f"Error removing unused keys for group {group_id}: {e}")
        return removed_keys_list

# I stored users seperate from keys to prevent un needed checks or issues; if we use user id's as the primary key then key updates would not work, 
# the other way around would make unnecessary set calls or checks that might end up overriding intended exceptions. 
# The other thing to prevent is if a user were in two groups that had different intended TZ's they would get two change calls everytime this ran
# This way users will only be processed once they join a group and that's it.
def insert_users(conn, group_id, users, userids):
    new_usersid = []
    try:
        create_table(conn, f"users_{group_id}", "userid TEXT PRIMARY KEY")
        with conn:
            c = conn.cursor()
            for userid in all_userids:
                c.execute(f'''
                    INSERT OR IGNORE INTO users_{group_id} (userid) 
                    VALUES (?)
                ''', (userid,))
                if c.rowcount > 0:
                    new_usersid.append(userid)
        logging.info(f"New users stored for group {group_id}: {new_usersid}")
        return new_usersid
    except sqlite3.Error as e:
        logging.error(f"Error inserting users for group {group_id}: {e}")
        return []

def remove_unused_users(conn, group_id, user_ids):
    try:
        c = conn.cursor()
        placeholders = ','.join('?' for _ in user_ids)
        query = f'''
            DELETE FROM users_{group_id} WHERE userid NOT IN ({placeholders})
            RETURNING userid
        '''
        c.execute(query, user_ids)
        removed_users = c.fetchall()
        conn.commit()
        
        if removed_users:
            logging.info(f"Users removed for group {group_id}")
        
        return removed_users
    except sqlite3.Error as e:
        logging.error(f"Error removing unused users for group {group_id}: {e}")
        return removed_users_list
##Vehicle Database portion

def insert_devices(conn, group_id, devices):
    new_devices = []
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
        return new_devices
    except sqlite3.Error as e:
        logging.error(f"Error inserting devices for group {group_id}: {e}")
        return []


def remove_old_devices(conn, group_id, current_device_ids):
    removed_devices=[]
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
        
        return removed_devices
    except sqlite3.Error as e:
        logging.error(f"Error removing old devices for group {group_id}: {e}")
        return []


###Get Vehicles#############################################################################################################################################################################
#This section is to get all vehicles in group, add the custom parameter if it is missing; return their nfc key, compare it with the database, and either add or remove them from the database, 
#and then in turn, the authlist

def get_vans_by_group(api, group_id, conn,add=False):
    try:
        create_table(conn, f"devices_{group_id}", "deviceId TEXT PRIMARY KEY, name TEXT")
        logging.info(f"Fetching devices for group ID: {group_id}")
        now_utc = datetime.now(timezone.utc)
        devices = api.get('Device', search={'groups': [{'id': group_id}], "fromDate": now_utc})
        
        filtered_devices = []
        for device in devices:
            custom_parameters = device.get('customParameters', [])
            if not any(param.get('description') == "Enable Authorised Driver List" for param in custom_parameters):
                new_param = {
                    "bytes": "CA==",
                    "description": "Enable Authorised Driver List",
                    "isEnabled": False,
                    "offset": 164
                }
                custom_parameters.append(new_param)

                # Update the device with the new custom parameter
                updated_device = device.copy()
                updated_device['customParameters'] = custom_parameters

                try:
                    api.set('Device', updated_device)
                    logging.info(f"Updated device {device['name']} with new custom parameter.")
                except Exception as e:
                    logging.error(f"Failed to update device {device['id']}: {e}")

            filtered_devices.append(device)

        if add:
            new_devices = insert_devices(conn, group_id, filtered_devices)
            return filtered_devices, new_devices
        else:
            removed_devices = remove_old_devices(conn, group_id, [device['id'] for device in filtered_devices])
            return removed_devices
    except MyGeotabException as e:
        logging.error(f"Geotab API error fetching devices for group {group_id}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error fetching devices for group {group_id}: {e}")
        raise MyGeotabException({"errors": [{"name": "UnexpectedError", "message": str(e)}]})

        return [], []
####Get Drivers###########################################################################################################################################################################
#This section is to get all drivers by group, return their nfc key, compare it with the database, and either add or remove them from the database, and then in turn, the authlist

def get_users_with_nfc_keys(api, group, conn):
    try:
        group_id = group['id']
        # Create table if not exists for the group_id
        create_table(conn, f"keys_{group_id}", 
                     "driverKeyType TEXT, id TEXT, keyId TEXT, serialNumber TEXT PRIMARY KEY")
        #We only want active Drivers 
        now_utc = datetime.now(timezone.utc)
        users = api.get('User', search={'driverGroups': [{'id': group_id}], "fromDate": now_utc ,"isDriver": True })
   
        """Fetch users and their keys - we only want users in the group of the loop (group id), we only want active users, (dateFrom); Is driver, we only need to return users that can or do have keys"""
        all_userids = []
        nfc_keys = []
        for user in users:
            if 'keys' in user:
                for key in user['keys']:
                    key_data = {
                        'driverKeyType': key.get('driverKeyType'),
                        'id': key.get('id'),
                        'keyId': key.get('keyId'),
                        'serialNumber': key.get('serialNumber')
                    }
                    nfc_keys.append(key_data)
            all_userids.append(user['id')
        # Get new keys inserted and removed from the database
        if patch_users:
            patch_users(users, group, conn, all_userid, group_id)
        new_keys = insert_keys(conn, group_id, nfc_keys)
        remove_keys = remove_unused_keys(conn, group_id, nfc_keys)
        all_keys = nfc_keys
        return new_keys, remove_keys, all_keys
    except Exception as e:
        logging.error(f"Error fetching users with NFC keys for group {group_id}: {e}")
        return []

def_patch_users(users, group, conn, all_userid, group_id)
   """This function will be used to set proper clearances for drivers, it will set timezones according to their group"""
    try:
        remove_unused_users(conn, group_id, all_userid)
        new_users = insert_users(conn, group_id, users, all_userid)
        if new_users:
            group_tz = os.getenv(f"{group['name']}", 'America/Vancouver')
            for user in users:
                if user['id'] in new_users
                    if user.get('timezoneid') != group_tz:

                    if user.get('securityGroup') in clearances_to_patch:


####Update Vehicles
#Geotab uses text messages to communicate to the iox reader instructions to either remove or add with the addtowhitelist part. 

#def send_text_message(api, vehicle_to_update, all_keys, add=True):
def send_text_message(api, vehicle_to_update, Keys, add=True,clear=False,Time=0):
    try:
        for key in Keys:
            data = {

    "device": {
        "id": vehicle_to_update
    },
    "isDirectionToVehicle": True,
    "messageContent": {
        "driverKey": key,
        "contentType": "DriverAuthList",
        "clearAuthList": clear,
        "addToAuthList": add
    }
}
            try:
                api.add("TextMessage",data)
                action = "added to" if add else "removed from"
                logging.info(f"Keys {action} vehicle with ID: {vehicle_to_update}")
                sleep(Time)
        
##logs    
            except MyGeotabException as e:
                logging.error(f"Geotab API error while sending text message to vehicle with ID: {vehicle_to_update}: {e}")
                
            except Exception as e:
                logging.error(f"Unexpected error while sending text message to vehicle with ID: {vehicle_to_update}: {e}")
                raise MyGeotabException({"errors": [{"name": "UnexpectedError", "message": str(e)}]})
##For Vehicles were removing or updating; should have been sent a null array so we need to call it outside of the for loop. 
        if clear:
            try:
                api.add("TextMessage",data)
                logging.info(f"All Keys removed from vehicle with ID: {vehicle_to_update}")
#More logging         
            except MyGeotabException as e:
                logging.error(f"Geotab API error while clearing all keys from vehicle with ID: {vehicle_to_update}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error while clearing all keys from vehicle with ID: {vehicle_to_update}: {e}")
                raise MyGeotabException({"errors": [{"name": "UnexpectedError", "message": str(e)}]})

    except MyGeotabException as e:
        logging.error(f"Geotab API error while processing keys for vehicle with ID: {vehicle_to_update}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while processing keys for vehicle with ID: {vehicle_to_update}: {e}")
        raise MyGeotabException({"errors": [{"name": "UnexpectedError", "message": str(e)}]})
#Back
def process_group(api, group, conn):
    if conn:
        group_id = group['id']
        new_keys, remove_keys, all_keys, group_id = get_users_with_nfc_keys(api, group, conn)
        filtered_devices, new_devices = get_vans_by_group(api, group_id, conn,add=True)
        conn.close()
        return new_keys, remove_keys, all_keys, filtered_devices, new_devices
    return [], [], [], [], []

####Main Process
def main():
    try:
        api, conn, credentials = authenticate(db_file)
        groups = api.get('Group', search=dict(active=True))
        filtered_groups = [group for group in groups if group['name'] in group_names]

        for group in filtered_groups:
            group_id = group['id']
            removed_devices = get_vans_by_group(api, group_id, conn,add=False)

## Need to add a break here to clear old devices before continuting           
      
            for device in removed_devices:
                if removed_devices:
                    vehicle_to_update = device['id']
                    logging.info(f"Removed device {device['name']} (ID: {device['id']}). Clearing all keys from whitelist.")
                    send_text_message(api, vehicle_to_update, None, add=False,clear=True,Time=0)

## Need to add a break here to clear old devices before continuting           

        
        for group in filtered_groups:
            new_keys, remove_keys, all_keys, group_id, filtered_devices, new_devices = process_group(api, group, conn)        
            
            for device in filtered_devices:
                vehicle_to_update = device['id']
                logging.info(f"Device {device['name']} found in group {group['name']} with ID: {device['id']}")
                
                if device['id'] in new_devices:
                    logging.info(f"{all_keys}")
                    logging.info(f"New device {device['name']} (ID: {device['id']}) found. Adding all keys to whitelist.")
                    send_text_message(api, vehicle_to_update, all_keys, add=True,clear=False,Time=0.02)
                else:
                    if new_keys:
                        send_text_message(api, vehicle_to_update, new_keys, add=True,clear=False,Time=0.01)
                    if remove_keys:
                        send_text_message(api, vehicle_to_update, remove_keys, add=False,clear=False,Time=0.01)
            
    except Exception as e:
        logging.error(f"Error in main process: {e}")

if __name__ == "__main__":
    main()
