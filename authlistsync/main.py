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
log_file_path = 'authlistlog.txt'
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# Get MyGeotab credentials and groups from environment variables
username = os.getenv('GEOTAB_USERNAME')
password = os.getenv('GEOTAB_PASSWORD')
database = os.getenv('GEOTAB_DATABASE')
group_names = os.getenv('GEOTAB_GROUPS', '').split(',')
db_file = 'authlist.db'
patch_users = os.getenv('PATCH_USERS', 'False')
patch_assets = os.getenv('PATCH_ASSETS', 'False')
patch_tz = os.getenv('PATCH_TZ', False)
patch_sc = os.getenv('PATCH_SC', False)
new_scid = os.getenv('NEW_SC_ID', None)
old_scid = os.getenv('OLD_SC_ID', None).split(',')
exception_group_id=os.getenv('EXCEPTION_GROUP_ID', None)
now = datetime.now()



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

def create_table(conn, table_name, table_schema, unique):
    try:
        with conn:
            conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {table_schema},
                    UNIQUE({unique})

                );
            ''')
    except sqlite3.Error as e:
        logging.error(f"Error creating table {table_name}: {e}")


def get_users_with_nfc_keys(api, group_id, group_name, conn, exception_keys):
    """
    Fetch and manage users with NFC keys for a specific group.

    This function creates a database table for the given group if it doesn't already exist. 
    It then fetches active users who are drivers (have keys assigned to them) from the API and 
    processes their NFC keys. 
    It then checks the database and adds missing keys and removes keys not returned by the api

    Parameters:
    api (object): The API object used to fetch users and their keys.
    group_id (str): The ID of the group for which to fetch users and keys.
    group_name (str): The name of the group for logging and display purposes.
    conn (object): The database connection object.
    exception_keys (list): A list of exception keys to be considered during processing.

    Returns:
    Lists: A list of dictionaries containing the users key object
        - new_keys (list): List of new keys inserted into the database.
        - remove_keys (list): List of keys removed from the database.
        - all_keys (list): List of all keys returned from the api.

    Raises:
    Exception: Logs and returns empty lists in case of any errors during the process.
    """
    
    try:
        # Create table if not exists for the group_id
        create_table(conn, f"keys_{group_id}", "driverKeyType TEXT, id TEXT, keyId TEXT, serialNumber TEXT PRIMARY KEY", "serialNumber")
        #We only want active Drivers 
        now_utc = datetime.now(timezone.utc)
        users = api.get('User', search={'companyGroups': [{'id': group_id}], "fromDate": now_utc ,"isDriver": True })
   
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
            all_userids.append(user['id'])

        # Get new keys inserted and removed from the database
        if patch_users:
            modify_users(api, users, conn, all_userids, group_id, group_name)
        all_keys = nfc_keys + [key for key in exception_keys if key['serialNumber'] not in {k['serialNumber'] for k in nfc_keys}]
        new_keys = insert_keys(conn, group_id, all_keys)
        remove_keys = remove_unused_keys(conn, group_id, all_keys)
        return new_keys, remove_keys, all_keys
    except Exception as e:
        logging.error(f"Error fetching users with NFC keys for group {group_id}: {e}")
        return [],[],[]

def get_exception_users(api , exception_group):
    """
    Fetch users with NFC keys for a specified exception group.

    This function retrieves users from the specified exception group and extracts their NFC keys. 
    It returns a list of key data that is combined with every other groups key table.

    Parameters:
    api (object): The API object used to fetch users and their keys.
    exception_group (str): The ID of the exception group for which to fetch users and keys.

    Returns:
    list: A list of dictionaries containing key data for each user in the exception group. 
          Each dictionary contains the following fields:
          - driverKeyType (str): The type of the driver key.
          - id (str): The user ID.
          - keyId (str): The key ID.
          - serialNumber (str): The serial number of the key.

    Raises:
    Exception: Logs and returns an empty list in case of any errors during the process.
    """
    try:
        exception_keys = []
        now_utc = datetime.now(timezone.utc)
        users = api.get('User', search={'companyGroups': [{'id': exception_group}], "fromDate": now_utc})
        for user in users:
            if 'keys' in user:
                for key in user['keys']:
                    key_data = {
                        'driverKeyType': key.get('driverKeyType'),
                        'id': key.get('id'),
                        'keyId': key.get('keyId'),
                        'serialNumber': key.get('serialNumber')
                    }
                    exception_keys.append(key_data)
 
        return exception_keys
    except Exception as e:
        logging.error(f"Error fetching users with NFC keys for exception group: {e}")
        return []

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
def modify_users(api, users, conn, all_userid, group_id, group_name):
    """
    This function manages user data by:
    - Creating a database table for the users of the specified group if not existing.
    - Removing users who are no longer part of the group.
    - Inserting new users into the database.
    - Setting the appropriate timezone for each user according to their group.
    - Updating security clearances for users if necessary.

    Parameters:
    api (object): The API object used to fetch and update user data.
    users (list): A list of user dictionaries to be processed.
    conn (object): The database connection object.
    all_userid (list): A list of user IDs that are part of the group.
    group_id (str): The ID of the group being processed.
    group_name (str): The name of the group being processed.

    Returns:
    None

    Raises:
    KeyError: If there is a missing key in the user dictionary.
    Exception: Logs any other exceptions encountered during the process.
    """
    try:
        create_table(conn, f"users_{group_id}", "userid TEXT PRIMARY KEY","userid")
        remove_unused_users(conn, group_id, all_userid)
        new_users = insert_users(conn, group_id, all_userid)
        if new_users:
            group_tz = os.getenv(f"{group_name}", 'America/Vancouver')            


            for user in users:
                if user['id'] in new_users:
                    updated = False
                    if user.get('timezoneid') != group_tz and patch_tz:
                        user['timezoneid'] = group_tz
                        updated = True


                    if patch_sc and any(sc['id'] in old_scid for sc in user['securityGroups']):
                        for sc in user['securityGroups']:
                            if sc['id'] in old_scid:
                                sc['id'] = new_scid
                                updated = True
                    if updated:
                        api.set('User', user)

    
    except KeyError as e:
        print(f"Key error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def insert_users(conn, group_id, all_userids):
    new_usersid = []
    try:
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

def remove_unused_users(conn, group_id, all_userids):
    try:
        c = conn.cursor()
        placeholders = ','.join('?' for _ in all_userids)
        query = f'''
            DELETE FROM users_{group_id} WHERE userid NOT IN ({placeholders})
            RETURNING userid
        '''
        c.execute(query, all_userids)
        removed_users = c.fetchall()
        conn.commit()
        
        if removed_users:
            logging.info(f"Users removed for group {group_id}")
        
        return removed_users
    except sqlite3.Error as e:
        logging.error(f"Error removing unused users for group {group_id}: {e}")
##Vehicle Database portion
##
##
def get_vans_by_group(api, group_id, group_name, conn,add=False):
    """
    Fetch and manage devices (vans) by group.

    This function creates a database table for the given group if it doesn't already exist. 
    It fetches devices from the API, optionally adds a custom parameter to enable the authorized driver list, 
    and updates the devices if necessary. It also handles inserting new devices and removing old ones from the database.

    Parameters:
    api (object): The API object used to fetch and update devices.
    group_id (str): The ID of the group for which to fetch devices.
    group_name (str): The name of the group to get the environment variable timezone.
    conn (object): The database connection object.
    add (bool): A flag indicating whether to split this function for the two times it runs; 
    also stops processing device changes during the remove phase

    Returns:
    tuple: A tuple containing:
        - filtered_devices (list): List of devices after processing.
        - new_devices (list, during second round): List of new devices inserted into the database if 'add' is True.
        - removed_devices (list, during first round): List of devices removed from the database if 'add' is False.

    Raises:
    MyGeotabException: Custom exception with error details if any issues occur during the process.
    """
    try:
        create_table(conn, f"devices_{group_id}", "serialNumber TEXT PRIMARY KEY, deviceId TEXT", "serialNumber")
        logging.info(f"Fetching devices for group ID: {group_id}")
        now_utc = datetime.now(timezone.utc)
        devices = api.get('Device', search={'groups': [{'id': group_id}], "fromDate": now_utc})
        filtered_devices = []
        for device in devices:
            updated = False
            custom_parameters = device.get('customParameters', [])
            if add and not any(param.get('description') == "Enable Authorised Driver List" for param in custom_parameters):
                new_param = {
                    "bytes": "CA==",
                    "description": "Enable Authorised Driver List",
                    "isEnabled": False,
                    "offset": 164
                }
                custom_parameters.append(new_param)
                updated = True

                # Update the device with the new custom parameter
            group_tz = os.getenv(f"{group_name}", 'America/Vancouver')
            if patch_assets and patch_tz and device.get('timeZoneId') != group_tz:
                device['timeZoneId'] = group_tz
                updated = True
            
            if  updated:

   
                updated_device = device.copy()
                updated_device['customParameters'] = custom_parameters

                try:
                    api.set('Device', updated_device)
                    logging.info(f"Updated device {device['name']}")
                except Exception as e:
                    logging.error(f"Failed to update device {device['id']}: {e}")

            all_devices = {
                'id': device.get('id'),
                'serialNumber': device.get('serialNumber')
            }

            filtered_devices.append(all_devices)
        del(devices)
        if add:
            new_devices = insert_devices(conn, group_id, filtered_devices)
            return filtered_devices, new_devices
        else:
            removed_devices = remove_old_devices(conn, group_id, [device['serialNumber'] for device in filtered_devices])
            return removed_devices
    except Exception as e:
        logging.error(f"Unexpected error fetching devices for group {group_id}: {e}")
        raise MyGeotabException({"errors": [{"name": "UnexpectedError", "message": str(e)}]})

        return [], []





def insert_devices(conn, group_id, devices):
    new_devices = []
    try:
        with conn:
            c = conn.cursor()
            for device in devices:
                c.execute(f'''
                    INSERT OR IGNORE INTO devices_{group_id} (deviceId, serialNumber) VALUES (?, ?)
                ''', (device['id'], device['serialNumber']))
                if c.rowcount > 0:
                    new_devices.append(device['id'])
        logging.debug(f"Devices inserted for group {group_id}: {new_devices}")
        if new_devices:    
            add_columns(conn, group_id, new_devices)
        return new_devices
    except sqlite3.Error as e:
        logging.error(f"Error inserting devices for group {group_id}: {e}")
        return []


def add_columns(conn, group_id, columns):
    """
    Adds new columns named by device id to a group's keys table in the database. When a column is added
    it is default 0, meaning that if our api fails during it loop and isn't able to update the row to
    1 we can retry

    This function checks the existing columns in the `keys_{group_id}` table and adds any missing 
    columns from the provided list of columns.

    Parameters:
    conn (object): The database connection object.
    group_id (str): The ID of the group whose keys table is being modified.
    columns (list): A list of column names to be added.

    Returns:
    None

    Raises:
    sqlite3.Error: Logs any SQLite errors encountered during the process.
    """
    
    
    try:
        with conn:
            cursor = conn.cursor()
            # Get the current columns in the table
            cursor.execute(f"PRAGMA table_info(keys_{group_id})")
            current_columns = [info[1] for info in cursor.fetchall()]

            for column in columns:
                if column not in current_columns:
                    cursor.execute(f'''
                        ALTER TABLE keys_{group_id} ADD COLUMN {column} INTEGER DEFAULT 0
                    ''')
                    logging.debug(f"Added missing column {column} to keys_{group_id}")
                else:
                    logging.debug(f"Column {column} already exists in keys_{group_id}")
    except sqlite3.Error as e:
        logging.error(f"Error adding columns to keys_{group_id}: {e}")


def remove_old_devices(conn, group_id, current_device_ids):
    removed_devices=[]
    try:
        c = conn.cursor()
        placeholders = ','.join('?' for _ in current_device_ids)
        query = f'''
            DELETE FROM devices_{group_id} WHERE serialNumber NOT IN ({placeholders})
            RETURNING deviceId
        '''
        c.execute(query, current_device_ids)
        removed_devices_tuples = c.fetchall()
        conn.commit()
        removed_devices = [device[0] for device in removed_devices_tuples]
        if removed_devices:
            remove_columns(conn, group_id, removed_devices)

        return removed_devices
    except sqlite3.Error as e:
        logging.error(f"Error removing old devices for group {group_id}: {e}")
        return []

def remove_columns(conn, group_id, columns_to_remove):
    """
    Remove specified columns from a database table for a specific group.

    This function modifies the table schema by creating a new table without the specified columns, 
    copying the data from the old table to the new table, dropping the old table, 
    and renaming the new table to the original table name. This is necessary as sqlite does not have a
    drop column function

    Parameters:
    conn (object): The database connection object.
    group_id (str): The ID of the group whose table columns are to be modified.
    columns_to_remove (list): A list of column names to be removed from the table.

    Returns:
    None

    Raises:
    sqlite3.Error: Logs any SQLite errors encountered during the process.
    """
    try:
        with conn:
            cursor = conn.cursor()

            # Get the current columns in the table
            cursor.execute(f"PRAGMA table_info(keys_{group_id})")
            current_columns_info = cursor.fetchall()
            current_columns = [info[1] for info in current_columns_info]

            # Determine the columns to keep
            columns_to_keep = [col for col in current_columns if col not in columns_to_remove]

            # Generate the new table schema without the columns to remove
            new_table_schema = ', '.join(columns_to_keep)
            old_table = f"keys_{group_id}"
            new_table = f"{old_table}_new"

            # Get the current column definitions
            column_definitions = ', '.join([
                f"{info[1]} {info[2]} {'PRIMARY KEY' if info[5] else ''} {'NOT NULL' if info[3] else ''}"
                for info in current_columns_info if info[1] in columns_to_keep
            ])

            # Create a new table with the remaining columns
            create_query = f'''
                CREATE TABLE {new_table} ({column_definitions})
            '''
            logging.debug(f"Create query: {create_query}")
            cursor.execute(create_query)

            # Copy data from the old table to the new table
            copy_data_query = f'''
                INSERT INTO {new_table} ({new_table_schema})
                SELECT {new_table_schema} FROM {old_table}
            '''
            logging.debug(f"Copy data query: {copy_data_query}")
            cursor.execute(copy_data_query)

            # Drop the old table
            cursor.execute(f"DROP TABLE {old_table}")

            # Rename the new table to the original table name
            cursor.execute(f"ALTER TABLE {new_table} RENAME TO {old_table}")

        logging.debug(f"Removed columns from keys_{group_id}: {columns_to_remove}")
    except sqlite3.Error as e:
        logging.error(f"Error removing columns from keys_{group_id}: {e}")

def update_device_column(conn, group_id, serial_number, column, value):
    """
    Update a specific column value for a device in the group's keys table.

    This function updates the value of a specified device id column and the current
    key's serial number for the row identifier
    in the `keys_{group_id}` table.

    Parameters:
    conn (object): The database connection object.
    group_id (str): The ID of the group whose keys table is being updated.
    serial_number (str): The serial number of the key row to be updated.
    column (str): The name of the column to be updated.
    value (any): The new value to set for the specified column.

    Returns:
    None

    Raises:
    sqlite3.Error: Logs any SQLite errors encountered during the update process.
    """
    
    
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(f'''
                UPDATE keys_{group_id}
                SET {column} = ?
                WHERE serialNumber = ?
            ''', (value, serial_number))
    except sqlite3.Error as e:
        logging.error(f"Error updating column {column} in keys_{group_id}: {e}")


def send_text_message(api, vehicle_to_update, Keys, group_id, conn, add=True, clear=False, Time=0, retries=3, delay=5):
    """
    Send a text message to update the authorization list of a vehicle.

    This function sends a text message to a specified vehicle to add or remove NFC keys 
    from its authorization list. It handles retries in case of failure and updates the 
    database accordingly.

    Parameters:
    api (object): The API object used to send messages.
    vehicle_to_update (str): The ID of the vehicle to update.
    Keys (list): A list of keys to add or remove from the vehicle's authorization list.
    group_id (str): The ID of the group associated with the vehicle.
    conn (object): The database connection object.
    add (bool): Indicates whether to add (True) or remove (False) the keys. Default is True.
    clear (bool): Indicates whether to clear the authorization list. Default is False.
    Time (int): The delay time in seconds between sending messages. Default is 0.
    retries (int): The number of retry attempts in case of failure. Default is 3.
    delay (int): The delay time in seconds between retry attempts. Default is 5.

    Returns:
    None

    Raises:
    MyGeotabException: Custom exception with error details if any issues occur during the process.
    """
    try:
        calls = []
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
            calls.append(['Add', {"typeName": 'TextMessage', "entity": data}])  
            
        if calls:
            for attempt in range(retries):
                try:
                    if len(calls) <= 50:
                        api.multi_call(calls)
                    else:
                        # Split into batches of 20
                        for i in range(0, len(calls), 50):
                            api.multi_call(calls[i:i + 50])
                    action = "added to" if add else "removed from"
                    logging.debug(f"Keys {action} device {vehicle_to_update} Keys: {Keys}")
                    sleep(Time)
                    break  # If successful, break out of the retry loop
                except Exception as e:
                    logging.error(f"Unexpected error while sending text messages to vehicle with ID: {vehicle_to_update} on attempt {attempt + 1}: {e}")
                    if attempt < retries - 1:
                        logging.info(f"Retrying in {delay} seconds...")
                        sleep(delay)
                    else:
                        logging.error(f"Failed after {retries} attempts")
                        raise MyGeotabException({"errors": [{"name": "UnexpectedError", "message": str(e)}]})
            if add:
                for key in Keys:
                    update_device_column(conn, group_id, key['serialNumber'], vehicle_to_update, 1)
        
        if clear:
            try:
                api.add("TextMessage", data)
                logging.info(f"All Keys removed from vehicle with ID: {vehicle_to_update}")
            except Exception as e:
                logging.error(f"Unexpected error while clearing all keys from vehicle with ID: {vehicle_to_update}: {e}")
                raise MyGeotabException({"errors": [{"name": "UnexpectedError", "message": str(e)}]})
    except Exception as e:
        logging.error(f"Unexpected error while processing keys for vehicle with ID: {vehicle_to_update}: {e}")
        raise MyGeotabException({"errors": [{"name": "UnexpectedError", "message": str(e)}]})

def search_failed(conn, group_id, column):
    """
    Search for keys in a specific group where a given column value is zero.

    This function queries the database for keys in a specific group's table 
    where the specified column has a value of zero. It returns a list of keys 
    that match this criterion; this is to provide a more resiliant retry 
    process for issues on a previous run.

    Parameters:
    conn (object): The database connection object.
    group_id (str): The ID of the group whose keys are being searched.
    column (str): The name of the column to be checked for a value of zero.

    Returns:
    list: A list of dictionaries, each containing the following key data:
        - driverKeyType (str): The type of the driver key.
        - id (str): The ID of the key.
        - keyId (str): The key ID.
        - serialNumber (str): The serial number of the key.

    Raises:
    sqlite3.Error: Logs any SQLite errors encountered during the query process.
    """
        
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(f'''
                SELECT driverKeyType, id, keyId, serialNumber
                FROM keys_{group_id}
                WHERE {column} = 0
            ''')
            results = cursor.fetchall()

            keys = []
            for row in results:
                key_data = {
                    'driverKeyType': row[0],
                    'id': row[1],
                    'keyId': row[2],
                    'serialNumber': row[3]
                }
                keys.append(key_data)

        logging.info(f"Found keys with {column} = 0 in keys_{group_id}: {keys}")
        return keys
    except sqlite3.Error as e:
        logging.error(f"Error searching for {column} = 0 in keys_{group_id}: {e}")
        return []




def process_group(api, group, conn, exception_keys):
    """
    Process a group to fetch NFC keys and update vehicle information.

    This function processes a specified group by fetching users with NFC keys, updating the vehicle
    authorization lists, and managing database records. It integrates functionalities to handle
    new and removed keys, as well as to update vehicle data.

    Parameters:
    api (object): The API object used to fetch and update data.
    group (dict): A dictionary containing all of the group's information from geotab.
    conn (object): The database connection object.
    exception_keys (list): A list of exception keys to be added to all groups.

    Returns:
    tuple: A tuple containing the following elements:
        - new_keys (list): List of new keys inserted into the database.
        - remove_keys (list): List of keys removed from the database.
        - all_keys (list): List of all processed keys.
        - group_id (str): The ID of the processed group.
        - group_name (str): The name of the processed group.
        - filtered_devices (list): List of devices after processing.
        - new_devices (list): List of new devices inserted into the database.

    Returns empty lists if the database connection is not provided.

    Raises:
    None
    """   
    if conn:
        group_id = group['id']
        group_name = group['name']
        new_keys, remove_keys, all_keys = get_users_with_nfc_keys(api, group_id, group_name, conn, exception_keys)
        filtered_devices, new_devices = get_vans_by_group(api, group_id, group_name, conn, add=True)
        return new_keys, remove_keys, all_keys, group_id, group_name, filtered_devices, new_devices
    return [], [], [], [], [], []

####Main Process
def main():
    try:
        api, conn, credentials = authenticate(db_file)
        groups = api.get('Group', search=dict(active=True))
        filtered_groups = [group for group in groups if group['name'] in group_names]
        for group in filtered_groups:
            group_id = group['id']
            group_name = group['name']
            removed_devices = get_vans_by_group(api, group_id, group_name,conn,add=False)
            logging.debug(f"Removed devices: {removed_devices}")

## Need to add a break here to clear old devices before continuting           
            for device in removed_devices:
                if removed_devices:
                    null_key = []
                    send_text_message(api, device, null_key, all_keys, group_id, add=False,clear=True,Time=0, retries=3, delay=5)
            del(removed_devices)

## Need to add a break here to clear old devices before continuting           
        exception_keys = get_exception_users(api, exception_group_id)
        
        for group in filtered_groups:
            new_keys, remove_keys, all_keys, group_id, group_name, filtered_devices, new_devices = process_group(api, group, conn, exception_keys)        
            
            for device in filtered_devices:
                """
Logic:
- If the device is new, add all keys to its whitelist.
- If the device is not new:
  - Remove any keys that need to be removed.
  - Add any new keys that need to be added.
  - Search for any keys that failed to update previously and retry adding them.
"""             
                vehicle_to_update = device['id']
                logging.info(f"Processing Device {vehicle_to_update} in group {group_name}")
                
                if device['id'] in new_devices:
                    logging.info(f"New device ID: {vehicle_to_update} found. Adding all keys to whitelist.")
                    send_text_message(api, vehicle_to_update, all_keys, group_id, conn, add=True,clear=False,Time=0.01,retries=3, delay=9)
                else:
                    if remove_keys:
                        send_text_message(api, vehicle_to_update, remove_keys, group_id, conn, add=False,clear=False,Time=0.00,retries=3, delay=6)
                    if new_keys:
                        send_text_message(api, vehicle_to_update, new_keys, group_id, conn, add=True,clear=False,Time=0.00,retries=3, delay=6)
                    retry_keys = search_failed(conn, group_id, vehicle_to_update)
                    if retry_keys:
                        send_text_message(api, vehicle_to_update, retry_keys,  group_id, conn, add=True,clear=False,Time=0.01,retries=3, delay=9)

        
        conn.close()
  
    except Exception as e:
        logging.error(f"Error in main process: {e}")
        conn.close()
if __name__ == "__main__":
    main()
