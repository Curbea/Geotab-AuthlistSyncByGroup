username = os.getenv('GEOTAB_USERNAME')
password = os.getenv('GEOTAB_PASSWORD')
database = os.getenv('GEOTAB_DATABASE')
group_names = os.getenv('GEOTAB_GROUPS', '').split(',')
db_file = 'authlist.db'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

api = API(username, password, database)
def search_texts_authlistcontent(api):
    try:
        now_utc = datetime.now(timezone.utc) - timedelta(days=1)
        logging.info(f"time {now_utc}")
        texts = api.get('TextMessage', search={"fromDate": now_utc}) # Can't seem to combine search with messagecontent or device id
        driver_auth_texts = [text for text in texts if text.get('messageContent', {}).get('contentType') == "DriverAuthList"] # to do find message delivery faliure and add.
        #Filter to last unique message sent to prevent spam

#        logging.info(f"Users: {json.dumps(driver_auth_texts, default=str)}")
        logging.info(f"Keys {Keys}")
            for key in Keys:
                 logging.info(f"Key {key}")
                 data = {

    "device": {
        "id": "b1B5"
    },
    "isDirectionToVehicle": True,
    "messageContent": {
        "driverKey": key,
        "contentType": "DriverAuthList",
        "clearAuthList": False,
        "addToAuthList": True
    }
}

                 api.add("TextMessage",data)
                 action = "added to" if add else "removed from"
                 logging.info(f"Keys {action} vehicle with ID: {vehicles_to_update}")
         except Exception as e:
             logging.error(f"Error sending text message to vehicle with ID: {vehicles_to_update}: {e}")
#For loop over vehicle id's
#Maybe check after like an hour if the vehicle should be driving? (recieving messages)
