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
        logging.info(f"time {now_utc}")  # Correct logging usage
        texts = api.get('TextMessage', search={"fromDate": now_utc}) # Can't seem to combine search with messagecontent or device id
        driver_auth_texts = [text for text in texts if text.get('messageContent', {}).get('contentType') == "DriverAuthList"] # to do find message delivery faliure and add.

        logging.info(f"Users: {json.dumps(driver_auth_texts, default=str)}")  # Correct logging usage
        return driver_auth_texts


