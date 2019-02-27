import json
import logging
# import ConfigParser

import requests as req

# config = ConfigParser.ConfigParser()
# config.read('config.ini')

LOG_FILE = 'default.log'
OBJECT_CODE_FILE = 'object_code.txt'
MASTER_STORE_URL = 'http://localhost:8080/store/'
OBJECT_DATA_FILE = 'object_data.json'

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=LOG_FILE)
logger = logging.getLogger('gismeteo_checker')
logger.setLevel(logging.INFO)


def get_object_code():
    with open(OBJECT_CODE_FILE) as file:

        return int(file.read())


def get_object_data(object_code):
    file_uri = "{}{}.json".format(MASTER_STORE_URL, object_code)
    logger.info("File URI: {}".format(file_uri))

    r = req.get(file_uri)

    return r.json()

    # async with ClientSession() as session:
    #     async with session.get(file_uri) as response:
    #
    #         return await response.json()


def save_object_data(object_data):
    if OBJECT_DATA_FILE:
        data_file_name = OBJECT_DATA_FILE
    else:
        data_file_name = "{}.json".format(object_data['index'])

    with open(data_file_name, 'w') as file:
        json.dump(object_data, file)


def main():
    logger.info('Start...')

    object_code = get_object_code()
    logger.info("Current object code: {}".format(object_code))

    object_data = get_object_data(object_code)
    logger.info("Object data received: {}".format(object_code))

    save_object_data(object_data)
    logger.info("Object data saved")

    logger.info('Completed')


if __name__ == "__main__":
    logger = logging.getLogger('logger')
    logger.setLevel('INFO')
    main()
