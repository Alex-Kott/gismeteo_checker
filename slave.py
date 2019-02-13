import json
import asyncio
import logging
from configparser import ConfigParser
from typing import Union, Dict

from aiohttp import ClientSession

config = ConfigParser(comment_prefixes='#')
config.read('config.ini')

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=config['GENERAL']['LOG_FILE'])
logger = logging.getLogger('gismeteo_checker')
logger.setLevel(logging.INFO)


def get_object_code() -> int:
    with open(config['SLAVE']['OBJECT_CODE_FILE']) as file:

        return int(file.read())


async def get_object_data(object_code: Union[str, int]) -> Dict:
    file_uri = f"{config['SLAVE']['MASTER_STORE_URL']}{object_code}.json"
    logger.info(f"File URI: {file_uri}")

    async with ClientSession() as session:
        async with session.get(file_uri) as response:

            return await response.json()


def save_object_data(object_data: Dict) -> None:
    if config['SLAVE'].get('OBJECT_DATA_FILE'):
        data_file_name = config['SLAVE']['OBJECT_DATA_FILE']
    else:
        data_file_name = f"{object_data['index']}.json"

    with open(data_file_name, 'w') as file:
        json.dump(object_data, file)


async def main():
    logger.info('Start...')

    object_code = get_object_code()
    logger.info(f"Current object code: {object_code}")

    object_data = await get_object_data(object_code)
    logger.info(f"Object data received: {object_data}")

    save_object_data(object_data)
    logger.info(f"Object data saved")

    logger.info('Completed')


if __name__ == "__main__":
    logger = logging.getLogger('logger')
    logger.setLevel('INFO')
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(main())
    event_loop.close()
