import json
import asyncio
import logging
import sys
from configparser import ConfigParser
from pathlib import Path
from typing import Union, Dict
import traceback

from aiohttp import ClientSession
from aiohttp.client import ContentTypeError


exec_path = Path(sys.argv[0])
script_path = exec_path.parent

try:
    config = ConfigParser(comment_prefixes='#')
    config.read(script_path / 'config.ini')

    logging.basicConfig(level=logging.ERROR,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=script_path / config['GENERAL']['LOG_FILE'])
    logger = logging.getLogger('gismeteo_checker')
    logger.setLevel(logging.INFO)
except Exception as e:
    with open('default.log', 'a') as file:
        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb, file=file)
        raise e


def get_object_code() -> int:
    with open(script_path / config['SLAVE']['OBJECT_CODE_FILE']) as file:

        return int(file.read())


async def get_object_data(object_code: Union[str, int]) -> Dict:
    file_uri = f"{config['SLAVE']['MASTER_STORE_URL']}{object_code}.json"
    logger.info(f"File URI: {file_uri}")

    async with ClientSession() as session:
        async with session.get(file_uri) as response:
            try:
                return await response.json()
            except ContentTypeError as exception:
                logger.exception('Bad response. Server says:')
                logger.error(await response.text())
                raise exception


def save_object_data(object_data: Dict) -> None:
    if config['SLAVE'].get('OBJECT_DATA_FILE'):
        data_file_name = script_path / config['SLAVE']['OBJECT_DATA_FILE']
    else:
        data_file_name = script_path / f"{object_data['index']}.json"

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
