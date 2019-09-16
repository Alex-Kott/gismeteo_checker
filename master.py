import json
import sys
from json.decoder import JSONDecodeError
import asyncio
import logging
from asyncio import sleep
from configparser import ConfigParser
from pathlib import Path
from typing import Tuple, Union
from datetime import datetime

import pandas as pd

from aiohttp import ClientSession

exec_path = Path(sys.argv[0])
script_path = exec_path.parent

config = ConfigParser(comment_prefixes='#')
config.read(script_path / 'config.ini')

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=script_path / config['GENERAL']['LOG_FILE'])
logger = logging.getLogger('gismeteo_checker')
logger.setLevel(logging.INFO)


def parse_input_file() -> pd.DataFrame:
    df = pd.read_excel(script_path / config['MASTER']['AZS_COORDINATES_FILE'])
    df = df.dropna(subset=['Координаты СШ', 'Координаты ВД'])

    return df.set_index('№ АЗС:')


def interpret_precipitation_type(type_code: int) -> str:
    precipitation_types = {
        0: 'No precipitation',
        1: 'Rain',
        2: 'Snow',
        3: 'Mixed precipitation',
    }
    return precipitation_types[type_code]


def interpret_precipitation_intensity(intensity_code: int) -> str:
    precipitation_intensity = {
        0: '-',
        1: 'Light rain / snow',
        2: 'Rain / snow',
        3: 'Heavy rain / snow'
    }

    return precipitation_intensity[intensity_code]


async def request_gismeteo(latitude: float, longitude: float):
    api_url = f'https://api.gismeteo.net/v2/weather/current/'
    params = {
        'latitude': str(latitude),
        'longitude': str(longitude)
    }
    headers = {
        'X-Gismeteo-Token': config['MASTER']['GISMETEO_TOKEN'],
        'Accept-Encoding': 'deflate,gzip'
    }
    async with ClientSession() as session:
        async with session.get(api_url, params=params, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                return data['response']
            else:
                connection_status = f"Response HTTP code: {response.status}"
                logger.error(connection_status)
                raise Exception(connection_status)


def save_weather_status(index: int, temperature: Union[int, float],
                        precipitation_type: str, precipitation_intensity: str):
    try:
        with open(config['MASTER']['WEATHER_RESULT_FILE']) as file:
            json_data = json.load(file)
    except FileNotFoundError:
        json_data = {}
    except JSONDecodeError:
        json_data = {}

    json_data[index] = {
        'index': index,
        'date': datetime.now().isoformat(),
        'temperature': temperature,
        'precipitation type': precipitation_type,
        'precipitation intensity': precipitation_intensity
    }

    try:
        with open(config['MASTER']['WEATHER_RESULT_FILE'], 'w') as file:
            json.dump(json_data, file)

        store_path = Path(config['MASTER']['STORE'])

        if not store_path.exists():
            store_path.mkdir()

        file_path = store_path / f'{index}.json'
        with open(script_path / file_path, 'w') as file:
            json.dump(json_data[index], file)

        logger.info(f'Gas station №{index} data saved')
    except Exception as e:
        logger.exception(e)
        logger.info(f'Gas station №{index} data saving canceled')


async def get_current_weather(latitude: float, longitude: float) -> Tuple[Union[int, float], str, str]:
    data = await request_gismeteo(latitude, longitude)

    temperature = data['temperature']['air']['C']
    precipitation_type = interpret_precipitation_type(data['precipitation']['type'])
    precipitation_intensity = interpret_precipitation_intensity(data['precipitation']['intensity'])

    return temperature, precipitation_type, precipitation_intensity


async def main():
    logger.info('Start...')
    azs_coordinates = parse_input_file()
    logger.info('Coordinates file loaded')

    logger.info('Start processing...')
    for index, row in azs_coordinates.iterrows():
        await sleep(0.3)
        logger.info(f'Request info for gas station №{index}')
        temperature, \
        precipitation_type, \
        precipitation_intensity = await get_current_weather(latitude=row['Координаты СШ'],
                                                            longitude=row['Координаты ВД'])

        logger.info(f'Gas station №{index}. Temperature: {temperature}, '
                    f'precipitation type: {precipitation_type}, intensity: {precipitation_intensity} ')

        save_weather_status(index, temperature, precipitation_type, precipitation_intensity)

    logger.info('Completed')


if __name__ == "__main__":
    logger = logging.getLogger('logger')
    logger.setLevel('INFO')
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(main())
    event_loop.close()
