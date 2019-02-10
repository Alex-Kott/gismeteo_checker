import json
from json.decoder import JSONDecodeError
import asyncio
import logging
from asyncio import sleep
from configparser import ConfigParser
from typing import Tuple, Union
from datetime import datetime

import pandas as pd

from aiohttp import ClientSession

config = ConfigParser(comment_prefixes='#')
config.read('config.ini')

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=config['FILES']['LOGS'])
logger = logging.getLogger('gismeteo_checker')
logger.setLevel(logging.INFO)


def parse_input_file() -> pd.DataFrame:
    df = pd.read_excel(config['FILES']['AZS_COORDINATES'])
    df = df.dropna(subset=['Координаты СШ', 'Координаты ВД'])

    return df.set_index('№ АЗС:')


def interpret_precipitation_type(type_code: int) -> str:
    precipitation_types = {
        0: 'Нет осадков',
        1: 'Дождь',
        2: 'Снег',
        3: 'Смешанные осадки',
    }
    return precipitation_types[type_code]


def interpret_precipitation_intensity(intensity_code: int) -> str:
    precipitation_intensity = {
        0: '-',
        1: 'Небольшой дождь / снег',
        2: 'Дождь / снег',
        3: 'Сильный дождь / снег'
    }

    return precipitation_intensity[intensity_code]


async def request_gismeteo(latitude: float, longitude: float):
    api_url = f'https://api.gismeteo.net/v2/weather/current/'
    params = {
        'latitude': str(latitude),
        'longitude': str(longitude)
    }
    headers = {
        'X-Gismeteo-Token': config['GISMETEO']['TOKEN'],
        'Accept-Encoding': 'deflate,gzip'
    }
    async with ClientSession() as session:
        async with session.get(api_url, params=params, headers=headers) as response:
            data = await response.json()

            return data['response']


def save_weather_status(index, temperature, precipitation_type, precipitation_intensity):
    try:
        with open(config['FILES']['WEATHER_RESULT']) as file:
            json_data = json.load(file)
    except FileNotFoundError:
        json_data = []
    except JSONDecodeError:
        json_data = []

    json_data.append({
        'azs_index': index,
        'Дата': datetime.now().isoformat(),
        'Температура': temperature,
        'Тип осадков': precipitation_type,
        'Интенсивность': precipitation_intensity
    })

    try:
        with open(config['FILES']['WEATHER_RESULT'], 'w') as file:
            json.dump(json_data, file)
            logger.info(f'Сохранили данные об АЗС №{index}')
    except Exception as e:
        logger.exception(e)
        logger.info(f'Не удалось сохранить данные об АЗС №{index}')


async def get_current_weather(latitude: float, longitude: float) -> Tuple[Union[int, float], str, str]:
    data = await request_gismeteo(latitude, longitude)

    temperature = data['temperature']['air']['C']
    precipitation_type = interpret_precipitation_type(data['precipitation']['type'])
    precipitation_intensity = interpret_precipitation_intensity(data['precipitation']['intensity'])

    return temperature, precipitation_type, precipitation_intensity


async def main():
    logger.info('Запуск...')
    azs_coordinates = parse_input_file()
    logger.info('Файл с координатами загружен')

    logger.info('Начинаю обработку...')
    for index, row in azs_coordinates.iterrows():
        await sleep(0.3)
        logger.info(f'Запрос информации об АЗС №{index}')
        tempretature, \
        precipitation_type, \
        precipitation_intensity = await get_current_weather(latitude=row['Координаты СШ'],
                                                            longitude=row['Координаты ВД'])

        logger.info(f'Получена информация об АЗС №{index}. Температура: {tempretature}, тип осадков: {precipitation_type}, интенсивность: {precipitation_intensity} ')

        save_weather_status(index, tempretature, precipitation_type, precipitation_intensity)

    logger.info('Все данные получены')


if __name__ == "__main__":
    logger = logging.getLogger('logger')
    logger.setLevel('INFO')
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(main())
    event_loop.close()
