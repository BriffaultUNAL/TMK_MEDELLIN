#!/usr/bin/python

import os
import yaml
import logging

from telegram import Bot

proyect_dir = os.path.dirname(os.path.abspath(__file__))
yml_credentials_dir = os.path.join(
    proyect_dir, '..', 'config', 'credentials.yml')

logging.basicConfig(
    level=logging.INFO,
    filename=(os.path.join(proyect_dir, '..', 'log', 'logs_main.log')),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)

with open(yml_credentials_dir, 'r') as f:

    try:
        config = yaml.safe_load(f)
        source3 = config['source3']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)


class TelegramBot:
    def __init__(self, token: str, chat_id: int):
        self.token = token
        self.chat_id = chat_id
        self.bot = Bot(token)

    async def message(self, mensaje: str):
        try:
            await self.bot.send_message(self.chat_id, mensaje)
        except Exception as e_2:
            logging.error(str(e_2), exc_info=True)


async def enviar_mensaje(message: str):
    bot = TelegramBot(**source3)
    await bot.message(message)
