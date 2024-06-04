from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.chrome.options import Options

import time
import logging
import sys
import os
import yaml
import sqlalchemy as sa
from sqlalchemy import text, Engine, Connection, Table, VARCHAR
from pandas.io.sql import SQLTable
from urllib.parse import quote
import pandas as pd
from src.telegram_bot import enviar_mensaje
import re
import asyncio
from datetime import datetime, timedelta
act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

path_to_edgedriver = os.path.join(
    proyect_dir, 'edgedriver', 'msedgedriver.exe')
log_dir = os.path.join(proyect_dir, 'log', 'logs_main.log')
yml_credentials_dir = os.path.join(proyect_dir, 'config', 'credentials.yml')

logging.basicConfig(
    level=logging.INFO,
    filename=(log_dir),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)


def get_engine(username: str, password: str, host: str, database: str, port: str = 3306, *_):
    return sa.create_engine(f"mysql+pymysql://{username}:{quote(password)}@{host}:{port}/{database}")


with open(yml_credentials_dir, 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1 = config['source1']
        source2 = config['source2']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)


def engine_1():
    return get_engine(**source1).connect()


def to_sql_replace(table: SQLTable, con: Engine | Connection, keys: list[str], data_iter):

    satable: Table = table.table
    ckeys = list(map(lambda s: s.replace(' ', '_'), keys))
    data = [dict(zip(ckeys, row)) for row in data_iter]
    values = ', '.join(f':{nm}' for nm in ckeys)
    stmt = f"REPLACE INTO {satable.name} VALUES ({values})"
    con.execute(text(stmt), data)


def webscraping(import_username: str, import_password: str, *_):

    options = Options()
    options = webdriver.EdgeOptions()
    options.use_chromium = True
    options.add_argument('--ignore-certificate-errors')
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("prefs", {
        "download.default_directory": os.path.join(act_dir)
    })

    service = Service(path_to_edgedriver)
    driver = webdriver.Edge(service=service, options=options)

    driver.get("https://dime.claro.com.co/Portal/Produccion/")

    time.sleep(5)

    login = driver.find_element('id', "btiniciar")
    login.click()

    time.sleep(5)

    user = driver.find_element('id', "Usuario")
    password = driver.find_element('id', "Contrase_a")

    user.send_keys(import_username)
    password.send_keys(import_password)

    time.sleep(2)

    login = driver.find_element('id', "btiniciar")
    login.click()

    time.sleep(2)

    driver.get(
        "https://dime.claro.com.co/Portal/Produccion/Ventas/TMK/ConsultarTmkAsignacionSupervisor")

    time.sleep(600)


def filter_characters(texto):
    return re.sub(r'[^a-zA-Z0-9\s]', '', str(texto))


def load():

    try:

        with engine_1() as con:

            df_hik = pd.read_excel(file_path := os.path.join(
                act_dir, 'Tmk_Base_Asignacion_Supervisor.xlsx'), header=0, sheet_name=None)
            df_hik = pd.concat(df_hik.values(), ignore_index=True)
            # df_hik = df_hik.applymap(filter_characters)
            df_hik = df_hik.drop(columns=['Id'])
            df_hik['FechaAsignacion'] = ''

            df_hik = df_hik.rename(columns={'NombreCampaña': 'NombreCampana',
                                            'IdCampaña': 'IdCampana',
                                            'AliadoAsignado': 'aliado',
                                            'Periodo': 'FechaCargue'})
            df_hik['FechaCargue'] = df_hik['FechaCargue'].astype(float)
            df_hik['FechaCargue'] = df_hik['FechaCargue'].map(
                lambda x: f'{x:.1f}')
            print(df_hik.columns)
            print(df_hik)

            asyncio.run(enviar_mensaje('Asignaciones_tmk_medellin'))

            if len(df_hik) == 0:
                asyncio.run(enviar_mensaje('Reporte sin datos'))
                asyncio.run(enviar_mensaje(
                    "____________________________________"))
                sys.exit()
            df_type = {
                col: sa.types.VARCHAR(length=64) for col in df_hik.columns}
            df_hik.to_sql(name='tb_base_venta_hogar',
                          con=con, if_exists='replace', index=False, dtype=df_type)

            count = pd.read_sql_query(
                'SELECT count(*) FROM bbdd_cs_med_tmk.tb_base_venta_hogar;', con)['count(*)'][0]

            asyncio.run(enviar_mensaje(f'{count} datos en la tabla'))
            asyncio.run(enviar_mensaje(f'{len(df_hik)} datos cargados'))
            asyncio.run(enviar_mensaje(
                "____________________________________"))
            os.remove(file_path)

    except KeyError as e:

        logging.error(str(e), exc_info=True)


def load_param():

    try:

        with engine_1() as con:

            df_base = pd.read_sql_query(
                'SELECT distinct NombreCampana FROM bbdd_cs_med_tmk.tb_base_venta_hogar', con)
            logging.info(len(df_base))
            df_param = pd.read_sql_query(
                'SELECT * FROM bbdd_cs_med_tmk.tb_base_parametros', con)
            logging.info(len(df_param))

            """nuevo_valor = 'dum_HOGAR'
            fila_nueva = len(df_base)  # Obtener el índice de la fila nueva
            df_base.loc[fila_nueva, 'NombreCampana'] = nuevo_valor"""

            df_val = ~df_base['NombreCampana'].isin(df_param['Nombre_base'])

            df_val = df_base[df_val]

            if len(df_val) > 0:
                print(df_val)
                asyncio.run(enviar_mensaje(
                    f'{df_val} parametros nuevos Medellin'))
                logging.info(df_val)
                for item in df_val['NombreCampana']:
                    act_date = datetime.now()
                    first_day = act_date.replace(day=1)
                    last_day = first_day.replace(
                        month=first_day.month % 12 + 1, day=1) - timedelta(days=1)
                    params_hogar = ['HOGAR', 'VENTA_CRUZADA',
                                    'VENTA_FIJO_CLIENTE', 'HOG', 'MOVIL_SIN_HOGAR']
                    """insert = False
                    for param in params_hogar:
                        if param in item:
                            con.execute(text(f"INSERT INTO tb_base_parametros_bquilla(Nombre_base, tipo_base, Fecha_carga,
                                                                                        Fecha_Fin) VALUES('{item}', 'HOGAR', '{first_day.strftime(' % Y-%m-%d')}',
                                                                                                          '{last_day.strftime(' % Y-%m-%d')}')"))
                            # enviar_mensaje(f'Nuevo parametro TMK: {item}, HOGAR')
                            insert = True
                            break
                    if insert:
                        con.execute(text(f"INSERT tb_base_parametros_bquilla(Nombre_base, tipo_base, Fecha_carga,
                                                                               Fecha_Fin) VALUES('{item}', 'MOVIL', '{first_day.strftime(' % Y-%m-%d')}',
                                                                                                 '{last_day.strftime(' % Y-%m-%d')}')"))"""
                    # enviar_mensaje(f'Nuevo parametro TMK: {item}, MOVIL')

                    print(first_day.strftime('%Y-%m-%d'),
                          last_day.strftime('%Y-%m-%d'))

    except KeyError as e:

        logging.error(str(e), exc_info=True)
