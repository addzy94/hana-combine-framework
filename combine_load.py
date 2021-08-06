# Imports #
from Crypto.Cipher import AES
import random, string, base64
from helper_library import *
import itertools
import argparse
import datetime
import logging
import yaml
import json
import sys
import re

# Argument Definitions #
parser = argparse.ArgumentParser()

parser.add_argument('--connection', help = 'HANA Connection ID')
parser.add_argument('--yamlFile', help = 'Location of YAML file')
parser.add_argument('--connectionFile', help = 'Location of Connection file')
parser.add_argument('--secret', help = 'Location of Secret')

parser.add_argument('--optional', help = 'Key Value Pairs')

# Argument Parsing #
try:
    args = parser.parse_args()
    optional_argument_dictionary = {}
    if args.optional is not None:
        optional_argument_dictionary = json.loads(args.optional)
    for arg in vars(args):
        if getattr(args, arg) is None and arg != 'optional':
            raise Exception
except Exception:
    logging.exception("Issue with Arguments")
    sys.exit(-1)

CONNECTION = args.connection
CONNECTION_LOCATION = args.connectionFile
SECRET_LOCATION = args.secret
YAML_LOCATION = args.yamlFile

# Logging Configuration #
DATETIME = datetime.datetime.today()
CURRENT_DATE = DATETIME.strftime("%Y-%m-%d")
CURRENT_TIME = DATETIME.time().strftime("%H:%M:%S_%f")
FILE_NAME = "/app/gec/edw/log/hana_combine_load_{}_{}_logFile.log".format(CURRENT_DATE, CURRENT_TIME)
FORMAT_STRING = "%(asctime)s: %(levelname)s: %(funcName)s: Line %(lineno)d:  %(message)s"
logging.basicConfig(filename = FILE_NAME,
                    level = logging.INFO,
                    filemode = "w",
                    format = FORMAT_STRING)

report_format = logging.Formatter(FORMAT_STRING)
report_handler = logging.StreamHandler(sys.stdout)
report_handler.setLevel(logging.INFO)
report_handler.setFormatter(report_format)
logging.getLogger().addHandler(report_handler)

# Reading Connection File #
connections = []

try:
    with open(CONNECTION_LOCATION) as file:
        file_object = yaml.load_all(file, Loader = yaml.FullLoader)
        for doc in file_object:
            step = {}
            for k,v in doc.items():
                v = str(v).replace("\n", " ").strip()
                step[k] = v
            connections.append(step)
except Exception:
    logging.exception("Issue with Reading Connections File")
    sys.exit(-1)

# Getting Secret Key #
KEY = ""
try:
    with open(SECRET_LOCATION) as file:
        KEY = file.readline().replace("\n", "")
except Exception:
    logging.exception("Issue with getting Secret Key")
    sys.exit(-1)

# Checking Connection Information #
address = user = password = ""
port = 0

try:
    connection_present = 0
    for connection in connections:
        if connection["UniqueID"] == CONNECTION:
            connection_present = 1
            address = connection["Address"]
            port = int(connection["Port"])
            user = connection["Username"]

            decryption_suite = AES.new(KEY, AES.MODE_CFB, user.zfill(16)[:16])
            password = decryption_suite.decrypt(base64.b64decode(connection["EncryptedPassword"])).decode("utf-8")
            break
except Exception:
    logging.exception("Issue with Reading Connections file")
    sys.exit(-1)

# Reading Query .YAML File #
TABLE_NAME = ""
instructions_list = COLUMNS = REPEAT_SEQUENCE = []

try:
    with open(YAML_LOCATION) as file:
        file_object = yaml.load_all(file, Loader = yaml.FullLoader)
        for doc in file_object:
            step = {}
            for k,v in doc.items():
                v = processed(v.replace("\n", " ").strip(), optional_argument_dictionary)
                step[k] = re.sub(r"\s+", " ", v)
            instructions_list.append(step)

    TABLE_NAME = instructions_list[0]["Table"]
    REPEAT_SEQUENCE = instructions_list[0]["RepeatSequence"].split(",")
    COLUMNS = [item.split("=")[0].strip() for item in REPEAT_SEQUENCE]
except Exception:
    logging.exception("Issue with Reading .YAML file")
    sys.exit(-1)

# Creating a Connection #
try:
    if connection_present == 1:
        connection = connection_setup(address, port, user, password)
    else:
        logging.error("Connection-ID not present in Connections File")
except Exception:
    logging.exception("Issue While Connecting")
    sys.exit(-1)

# Generating Iterable Column Lists #
try:
    MASTER_LIST = []
    for item_params in REPEAT_SEQUENCE:
        item_array = item_params.strip().split("=")
        item_list = []
        if item_array[1] == "DISTINCT":
            column = item_array[0]
            query = f"SELECT DISTINCT {column} FROM {TABLE_NAME};"
            result, cursor = fire_query(connection, query)
            if result is True:
                distinct_values_list = []
                for row in cursor:
                    distinct_values_list.append(row[0])
                MASTER_LIST.append(distinct_values_list)
        elif item_array[1].strip().split(";")[0] in ("GEOM", "LINE"):
            type = item_array[1].strip().split(";")[0]
            column = item_array[0]
            query = f"SELECT MIN({column}), MAX({column}) FROM {TABLE_NAME};"
            result, cursor = fire_query(connection, query)
            if result is True:
                range_list = []
                for row in cursor:
                    mn, mx = row[0], row[1]
                range_details = item_array[1].strip().split(";")[1:]
                start, end, splits = [int(val) for val in range_details]
                range_list = gen_range_list(type, mn, start, end, mx, splits)
                MASTER_LIST.append(range_list)
        else:
            date_details = item_array[1]
            start, num = [int(val) for val in date_details.strip().split(";")]
            date_list = gen_date_list(start, num)
            MASTER_LIST.append(date_list)
except Exception:
    logging.exception("Issue while getting iterable columns")
    sys.exit(-1)

# Running Query .YAML #
count = 0
try:
    for combo in itertools.product(*MASTER_LIST):
        count = count + 1
        CONDITION_SEQUENCE = gen_where_clause(COLUMNS, combo)
        logging.info(CONDITION_SEQUENCE)
        for item in instructions_list[1:]:
            query = fstr(item["TargetQuery"], CONDITION_SEQUENCE)
            desc = item["Description"]
            result, cursor = fire_query(connection, query)
            logging.info("Result for {}: {}, Updated: {}".format(desc, result, cursor.rowcount))
except Exception:
    logging.exception("Issue with running the queries")
    sys.exit(-1)

logging.info(f"Process Complete! {count}")
