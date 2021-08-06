from hdbcli import dbapi
import numpy as np
import itertools
import datetime
import logging
import yaml
import re

def connection_setup(address, port, user, password):
    connection = dbapi.connect(address = address,
                               port = port,
                               user = user,
                               password = password)

    if connection.isconnected() is True:
        logging.info("### Connection Established. ###")
        return(connection)

def fire_query(connection, query):
    cursor = connection.cursor()
    result = cursor.execute(query)
    return(result, cursor)

def get_rows(cursor):
    column_list = []
    for index, column in enumerate(cursor.description):
        column_list.append([column[0], -1])
    return(column_list)

def gen_where_clause(columns, values):
    condition_clauses = []
    for column, value in zip(columns, values):
        if value is None:
            condition = f"{column} IS NULL"
        elif type(value) is int or type(value) is float:
            condition = f"{column} = {value}"
        elif type(value) is tuple:
            condition = f"{column} >= {value[0]} AND {column} < {value[1]}"
        else:
            condition = f"{column} = \'{value}\'"
        condition_clauses.append(condition)

    condition_string = " AND ".join(condition_clauses)
    return(condition_string)

def fstr(template, CONDITION_SEQUENCE):
    return eval(f"f'''{template}'''")

def gen_date_list(start_delta, no_of_days):
    date_list = []
    start_date = datetime.datetime.today() + datetime.timedelta(days = start_delta)
    for x in range(no_of_days):
        day = start_date + datetime.timedelta(days = x)
        date_list.append(day.strftime("%Y-%m-%d"))
    return(date_list)

def gen_range_list(type, mn, start, end, mx, splits):
    raw_list = []
    if type == "LINE":
        raw_list = np.linspace(start, end, splits).tolist()
    elif type == "GEOM":
        raw_list = np.geomspace(start, end, splits).tolist()
    range_list = list(zip(raw_list, raw_list[1:]))
    range_list.insert(0, (mn, start))
    range_list.append((end, mx))
    range_list.append(None)
    return(range_list)

def processed(string_text, dictionary):
    replace = lambda match : dictionary.get(match.group("key"), match.group(0))
    replaced_string = re.sub('\$(?P<key>[a-zA-Z_]+)', replace, string_text)
    return(replaced_string)
