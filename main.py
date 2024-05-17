"""
Tool to convert txt or csv files into sql lite
"""
import os
import glob
import sqlite3
import time
import traceback
from datetime import datetime
import argparse

NUM_TXT_MAP = {
    "0": "zero",
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine"
}


def file_importer(folder:str, file_type: str = None, db_name: str = None ):

    if db_name is None:
        db_name = folder.split("/")[-1]
    files = find_files(folder, file_type)
    if not files:
        if file_type is None:
            e_txt = f"No files in {folder}"
        else:
            e_txt = f"No files with ending.{file_type} in {folder}"
        raise FileNotFoundError(e_txt)

    init_db(db_name)
    for file in files:
        # if file.split("\\")[-1] != "zoneaccount.txt":
        #     continue
        # print(file.split("\\")[-1])
        file_name = file.split("\\")[-1]
        with open(file, "r") as f:
            data = f.read()
        data = data.split("\n")
        delimiter = find_out_delim(data)
        # split rows by delimiter
        data = [row.split(delimiter) for row in data]
        if data[-1] == [""]:
            data =data[:-1]
        data = [empty_str_to_none(row) for row in data]
        e, table_name = create_table(db_name, file_name, data)
        if table_name:
            print(f"table {table_name} created from {file_name}")
        else:
            etype= type(e).__name__
            print(f"conversion of {file_name} errored with {etype}: -> {e}")


def find_files(folder, file_type: str = None):
    if file_type is not None:
        return [f for f in glob.glob(f"{folder}/*.{file_type}")]
    return [f for f in glob.glob(f"{folder}/*")]


def find_out_delim(rows):
    delims = [",", ";", "\t", "|"]

    rows_to_compare = 100 if len(rows) > 101 else len(rows) - 1
    for delim in delims:
        sub_frame = rows[0: rows_to_compare]
        attr_count  = 1
        for idx, row in enumerate(sub_frame):
            row = row.split(delim)
            if idx == 0:
                attr_count = len(row)
            else:
                if len(row) != attr_count:
                    break

        if attr_count > 1:
            return delim

    raise Exception("Could not identify the delimiter")


def create_table(db_name, file_name, data):

    status = False
    err = None

    table_name = file_name.split(".")[0]
    if table_name[0].isnumeric():
        table_name= f"{NUM_TXT_MAP[table_name[0]]}_{table_name[1:]}"
    keys = data[0]
    keys_types_map = define_data_types(keys, data, col_search=100)
    if not keys_types_map:
        keys_types_map = {k: "varchar" for k in keys}


    column_def_str = ",".join([f"{k} {v}" for k, v in keys_types_map.items()])
    insert_sql = f"""INSERT INTO {table_name}({','.join(keys)})
                    VALUES({','.join(["?" for x in range(len(keys))])})
    """


    sql = f"""CREATE TABLE {table_name}({column_def_str})"""

    conn = None
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        cursor.execute(sql)
        conn.commit()

        for x in data[1:]:
            params = tuple(x)
            cursor.execute(insert_sql, params)
        conn.commit()
        status = table_name
    except sqlite3.Error as e:
        err = e
        status = False
    finally:
        if conn:
            conn.close()
        return err, status


def empty_str_to_none(row: list):

    for idx, v in enumerate(row):
        if v == '':
            row[idx] = None
    return row


def define_data_types(keys, rows, col_search: int = 10):

    # strip header row
    rel_rows = rows[1:]
    col_search = col_search if len(rel_rows) > col_search else len(rel_rows)
    if col_search == 0:
        return None
    col_type_map = {}

    for idx, key in enumerate(keys):
        try:
            col = [x[idx] for x in rel_rows[0 : col_search]]
        except IndexError as e:
            raise e


        _type = get_type(col)
        col_type_map[key] = _type

    return col_type_map


def is_type_none_wrapper(s, fn):
    if s is None:
        return False
    return fn(s)


def get_type(col: list):
    for x in col:
        t = find_value(x)
        if t:
            return t
    return "varchar"


def find_value(s):
    if is_type_none_wrapper(s, is_int):
        return "int"
    if is_type_none_wrapper(s, is_float):
        return "real"
    if is_type_none_wrapper(s, is_date):
        return "date"
    return False


def is_int(s) -> bool:
    if "." in s:
        return False
    try:
        tmp = int(s)
        return True
    except ValueError:
        return False


def is_float(s) -> bool:
    if not "." in s:
        return False
    try:
        tmp = float(s)
        return True
    except ValueError:
        return False


def is_date(s, date_format='%Y-%m-%d'):
    try:
        tmp = datetime.strptime(s, date_format)
        return True
    except ValueError:
        return False


def to_bool(s):
    if s.lower() in ['true', 'false']:
        return True
    return False


def init_db(filename):
    """ create a database connection to an SQLite database """

    if os.path.exists(filename):
        os.remove(filename)

    conn = None
    try:
        conn = sqlite3.connect(filename)
        print(sqlite3.sqlite_version)
    except sqlite3.Error as e:
        print(e)
        print(traceback.format_exc())
    finally:
        if conn:
            conn.close()


def analyse(query, db):

    conn = None
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()

        cursor.execute(query)

        headers = [desc[0] for desc in cursor.description]
        # print(headers)
        res = [{key: x[i] for i, key in enumerate(headers)} for x in cursor.fetchall()]
        for i, v in enumerate(res):
            print(f"{i+1} | {v}")

    except sqlite3.Error as e:
        print(e)
        print(traceback.format_exc())
    finally:
        if conn:
            conn.close()






if __name__ == '__main__':


    file_importer(FOLDER1, db_name="one")
    file_importer(FOLDER2, db_name= "two")










