# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
import logging
import os
import json


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=['playerID', 'teamID', 'yearID', 'stint'])

    print("Created table = " + str(csv_tbl))


def t_find_by_template():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols=['playerID', 'teamID', 'yearID', 'stint']
    fields=['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    tmp={'teamID': 'BOS', 'yearID': '1960'}
    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
    res=csv_tbl.find_by_template(template=tmp, field_list=fields)
    print("Query result ='\n'", json.dumps(res, indent=3))


def t_find_by_pk():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    key_vals = ['willite01', 'BOS', '1960', '1']
    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
    res=csv_tbl.find_by_primary_key(key_vals,fields)
    print("Query result ='\n'", json.dumps(res, indent=3))


def t_find_by_primary_key_and_template():

    connect_info={
        "directory":data_dir,
        "file_name":"People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    r = csv_tbl.find_by_primary_key(['willite01'],
                                  field_list=['playerID', 'nameLast', 'throws', 'bats', 'birthCountry'])
    print("Find by key returned ="+str(r))

    t = {"playerID": "willite01"}
    r = csv_tbl.find_by_template(t,
                               field_list = ['playerID', 'nameLast', 'throws', 'bats', 'birthCountry'])
    print("Find by template ="+str(t))


def t_matches():

    r = {
        "playerID": "webstra01",
        "teamID": "BOS",
        "yearID": "1960",
        "AB": "3",
        "H": "0",
        "HR": "0",
        "RBI": "1"
    }
    tmp = {'playerID': 'webstra01'}
    test = CSVDataTable.matches_template(r, tmp)
    print("Matches = ", test)


def t_insert():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
    new_record = {'playerID': 'aaa', 'yearID': '1997', 'teamID': '1997', 'stint': '1'}
    #csv_tbl.insert(new_record)
    #print(csv_tbl.find_by_template({'playerID': 'aaa', 'yearID': '1997'}))

def t_delete_by_key():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    key_fields = ['aaa', '1997', '1997', '1']
    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
    res = csv_tbl.delete_by_key(key_fields)
    print("Query result =", res)


def t_delete_by_template():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
    tmp = {'playerID': 'aaa', 'teamID': '1997'}
    res = csv_tbl.delete_by_template(tmp)
    print("Query result =", res)


def t_update_by_key():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
    key_fields = ['aaa', '1997', '1997', '1']
    new_values = {'yearID' : '2000'}
    res = csv_tbl.update_by_key(key_fields, new_values)
    print("Query result =", res)


def t_update_by_template():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
    tmp = {'playerID': 'aaa', 'teamID': '1997'}
    new_values = {'yearID' : '2005'}
    res = csv_tbl.update_by_template(tmp, new_values)
    print("Query result =", res)
t_load()
#t_find_by_template()
#t_find_by_pk()
