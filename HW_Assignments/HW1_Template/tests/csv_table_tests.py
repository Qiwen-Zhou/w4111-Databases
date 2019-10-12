from src.CSVDataTable import CSVDataTable
import logging
import os
import time
import json


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.ERROR)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load_good():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_load_good" + " ********************")
        f.write("\nTable is "+connect_info["file_name"] +" and key_columns are ['playerID','teamID', 'yearID', 'stint']")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=['playerID','teamID', 'yearID', 'stint'])
        with open(file, 'a+') as f:
            f.write("\nLoaded table = \n"+str(csv_tbl))
            f.write("\nThis is the correct answer.")

    except Exception as de:
        with open(file, 'a+') as f:
            f.write("\nLoad failed. Exception = "+str(de))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end test_load_good" + " ********************")


def t_load_fail1():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_load_fail1" + " ********************")
        f.write("\nTable is " + connect_info["file_name"] + " and key_columns are ['stint']")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=['stint'])
        with open(file, 'a+') as f:
            f.write("\nLoaded table = \n"+str(csv_tbl))
            f.write("\nThis is the wrong answer")

    except Exception as de:
        with open(file, 'a+') as f:
            f.write("\nLoad failed. Exception = "+str(de))
            f.write("\nThis is the correct answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end test_load_fail1" + " ********************")


def t_load_fail2():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_load_fail2" + " ********************")
        f.write("\nTable is "+connect_info["file_name"] +" and key_columns are ['orderNumber', 'cat']")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=['orderNumber', 'cat'])
        with open(file, 'a+') as f:
            f.write("\nLoaded table = \n"+str(csv_tbl))
            f.write("\nThis is the wrong answer")

    except Exception as de:
        with open(file, 'a+') as f:
            f.write("\nLoad failed. Exception = "+str(de))
            f.write("\nThis is the correct answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end test_load_fail2" + " ********************")



def t_find_by_pk():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    key_vals = ['willite01', 'BOS', '1960', '1']
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_find_by_primary_key" + " ********************")
        f.write("\nThe key columns of the table are " + str(key_cols))
        f.write("\nThe values for the key_columns to use to find a record are " + str(key_vals))
        f.write("\nThe subset of the fields of the record to return is " + str(fields))
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
        res=csv_tbl.find_by_primary_key(key_vals,fields)
        with open(file, 'a+') as f:
            f.write("\nQuery result ='\n'"+str(json.dumps(res, indent=3)))
            f.write("\nThis is the right answer.")
    except Exception as err:
        with open(file, 'a+') as f:
            f.write("\nFind failed. Exception = "+str(err))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_find_by_primary_key" + " ********************")

def t_find_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    tmp={'teamID': 'BOS', 'yearID': '1960'}
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_find_by_template" + " ********************")
        f.write("\nThe template to use to find a record is " + str(tmp))
        f.write("\nThe subset of the fields of the record to return is " + str(fields))
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
        res=csv_tbl.find_by_template(template=tmp, field_list=fields)
        with open(file, 'a+') as f:
            f.write("\nQuery result ='\n'"+str(json.dumps(res, indent=3)))
            f.write("\nThis is the right answer.")
    except Exception as err:
        with open(file, 'a+') as f:
            f.write("\nFind failed. Exception = " + str(err))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_find_by_template" + " ********************")


def t_insert_good():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    new_record = {'playerID': 'aardsda01', 'yearID': '1999', 'teamID': 'ATL', 'stint': '0'}
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_insert_good" + " ********************")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
        key = ['aardsda01', 'ATL', '1999', '0']
        with open(file, 'a+') as f:
            f.write("\nLooking up with key = " + str(key))
        r1 = csv_tbl.find_by_template(new_record)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'"+str(json.dumps(r1, indent=3)))
            f.write("\nAttempt to insert good row = "+str(json.dumps(new_record, indent=2)))
        csv_tbl.insert(new_record)
        res = csv_tbl.find_by_primary_key(key)
        with open(file, 'a+') as f:
            f.write("\nQuery result after insert='\n'"+str(json.dumps(res, indent=3)))
            f.write("\nThis is the right answer.")
    except Exception as err:
        with open(file, 'a+') as f:
            f.write("\nInsert failed. Exception = "+str(err))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_insert_good" + " ********************")


def t_insert_fail():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    new_record = {'playerID': 'aardsda01', 'yearID': '2015', 'teamID': 'ATL', 'stint': '1'}
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_insert_fail" + " ********************")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
        key = ['aardsda01', 'ATL', '2015', '1']
        with open(file, 'a+') as f:
            f.write("\nLooking up with key = " + str(key))
        r1 = csv_tbl.find_by_template(new_record)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'" + str(json.dumps(r1, indent=3)))
            f.write("\nAttempt to insert bad row = " + str(json.dumps(new_record, indent=2)))
        csv_tbl.insert(new_record)
        with open(file, 'a+') as f:
            f.write("\nThis is the wrong answer.")
    except Exception as err:
        with open(file, 'a+') as f:
            f.write("\nInsert failed. Exception = " + str(err))
            f.write("\nThis is the right answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_insert_fail" + " ********************")


def t_delete_by_key():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    key_fields = ['aardsda01', 'ATL', '1999', '0']
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_delete_by_key" + " ********************")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
        new_record = {'playerID': 'aardsda01', 'yearID': '1999', 'teamID': 'ATL', 'stint': '0'}
        csv_tbl.insert(new_record)
        with open(file, 'a+') as f:
            f.write("\nLooking up with key = "+str(key_fields))
        r1 = csv_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'"+ str(json.dumps(r1, indent=3)))
        r2 = csv_tbl.delete_by_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nDelete returned "+ str(r2))
        r3 = csv_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nQuery result after delete = "+ str(json.dumps(r3, indent=2)))
            f.write("\nThis is the correct answer.")
    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nDelete failed. Exception = " + str(e))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end test_delete_by_key" + " ********************")


def t_delete_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    tmp = {'playerID': 'aardsda01', 'yearID': '1999'}
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_delete_by_template" + " ********************")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
        new_record = {'playerID': 'aardsda01', 'yearID': '1999', 'teamID': 'ATL', 'stint': '0'}
        csv_tbl.insert(new_record)
        with open(file, 'a+') as f:
            f.write("\nLooking up with template = "+str(tmp))
        r1 = csv_tbl.find_by_template(tmp)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'"+str (json.dumps(r1, indent=3)))
        r2 = csv_tbl.delete_by_template(tmp)
        with open(file, 'a+') as f:
            f.write("\nDelete returned "+str (r2))
        r3 = csv_tbl.find_by_template(tmp)
        with open(file, 'a+') as f:
            f.write("\nQuery result after delete='\n'" +str(json.dumps(r3, indent=3)))
            f.write("\nThis is the correct answer")
    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nDelete failed. Exception = " +str(e))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end test_delete_by_template" + " ********************")


def t_update_by_key_good():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_update_by_key_good" + " ********************")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
        new_record = {'playerID': 'aardsda01', 'yearID': '1999', 'teamID': 'ATL', 'stint': '0'}
        csv_tbl.insert(new_record)
        key_fields = ['aardsda01', 'ATL', '1999', '0']
        with open(file, 'a+') as f:
            f.write("\nLooking up with key = " + str(key_fields))
        r1 = csv_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'" +str(json.dumps(r1, indent=3)))
        new_values = {"lgID": 'AL'}
        with open(file, 'a+') as f:
            f.write("\nAttempt to update this row with new values = " + str(json.dumps(new_values, indent=3)))
        r2 = csv_tbl.update_by_key(key_fields,new_values)
        with open(file, 'a+') as f:
            f.write("\nUpdate returned "+str(r2))
        r3 = csv_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nQuery result after update='\n'" + str(json.dumps(r3, indent=3)))
            f.write("\nThis is the correct answer.")
    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nUpdate failed. Exception = " + str(e))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_update_by_key_good" + " ********************")


def t_update_by_key_fail():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_update_by_key_fail" + " ********************")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
        key_fields = ['aardsda01', 'ATL', '2015', '1']
        with open(file, 'a+') as f:
            f.write("\nLooking up with key = " + str(key_fields))
        r1 = csv_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'" + str(json.dumps(r1, indent=3)))
        new_values = {'yearID': '2008', 'teamID': 'BOS'}
        with open(file, 'a+') as f:
            f.write("\nAttempt to update this row with bad new values = "+str(json.dumps(new_values, indent=3)))
        r2 = csv_tbl.update_by_key(key_fields,new_values)
        with open(file, 'a+') as f:
            f.write("\nUpdate returned " + str(r2))
        r3 = csv_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nQuery result after update='\n'" + str(json.dumps(r3, indent=3)))
            f.write("\nThis is the wrong answer")
    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nUpdate failed. Exception = " + str(e))
            f.write("\nThis is the correct answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_update_by_key_fail" + " ********************")


def t_update_by_template_good():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_update_by_tamplate_good" + " ********************")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
        new_record = {'playerID': 'aardsda01', 'yearID': '1999', 'teamID': 'ATL', 'stint': '0'}
        csv_tbl.insert(new_record)
        tmp = {'playerID': 'aardsda01', 'yearID':'1999'}
        with open(file, 'a+') as f:
            f.write("\nLooking up with template = " + str(tmp))
        r1 = csv_tbl.find_by_template(tmp)
        new_values = {'AB': '1'}
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'" + str(json.dumps(r1, indent=3)))
            f.write("\nAttempt to update this row with new values = " + str(json.dumps(new_values, indent=3)))
        r2 = csv_tbl.update_by_template(tmp, new_values)
        with open(file, 'a+') as f:
            f.write("\nUpdate returned " + str(r2))
        r3 = csv_tbl.find_by_template(tmp)
        with open(file, 'a+') as f:
            f.write("\nQuery result after update='\n'" + str(json.dumps(r3, indent=3)))
            f.write("\nThis is the correct answer")

    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nUpdate failed. Exception = " + str(e))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_update_by_tamplate_good" + " ********************")

def t_update_by_template_fail():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    file = "csv_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_update_by_tamplate_fail" + " ********************")
    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)
        tmp = {'playerID': 'aardsda01', 'yearID': '2015', 'stint': '1', 'teamID': 'ATL'}

        with open(file, 'a+') as f:
            f.write("\nLooking up with template = " + str(tmp))
        r1 = csv_tbl.find_by_template(tmp)
        new_values = {'yearID': '2008', 'teamID': 'BOS'}
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'" + str(json.dumps(r1, indent=3)))
            f.write("\nAttempt to update this row with bad new values = " + str(json.dumps(new_values, indent=3)))
        r2 = csv_tbl.update_by_template(tmp, new_values)
        with open(file, 'a+') as f:
            f.write("\nUpdate returned" + str(r2))
        r3 = csv_tbl.find_by_template(tmp)
        with open(file, 'a+') as f:
            f.write("\nQuery result after update='\n'"+str(json.dumps(r3, indent=3)))
            f.write("\nThis is the wrong answer")

    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nUpdate failed. Exception = "+str(e))
            f.write("\nThis is the correct answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_update_by_tamplate_fail" + " ********************")




#t_load_good()
#t_load_fail1()
#t_load_fail2()
#t_find_by_pk()
#t_find_by_template()
#t_insert_good()
#t_insert_fail()
#t_delete_by_key()
#t_delete_by_template()
#t_update_by_key_good()
#t_update_by_key_fail()
#t_update_by_template_good()
t_update_by_template_fail()