from src.RDBDataTable import RDBDataTable

import json

def load_good():
    connect_info = {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    file = "db_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_load_good" + " ********************")
        f.write("\nTable is batting and key_columns are ['playerID','teamID', 'yearID', 'stint']")
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    try:
        rdb_tbl = RDBDataTable("batting", connect_info,key_columns=key_cols)
        with open(file, 'a+') as f:
            f.write("\nLoaded table = \n")
            f.write(str(rdb_tbl))
            f.write("\nThis is the correct answer")

    except Exception as de:
        with open(file,'a+') as f:
            f.write("\nLoad failed. Exception = "+ str(de))
            f.write("\nThis is the wrong answer.")
    with open(file,'a+') as f:
        f.write("\n******************** " + "end_test_load_good" + " ********************")


def load_fail1():
    connect_info = {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_load_fail1" + " ********************")
        f.write("\nTable is batting and key_columns is ['stint']")

    key_cols = ['stint']
    try:
        rdb_tbl = RDBDataTable("batting", connect_info,key_columns=key_cols)
        with open(file, 'a+') as f:
            f.write("\nLoaded table = \n"+str(rdb_tbl))
            f.write("\nThis is the wrong answer")

    except Exception as de:
        with open(file, 'a+') as f:
            f.write("\nLoad failed. Exception = "+str(de))
            f.write("\nThis is the correct answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_load_fail1" + " ********************")


def load_fail2():
    connect_info = {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_load_fail2" + " ********************")
        f.write("\nTable is batting and key_columns is ['batting']")
    key_cols = ['batting']
    try:
        rdb_tbl = RDBDataTable("batting", connect_info,key_columns=key_cols)
        with open(file, 'a+') as f:
            f.write("\nLoaded table = \n"+str(rdb_tbl))
            f.write("\nThis is the wrong answer")

    except Exception as de:
        with open(file, 'a+') as f:
            f.write("\nLoad failed. Exception = "+str(de))
            f.write("\nThis is the correct answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_load_fail2" + " ********************")


def t_find_by_pk():

    connect_info = {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db":"lahman2019raw"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    key_vals = ['willite01', 'BOS', '1960', '1']
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_find_by_primary_key" + " ********************")
        f.write("\nThe key columns of the table are "+str(key_cols))
        f.write("\nThe values for the key_columns to use to find a record are "+str(key_vals))
        f.write("\nThe subset of the fields of the record to return is "+str(fields))
    try:
        rdb_tbl = RDBDataTable("batting", connect_info, key_columns=key_cols)
        res=rdb_tbl.find_by_primary_key(key_vals,fields)
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
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db":"lahman2019raw"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    tmp={'teamID': 'BOS', 'yearID': '1960'}
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_find_by_template" + " ********************")
        f.write("\nThe template to use to find a record is " + str(tmp))
        f.write("\nThe subset of the fields of the record to return is " + str(fields))
    try:
        rdb_tbl = RDBDataTable("batting", connect_info, key_columns=key_cols)
        res=rdb_tbl.find_by_template(tmp, fields)
        with open(file, 'a+') as f:
            f.write("\nQuery result ='\n'"+str(json.dumps(res, indent=3)))
            f.write("\nThis is the right answer.")
    except Exception as err:
        with open(file, 'a+') as f:
            f.write("\nFind failed. Exception = "+str(err))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_find_by_template" + " ********************")


def t_insert_good():

    connect_info = {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    new_record = {'playerID': 'aardsda01', 'yearID': '1999', 'teamID': 'ATL', 'stint': '0'}
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_insert_good" + " ********************")
    try:
        rdb_tbl = RDBDataTable("batting", connect_info, key_columns=key_cols)
        key = ['aardsda01','ATL','1999','0']
        with open(file, 'a+') as f:
            f.write("\nLooking up with key = "+str(key))
        r1 = rdb_tbl.find_by_primary_key(key)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'"+str(json.dumps(r1, indent=3)))
            f.write("\nAttempt to insert good row = "+str(json.dumps(new_record, indent=2)))
        rdb_tbl.insert(new_record)
        res = rdb_tbl.find_by_primary_key(key)
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
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    new_record = {'playerID': 'aardsda01', 'yearID': '1999', 'teamID': 'ATL', 'stint': '0'}
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_insert_fail" + " ********************")
    try:
        rdb_tbl = RDBDataTable("batting", connect_info, key_columns=key_cols)
        key = ['aardsda01', 'ATL', '1999', '0']
        with open(file, 'a+') as f:
            f.write("\nLooking up with key = " + str(key))
        r1 = rdb_tbl.find_by_primary_key(key)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'" + str(json.dumps(r1, indent=3)))
            f.write("\nAttempt to insert bad row = " + str(json.dumps(new_record, indent=2)))
        rdb_tbl.insert(new_record)
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
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    key_fields = ['aardsda01', 'ATL', '1999', '0']
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_delete_by_key" + " ********************")
    try:
        rdb_tbl = RDBDataTable("batting", connect_info, key_columns=key_cols)
        with open(file, 'a+') as f:
            f.write("\nLooking up with key = "+str(key_fields))
        r1 = rdb_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'"+ str(json.dumps(r1, indent=3)))
        r2 = rdb_tbl.delete_by_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nDelete returned "+ str(r2))
        r3 = rdb_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nQuery result after delete = "+ str(json.dumps(r3, indent=2)))
            f.write("\nThis is the correct answer.")
    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nDelete failed. Exception = "+str( e))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end test_delete_by_key" + " ********************")


def t_delete_by_template():
    connect_info = {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    tmp = {'playerID': 'aardsda01', 'yearID': '1997'}
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_delete_by_template" + " ********************")
    try:
        rdb_tbl = RDBDataTable("batting", connect_info, key_columns=key_cols)
        with open(file, 'a+') as f:
            f.write("\nLooking up with template = "+str(tmp))
        r1 = rdb_tbl.find_by_template(tmp)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'"+str (json.dumps(r1, indent=3)))
        r2 = rdb_tbl.delete_by_template(tmp)
        with open(file, 'a+') as f:
            f.write("\nDelete returned "+str (r2))
        r3 = rdb_tbl.find_by_template(tmp)
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
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_update_by_key_good" + " ********************")
    try:
        rdb_tbl = RDBDataTable("batting", connect_info, key_columns=key_cols)
        key_fields = ['aardsda01', 'ATL', '1998', '0']
        with open(file, 'a+') as f:
            f.write("\nLooking up with key = "+str(key_fields))
        r1 = rdb_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'" +str(json.dumps(r1, indent=3)))
        new_values = {"lgID": 'AL'}
        with open(file, 'a+') as f:
            f.write("\nAttempt to update this row with new values = "+str(json.dumps(new_values, indent=3)))
        r2 = rdb_tbl.update_by_key(key_fields,new_values)
        with open(file, 'a+') as f:
            f.write("\nUpdate returned "+str(r2))
        r3 = rdb_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nQuery result after update='\n'"+str(json.dumps(r3, indent=3)))
            f.write("\nThis is the correct answer.")
    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nUpdate failed. Exception = "+str(e))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_update_by_key_good" + " ********************")


def t_update_by_key_fail():
    connect_info = {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_update_by_key_fail" + " ********************")
    try:
        rdb_tbl = RDBDataTable("batting", connect_info, key_columns=key_cols)
        key_fields = ['aardsda01', 'ATL', '1998', '0']
        with open(file, 'a+') as f:
            f.write("\nLooking up with key = "+str(key_fields))
        r1 = rdb_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'"+str(json.dumps(r1, indent=3)))
        new_values = {'yearID': '2015','stint':'1'}
        with open(file, 'a+') as f:
            f.write("\nAttempt to update this row with bad new values = "+str(json.dumps(new_values, indent=3)))
        r2 = rdb_tbl.update_by_key(key_fields,new_values)
        with open(file, 'a+') as f:
            f.write("\nUpdate returned "+str(r2))
        r3 = rdb_tbl.find_by_primary_key(key_fields)
        with open(file, 'a+') as f:
            f.write("\nQuery result after update='\n'"+str(json.dumps(r3, indent=3)))
            f.write("\nThis is the wrong answer")
    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nUpdate failed. Exception = "+str(e))
            f.write("\nThis is the correct answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_update_by_key_fail" + " ********************")


def t_update_by_template_good():

    connect_info = {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_update_by_tamplate_good" + " ********************")
    try:
        rdb_tbl = RDBDataTable("batting", connect_info, key_columns=key_cols)
        tmp = {'playerID': 'aardsda01', 'yearID':'1998', 'stint':'0', 'teamID':'ATL'}
        with open(file, 'a+') as f:
            f.write("\nLooking up with template = "+ str(tmp))
        r1 = rdb_tbl.find_by_template(tmp)
        new_values = {'AB': '1'}
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'"+ str(json.dumps(r1, indent=3)))
            f.write("\nAttempt to update this row with new values = " + str(json.dumps(new_values, indent=3)))
        r2 = rdb_tbl.update_by_template(tmp, new_values)
        with open(file, 'a+') as f:
            f.write("\nUpdate returned "+str(r2))
        r3 = rdb_tbl.find_by_template(tmp)
        with open(file, 'a+') as f:
            f.write("\nQuery result after update='\n'"+str(json.dumps(r3, indent=3)))
            f.write("\nThis is the correct answer")

    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nUpdate failed. Exception = "+str(e))
            f.write("\nThis is the wrong answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_update_by_tamplate_good" + " ********************")


def t_update_by_template_fail():
    connect_info = {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }
    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    file = "rdb_table_test.txt"
    with open(file, 'a+') as f:
        f.write("\n\n******************** " + "test_update_by_tamplate_fail" + " ********************")
    try:
        rdb_tbl = RDBDataTable("batting", connect_info, key_columns=key_cols)
        tmp = {'playerID': 'aardsda01', 'yearID':'2015', 'stint':'0', 'teamID':'ATL'}
        with open(file, 'a+') as f:
            f.write("\nLooking up with template = "+str(tmp))
        r1 = rdb_tbl.find_by_template(tmp)
        new_values = {'stint':'1'}
        with open(file, 'a+') as f:
            f.write("\nReturned row = '\n'"+ str(json.dumps(r1, indent=3)))
            f.write("\nAttempt to update this row with bad new values = "+str(json.dumps(new_values, indent=3)))
        r2 = rdb_tbl.update_by_template(tmp, new_values)
        with open(file, 'a+') as f:
            f.write("\nUpdate returned"+str( r2))
        r3 = rdb_tbl.find_by_template(tmp)
        with open(file, 'a+') as f:
            f.write("\nQuery result after update='\n'"+str(json.dumps(r3, indent=3)))
            f.write("\nThis is the wrong answer")

    except Exception as e:
        with open(file, 'a+') as f:
            f.write("\nUpdate failed. Exception = "+str(e))
            f.write("\nThis is the correct answer.")
    with open(file, 'a+') as f:
        f.write("\n******************** " + "end_test_update_by_tamplate_fail" + " ********************")




load_good()
#load_fail1()
#load_fail2()
#t_find_by_pk()
#t_find_by_template()
#t_insert_good()
#t_insert_fail()
#t_delete_by_key()
#t_delete_by_template()
#t_update_by_key_good()
#t_update_by_key_fail()
#t_update_by_template_good()
#t_update_by_template_fail()
