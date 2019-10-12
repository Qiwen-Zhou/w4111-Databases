
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()



    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r,seen):
        if self._rows is None:
            self._rows = []
        key_fields=[]
        for key in self._data["key_columns"]:
            key_fields.append(r[key])
        if tuple(key_fields) in seen:
            raise Exception(' Duplicate key in Row = ', r)
        else:
            seen.add(tuple(key_fields))
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            seen=set()
            for r in csv_d_rdr:
                for key in self._data["key_columns"]:
                    if key not in r:
                        raise Exception(' Key column not in table.')
                self._add_row(r,seen)

        self._data["table_columns"] = list(self._rows[0].keys())

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")


    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    @staticmethod
    def get_fields(row, field_list):

        if  field_list is None:
            return row
        result = {}
        for f in field_list:
            result[f] = row[f]
        return result

    def _validate_template_and_fields(self,tmp,fields):
        c_set=set(self._data["table_columns"])
        if tmp is not None:
            t_set=set(tmp.keys())
        else:
            t_set=None
        if fields is not None:
            f_set=set(fields)
        else:
            f_set=None
        if f_set is not None and not f_set.issubset(c_set):
            raise Exception("Fields are invalid.")
        if t_set is not None and not t_set.issubset(c_set):
            raise Exception("Template are invalid.")

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        key_cols = self._data['key_columns']
        tmp = dict(zip(key_cols, key_fields))
        res = self.find_by_template(template = tmp, field_list=field_list)

        if res and len(res) > 0:
            res = res[0]
        else:
            res = None
        return res


    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """

        res = []
        for row in self._rows:
            if self.matches_template(row, template):
                res.append(self.get_fields(row, field_list))

        return res


    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        key_cols = self._data['key_columns']
        tmp = dict(zip(key_cols, key_fields))
        return self.delete_by_template(tmp)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        num = 0
        for row in self._rows:
            if self.matches_template(row, template):
                num += 1
                self._rows.remove(row)
        return num

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        key_columns = self._data['key_columns']
        tmp = dict(zip(key_columns,key_fields))
        return self.update_by_template(tmp,new_values)


    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        num = 0
        for i in range(len(self._rows)):
            if self.matches_template(self._rows[i], template):
                change_key_val=False
                tmp_row={}
                for key in self._rows[i]:
                    tmp_row[key] = self._rows[i][key]
                for key in new_values:
                    if key in self._data["key_columns"]:
                        change_key_val=True
                    tmp_row[key] = new_values[key]
                if change_key_val:
                    key_fields=[]
                    for key in self._data["key_columns"]:
                        key_fields.append(tmp_row[key])
                    rows = self.find_by_primary_key(key_fields)
                    if rows:
                        raise Exception('Duplicate key in row = ', rows)

                self._rows[i]=tmp_row
                num += 1
        return num


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        tmp = {}
        for key in self._data["key_columns"]:
            tmp[key]=new_record[key]
        rows = self.find_by_template(tmp)

        if rows and len(rows) > 0:
            raise Exception('Duplicate key in row = ', rows)
        else:
            self._rows.append(new_record)


    def get_rows(self):
        return self._rows

