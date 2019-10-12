from src.BaseDataTable import BaseDataTable
import pymysql
import json
import logging
import pandas as pd
logger = logging.getLogger()


class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
        }


        self.db_cnx = pymysql.connect(host='localhost',
                        user=self._data["connect_info"]["user"],
                        password=self._data["connect_info"]["password"],
                        db=self._data["connect_info"]["db"],
                        charset='utf8mb4',
                        cursorclass=pymysql.cursors.DictCursor)


        self.load()

        self._logger = logging.getLogger()
        self._logger.debug("RDBDataTable.__init__: data = " + json.dumps(self._data, indent=2))


    def __str__(self):

        result = "RDBDataTable:\n"
        result += json.dumps(self._data, indent=2)

        row_count = self.get_row_count()
        result += "\nNumber of rows = " + str(row_count)

        some_rows = pd.read_sql(
            "select * from " + self._data["table_name"] + " limit 10",
            con=self.db_cnx
        )
        result += "First 10 rows = \n"
        result += str(some_rows)

        return result

    def get_row_count(self):


        sql = "select count(*) as count from " + self._data["table_name"]
        res, d = self.run_q(sql, args=None, fetch=True, conn=self.db_cnx, commit=True)
        row_count = d[0]['count']

        return row_count


    def load(self):
        sql1 = ' alter table ' + self._data["table_name"] + ' drop primary key;'
        sql2 = 'alter table batting add primary key '+ '(' + ','.join(self._data["key_columns"]) + ')'

        #print(sql1,sql2)
        cur = self.db_cnx.cursor()
        cur.execute(sql1)
        cur.execute(sql2)

    def run_q(self, sql, args=None, fetch=True, cur=None, conn=None, commit=True):
        '''
        Helper function to run an SQL statement.

        :param sql: SQL template with placeholders for parameters.
        :param args: Values to pass with statement.
        :param fetch: Execute a fetch and return data.
        :param conn: The database connection to use. The function will use the default if None.
        :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
        :param commit: This is wizard stuff. Do not worry about it.

        :return: A tuple of the form (execute response, fetched data)
        '''

        cursor_created = False
        connection_created = False

        try:

            if conn is None:
                connection_created = True
                conn = self.db_cnx

            if cur is None:
                cursor_created = True
                cur = conn.cursor()

            if args is not None:
                log_message = cur.mogrify(sql, args)
            else:
                log_message = sql

            logger.debug("Executing SQL = " + log_message)

            res = cur.execute(sql, args)

            if fetch:
                data = cur.fetchall()
            else:
                data = None

            if commit == True:
                conn.commit()

        except Exception as e:
            raise (e)

        return res, data

    @staticmethod
    def get_select_fields(fields):

        if fields is None or fields == []:
            field_list = " * "
        else:
            field_list = ",".join(fields)

        return field_list

    @staticmethod
    def template_to_where_clause(template):

        if template is None or template == {}:
            w_clause = None
            args = None
        else:
            terms = []
            args = []
            for k, v in template.items():
                terms.append(k + "=%s")
                args.append(v)

            w_clause = "where " + (" and ".join(terms))

        return w_clause, args

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        key_cols = self._data['key_columns']
        tmp = dict(zip(key_cols, key_fields))
        res = self.find_by_template(tmp, field_list)

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
        w_clause, args = self.template_to_where_clause(template)
        field_list = self.get_select_fields(field_list)
        sql = "select " + field_list + " from " + self._data["table_name"] + " " + w_clause
        res,data = self.run_q(sql,args,commit=True)
        return data

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
        w_clause, args = self.template_to_where_clause(template)
        sql = "delete from " + self._data["table_name"] + " " + w_clause
        res,data = self.run_q(sql,args,commit=True)
        return res

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        key_cols = self._data['key_columns']
        tmp = dict(zip(key_cols, key_fields))
        return self.update_by_template(tmp, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """


        w_clause, args1 = self.template_to_where_clause(template)

        fields = []
        values = []
        for k, v in new_values.items():
            fields.append(k + '=' + '%s')
            values.append(v)
        s_clause = ', '.join(fields)

        sql = "update " + self._data["table_name"] + " set " + s_clause + " " + w_clause

        res, data = self.run_q(sql, values+args1, commit=True)

        return res


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        fields = []
        values = []
        for k, v in new_record.items():
            fields.append(k)
            values.append(v)

        sql = "insert into " + self._data["table_name"] + " (" +  ",".join(fields) + ")" + " values " + "(" + ','.join(['%s']*len(values)) + ")"

        self.run_q(sql, values, commit=True)

