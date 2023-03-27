import queue

import redshift_connector
redshift_connector.paramstyle = 'qmark'
import threading
from queue import Queue


def type_cast(val):
    try:
        val = eval(val)
    except Exception:
        val = val
    finally:
        return val

class Database:

    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

        self.conn = self.connect_redshift()

    def _thread(self, queue):
        while self.thread_running:
            args, kwargs = queue.get()
            self.insert_data(*args, **kwargs)

    def thread(self):
        self.thread_running = True
        data_queue = queue.Queue()
        self.thread = threading.Thread(target=self._thread, args=(data_queue,))
        self.thread.start()
        return data_queue

    def stopthread(self):
        self.thread_running = False
        self.thread.join()

    def connect_redshift(self):
        """Connect to redshift database"""
        print("Connecting to database")
        # conn = redshift_connector.connect(
        #     host='default.792104094313.us-east-1.redshift-serverless.amazonaws.com',
        #     database='motivated_leads',
        #     user='admin',
        #     password='Admin1234'
        #  )
        conn = redshift_connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )
        conn.autocommit = True
        print("Connected")
        return conn


    def insert_data(self, table_name, column_names, data, date_columns=[], date_format='MM/DD/YYYY', multiple_rows=True, auto_type_cast=True):
        placeholder = ['?'] * len(column_names)
        for date_column in date_columns:
            placeholder[date_column] = f"to_date(?, '{date_format}')"
        if auto_type_cast:
            print("Type casting")
            new_data = []
            for row in data:
                this_row = []
                for val in row:
                    typecast_val = type_cast(val)
                    this_row.append(typecast_val)
                new_data.append(this_row)
            data = new_data
            print("Type casted data first row: ", data[0])
        column_names_string = ', '.join([f'"{column_name}"' for column_name in column_names])
        QUERY = f"INSERT INTO {table_name} ({column_names_string}) VALUES ({','.join(placeholder)})"
        cursor = self.conn.cursor()
        if not multiple_rows:
            cursor.execute(QUERY, data)
        else:
            cursor.executemany(QUERY, data)
        self.conn.commit()

    def get_query(self, query, auto_typecast=True):
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        data = []
        for row in rows:
            this_row = []
            if auto_typecast:
                for val in row:
                    this_row.append(type_cast(val))
            else:
                this_row = row
            data.append(this_row)
        return data


