#!/usr/bin/env python
# coding: utf-8

# In[34]:
import pyodbc
import csv
import pandas
"""
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-4GQVL9B;'
                      'Database=TestDB;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
"""

class DB_API_FUNCTIONS:
    def __init__(self, server_name,database_name, 
                 username = None,
                 password = None,
                 driver = 'SQL Server',
                 trust_server_certificate = 'no',
                 connection_timeout = '30',
                 encrypt = 'no'   
                 ):
        self.server_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.trust_server_certificate = trust_server_certificate
        self.driver = driver
        self.encrypt = encrypt
        self.connection_timeout = connection_timeout

        if username is None:
            self.conn = pyodbc.connect('Driver={'+self.driver+'};'
                                  'Server='+self.server_name+';'  #DESKTOP-4GQVL9B;'
                                  'Database='+self.database_name+';'
                                  'Trusted_Connection='+self.trust_server_certificate+';')
        else:
            connstr = (
                'Driver=' + self.driver +';'
                'Server=' + self.server_name + ';'
                'Database=' + self.database_name + ';'
                'UID=' + self.username + ';'
                'PWD=' + self.password + ';'
                'Encrypt=' + self.encrypt + ';'
                'TrustServerCertificate=' + self.trust_server_certificate + ';'
                'Connection Timeout=' + self.connection_timeout+ ';'
                )
            self.conn = pyodbc.connect(connstr)  

        print('Connection Established')

    def close_connection(self):
        try:
            if hasattr(self, 'cursor'):
                self.cursor.close()
        except:
            v = 10

        self.conn.close()

    def get_table_names(self):

        query = 'USE '+self.database_name+'; SELECT * FROM sys.Tables;'
        results = pandas.read_sql_query(query, self.conn)
        print(results)
        v = results['Total Number of vehicles'][0]
        return v

    def get_total_vehicle_count(self):
        '''
         This functions returns the total vehicle count.

         Input Params   : None

         Output         : Total Vehicle Count

         Usage          : get_total_vehicle_count()
        '''
        query = 'EXEC ['+self.database_name+'].[dbo].[Count_P_Plant]'
        results = pandas.read_sql_query(query, self.conn)
        v = results['Total Number of vehicles'][0]
        return v

    def get_vehicle_brand_count(self,vehicle_brand):
        '''
         This functions returns count of requested brand of vehicle in json Format.

         Input Params   : vehicle_brand_name as string or in tuple format

         Output         : Total Vehicle Count of the requested brand.

         Usage          : get_vehicle_brand_count('Tata')
                          get_vehicle_brand_count(('Tata'))
        '''
        self.cursor = self.conn.cursor()
        sql = """\
        EXEC [dbo].[Total Brands] @Plant=?
        """
        self.cursor.execute(sql, vehicle_brand)
        v = {'results': [dict(zip([column[0] for column in self.cursor.description], row))
                         for row in self.cursor.fetchall()]}
        res = v['results'][0]['Total Number of Vehicles']

        self.cursor.close()
        return res

    def business_summary_this_month(self):
        '''
         This functions returns business summary table in json Format.

         Input Params   : None

         Output         : Business Summary.

         Usage          : business_summary_this_month()
        '''
        self.cursor = self.conn.cursor()
        sql = """\
        EXEC [dbo].[Business_Summary]
        """

        self.cursor.execute(sql)
        v = {'results': [dict(zip([column[0] for column in self.cursor.description], row))
                         for row in self.cursor.fetchall()]}
        res = v['results'][0]
        res['Fuel_Trip_Consumed_Litres']= float(res['Fuel_Trip_Consumed_Litres'])
        self.cursor.close()
        return res


    def business_summary(self,askType):
        '''
         This functions returns business summary table of this year or this month based on the ask in dict Format.

         Input Params   : askType as string
                        Supported: 'this month'
                                    'this year'

         Output         : business summary table of this year or this month.

         Usage          : business_summary('this month')

        '''
        self.cursor = self.conn.cursor()
        sql = """\
        EXEC [dbo].[Business_Summary_Main] @Param=?
        """
        self.cursor.execute(sql, askType)
        v = {'results': [dict(zip([column[0] for column in self.cursor.description], row))
                         for row in self.cursor.fetchall()]}
        res = v['results'][0]
        res['Fuel_Trip_Consumed_Litres']= float(res['Fuel_Trip_Consumed_Litres'])
        self.cursor.close()
        return res


if __name__ == "__main__":
    Server='DESKTOP-4GQVL9B'
    Database='TestDB'

    vin = DB_API_FUNCTIONS(Server, Database)
    print(vin.get_total_vehicle_count())
    print(vin.get_vehicle_brand_count('Tata'))
    vin.close_connection()

