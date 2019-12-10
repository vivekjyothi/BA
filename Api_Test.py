from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from DB_API_FUNCS import DB_API_FUNCTIONS as DBF
from flask_cors import CORS
from json import dumps
from flask import jsonify


driver = "{SQL Server Native Client 11.0}"
server = 'tcp:fleetcoserver.database.windows.net,1433'
database = 'fleetcoassistant'
username = 'fleetcoba'
password = 'aashvik.123'
encrypt = 'yes'
TrustServerCertificate = 'no'
Connection_Timeout='30'

db_connect = DBF( server_name = server, 
                  database_name= database,
                  username = username,
                  password = password,
                  encrypt = encrypt,
                  trust_server_certificate = TrustServerCertificate,
                  connection_timeout = Connection_Timeout,
                  driver = driver)


app = Flask(__name__)
api = Api(app)
CORS(app)

class TableNames(Resource):
    def get(self):
        vcount = db_connect.get_table_names()
        result = ({'result': str(vcount)})
        return jsonify(result)

class VehicleCount(Resource):
    def get(self):
        vcount = db_connect.get_total_vehicle_count()
        result = ({'result': str(vcount)})
        #print(result)
        return jsonify(result)


class VehicleBrandCount(Resource):
    def get(self, brand_name):
        print(brand_name)
        vcount = db_connect.get_vehicle_brand_count(brand_name)
        result = {'result': str(vcount)}
        #print(result)
        return jsonify(result)


class BusinessAssitant(Resource):
    def get(self):
        vcount = db_connect.business_summary_this_month()
        #result = ({'result': vcount})

        result = {}
        result['result']=vcount
        return result

class BusinessAssitantMain(Resource):
    def get(self,ask_type):
        response = db_connect.business_summary(ask_type)
        result = {}
        result['result']=response
        return result

class Home(Resource):
    def get(self):
        return jsonify({'result' :'I am Alive'})

api.add_resource(Home, '/')
api.add_resource(TableNames, '/tablenames')
api.add_resource(VehicleCount, '/vehiclecount')
api.add_resource(VehicleBrandCount, '/vehiclebrandcount/<brand_name>')
api.add_resource(BusinessAssitant, '/thismonthsummary')
api.add_resource(BusinessAssitantMain, '/business_summary/<ask_type>')

if __name__ == '__main__':
     app.run(host ='0.0.0.0', threaded=True, port='5000',debug=True)