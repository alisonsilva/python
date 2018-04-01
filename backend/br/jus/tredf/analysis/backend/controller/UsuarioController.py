from flask import Flask, jsonify, request
from br.jus.tredf.analysis.backend.conf.configuration import Configuration
from br.jus.tredf.analysis.backend.service.ipadd_service import IpAddressService

from br.jus.tredf.analysis.backend.conf.databaseconfiguration import DatabaseConfiguration
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

app = Flask(__name__)
app.config.from_object(DatabaseConfiguration)
db = SQLAlchemy(app)
migrate = Migrate(app, db)

from br.jus.tredf.analysis.backend.model import models


@app.route('/backend/log/logs', methods=['GET'])
def get_logs():
    return jsonify(Configuration.get_val(section_name='USUARIO', val_name='recuperado'))

@app.route('/backend/ip/all', methods=['GET'])
def get_ips():
    ipaddress_serv = IpAddressService()
    ips = ipaddress_serv.listaIps()
    lstips = []
    for ip in ips:
        rspip = {'id': ip.id, 'value': ip.value}
        lstips.append(rspip)
    rsp = {'ips': lstips}
    return jsonify(rsp)

@app.route('/backend/log/<int:log_id>', methods=['GET'])
def get_log(log_id):
    #task = [task for task in tasks if task['id'] == task_id]
    #if len(task) == 0:
    #    abort(404)
    return jsonify({'log': 'log info', 'numero': 23})

@app.route('/backend/log/novo', methods=['POST'])
def create_log():
    if not request.json or not 'mensagem' in request.json:
        #abort(400)
        pass
    log = {
        'id': 1,
        'mensagem': request.json['mensagem'],
        'numero': request.json.get('numero', '')
    }
    #insert new log
    return jsonify({'log': log}), 201

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'})



if __name__ == '__main__':
    app.run(debug=True)