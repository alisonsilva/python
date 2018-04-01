#coding=utf8

from subprocess import Popen, PIPE
import sys
import json
import time
import copy

password = 'admin'
cluster = 'SABAD'

def execute_command(command, isShell=False):
    return Popen(command, shell=isShell, stdout=PIPE)

def get_percent_completado(qtd, atual):
    return atual*100/qtd

def update_progress(progress):
    '''
        Imprime um barra de progresso.
        O parametro progress está definido em percentual
    '''
    sys.stdout.write('\r[{0}] {1}%'.format('#'*(progress/10) + ' '*(10 - (progress/10)), progress))
    sys.stdout.flush()

def get_servicos():
    cmd = ['curl', '-u', 'admin:' + password,
           '-H', '\'X-Request-By: ambari\'',
           '-X', 'get', 'http://srv-bi.tre-df.gov.br:8181/api/v1/clusters/' + cluster + '/services/']
    ret = execute_command(cmd)
    output = ret.communicate()[0]
    if output is None or not (output.find("200") or not (output.find("202"))):
        print('Erro parando serviço: Spark2 - {}'.format(output))
        sys.exit(1)

    return json.loads(output)


def stop_service(cmdBasico, url, crp, serviceName):
    cmdSpark2 = cmdBasico[:]

    cmdSpark2.append(json.dumps(crp))
    cmdSpark2.append(url.format(serviceName))

    ret = execute_command(cmdSpark2)
    output = ret.communicate()[0]
    if output is None or not (output.find("200") or not (output.find("202"))):
        print('Erro parando serviço: {} - {}'.format(serviceName, output))
        sys.exit(1)

def get_servicos_rodando():
    servicos = get_servicos()
    for servico in servicos['items']:
        serviceName = servico['ServiceInfo']['service_name']
        cmd = ['curl', '-u', 'admin:'+password,
               '-H', "X-Requested-By: ambari",
               '-i', '-k', '-X', 'GET',
               'http://srv-bi.tre-df.gov.br:8181/api/v1/clusters/SABAD/services/'+serviceName]
        ret = execute_command(cmd)
        output = ret.communicate()[0]
        if output is None or not (output.find("200") or not (output.find("202"))):
            print('Erro parando serviço: {} - {}'.format(serviceName, output))
            sys.exit(1)
        idx = output.index('{')
        output = output[idx:]
        infoServico = (json.loads(output))['ServiceInfo']
        estado = infoServico['state']
        if estado == 'STARTED' or estado == 'STOPPING':
            return True
    return False

if __name__ == '__main__':
    print('Parando serviços no ambari. Isso pode demorar alguns minutos.')
    servicos = get_servicos()
    qtdServicos = len(servicos['items'])
    crpMdl = {"RequestInfo": {"context": "Stop SERVICE ({0}) via REST"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}
    url = 'http://srv-bi.tre-df.gov.br:8181/api/v1/clusters/' + cluster + '/services/{0}'
    cmdBasico = ['curl', '-u',
    'admin:'+ password, '-i', '-H',
    '\'X-Requested-By: ambari - stop\'', '-X',
    'PUT', '-d']
    progress = 0
    for item in servicos['items']:
        serviceName = item['ServiceInfo']['service_name']
        crp = copy.deepcopy(crpMdl)
        crp['RequestInfo']['context'] = crp['RequestInfo']['context'].format(serviceName)
        stop_service(cmdBasico, url, crp, serviceName)
        progress = progress + 1
        update_progress(get_percent_completado(qtdServicos, progress))

    print('Por favor, espere até os serviços pararem...')
    tmp = 100
    servicoRodando = False
    for i in range(tmp+1):
        time.sleep(10)
        update_progress(get_percent_completado(tmp, i))
        servicoRodando = get_servicos_rodando()
        if servicoRodando == False:
            break
    update_progress(get_percent_completado(tmp, 18))

    cmdStopAmbariAgent = ['ambari-agent', 'stop']
    cmdStopAmbariService = ['ambari-server', 'stop']

    execute_command(cmdStopAmbariAgent, isShell=False)
    execute_command(cmdStopAmbariService, isShell=False)

    time.sleep(5)
