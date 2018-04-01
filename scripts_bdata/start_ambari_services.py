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
    srvs = open('lst_servicos.lst', 'r')
    linhas = srvs.readlines()
    return linhas


def start_service(cmdBasico, url, crp, serviceName):
    cmdIniciarServico = cmdBasico[:]

    cmdIniciarServico.append(json.dumps(crp))
    cmdIniciarServico.append(url.format(serviceName))

    ret = execute_command(cmdIniciarServico)
    output = ret.communicate()[0]
    if output is None or not (output.find("200") or not (output.find("202"))):
        print('Erro iniciando serviço: {} - {}'.format(serviceName, output))
        sys.exit(1)

if __name__ == '__main__':
    servicos = get_servicos()
    qtdServicos = len(servicos)
    crpMdl = {"RequestInfo": {"context": "Start SERVICE ({0}) via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}
    url = 'http://srv-bi.tre-df.gov.br:8181/api/v1/clusters/' + cluster + '/services/{0}'
    cmdBasico = ['curl', '-u',
    'admin:'+ password, '-i', '-H',
    '\'X-Requested-By: ambari - stop\'', '-X',
    'PUT', '-d']
    progress = 0
    for serviceName in servicos:
        serviceName = serviceName.strip()
        if serviceName.startswith('#'):
            continue
        crp = copy.deepcopy(crpMdl)
        crp['RequestInfo']['context'] = crp['RequestInfo']['context'].format(serviceName)
        start_service(cmdBasico, url, crp, serviceName)
        progress = progress + 1
        update_progress(get_percent_completado(qtdServicos, progress))

    print('Por favor, espere até os serviços iniciarem...')
    tmp = 90
    for i in range(tmp+1):
        time.sleep(1)
        update_progress(get_percent_completado(tmp, i))


