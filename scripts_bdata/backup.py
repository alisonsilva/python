#coding=utf8

from subprocess import Popen, PIPE
import sys
from datetime import datetime

def execute_command(command, isShell=False):
    return Popen(command, shell=isShell, stdout=PIPE)

def backup(now, origem, destino, dirDest):
    ano = now.year
    mes = now.month
    dia = now.day
    amd = '%02d%02d%02d' % (ano,mes,dia)

    hour = now.hour
    min = now.minute
    hm = '%02d%02d' % (hour,min)

    fileName = destino+'_'+amd+'_'+hm+'.tar.gz'
    cmd = ['tar', '-zcvf', dirDest+fileName, origem]
    ret = execute_command(cmd)
    output = ret.communicate()[0]
    if output is None:
        print('Erro executando backup: {} - {}'.format(fileName, output))
        sys.exit(1)

def update_progress(progress):
    sys.stdout.write('\r{0}'.format(progress))
    sys.stdout.flush()

if __name__ == '__main__':
    now = datetime.now()

    update_progress('Backup de ambari-server....')
    backup(now, '/etc/ambari-server/conf', 'ambari-server', dirDest='/opt/backup/')
    update_progress('OK\n')

    update_progress('Backup de ambari-agent....')
    backup(now, '/etc/ambari-agent/conf', 'ambari-agent', dirDest='/opt/backup/')
    update_progress('OK\n')

    update_progress('Backup de namenode ....')
    backup(now, '/hadoop/hdfs/namenode', 'namenode', dirDest='/opt/backup/')
    update_progress('OK\n')
