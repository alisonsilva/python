# coding=utf8

import cx_Oracle
import os
import sys
import time
import threading
from threading import Lock
from cx_Oracle import DatabaseError
from oracle_hdfs.configuration import Configuration
from oracle_hdfs.secao import Secao
from oracle_hdfs.partition import Partition


class EscritaArquivos(threading.Thread):
    __thread_lock = Lock()

    def __init__(self, partition):
        threading.Thread.__init__(self)
        self.partitionExecution = partition

    def run(self):
        # execucao paralela para recuperacao de particoes
        con = self.get_conexao()
        self.escreve_dados_arquivo(con, self.partitionExecution)
        con.close()

    def get_conexao(self):
        tries = 1
        con = None
        while tries < 5:
            EscritaArquivos.__thread_lock.acquire()
            try:
                usuario = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='usuario')
                senha = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='senha')
                host = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='host')
                servico = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='servico')
                url = usuario + '/' + senha + '@' + host + '/' + servico
                con = cx_Oracle.connect(url)
            except DatabaseError as e:
                print('Erro estabelecendo conexao com o banco de dados: ' + str(e))
                EscritaArquivos.__thread_lock.release()
                time.sleep(20)
            else:
                tries = 6
                EscritaArquivos.__thread_lock.release()
            tries += 1
        return con

    def escreve_dados_arquivo(self, conexao, partition):
        raiz_nome_arquivo = Configuration.get_val(section_name='ARQUIVO_REGISTRO_RAE', val_name='raiz_nome_arquivo')
        caminho = Configuration.get_val(section_name='ARQUIVO_REGISTRO_RAE', val_name='caminho_destino')
        nome_arquivo = '{0}/{1}_{2}.arq'.format(caminho, raiz_nome_arquivo, partition)
        nome_arquivo_log = '{0}/log/{1}_{2}.log'.format(caminho, raiz_nome_arquivo, partition)
        append_write = 'w'
        if os.path.exists(nome_arquivo):
            append_write = 'a'

        fhandle = open(nome_arquivo, append_write)
        floghandle = open(nome_arquivo_log, 'w')
        try:
            cursor = conexao.cursor()
            consulta = 'SELECT * FROM ADMDMELEITOR.DM_REGISTRO_RAE PARTITION ({0})'.format(partition)
            cursor.execute(consulta)
            for tuple in cursor:
                try:
                    linha = ','.join('%s' % str(i or '') for i in tuple)
                    linha += '\n'
                    fhandle.write(linha)
                except TypeError:
                    floghandle.write(tuple)
                    floghandle.write('\n')

        except DatabaseError as e:
            msg = 'Erro ao executar consulta ao banco: {0}\n'.format(e)
            print(msg)
            floghandle.write(msg)
        finally:
            fhandle.close()
            floghandle.close()



def update_progress(progress):
    '''
        Imprime um barra de progresso.
        O parametro progress está definido em percentual
    '''
    sys.stdout.write('\r[{0}] {1}%'.format('#'*(progress/10) + ' '*(10 - (progress/10)), progress))
    sys.stdout.flush()


def calcula_progress(atual, total):
    return (atual*100)/total


def executa_recuperacao_dm_registro_rae():
    thread_max = int(Configuration.get_val(section_name='CONFIG', val_name='qtd_threads'))
    prt = Partition()
    particoes = prt.get_partitions(os.path.dirname(__file__))
    total = len(particoes)
    update_progress(calcula_progress(1, total))

    thread_list = []

    count_partitions = 0
    for particao in particoes: # para todas as partições
        particao = particao.split()[0]

        nt = EscritaArquivos(particao)
        thread_list.append(nt)
        nt.start()

        count_partitions += 1
        if len(thread_list) >= thread_max:
            for t in thread_list:
                t.join()
            thread_list = []
            update_progress(calcula_progress(count_partitions, total))

    if len(thread_list) > 0:
        for t in thread_list:
            count_partitions += 1
            t.join()
        update_progress(calcula_progress(count_partitions, total))


if __name__ == '__main__':
    executa_recuperacao_dm_registro_rae()
