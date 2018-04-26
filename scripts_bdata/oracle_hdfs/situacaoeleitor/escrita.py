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

    def __init__(self, partition, secoes, uf):
        threading.Thread.__init__(self)
        self.partitionExecution = partition
        self.secoes = secoes
        self.uf = uf

    def run(self):
        # execucao paralela para recuperacao de particoes
        ttl_con = 1
        con = self.get_conexao()
        qtd_iteracoes = int(Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='qtd_iteracoes'))
        for secao in self.secoes:  # para todas as secoes
            self.escreve_dados_arquivo(con, self.partitionExecution, secao, self.uf)
            # quantidade máxima de iterações com uma mesma conexão
            if ttl_con >= qtd_iteracoes:
                con.close()
                con = self.get_conexao()
                if con is None:
                    print('Nao foi possivel recuperar conexao com o banco de dados: particao {0}, secao {1}'.
                          format(self.partitionExecution, secao))
                    break
                ttl_con = 0

            ttl_con += 1
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

    def escreve_dados_arquivo(self, conexao, partition, secao, uf):
        raiz_nome_arquivo = Configuration.get_val(section_name='ARQUIVO_SITUACAO_ELEITOR', val_name='raiz_nome_arquivo')
        caminho = Configuration.get_val(section_name='ARQUIVO_SITUACAO_ELEITOR', val_name='caminho_destino')
        nome_arquivo = '{0}/{1}_{2}_{3}.arq'.format(caminho, raiz_nome_arquivo, uf, partition)
        nome_arquivo_log = '{0}/log/{1}_{2}_{3}.log'.format(caminho, raiz_nome_arquivo, uf, partition)
        append_write = 'w'
        if os.path.exists(nome_arquivo):
            append_write = 'a'

        fhandle = open(nome_arquivo, append_write)
        floghandle = open(nome_arquivo_log, 'w')
        try:
            in_vars = ','.join('%d' % i for i in secao)
            cursor = conexao.cursor()
            consulta = 'SELECT IDW_DATA, IDW_HIER_SECAO,IDW_GRP1_CARACT_ELEITOR,IDW_OCUPACAO,IDW_HIER_LOGRADOURO, \
                                   IDW_ELEITOR, IDW_ENDERECO_ELEITOR, NR_DAT_NASCIMENTO, NR_ANOMES, \
                                   CD_OBJETO_ELEITOR FROM ADMDMELEITOR.FT_SS_SITUACAO_ELEITOR PARTITION ({0}) \
                                   WHERE IDW_HIER_SECAO in ({1})'.format(partition, in_vars)
            cursor.execute(consulta)
            for idwData, idwHier, idwGrp, idwOcu, idwLogra, idwEleitor, idwEnd, datNasc, nrAnoMes, coObjEleitor \
                    in cursor:
                try:
                    linha = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (str(idwData or ''), str(idwHier or ''),
                                                                 str(idwGrp or ''), str(idwOcu or ''),
                                                                 str(idwLogra or ''), str(idwEleitor or ''),
                                                                 str(idwEnd or ''), str(datNasc or ''),
                                                                 str(nrAnoMes or ''), coObjEleitor)
                    fhandle.write(linha)
                except TypeError:
                    err_line = 'valores: {0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}\n'.\
                        format(idwData, idwHier, idwGrp, idwOcu, idwLogra, idwEleitor,
                               idwEnd, datNasc, nrAnoMes, coObjEleitor)
                    floghandle.write(err_line)

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


def executa_recuperacao_ft_ss_situacao_eleitor(uf):
    thread_max = int(Configuration.get_val(section_name='CONFIG', val_name='qtd_threads'))
    bucket_size = int(Configuration.get_val(section_name='CONFIG', val_name='bucket_size'))
    sc = Secao()
    prt = Partition()
    secoes = sc.get_zonas(uf)
    particoes = prt.get_partitions(os.path.dirname(__file__))
    total = len(particoes)
    update_progress(calcula_progress(1, total))

    thread_list = []

    bucket_section = sc.bucketing_sections(bucketSize=bucket_size, secoes=secoes)
    count_partitions = 0
    for particao in particoes: # para todas as partições

        particao = particao.split()[0]

        nt = EscritaArquivos(particao, bucket_section, uf)
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
    executa_recuperacao_ft_ss_situacao_eleitor(3)
