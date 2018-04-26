#coding=utf8

import cx_Oracle
import os
import sys
import time
import threading
from threading import Lock
from cx_Oracle import DatabaseError
from oracle_hdfs.configuration import Configuration
from oracle_hdfs.perfileleitorlocvot.localvotacao import LocalVotacao
from oracle_hdfs.partition import Partition


class EscritaArquivos(threading.Thread):
    __thread_lock = Lock()

    def __init__(self, partition, locais, uf):
        threading.Thread.__init__(self)
        self.partitionExecution = partition
        self.locais = locais
        self.uf = uf

    def run(self):
        # execucao paralela para recuperacao de particoes
        ttl_con = 1
        con = self.__get_conexao()
        qtd_iteracoes = int(Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='qtd_iteracoes'))
        for local in self.locais:  ## para todas o locais
            self.__escreve_dados_arquivo(con, self.partitionExecution, local, self.uf)
            ## quantidade máxima de iterações com uma mesma conexão
            if ttl_con >= qtd_iteracoes:
                con.close()
                con = self.__get_conexao()
                if con is None:
                    print('Nao foi possivel recuperar conexao com o banco de dados: particao {0}, secao {1}'.
                          format(self.partitionExecution, local))
                    break
                ttl_con = 0

            ttl_con += 1

    def __get_conexao(self):
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

    def __escreve_dados_arquivo(self, conexao, partition, secao, uf):
        raizNomeArquivo = Configuration.get_val(section_name='ARQUIVO_PERFIL_ELEITOR_LOCVOT', val_name='raiz_nome_arquivo')
        caminho = Configuration.get_val(section_name='ARQUIVO_PERFIL_ELEITOR_LOCVOT', val_name='caminho_destino')
        nome_arquivo = '{0}/{1}_{2}_{3}.arq'.format(caminho, raizNomeArquivo, uf, partition)
        nome_arquivo_log = '{0}/log/{1}_{2}_{3}.log'.format(caminho, raizNomeArquivo, uf, partition)
        append_write = 'w'
        if os.path.exists(nome_arquivo):
            append_write = 'a'

        fhandle = open(nome_arquivo, append_write)
        floghandle = open(nome_arquivo_log, 'w')
        try:
            in_vars = ','.join('%d' % i for i in secao)
            cursor = conexao.cursor()
            consulta = 'SELECT IDW_DATA, \
                                   IDW_GRP1_CARACT_ELEITOR, \
                                   IDW_HIER_LOCAL_VOTACAO, \
                                   QT_ELEITOR, \
                                   NR_ANOMES, \
                                   QT_ELEITOR_BIOMETRIA \
                                   FROM ADMDMELEITOR.FT_SS_PERFIL_ELEITOR_LOCVOT PARTITION ({0}) \
                                   WHERE IDW_HIER_LOCAL_VOTACAO in ({1})'.format(partition, in_vars)
            cursor.execute(consulta)
            for idwData, idwGrp1, idwHierLocalVot, qtdEleitor, nrAnoMes, qtdEleitorBio in cursor:
                try:
                    linha = '%s,%s,%s,%s,%s,%s\n' % (str(idwData or ''), str(idwGrp1 or ''),
                                                     str(idwHierLocalVot or ''), str(qtdEleitor or ''),
                                                     str(nrAnoMes or ''), str(qtdEleitorBio or ''))
                    fhandle.write(linha)
                except TypeError:
                    errLine = 'valores: {0}, {1}, {2}, {3}, {4}, {5}\n'.\
                        format(str(idwData or ''), str(idwGrp1 or ''),
                               str(idwHierLocalVot or ''), str(qtdEleitor or ''),
                               str(nrAnoMes or ''), str(qtdEleitorBio or ''))
                    floghandle.write(errLine)

        except DatabaseError as e:
            msg = 'Erro ao executar consulta ao banco: {0}\n'.format(e)
            print(msg)
            floghandle.write(msg)
        finally:
            fhandle.close()
            floghandle.close()


def __update_progress(progress):
    sys.stdout.write('\r[{0}] {1}%'.format('#'*(progress/10) + ' '*(10 - (progress/10)), progress))
    sys.stdout.flush()


def __calcula_progress(atual, total):
    return (atual*100)/total


def executa_recuperacao_ft_ss_perfil_eleitor_locvot(uf):
    thread_max = int(Configuration.get_val(section_name='CONFIG', val_name='qtd_threads'))
    bucket_size = int(Configuration.get_val(section_name='CONFIG', val_name='bucket_size'))
    loc_votacao = LocalVotacao()
    prt = Partition()
    locais_votacao = loc_votacao.get_locais_votacao(uf)
    particoes = prt.get_partitions(os.path.dirname(__file__))
    total = len(particoes)
    __update_progress(__calcula_progress(1, total))

    thread_list = []

    bucket_section = loc_votacao.bucketing_locais(bucket_size=bucket_size, locais=locais_votacao)
    count_partitions = 0
    for particao in particoes: ## para todas as partições

        particao = particao.split()[0]

        nt = EscritaArquivos(particao, bucket_section, uf)
        thread_list.append(nt)
        nt.start()

        count_partitions += 1
        if len(thread_list) >= thread_max:
            for t in thread_list:
                t.join()
            thread_list = []
            __update_progress(__calcula_progress(count_partitions, total))

    if len(thread_list) > 0:
        for t in thread_list:
            count_partitions += 1
            t.join()
        __update_progress(__calcula_progress(count_partitions, total))


if __name__ == '__main__':
    executa_recuperacao_ft_ss_perfil_eleitor_locvot(3)
