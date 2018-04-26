#!/usr/bin/python
# coding=utf8

import os
from datetime import datetime
from cx_Oracle import DatabaseError
from oracle_hdfs.configuration import Configuration
from oracle_hdfs.atualizacao.geral import Geral
from oracle_hdfs.secao import Secao


class AtualizaFtEvJustificativa(object):

    def executa_baixa(self, secoes):
        ttl_con = 1
        last_value = Geral.get_last_value('FT_EV_JUSTIFICATIVA', 'IDW_DATA_JUSTIFICATIVA', 'eleitor_analyst')
        con = Geral.get_conexao()
        qtd_iteracoes = int(Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='qtd_iteracoes'))
        for secao in secoes:  # para todas as seções
            self.__escreve_dados_arquivo(con, secao, last_value[0][0])
            # quantidade máxima de iterações com uma mesma conexão
            if ttl_con >= qtd_iteracoes:
                con.close()
                con = Geral.get_conexao()
                if con is None:
                    print('Nao foi possivel recuperar conexao com o banco de dados: secao {0}'.
                          format(secao))
                    break
                ttl_con = 0
            ttl_con += 1

        if con is not None:
            con.close()

    def __escreve_dados_arquivo(self, conexao, secao, idw_data):
        raiz_nome_arquivo = Configuration.get_val(section_name='ARQUIVO_FT_EV_JUSTIFICATIVA', val_name='raiz_nome_arquivo')
        caminho = Configuration.get_val(section_name='ARQUIVO_FT_EV_JUSTIFICATIVA', val_name='caminho_destino')
        nome_arquivo = '{0}/{1}.arq'.format(caminho, raiz_nome_arquivo)
        nome_arquivo_log = '{0}/log/{1}.log'.format(caminho, raiz_nome_arquivo)
        append_write = 'w'
        if os.path.exists(nome_arquivo):
            append_write = 'a'

        fhandle = open(nome_arquivo, append_write)

        append_write = 'w'
        if os.path.exists(nome_arquivo_log):
            append_write = 'a'
        floghandle = open(nome_arquivo_log, append_write)
        try:
            in_vars = ','.join('%d' % i for i in secao)
            cursor = conexao.cursor()
            consulta = 'select * from admdmeleitor.FT_EV_JUSTIFICATIVA just \
                        where JUST.IDW_DATA_JUSTIFICATIVA > {0}  \
                        and just.DM_HIER_SECAO in ({1})'.format(idw_data, in_vars)
            cursor.execute(consulta)
            for tuple in cursor:
                try:
                    linha = ','.join('%s' % (str(i or '')) for i in tuple)
                    linha += '\n'
                    fhandle.write(linha)
                except TypeError:
                    err_line = ','.join('%s' % (str(i or '')) for i in tuple)
                    err_line += '\n'
                    floghandle.write(err_line)

        except DatabaseError as e:
            momento = 'd%02d%02d%-02d%02d%02d%'.format(datetime.year, datetime.month,
                                                  datetime.day, datetime.hour,
                                                  datetime.minute, datetime.second)
            msg = '{0} - Erro ao executar consulta ao banco: {1}\n'.format(momento, e)
            floghandle.write(msg)
        finally:
            fhandle.close()
            floghandle.close()


def __get_secoes():
    SC_DF = 3
    SC_ZZ = 26
    sc = Secao()
    secoes = sc.get_zonas(SC_DF)
    secoes = secoes + sc.get_zonas(SC_ZZ)
    return secoes


def roda():
    secoes = __get_secoes()
    sc = Secao()
    bucket_size = int(Configuration.get_val(section_name='CONFIG', val_name='bucket_size'))
    bucket_section = sc.bucketing_sections(bucketSize=bucket_size, secoes=secoes)
    atualizacao = AtualizaFtEvJustificativa()
    atualizacao.executa_baixa(bucket_section)


if __name__ == '__main__':
    roda()
