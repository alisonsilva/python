#!/usr/bin/python
# coding=utf8

import os
from datetime import datetime
from cx_Oracle import DatabaseError
from oracle_hdfs.configuration import Configuration

class AtualizacaoTabela(object):

    def executa_baixa(self):
        pass

    def escreve_dados_arquivo(self, conexao, idw_data, config, consulta):
        raiz_nome_arquivo = Configuration.get_val(section_name=config, val_name='raiz_nome_arquivo')
        caminho = Configuration.get_val(section_name=config, val_name='caminho_destino')
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
            cursor = conexao.cursor()
            consulta = consulta.format(idw_data)
            cursor.execute(consulta)
            for tupla in cursor:
                try:
                    linha = ','.join('%s' % (str(i or '')).replace(',', '') for i in tupla)
                    linha += '\n'
                    fhandle.write(linha)
                except TypeError:
                    err_line = ','.join('%s' % (str(i or '')) for i in tupla)
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


