# coding=utf8

import cx_Oracle
import os
import time
from threading import Lock
from cx_Oracle import DatabaseError
from oracle_hdfs.configuration import Configuration


class RetrieveMetadata(object):
    __thread_lock = Lock()

    def get_table_names(self):
        path = os.path.join('.', 'table_names.txt')
        arq_tables = open(path, 'r')
        lines = arq_tables.readlines()
        arq_tables.close()
        ret_lines = []
        for linha in lines:
            if linha.startswith('#'):
                continue
            ret_lines.append(linha.strip())
        return ret_lines

    def get_metadata_from_table(self, table_name):
        con = self.__get_conexao()
        if con is None:
            print('Not possible to establish connection with the database')
            exit(1)
        cursor = con.cursor()
        consulta = 'SELECT * FROM {0} \
                    where rownum <= 1'.format(table_name)
        cursor.execute(consulta)
        col_info = [(row[0],
                     row[1],
                     row[4]) for row in cursor.description]
        con.close()
        return col_info

    def get_table_definition(self, rows, table_name):
        tb_def = 'create external table if not exists eleitor.' + table_name + '('
        id_column = 0
        for info in rows:
            tp = None
            if info[1] is cx_Oracle.NUMBER:
                vl = info[2]
                if vl > 6:
                    tp = 'BIGINT'
                elif vl > 3:
                    tp = 'INT'
                else:
                    tp = 'SMALLINT'
            else:
                tp = 'STRING'

            column = '{0} {1}'.format(info[0], tp)
            tb_def = tb_def + column
            id_column += 1
            if id_column < len(rows):
                tb_def += ', \n'
        tb_def += ') row format delimited fields terminated by \',\' location \'/eleitor/' + table_name + '\';'
        return tb_def

    def __get_conexao(self):
        tries = 1
        con = None
        while tries < 5:
            RetrieveMetadata.__thread_lock.acquire()
            try:
                usuario = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='usuario')
                senha = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='senha')
                host = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='host')
                servico = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='servico')
                url = usuario + '/' + senha + '@' + host + '/' + servico
                con = cx_Oracle.connect(url)
            except DatabaseError as e:
                print('Erro estabelecendo conexao com o banco de dados: ' + str(e))
                RetrieveMetadata.__thread_lock.release()
                time.sleep(20)
            else:
                tries = 6
                RetrieveMetadata.__thread_lock.release()
            tries += 1
        return con

    def escreve_script(self, table_script):
        nome_arquivo = Configuration.get_val(section_name='SCRIPT_CRIACAO_HIVE', val_name='nome_arquivo')
        caminho = Configuration.get_val(section_name='SCRIPT_CRIACAO_HIVE', val_name='caminho_destino')
        nome_arquivo = os.path.join(caminho, nome_arquivo)
        append_write = 'w'
        if os.path.exists(nome_arquivo):
            append_write = 'a'

        fhandle = open(nome_arquivo, append_write)
        fhandle.write(table_script)
        fhandle.write('\n'*3)
        fhandle.close()


if __name__ == '__main__':
    retrieve = RetrieveMetadata()
    table_names = retrieve.get_table_names()
    for tb in table_names:
        col_info = retrieve.get_metadata_from_table('ADMDMELEITOR.' + tb)
        table_def = retrieve.get_table_definition(col_info, tb.lower())
        retrieve.escreve_script(table_def)

