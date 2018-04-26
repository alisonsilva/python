# coding=utf8

import cx_Oracle
import time
import pyhs2 as hive
from cx_Oracle import DatabaseError
from oracle_hdfs.configuration import Configuration
from threading import Lock

class Geral(object):
    __thread_lock = Lock()

    @staticmethod
    def get_last_value(table_name, column_name, role=None):
        ret_values = []
        hive_server = Configuration.get_val(section_name='CONEXAO_HIVE', val_name='host')
        hive_port = int(Configuration.get_val(section_name='CONEXAO_HIVE', val_name='port'))
        hive_user = Configuration.get_val(section_name='CONEXAO_HIVE', val_name='usuario')
        hive_pass = Configuration.get_val(section_name='CONEXAO_HIVE', val_name='senha')
        ldap_domain = Configuration.get_val(section_name='CONEXAO_HIVE', val_name='domain')
        hive_database = Configuration.get_val(section_name='CONEXAO_HIVE', val_name='database')

        conn = hive.connect(host=hive_server,
                            port=hive_port,
                            authMechanism='LDAP',
                            user=hive_user +'@'+ldap_domain,
                            password=hive_pass,
                            database=hive_database)
        cursor = conn.cursor()
        if not role is None:
            cursor.execute('set role ' + role)
        cursor.execute('select MAX({0}) AS MAX_ID from {1}'.format(column_name, table_name))
        for val in cursor.fetch():
            ret_values.append(val)
        conn.close()

        return ret_values


    @staticmethod
    def get_conexao():
        tries = 1
        con = None
        while tries < 5:
            Geral.__thread_lock.acquire()
            try:
                usuario = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='usuario')
                senha = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='senha')
                host = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='host')
                servico = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='servico')
                url = usuario + '/' + senha + '@' + host + '/' + servico
                con = cx_Oracle.connect(url)
            except DatabaseError as e:
                print('Erro estabelecendo conexao com o banco de dados: ' + str(e))
                Geral.__thread_lock.release()
                time.sleep(20)
            else:
                tries = 6
                Geral.__thread_lock.release()
            tries += 1
        return con


if __name__ == '__main__':
    values = Geral.get_last_value('FT_EV_JUSTIFICATIVA', 'IDW_DATA_JUSTIFICATIVA', 'eleitor_analyst')
    for val in values:
        print val[0]