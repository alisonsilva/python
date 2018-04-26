#!/usr/bin/python

import os

if __name__ == '__main__':
    os.environ['PYTHONPATH'] = '/root/scripts:' \
                               '/root/scripts/oracle_hdfs:' \
                               '/root/scripts/oracle_hdfs/atualizacao:' \
                               '/root/scripts/oracle_hdfs/atualizacao/tabelas'
    os.system('/root/scripts/run_importa_dados.py ARQUIVO_DM_ENDERECO_ELEITOR')