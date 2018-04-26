#!/usr/bin/python
# coding=utf8


import sys
from oracle_hdfs.importadadoshdfs import ImportDadosHdfs

if __name__ == '__main__':
    qtd_arguments = len(sys.argv)
    if qtd_arguments != 2:
        print('E necessario indicar o arquivo a ser importado')
        exit(1)

    scr = sys.argv[1]
    ATUALIZAR = 2
    importa_dados = ImportDadosHdfs()
    importa_dados.importa_dados_hdfs(ATUALIZAR, scr)
