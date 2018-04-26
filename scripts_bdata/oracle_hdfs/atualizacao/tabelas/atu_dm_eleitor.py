#!/usr/bin/python
# coding=utf8

from oracle_hdfs.atualizacao.geral import Geral
from oracle_hdfs.atualizacao.atualizacao_tabela import AtualizacaoTabela

class AtualizacaoDmEleitor(AtualizacaoTabela):

    def executa_baixa(self):
        last_value = Geral.get_last_value('DM_ELEITOR', 'idw_eleitor', 'eleitor_analyst')
        con = Geral.get_conexao()
        self.__escreve_dados_arquivo(con, last_value[0][0])
        if con is not None:
            con.close()

    def __escreve_dados_arquivo(self, conexao, idw_data):
        consulta = 'select * from admdmeleitor.DM_ELEITOR eleitor \
                        where eleitor.idw_eleitor > {0}'
        super(AtualizacaoDmEleitor, self).escreve_dados_arquivo(conexao, idw_data, 'ARQUIVO_DM_ELEITOR', consulta)


def roda():
    atualizacao = AtualizacaoDmEleitor()
    atualizacao.executa_baixa()


if __name__ == '__main__':
    roda()