#!/usr/bin/python
# coding=utf8

from oracle_hdfs.atualizacao.geral import Geral
from oracle_hdfs.atualizacao.atualizacao_tabela import AtualizacaoTabela

class AtualizacaoDmEnderecoEleitor(AtualizacaoTabela):

    def executa_baixa(self):
        last_value = Geral.get_last_value('DM_ENDERECO_ELEITOR', 'idw_endereco_eleitor', 'eleitor_analyst')
        con = Geral.get_conexao()
        self.__escreve_dados_arquivo(con, last_value[0][0])
        if con is not None:
            con.close()

    def __escreve_dados_arquivo(self, conexao, idw_data):
        consulta = 'select * from admdmeleitor.dm_endereco_eleitor eleitor \
                        where eleitor.idw_endereco_eleitor > {0}'
        super(AtualizacaoDmEnderecoEleitor, self).escreve_dados_arquivo(conexao, idw_data, 'ARQUIVO_DM_ENDERECO_ELEITOR', consulta)


def roda():
    atualizacao = AtualizacaoDmEnderecoEleitor()
    atualizacao.executa_baixa()


if __name__ == '__main__':
    roda()