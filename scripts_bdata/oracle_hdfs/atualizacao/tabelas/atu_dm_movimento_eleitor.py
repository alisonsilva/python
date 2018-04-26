#!/usr/bin/python
# coding=utf8

from oracle_hdfs.atualizacao.geral import Geral
from oracle_hdfs.atualizacao.atualizacao_tabela import AtualizacaoTabela


class AtualizaDmMovimentoEleitor(AtualizacaoTabela):

    def executa_baixa(self):
        last_value = Geral.get_last_value('dm_movimento_eleitor', 'idw_movimento_eleitor', 'eleitor_analyst')
        con = Geral.get_conexao()
        self.__escreve_dados_arquivo(con, last_value[0][0])
        if con is not None:
            con.close()

    def __escreve_dados_arquivo(self, conexao, idw_data):
        consulta = 'select * from admdmeleitor.dm_movimento_eleitor just \
                        where JUST.idw_movimento_eleitor > {0}'
        super(AtualizaDmMovimentoEleitor, self).escreve_dados_arquivo(conexao, idw_data, 'ARQUIVO_DM_MOVIMENTO_ELEITOR', consulta)


def roda():
    atualizacao = AtualizaDmMovimentoEleitor()
    atualizacao.executa_baixa()


if __name__ == '__main__':
    roda()
