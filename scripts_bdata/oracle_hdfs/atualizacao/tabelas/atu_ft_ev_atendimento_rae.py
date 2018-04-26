#!/usr/bin/python
# coding=utf8

from oracle_hdfs.atualizacao.geral import Geral
from oracle_hdfs.atualizacao.atualizacao_tabela import AtualizacaoTabela

class AtualizacaoAtendimentoRae(AtualizacaoTabela):

    def executa_baixa(self):
        last_value = Geral.get_last_value('ft_ev_atendimento_rae', 'idw_atendimento_rae', 'eleitor_analyst')
        con = Geral.get_conexao()
        self.__escreve_dados_arquivo(con, last_value[0][0])
        if con is not None:
            con.close()

    def __escreve_dados_arquivo(self, conexao, idw_data):
        consulta = 'select * from admdmeleitor.ft_ev_atendimento_rae eleitor \
                        where eleitor.idw_atendimento_rae > {0}'
        super(AtualizacaoAtendimentoRae, self).escreve_dados_arquivo(conexao, idw_data, 'ARQUIVO_FT_EV_ATENDIMENTO_RAE', consulta)


def roda():
    atualizacao = AtualizacaoAtendimentoRae()
    atualizacao.executa_baixa()


if __name__ == '__main__':
    roda()