#!/usr/bin/python
# coding=utf8

from oracle_hdfs.atualizacao.geral import Geral
from oracle_hdfs.atualizacao.atualizacao_tabela import AtualizacaoTabela

class AtualizacaoEvAbstencao(AtualizacaoTabela):

    def executa_baixa(self):
        last_value = Geral.get_last_value('ft_ev_abstencao', 'idw_data_processamento', 'eleitor_analyst')
        con = Geral.get_conexao()
        self.__escreve_dados_arquivo(con, last_value[0][0])
        if con is not None:
            con.close()

    def __escreve_dados_arquivo(self, conexao, idw_data):
        consulta = 'select * from admdmeleitor.ft_ev_abstencao abst \
                    where abst.idw_data_processamento > {0} and ABST.IDW_HIER_LOCALIDADE in ( \
                    select l.idw_hier_localidade from ADMDMELEITOR.DM_HIER_LOCALIDADE l \
                    where l.cd_uf = 20 or L.CD_UF = 28)'
        super(AtualizacaoEvAbstencao, self).escreve_dados_arquivo(conexao, idw_data, 'ARQUIVO_FT_EV_ABSTENCAO', consulta)


def roda():
    atualizacao = AtualizacaoEvAbstencao()
    atualizacao.executa_baixa()


if __name__ == '__main__':
    roda()