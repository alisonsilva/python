#!/usr/bin/python
# coding=utf8

import sys
from oracle_hdfs.atualizacao.tabelas import atu_dm_movimento_eleitor, atu_ft_ev_justificativa, atu_dm_registro_rae, atu_dm_eleitor
from oracle_hdfs.atualizacao.tabelas import atu_dm_endereco_eleitor, atu_ft_ev_atendimento_rae, atu_ft_ev_abstencao
from oracle_hdfs.atualizacao.tabelas import atu_ft_perf_eleitor_munic_zona

if __name__ == '__main__':
    qtd_arguments = len(sys.argv)
    if qtd_arguments != 2:
        print('E necessario indicar o script a ser executado')
        exit(1)

    scr = sys.argv[1]
    if scr == 'atu_dm_movimento_eleitor':
        atu_dm_movimento_eleitor.roda()
    elif scr == 'atu_ft_ev_justificativa':
        atu_ft_ev_justificativa.roda()
    elif scr == 'atu_dm_registro_rae':
        atu_dm_registro_rae.roda()
    elif scr == 'atu_dm_eleitor':
        atu_dm_eleitor.roda()
    elif scr == 'atu_dm_endereco_eleitor':
        atu_dm_endereco_eleitor.roda()
    elif scr == 'atu_ft_ev_atendimento_rae':
        atu_ft_ev_atendimento_rae.roda()
    elif scr == 'atu_ft_ev_abstencao':
        atu_ft_ev_abstencao.roda()
    elif scr == 'atu_ft_perf_eleitor_munic_zona':
        atu_ft_perf_eleitor_munic_zona.roda()
    else:
        print('O script indicado nao foi encontrado')
        exit(1)
