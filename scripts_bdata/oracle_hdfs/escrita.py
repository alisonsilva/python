#coding=utf8

import cx_Oracle
import os
import sys
from cx_Oracle import DatabaseError
from oracle_hdfs.configuration import Configuration
from oracle_hdfs.secao import Secao
from oracle_hdfs.partition import Partition

class EscritaArquivos(object):

    def get_conexao(self):
        usuario = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='usuario')
        senha = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='senha')
        host = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='host')
        servico = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='servico')
        url = usuario + '/' + senha + '@' + host + '/' + servico
        con = cx_Oracle.connect(url)
        return con

    def escreve_dados_arquivo(self, conexao, partition, secao):
        raizNomeArquivo = Configuration.get_val(section_name='ARQUIVO', val_name='raiz_nome_arquivo')
        caminho = Configuration.get_val(section_name='ARQUIVO', val_name='caminho_destino')
        nomeArquivo = '{0}/{1}_{2}.arq'.format(caminho, raizNomeArquivo, partition)
        append_write = 'w'
        if os.path.exists(nomeArquivo):
            append_write = 'a'

        fhandle = open(nomeArquivo, append_write)
        floghandle = open(nomeArquivo+'.log', 'w')
        try:
            cursor = conexao.cursor()
            consulta = 'SELECT IDW_DATA, IDW_HIER_SECAO,IDW_GRP1_CARACT_ELEITOR,IDW_OCUPACAO,IDW_HIER_LOGRADOURO, \
                                   IDW_ELEITOR, IDW_ENDERECO_ELEITOR, NR_DAT_NASCIMENTO, NR_ANOMES, \
                                   CD_OBJETO_ELEITOR FROM ADMDMELEITOR.FT_SS_SITUACAO_ELEITOR PARTITION ({0}) \
                                   WHERE IDW_HIER_SECAO = :mksecao'.format(partition)
            cursor.execute(consulta, mksecao=secao)
            for idwData, idwHier, idwGrp, idwOcu, idwLogra, idwEleitor, idwEnd, datNasc, nrAnoMes, coObjEleitor in cursor:
                try:
                    linha = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' %(str(idwData or ''), str(idwHier or ''),
                                                                str(idwGrp or ''), str(idwOcu or ''),
                                                                str(idwLogra or ''), str(idwEleitor or ''),
                                                                str(idwEnd or ''), str(datNasc or ''),
                                                                str(nrAnoMes or ''), coObjEleitor)
                    fhandle.write(linha)
                except TypeError:
                    errLine = 'valores: {0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}\n'.\
                        format(idwData, idwHier, idwGrp, idwOcu, idwLogra, idwEleitor,
                               idwEnd, datNasc, nrAnoMes, coObjEleitor)
                    floghandle.write(errLine)

        except DatabaseError as e:
            print('Erro ao executar consulta ao banco: {0}'.format(e))
        else:
            fhandle.close()
            floghandle.close()

def update_progress(progress):
    '''
        Imprime um barra de progresso.
        O parametro progress está definido em percentual
    '''
    sys.stdout.write('\r[{0}] {1}%'.format('#'*(progress/10) + ' '*(10 - (progress/10)), progress))
    sys.stdout.flush()

def calcula_progress(atual, total):
    return (atual*100)/total

def executa_recuperacao_ft_ss_situacao_eleitor(uf):
    sc = Secao()
    prt = Partition()
    secoes = sc.get_zonas(uf)
    particoes = prt.get_partitions()
    esc = EscritaArquivos()
    total = len(particoes)*len(secoes)
    update_progress(calcula_progress(1, total))

    for particao in particoes: ## para todas as partições
        ttlCon = 1
        particao = particao.split()[0]
        con = esc.get_conexao()
        for secao in secoes:   ## para todas as secoes
            secao = secao[0]
            esc.escreve_dados_arquivo(con, particao, secao)

            if ttlCon == 100:
                con.close()
                con = esc.get_conexao()
                ttlCon = 1

            ttlCon = ttlCon + 1

        update_progress(calcula_progress(1, total))

if __name__ == '__main__':
    executa_recuperacao_ft_ss_situacao_eleitor()