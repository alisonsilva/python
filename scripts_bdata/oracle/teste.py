
import cx_Oracle
from cx_Oracle import DatabaseError
from oracle.configuration import Configuration


def get_conexao():
    usuario = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='usuario')
    senha = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='senha')
    host = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='host')
    servico = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='servico')
    url = usuario + '/' + senha + '@' + host + '/' + servico
    con = cx_Oracle.connect(url)
    return con

def escreve_dados_arquivo(conexao, partition, secao):
    try:
        in_vars = ','.join('%d' % i for i in secao)
        cursor = conexao.cursor()
        consulta = 'SELECT IDW_DATA, IDW_HIER_SECAO,IDW_GRP1_CARACT_ELEITOR,IDW_OCUPACAO,IDW_HIER_LOGRADOURO, \
                               IDW_ELEITOR, IDW_ENDERECO_ELEITOR, NR_DAT_NASCIMENTO, NR_ANOMES, \
                               CD_OBJETO_ELEITOR FROM ADMDMELEITOR.FT_SS_SITUACAO_ELEITOR PARTITION ({0}) \
                               WHERE IDW_HIER_SECAO in ({1})'.format(partition, in_vars)
        cursor.execute(consulta)
        for idwData, idwHier, idwGrp, idwOcu, idwLogra, idwEleitor, idwEnd, datNasc, nrAnoMes, coObjEleitor in cursor:
            linha = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' %(str(idwData or ''), str(idwHier or ''),
                                                        str(idwGrp or ''), str(idwOcu or ''),
                                                        str(idwLogra or ''), str(idwEleitor or ''),
                                                        str(idwEnd or ''), str(datNasc or ''),
                                                        str(nrAnoMes or ''), coObjEleitor)
            print(linha)
    except DatabaseError as e:
        print('Erro ao executar consulta ao banco: {0}'.format(e))
    else:
        conexao.close()

if __name__ == '__main__':
    con = get_conexao()
    escreve_dados_arquivo(con, 'NASC_1964', (20768992, 20768998, 20769005, 20769006, 20749754, 20751132, 20751562, 20751563, 20751564, 20751565))

