#coding=utf8

import cx_Oracle
from oracle.configuration import Configuration

class Secao(object):

    def get_zonas(self, uf):
        usuario = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='usuario')
        senha = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='senha')
        host = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='host')
        servico = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='servico')
        url = usuario + '/' + senha + '@' + host + '/' + servico
        con = cx_Oracle.connect(url)

        cursor = con.cursor()

        ## idw_uf DF - 3; idw_uf ZZ 26
        cursor.execute('select IDW_HIER_SECAO from ADMDMELEITOR.DM_HIER_SECAO \
                        where idw_uf = :idUf order by IDW_HIER_SECAO', idUf=uf)
        retorno = []
        for secao in cursor:
            retorno.append(secao)
        con.close()
        return retorno


    def get_bucket_section(self, uf, bucketSize):
        z = Secao()
        secoes = z.get_zonas(uf)

        count = 0
        bucketSection = []
        while count * bucketSize < len(secoes):
            tsect = secoes[count * bucketSize:(count * bucketSize + bucketSize)]
            bucketSection.append(tsect)
            count = count + 1

        return bucketSection

    def bucketing_sections(self, bucketSize, secoes):
        count = 0
        bucketSection = []
        while count * bucketSize < len(secoes):
            tsect = secoes[count * bucketSize:(count * bucketSize + bucketSize)]
            linha_bck = []
            for sc in tsect:
                linha_bck.append(sc[0])
            bucketSection.append(linha_bck)
            count = count + 1

        return bucketSection


if __name__ == '__main__':
    z = Secao()
    secoes = z.get_zonas(3)

    count = 0
    qtdBucket = 10
    bucketSection = z.bucketing_sections(bucketSize=10, secoes=secoes)
    for bk in bucketSection:
        print(bk)

