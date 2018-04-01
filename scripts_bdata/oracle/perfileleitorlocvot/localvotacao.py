#coding=utf8

import cx_Oracle
from oracle.configuration import Configuration

class LocalVotacao(object):

    def get_locais_votacao(self, uf):
        usuario = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='usuario')
        senha = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='senha')
        host = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='host')
        servico = Configuration.get_val(section_name='CONEXAO_ORACLE', val_name='servico')
        url = usuario + '/' + senha + '@' + host + '/' + servico
        con = cx_Oracle.connect(url)

        cursor = con.cursor()

        ## idw_uf DF - 3; idw_uf ZZ 26
        cursor.execute('select IDW_HIER_LOCAL_VOTACAO from ADMDMELEITOR.DM_HIER_LOCAL_VOTACAO \
                        where idw_uf = :idUf order by IDW_HIER_LOCAL_VOTACAO', idUf=uf)
        retorno = []
        for local_votacao in cursor:
            retorno.append(local_votacao)
        con.close()
        return retorno


    def get_bucket_section(self, uf, bucket_size):
        locais = self.get_locais_votacao(uf)

        count = 0
        bucket_section = []
        while count * bucket_size < len(locais):
            tsect = locais[count * bucket_size:(count * bucket_size + bucket_size)]
            bucket_section.append(tsect)
            count = count + 1

        return bucket_section

    def bucketing_locais(self, bucket_size, locais):
        count = 0
        bucketSection = []
        while count * bucket_size < len(locais):
            tsect = locais[count * bucket_size:(count * bucket_size + bucket_size)]
            linha_bck = []
            for sc in tsect:
                linha_bck.append(sc[0])
            bucketSection.append(linha_bck)
            count = count + 1

        return bucketSection


if __name__ == '__main__':
    z = LocalVotacao()
    secoes = z.get_locais_votacao(3)

    count = 0
    qtdBucket = 20
    bucket_section = z.bucketing_locais(bucket_size=20, locais=secoes)
    for bk in bucket_section:
        print(bk)