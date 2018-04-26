#coding=utf8


from oracle_hdfs.importadadoshdfs import ImportDadosHdfs


if __name__ == '__main__':
    importa_dados = ImportDadosHdfs()
    IMPORTAR = 1
    ATUALIZAR = 2
    flag_recover = True
    opt = 2
    while flag_recover:
        try:
            opt = int(input('(1 - Importar, 2 - Atualizar): '))
            if opt not in [IMPORTAR, ATUALIZAR]:
                print('>>>>>> Valor deve ser 1 ou 2 <<<<<<')
                continue
        except (ValueError, NameError, SyntaxError) as e:
            print('>>>>>> Valor deve ser 1 ou 2 <<<<<<')
        else:
            flag_recover = False

    secao_ini = str(input('Entre com o nome da seção ini: '))

    importa_dados.importa_dados_hdfs(opt, secao_ini)
