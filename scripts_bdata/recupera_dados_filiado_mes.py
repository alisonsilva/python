# coding=utf8

import oracle_hdfs.filiadomes.escrita as escrita

if __name__ == '__main__':
    exit_loop = False
    UF_BRASILIA = 3
    UF_ZZ = 26
    uf = 3
    escrita.executa_recuperacao_ft_ss_filiado_mes(UF_BRASILIA)
'''    
        while not exit_loop:
        try:
            uf = int(input('Entre com a uf (3 - Brasília, 26 - ZZ): '))
            if uf not in [UF_BRASILIA, UF_ZZ]:
                print('>>>>>> Valor deve ser 3 ou 26 <<<<<<')
                continue
        except (ValueError, NameError, SyntaxError) as e:
            print('>>>>>> Valor deve ser 3 ou 26 <<<<<<')
        else:
            exit_loop = True
'''
#   escrita.executa_recuperacao_ft_ss_situacao_eleitor(uf)




