#coding=utf8

import os

class Partition(object):

    def get_partitions(self):
        path = os.path.join(os.path.dirname(__file__), '', 'partitions.txt')
        arqPartition = open(path, 'r')
        lines = arqPartition.readlines()
        arqPartition.close()
        ret_lines = []
        for linha in lines:
            if linha.startswith('#'):
                continue
            ret_lines.append(linha)
        return ret_lines

if __name__ == '__main__':
    part = Partition()
    linhas = part.get_partitions()
    for linha in linhas:
        linhaSpl = linha.split()
        print('Partition name: {}'.format(linhaSpl[0]))

