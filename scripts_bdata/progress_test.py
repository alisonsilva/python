#coding=utf8

import time
import sys

def update_progress(progress):
    '''
        Imprime um barra de progresso.
        O parametro progress está definido em termos de permecentual
    '''
    sys.stdout.write('\r[{0}] {1}%'.format('#'*(progress/10) + ' '*(10 - (progress/10)), progress))
    sys.stdout.flush()

for i in range(100):
    time.sleep(0.3)
    update_progress(i)
