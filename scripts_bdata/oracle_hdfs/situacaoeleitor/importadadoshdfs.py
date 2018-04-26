#coding=utf8

from subprocess import Popen, PIPE
import sys
import fnmatch
import os
from os import listdir
from os.path import join
from oracle_hdfs.configuration import Configuration


class ImportDadosHdfs(object):

    def __get_percent_completado(self, qtd, atual):
        return atual*100/qtd

    def __update_progress(self, progress):
        sys.stdout.write('\r[{0}] {1}%'.format('#'*(progress/10) + ' '*(10 - (progress/10)), progress))
        sys.stdout.flush()

    def __run_cmd(self, args_list):
        """
        run linux commands
        """
        proc = Popen(args_list, stdout=PIPE, stderr=PIPE)
        s_output, s_err = proc.communicate()
        s_return = proc.returncode
        return s_return, s_output, s_err

    def __list_files(self):
        mypath = Configuration.get_val(section_name='ARQUIVO_SITUACAO_ELEITOR', val_name='caminho_destino')
        files = [f for f in fnmatch.filter(listdir(mypath), '*.arq')]
        return files

    def importa_dados_hdfs(self):
        files = self.__list_files()
        for f in files:
            print(f)

        hadoop_file = Configuration.get_val(section_name='ARQUIVO_SITUACAO_ELEITOR', val_name='arquivo_hadoop')
        path_to_files = Configuration.get_val(section_name='ARQUIVO_SITUACAO_ELEITOR', val_name='caminho_destino')

        print('Checking hadoop file {0}'.format(hadoop_file))

        (ret, out, err) = self.__run_cmd(['hdfs', 'dfs', '-test', '-e', hadoop_file])

        if ret == 0:
            print('Haddoop file already exists. Removing existing hadoop file {0}'.format(hadoop_file))
            (ret, out, err) = self.__run_cmd(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', hadoop_file])
        elif err:
            print('Error testing hadoop file {0} existence'.format(hadoop_file))
            print('Error: ' + str(err))
            return

        print('Creating a new directory on hadoop')
        (ret, out, err) = self.__run_cmd(['hdfs', 'dfs', '-mkdir', hadoop_file])
        if ret != 0:
            print('Error creating new file {0}'.format(hadoop_file))
            print('Error message: ' + str(err))

        qtd = 0
        for f in files:
            qtd += 1
            full_name = join(path_to_files, f)
            path_name_ok = join(path_to_files, 'ok')
            path_name_error = join(path_to_files, 'error')
            lst_number = f.split('_')
            lst_number = lst_number[len(lst_number)-1]
            part_name = join(hadoop_file, lst_number)
            print('Importing file: {0} to {1}'.format(full_name, part_name))
            (ret, out, err) = self.__run_cmd(['hdfs', 'dfs', '-put', full_name, part_name])
            if ret == 0:
                print('Import done successfully')
                print('Moving file to ok directory')
                full_name_ok = join(path_name_ok, f)
                (ret, out, err) = self.__run_cmd(['mv', full_name, full_name_ok])
                (ret, out, err) = self.__run_cmd(['tar', '-czf', full_name_ok + '.tar.gz', full_name_ok])
                (ret, out, err) = self.__run_cmd(['rm', '-f', full_name_ok])
            else:
                print('Error appending to hadoop new file: ' + str(err))
                full_name_error = join(path_name_error, f)
                full_name_error_log = join(path_name_error, f) + '.log'
                (ret, out, err) = self.__run_cmd(['mv', full_name, full_name_error])
                append_write = 'w'
                if os.path.exists(full_name_error_log):
                    append_write = 'a'

                fhandle = open(full_name_error_log, append_write)
                fhandle.write(out)
                fhandle.close()

                self.__update_progress(self.__get_percent_completado(qtd, len(files)))
