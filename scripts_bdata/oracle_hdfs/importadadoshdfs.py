#!/usr/bin/python
# coding=utf8

from subprocess import Popen, PIPE
import sys
import fnmatch
import os
import datetime
from os import listdir
from os.path import join
from oracle_hdfs.configuration import Configuration


class ImportDadosHdfs(object):

    @staticmethod
    def __get_percent_completado(qtd, atual):
        return atual*100/qtd

    @staticmethod
    def __update_progress(progress):
        sys.stdout.write('\r[{0}] {1}%'.format('#'*(progress/10) + ' '*(10 - (progress/10)), progress))
        sys.stdout.flush()

    @staticmethod
    def __run_cmd(args_list):
        """
        run linux commands
        """
        proc = Popen(args_list, stdout=PIPE, stderr=PIPE)
        s_output, s_err = proc.communicate()
        s_return = proc.returncode
        return s_return, s_output, s_err

    @staticmethod
    def __list_files(ini_section_name):
        mypath = Configuration.get_val(section_name=ini_section_name, val_name='caminho_destino')
        if not mypath:
            return []
        files = [f for f in fnmatch.filter(listdir(mypath), '*.arq')]
        return files

    def importa_dados_hdfs(self, opt, ini_section_name):
        '''
        Import the data from ELEITOR database into HDFS.
        :param opt: 1 - Import, 2 - Update
        :param ini_section_name: define the section in the ini file
        :return
        '''
        files = ImportDadosHdfs.__list_files(ini_section_name)
        for f in files:
            print(f)

        hadoop_file = Configuration.get_val(section_name=ini_section_name, val_name='arquivo_hadoop')
        if not hadoop_file:
            print('It was not possible to find hadoop file path on ini file according to the section {0}'.
                  format(ini_section_name))
            exit(1)

        path_to_files = Configuration.get_val(section_name=ini_section_name, val_name='caminho_destino')

        print('Checking hadoop file {0}'.format(hadoop_file))

        (ret, out, err) = ImportDadosHdfs.__run_cmd(['hdfs', 'dfs', '-test', '-e', hadoop_file])

        if opt == 1:
            if ret == 0:
                print('Haddoop file already exists. Removing existing hadoop file {0}'.format(hadoop_file))
                (ret, out, err) = ImportDadosHdfs.__run_cmd(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', hadoop_file])
            elif err:
                print('Error testing hadoop file {0} existence'.format(hadoop_file))
                print('Error: ' + str(err))
                return

            print('Creating a new directory on hadoop')
            (ret, out, err) = ImportDadosHdfs.__run_cmd(['hdfs', 'dfs', '-mkdir', hadoop_file])
            if ret != 0:
                print('Error creating new file {0}'.format(hadoop_file))
                print('Error message: ' + str(err))
        elif opt == 2:
            if ret != 0:
                print('Creating a new directory on hadoop')
                (ret, out, err) = ImportDadosHdfs.__run_cmd(['hdfs', 'dfs', '-mkdir', hadoop_file])
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
            print('Importing file: {0} to {1} - {2}/{3}'.format(full_name, part_name, qtd, len(files)))
            if opt == 1:
                (ret, out, err) = ImportDadosHdfs.__run_cmd(['hdfs', 'dfs', '-put', full_name, part_name])
            elif opt == 2:
                (ret, out, err) = ImportDadosHdfs.__run_cmd(['hdfs', 'dfs', '-appendToFile', full_name, part_name])
            if ret == 0:
                now = datetime.datetime.now()
                date_tag = '%d%02d%02d-%02d%02d' % (now.year, now.month, now.day, now.hour, now.minute)
                print('Import done successfully')
                print('Moving file to ok directory')
                full_name_ok = join(path_name_ok, f)
                (ret, out, err) = ImportDadosHdfs.__run_cmd(['mv', full_name, full_name_ok])
                (ret, out, err) = ImportDadosHdfs.__run_cmd(['tar', '-czf', full_name_ok + '_' + date_tag + '.tar.gz', full_name_ok])
                (ret, out, err) = ImportDadosHdfs.__run_cmd(['rm', '-f', full_name_ok])
            else:
                print('Error appending to hadoop new file: ' + str(err))
                full_name_error = join(path_name_error, f)
                full_name_error_log = join(path_name_error, f) + '.log'
                (ret, out, err) = ImportDadosHdfs.__run_cmd(['mv', full_name, full_name_error])
                append_write = 'w'
                if os.path.exists(full_name_error_log):
                    append_write = 'a'

                fhandle = open(full_name_error_log, append_write)
                fhandle.write(out)
                fhandle.close()

                ImportDadosHdfs.__update_progress(ImportDadosHdfs.__get_percent_completado(qtd, len(files)))
