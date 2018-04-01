from configparser import ConfigParser
import os


class Configuration(object):
    _conf = None

    @staticmethod
    def get_track_modifications():
        return False

    @classmethod
    def instatiate(cls):
        if cls._conf is None:
            cls._conf = ConfigParser()
            path = os.path.join(os.path.dirname(__file__), '', 'config.ini')
            cls._conf.read(path)


    @classmethod
    def get_section(cls, section_name):
        if cls._conf is None:
            Configuration.instatiate()
        try:
            val = cls._conf[section_name]
        except KeyError:
            val = None
        return val

    @classmethod
    def get_val(cls, section_name='', val_name=''):
        if cls._conf is None:
            Configuration.instatiate()
        try:
            val = cls._conf[section_name][val_name]
        except KeyError:
            val = None
        return val



if __name__ == '__main__':
    sct = Configuration.get_section(section_name="USUARIO")