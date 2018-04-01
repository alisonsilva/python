from br.jus.tredf.analysis.backend.conf.configuration import Configuration
import os

class DatabaseConfiguration:
    basedir = os.path.abspath(os.path.dirname(__file__))
    #    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
    #        'sqlite:///' + os.path.join(basedir, 'app.db')
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
                              Configuration.get_val(section_name='DATA_BASE', val_name='db_uri')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

if __name__ == '__main__':
    dbc = DatabaseConfiguration()
    print(dbc.SQLALCHEMY_DATABASE_URI)