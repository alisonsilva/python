from flask import Flask
from br.jus.tredf.analysis.backend.conf.databaseconfiguration import DatabaseConfiguration
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

app = Flask(__name__)
app.config.from_object(DatabaseConfiguration)
db = SQLAlchemy(app)
migrate = Migrate(app, db)

from br.jus.tredf.analysis.backend.model import models


