from br.jus.tredf.analysis.backend.conf import app, db
from br.jus.tredf.analysis.backend.model.models import User, Post, IpAddress, LogEntry, Occurrence


@app.shell_context_processor
def make_shell_context():
    return {'db': db,
            'User': User,
            'Post': Post,
            'IpAddress': IpAddress,
            'LogEntry': LogEntry,
            'Occurrence': Occurrence
            }