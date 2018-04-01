from datetime import datetime
from br.jus.tredf.analysis.backend.conf import db

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True)
    email = db.Column(db.String(120), index=True, unique=True)
    password_hash = db.Column(db.String(128))
    posts = db.relationship('Post', backref='author', lazy='dynamic')

    def __repr__(self):
        return '<User {}>'.format(self.username)

class Post(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    body = db.Column(db.String(140))
    timestamp = db.Column(db.DateTime, index=True, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))

    def __repr__(self):
        return '<Post {}>'.format(self.body)

class IpAddress(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    value = db.Column(db.String(20))
    log_entries = db.relationship('LogEntry', backref='ip_address', lazy='select')
    occurrences = db.relationship('Occurrence', backref='ip_address', lazy='select')

class LogEntry(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    instant = db.Column(db.DateTime, index=True, default=datetime.utcnow)
    request = db.Column(db.String(100))
    status = db.Column(db.Integer)
    user_agent = db.Column(db.String(255))
    ip_addressid = db.Column(db.Integer, db.ForeignKey('ip_address.id'), nullable=False)

class Occurrence(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    threshold = db.Column(db.Integer)
    duration = db.Column(db.String(20))
    start_date = db.Column(db.DateTime, default=datetime.utcnow)
    comments = db.Column(db.String(255))
    qtd_found = db.Column(db.Integer)
    ip_addressid = db.Column(db.Integer, db.ForeignKey("ip_address.id"), nullable=False)

