"""Correcting lazy parameter

Revision ID: f8e787e8e016
Revises: 
Create Date: 2018-02-15 22:13:59.833654

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'f8e787e8e016'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('ip_address', sa.Column('id', sa.Integer(), nullable=False))
    op.add_column('ip_address', sa.Column('value', sa.String(length=20), nullable=True))
    op.drop_index('VALUE', table_name='ip_address')
    op.drop_column('ip_address', 'VALUE')
    op.drop_column('ip_address', 'ID')
    op.add_column('log_entry', sa.Column('id', sa.Integer(), nullable=False))
    op.add_column('log_entry', sa.Column('instant', sa.DateTime(), nullable=True))
    op.add_column('log_entry', sa.Column('ip_addressid', sa.Integer(), nullable=False))
    op.add_column('log_entry', sa.Column('request', sa.String(length=100), nullable=True))
    op.add_column('log_entry', sa.Column('status', sa.Integer(), nullable=True))
    op.add_column('log_entry', sa.Column('user_agent', sa.String(length=255), nullable=True))
    op.create_index(op.f('ix_log_entry_instant'), 'log_entry', ['instant'], unique=False)
    op.drop_constraint('FKLOG_ENTRY267195', 'log_entry', type_='foreignkey')
    op.create_foreign_key(None, 'log_entry', 'ip_address', ['ip_addressid'], ['id'])
    op.drop_column('log_entry', 'USER_AGENT')
    op.drop_column('log_entry', 'IP_ADDRESSID')
    op.drop_column('log_entry', 'ID')
    op.drop_column('log_entry', 'STATUS')
    op.drop_column('log_entry', 'INSTANT')
    op.drop_column('log_entry', 'REQUEST')
    op.add_column('occurrence', sa.Column('comments', sa.String(length=255), nullable=True))
    op.add_column('occurrence', sa.Column('duration', sa.String(length=20), nullable=True))
    op.add_column('occurrence', sa.Column('id', sa.Integer(), nullable=False))
    op.add_column('occurrence', sa.Column('ip_addressid', sa.Integer(), nullable=False))
    op.add_column('occurrence', sa.Column('qtd_found', sa.Integer(), nullable=True))
    op.add_column('occurrence', sa.Column('start_date', sa.DateTime(), nullable=True))
    op.add_column('occurrence', sa.Column('threshold', sa.Integer(), nullable=True))
    op.drop_constraint('FKOCCURRENCE605496', 'occurrence', type_='foreignkey')
    op.create_foreign_key(None, 'occurrence', 'ip_address', ['ip_addressid'], ['id'])
    op.drop_column('occurrence', 'THRESHOLD')
    op.drop_column('occurrence', 'QTD_FOUND')
    op.drop_column('occurrence', 'COMMENTS')
    op.drop_column('occurrence', 'START_DATE')
    op.drop_column('occurrence', 'IP_ADDRESSID')
    op.drop_column('occurrence', 'ID')
    op.drop_column('occurrence', 'DURATION')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('occurrence', sa.Column('DURATION', mysql.VARCHAR(length=20), nullable=False))
    op.add_column('occurrence', sa.Column('ID', mysql.INTEGER(display_width=5), nullable=False))
    op.add_column('occurrence', sa.Column('IP_ADDRESSID', mysql.INTEGER(display_width=10), autoincrement=False, nullable=False))
    op.add_column('occurrence', sa.Column('START_DATE', mysql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), nullable=False))
    op.add_column('occurrence', sa.Column('COMMENTS', mysql.VARCHAR(length=255), nullable=False))
    op.add_column('occurrence', sa.Column('QTD_FOUND', mysql.INTEGER(display_width=5), autoincrement=False, nullable=False))
    op.add_column('occurrence', sa.Column('THRESHOLD', mysql.INTEGER(display_width=5), autoincrement=False, nullable=False))
    op.drop_constraint(None, 'occurrence', type_='foreignkey')
    op.create_foreign_key('FKOCCURRENCE605496', 'occurrence', 'ip_address', ['IP_ADDRESSID'], ['ID'])
    op.drop_column('occurrence', 'threshold')
    op.drop_column('occurrence', 'start_date')
    op.drop_column('occurrence', 'qtd_found')
    op.drop_column('occurrence', 'ip_addressid')
    op.drop_column('occurrence', 'id')
    op.drop_column('occurrence', 'duration')
    op.drop_column('occurrence', 'comments')
    op.add_column('log_entry', sa.Column('REQUEST', mysql.VARCHAR(length=100), nullable=False))
    op.add_column('log_entry', sa.Column('INSTANT', mysql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), nullable=False))
    op.add_column('log_entry', sa.Column('STATUS', mysql.INTEGER(display_width=3), autoincrement=False, nullable=False))
    op.add_column('log_entry', sa.Column('ID', mysql.INTEGER(display_width=10), nullable=False))
    op.add_column('log_entry', sa.Column('IP_ADDRESSID', mysql.INTEGER(display_width=10), autoincrement=False, nullable=False))
    op.add_column('log_entry', sa.Column('USER_AGENT', mysql.VARCHAR(length=255), nullable=False))
    op.drop_constraint(None, 'log_entry', type_='foreignkey')
    op.create_foreign_key('FKLOG_ENTRY267195', 'log_entry', 'ip_address', ['IP_ADDRESSID'], ['ID'])
    op.drop_index(op.f('ix_log_entry_instant'), table_name='log_entry')
    op.drop_column('log_entry', 'user_agent')
    op.drop_column('log_entry', 'status')
    op.drop_column('log_entry', 'request')
    op.drop_column('log_entry', 'ip_addressid')
    op.drop_column('log_entry', 'instant')
    op.drop_column('log_entry', 'id')
    op.add_column('ip_address', sa.Column('ID', mysql.INTEGER(display_width=10), nullable=False))
    op.add_column('ip_address', sa.Column('VALUE', mysql.VARCHAR(length=20), nullable=False))
    op.create_index('VALUE', 'ip_address', ['VALUE'], unique=True)
    op.drop_column('ip_address', 'value')
    op.drop_column('ip_address', 'id')
    # ### end Alembic commands ###