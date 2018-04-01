import ssl
from ldap3 import Server, Connection, SUBTREE, Tls
from ldap3.core.exceptions import LDAPBindError, LDAPCommunicationError
from br.jus.tredf.analysis.backend.conf.configuration import Configuration
from flask import json


class LdapService(object):
    def user_info(self, username, password):
        conf = Configuration()
        search_base = conf.get_val('LDAP', 'ldap_base_dn')
        search_filter = conf.get_val('LDAP', 'ldap_search_filter')

        try:
            conn = self.connect_to_server(username, password)
            conn.search(search_base=search_base,
                        search_filter=search_filter.format(username),
                        search_scope=SUBTREE,
                        attributes=["userPrincipalName", "name", "mail", "displayName", "userAccountControl"],
                        get_operational_attributes=True)
        except (LDAPBindError, LDAPCommunicationError):
            return None
        return json.loads(conn.response_to_json())

    def connect_to_server(self, username, password):
        conf = Configuration()
        host = conf.get_val('LDAP', 'ldap_host')
        domain = conf.get_val('LDAP', 'ldap_domain')
        tls_configuration = Tls(validate=ssl.CERT_NONE, version=ssl.PROTOCOL_SSLv23)
        user = '{}@{}'.format(username, domain)
        server = Server(host, use_ssl=True, tls=tls_configuration)
        conn = Connection(server,
                          user=user,
                          password=password,
                          auto_bind=True)
        return conn

    def connect(self):
        conf = Configuration()
        user = conf.get_val('LDAP', 'ldap_user')
        psswd = conf.get_val('LDAP', 'ldap_password')
        try:
            conn = self.connect_to_server(user, psswd)
        except (LDAPBindError, LDAPCommunicationError):
            return None
        return conn


if __name__ == '__main__':
    ldap = LdapService()
    #con = ldap.connect()
    #print(con)
    print(ldap.user_info('alison.silva', 'AliSil12'))
