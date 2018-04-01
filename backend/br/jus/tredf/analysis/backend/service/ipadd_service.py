from br.jus.tredf.analysis.backend.model.ipadd_dao import IpAddressDao

class IpAddressService():

    def listaIps(self):
        ipadd_dao = IpAddressDao()
        ips = list(ipadd_dao.todosIps())
        return ips

if __name__ == '__main__':
    ipaddress_serv = IpAddressService()
    ips = ipaddress_serv.listaIps()
    print('qtd ips: ',len(ips) )
    for ip in ips:
        print('id:{}, ip: {}'.format(ip.id, ip.value))