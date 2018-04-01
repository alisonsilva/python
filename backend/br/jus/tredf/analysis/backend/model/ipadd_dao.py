from br.jus.tredf.analysis.backend.model.models import IpAddress



class IpAddressDao():

    def todosIps(self):
        ips = IpAddress.query.all()
        return ips


if __name__ == '__main__':
    ipaddress_dao = IpAddressDao()
    ips = ipaddress_dao.todosIps()
    ips = list(ips)
    print('qtd ips: ',len(ips) )
    for ip in ips:
        print('id:{}, ip: {}'.format(ip.id, ip.value))
