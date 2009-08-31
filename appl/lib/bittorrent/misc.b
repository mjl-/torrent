implement Misc;

include "torrentpeer.m";
	sys: Sys;
	rand: Rand;
include "ip.m";
	ipmod: IP;
	IPaddr: import ipmod;

init()
{
	sys = load Sys Sys->PATH;
	rand = load Rand Rand->PATH;
	rand->init(sys->pctl(0, nil)^sys->millisec());
	ipmod = load IP IP->PATH;
	ipmod->init();
}

randomize[T](a: array of T)
{
	for(i := 0; i < len a; i++) {
		j := rand->rand(len a);
		(a[i], a[j]) = (a[j], a[i]);
	}
}

maskinitialized := 0;
ip4mask:        IPaddr;
ip6mask:	IPaddr;

maskip(ipstr: string): string
{
	if(!maskinitialized) {
		(ok, mask) := IPaddr.parsemask(Torrentpeer->ip4maskstr);
		if(ok != 0)
			raise "bad ip4mask";
		ip4mask = mask;
		(ok, mask) = IPaddr.parsemask(Torrentpeer->ip6maskstr);
		if(ok != 0)
			raise "bad ip6mask";
		ip6mask = mask;
		maskinitialized = 1;
	}

        (ok, ip) := IPaddr.parse(ipstr);
        if(ok != 0)
                return ipstr;
	mask := ip4mask;
	if(!ip.isv4())
		mask = ip6mask;
        return ip.mask(mask).text();
}
