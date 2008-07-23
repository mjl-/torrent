implement Misc;

include "torrentget.m";
include "ip.m";

sys: Sys;
rand: Rand;
ipmod: IP;

IPaddr: import ipmod;

init(randmod: Rand)
{
	sys = load Sys Sys->PATH;
	rand = randmod;
	ipmod = load IP IP->PATH;
	ipmod->init();
}

randomize[T](a: array of T)
{
	for(i := 0; i < len a; i++) {
		newi := rand->rand(len a);
		tmp := a[i];
		a[i] = a[newi];
		a[newi] = tmp;
	}
}

sort[T](a: array of T, cmp: ref fn(a, b: T): int)
{ 
        for(i := 1; i < len a; i++) { 
                tmp := a[i]; 
                for(j := i; j > 0 && cmp(a[j-1], tmp) > 0; j--) 
                        a[j] = a[j-1]; 
                a[j] = tmp; 
        } 
}

readfile(f: string): (string, string)
{
	fd := sys->open(f, Sys->OREAD);
	if(fd == nil)
		return (nil, sys->sprint("open: %r"));
	(d, err) := readfd(fd);
	if(err != nil)
		return (nil, err);
	return (string d, err);
}

readfd(fd: ref Sys->FD): (array of byte, string)
{
	n := sys->readn(fd, buf := array[32*1024] of byte, len buf);
	if(n < 0)
		return (nil, sys->sprint("read: %r"));
	return (buf[:n], nil);
}

hex(d: array of byte): string
{
	s := "";
	for(i := 0; i < len d; i++)
		s += sys->sprint("%02x", int d[i]);
	return s;
}

l2a[T](l: list of T): array of T
{
	a := array[len l] of T;
	i := 0;
	for(; l != nil; l = tl l)
		a[i++] = hd l;
	return a;
}

maskinitialized := 0;
ip4mask:        IPaddr;
ip6mask:	IPaddr;

maskip(ipstr: string): string
{
	if(!maskinitialized) {
		(ok, mask) := IPaddr.parsemask(Torrentget->ip4maskstr);
		if(ok != 0)
			raise "bad ip4mask";
		ip4mask = mask;
		(ok, mask) = IPaddr.parsemask(Torrentget->ip6maskstr);
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
