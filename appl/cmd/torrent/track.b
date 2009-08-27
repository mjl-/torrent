implement Torrenttrack;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "string.m";
	str: String;
include "rand.m";
	rand: Rand;
include "tables.m";
	tables: Tables;
	Strhash: import tables;
include "ip.m";
	ip: IP;
	IPaddr: import ip;
include "bittorrent.m";
	bittorrent: Bittorrent;
	Bee: import bittorrent;
include "cgi.m";
	cgi: Cgi;
	Fields: import cgi;

Torrenttrack: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

dflag:	int;
statefile:	string;
hashesfile:	string;
interval := 600;
maxpeers := 30;

Peer: adt {
	peerid:	array of byte;
	ip:	IPaddr;
	port:	int;
};

Tracked: adt {
	hash:	array of byte;
	hashhex:	string;
	peers:	array of ref Peer;

	stir:	fn(t: self ref Tracked);
	remove:	fn(t: self ref Tracked, ip: IPaddr, port: int);
	have:	fn(t: self ref Tracked, ip: IPaddr, port: int): int;
	add:	fn(t: self ref Tracked, p: ref Peer);
};

Trackreq: adt {
	hash,
	peerid:		array of byte;
	hashhex,
	peeridhex:	string;
	host:	string;
	ip:	IPaddr;
	port:	int;
	up,
	down,
	left:	big;
	event:	string;
	compact:	int;
};

hashes: ref Strhash[string];
tracked: ref Strhash[ref Tracked];

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	str = load String String->PATH;
	rand = load Rand Rand->PATH;
	rand->init(sys->millisec()^sys->pctl(0, nil));
	tables = load Tables Tables->PATH;
	ip = load IP IP->PATH;
	ip->init();
	cgi = load Cgi Cgi->PATH;
	cgi->init();
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init();

	tracked = tracked.new(64, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-d] [-h hashesfile] [-i interval] [-m maxpeers] [-s statefile] scgiaddr");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	dflag++;
		'h' =>	hashesfile = arg->arg();
		'i' =>	interval = int arg->arg();
		'm' =>	maxpeers = int arg->arg();
		's' =>	statefile = arg->arg();
		* =>	arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();
	scgiaddr := hd args;
	if(hashesfile != nil)
		readhashes();
	if(statefile != nil)
		readstate();

	(aok, aconn) := sys->announce(scgiaddr);
	if(aok < 0)
		fail(sprint("announce %q: %r", scgiaddr));
	for(;;) {
		(ok, conn) := sys->listen(aconn);
		if(ok < 0) {
			warn(sprint("listen: %r"));
			continue;
		}
		fd := sys->open(conn.dir+"/data", Sys->ORDWR);
		if(fd == nil) {
			warn(sprint("open connection data: %r"));
			continue;
		}
		{
			# note: we're doing one request at a time.
			# if we can block at all (to httpd (to client)), we should make sure it isn't for too long.
			serve(fd);
		} exception e {
		"scgi:*" =>
			err := e[len "scgi:":];
			bresp := ref Bee.Dict (array[] of {Bee.makekey("failure reason", ref Bee.String (array of byte err))});
			sys->fprint(fd, "status: 200 ok\r\n\r\n%s", string bresp.pack());
		"http:*" =>
			sys->fprint(fd, "status: 500 fail\r\ncontent-type: text/plain; charset=utf-8\r\n\r\n%s\n", e[len "scgi:":]);
		}
		fd = nil;
	}
}

error(s: string)
{
	raise "scgi:"+s;
}

httperror(s: string)
{
	raise "http:"+s;
}

readscgi(b: ref Iobuf): ref Strhash[array of byte]
{
	lenstr := "";
length:	for(;;)
		case c := b.getc() {
		bufio->EOF =>
			error("eof reading scgi request length");
		bufio->ERROR =>
			error(sprint("reading scgi request: %r"));
		':' =>
			break length;
		'0' to '9' =>
			lenstr[len lenstr] = c;
		* =>
			error(sprint("bad char %c (%d) reading scgi request: %r", c, c));
		}
	n := int lenstr;
	if(n > 64*1024)
		error(sprint("bogusly long scgi request of %d bytes", n));
	h := 0;
	d := array[n] of byte;
	while(h < n) {
		nn := b.read(d[h:], n-h);
		if(nn < 0)
			error(sprint("reading scgi request body: %r"));
		if(nn == 0)
			error(sprint("short scgi request, need %d, got %d", n, h));
		h += nn;
	}

	r := Strhash[array of byte].new(8, nil);
	o := 0;
	while(o < len d) {
		(k, no) := get(d, o);
		(v, nno) := get(d, no);
		if(o == no || nno == no)
			error("malformatted scgi request");
		r.add(string k, v);
		o = nno;
	}
	return r;
}

get(d: array of byte, s: int): (array of byte, int)
{
	for(e := s; e < len d; e++)
		if(d[e] == byte 0)
			return (d[s:e], e+1);
	return (nil, s);
}

get20(f: ref Fields, k: string): array of byte
{
	v := f.getbytes(k);
	if(len v == 0)
		error(sprint("key %#q missing/empty", k));
	if(len v != 20)
		error(sprint("key %#q is %d bytes long, expected %d", k, len v, 20));
	return v;
}

getint(f: ref Fields, k: string): int
{
	v := f.get(k);
	if(len v == 0)
		error(sprint("key %#q missing/empty", k));
	(r, rem) := str->toint(v, 10);
	if(rem != nil)
		error(sprint("key %#q has bad value", k));
	return r;
}

getbig(f: ref Fields, k: string): big
{
	v := f.get(k);
	if(len v == 0)
		error(sprint("key %#q missing/empty", k));
	(r, rem) := str->tobig(v, 10);
	if(rem != nil)
		error(sprint("key %#q has bad value", k));
	return r;
}

trackrequest(f: ref Fields, remip: IPaddr): ref Trackreq
{
	hash :=	get20(f, "info_hash");
	peerid := get20(f, "peer_id");
	host := f.get("ip");  # optional
	port := getint(f, "port");
	up := getbig(f, "uploaded");
	down := getbig(f, "downloaded");
	left := getbig(f, "left");
	event := f.get("event");
	case event {
	"" or
	"started" or
	"completed" or
	"stopped" =>	;
	* =>	error(sprint("bad 'event' %#q", event));
	}
	compact := int f.get("compact");

	return ref Trackreq (hash, peerid, hex(hash), hex(peerid), host, remip, port, up, down, left, event, compact);
}

serve(fd: ref Sys->FD)
{
	b := bufio->fopen(fd, bufio->OREAD);
	if(b == nil)
		error(sprint("fopen: %r"));
	
	d := readscgi(b);
	ipstr := string d.find("REMOTE_ADDR");
	(ipok, remip) := IPaddr.parse(ipstr);
	if(ipok != 0)
		error(sprint("bad remote ip address %#q", ipstr));

	case rm := string d.find("REQUEST_METHOD") {
	"GET" or
	"HEAD" =>	;
	* =>	error(sprint("unexpected request method %q", rm));
	}

	qsbuf := d.find("QUERY_STRING");
	if(qsbuf == nil)
		error("query string is missing");
	f := cgi->unpack(string qsbuf);
	tr := trackrequest(f, remip);

	if(hashes != nil && hashes.find(tr.hashhex) == nil)
		error(sprint("no such hash %#q", tr.hashhex));

	t := tracked.find(tr.hashhex);
	if(t == nil) {
say(sprint("new torrent, hash %q", tr.hashhex));
		t = ref Tracked (tr.hash, tr.hashhex, nil);
		tracked.add(tr.hashhex, t);
	}

	npeers := maxpeers;
	case tr.event {
	"stopped" =>
		npeers = 0;
		t.remove(tr.ip, tr.port);
say(sprint("removing peer %q", tr.ip.text()));
	* =>
		if(npeers > len t.peers)
			npeers = len t.peers;
		if(!t.have(tr.ip, tr.port)) {
say(sprint("adding peer %q", tr.ip.text()));
			t.add(ref Peer (tr.peerid, tr.ip, tr.port));
		}
	}
say(sprint("returning %d peers to %s!%d (%q)", npeers, tr.ip.text(), tr.port, tr.host));
	return respondpeers(fd, tr, t.peers[:npeers]);
}

respondpeers(fd: ref Sys->FD, tr: ref Trackreq, a: array of ref Peer)
{
	peersdict: ref Bee;
	if(tr.compact) {
		n := maxpeers;
		if(n > len a)
			n = len a;
		r := array[6*n] of byte;
		h := 0;
		for(i := 0; i < n; i++) {
			p := a[i];
			if(!p.ip.isv4())
				continue;
			r[h*6:] = p.ip.v4();
			p16(r[h*6+4:], p.port);
			h++;
say(sprint("compact, %s!%d", p.ip.text(), p.port));
		}
		r = r[:6*h];
		peersdict = ref Bee.String (r);
	} else {
		n := maxpeers;
		if(n > len a)
			n = len a;
		r := array[n] of ref Bee;
		for(i := 0; i < n; i++) {
			p := a[i];
			bpeerid := Bee.makekey("id", ref Bee.String (p.peerid));
			bip := Bee.makekey("ip", ref Bee.String (array of byte p.ip.text()));
			bport := Bee.makekey("port", ref Bee.Integer (big p.port));
			r[i] = ref Bee.Dict (array[] of {bpeerid, bip, bport});
say(sprint("big, %s!%d", p.ip.text(), p.port));
		}
		peersdict = ref Bee.List (r);
	}
	binterval := Bee.makekey("interval", ref Bee.Integer (big interval));
	bpeers := Bee.makekey("peers", peersdict);
	bresp := ref Bee.Dict (array[] of {binterval, bpeers});
	buf := bresp.pack();
	if(sys->fprint(fd, "status: 200 ok\r\n\r\n") < 0 || sys->write(fd, buf, len buf) != len buf)
		httperror(sprint("write: %r"));
}

Tracked.stir(t: self ref Tracked)
{
	rand->init(sys->millisec()^sys->pctl(0, nil));
	n := maxpeers;
	if(n > len t.peers)
		n = len t.peers;
	for(i := 0; i < n; i++) {
		j := rand->rand(len t.peers);
		(t.peers[i], t.peers[j]) = (t.peers[j], t.peers[i]);
	}
}

Tracked.remove(t: self ref Tracked, ip: IPaddr, port: int)
{
	for(i := 0; i < len t.peers; i++)
		if(t.peers[i].port == port && t.peers[i].ip.eq(ip)) {
			t.peers[i] = t.peers[len t.peers-1];
			t.peers = t.peers[:len t.peers-1];
			return;
		}
}

Tracked.have(t: self ref Tracked, ip: IPaddr, port: int): int
{
	for(i := 0; i < len t.peers; i++)
		if(t.peers[i].port == port && t.peers[i].ip.eq(ip))
			return 1;
	return 0;
}

Tracked.add(t: self ref Tracked, p: ref Peer)
{
	npeers := array[len t.peers+1] of ref Peer;
	npeers[:] = t.peers;
	npeers[len t.peers] = p;
	t.peers = npeers;
}

readhashes()
{
	hashes = hashes.new(32, nil);
	b := bufio->open(hashesfile, Bufio->OREAD);
	if(b == nil)
		fail(sprint("open %q: %r", hashesfile));
	nr := 0;
	for(;;) {
		l := b.gets('\n');
		if(l == nil)
			break;
		nr++;
		if(l[len l-1] == '\n')
			l = l[:len l];
		if(len l != 40)
			fail(sprint("%q:%d: bad line, expected hash of 40 chars, saw %d chars", hashesfile, nr, len l));
		hashes.add(l, l);
	}
}

readstate()
{
	fail("xxx not yet implemented");
}

p16(d: array of byte, v: int)
{
	d[0] = byte (v>>8);
	d[1] = byte (v>>0);
}

hex(d: array of byte): string
{
	s := "";	
	for(i := 0; i < len d; i++)
		s += sprint("%02x", int d[i]);
	return s;
}

warn(s: string)
{
	sys->fprint(sys->fildes(2), "%s\n", s);
}

say(s: string)
{
	if(dflag)
		warn(s);
}

fail(s: string)
{
	warn(s);
	raise "fail:"+s;
}
