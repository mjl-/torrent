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
include "daytime.m";
	daytime: Daytime;
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
statefile0,
statefile1:	string;
sffd0, sffd1: ref Sys->FD;
statelast:	int;
flushtime:	int;
hashesfile:	string;
interval := 1200;
maxpeers := 30;
dialvalid := 3600;
Dialtime: con 15*1000;
Timeslack: con 30;

# Peer.state, reachability
Trying, Good, Bad: con iota;
Peer: adt {
	peerid:	array of byte;
	ip:	IPaddr;
	port:	int;
	mtime:	int;
	dialtime:	int;
	state:	int;
};

Tracked: adt {
	hash:	array of byte;
	hashhex:	string;
	peers:	array of ref Peer;

	stir:	fn(t: self ref Tracked, want: int): int;
	remove:	fn(t: self ref Tracked, ip: IPaddr, port: int, valid: int);
	find:	fn(t: self ref Tracked, ip: IPaddr, port: int): ref Peer;
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
verifyc: chan of (ref Peer, int);

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	str = load String String->PATH;
	daytime = load Daytime Daytime->PATH;
	rand = load Rand Rand->PATH;
	rand->init(sys->millisec()^sys->pctl(0, nil));
	tables = load Tables Tables->PATH;
	ip = load IP IP->PATH;
	ip->init();
	cgi = load Cgi Cgi->PATH;
	cgi->init();
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init();

	sys->pctl(Sys->NEWPGRP, nil);

	tracked = tracked.new(64, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-d] [-h hashesfile] [-i interval] [-m maxpeers] [-s statefile statefile flushtime] scgiaddr");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	dflag++;
		'h' =>	hashesfile = arg->arg();
		'i' =>	interval = int arg->arg();
		'm' =>	maxpeers = int arg->arg();
		's' =>	statefile0 = arg->arg();
			statefile1 = arg->arg();
			sffd0 = sys->open(statefile0, Sys->ORDWR);
			if(sffd0 == nil)
				fail(sprint("open %q: %r", statefile0));
			sffd1 = sys->open(statefile1, Sys->ORDWR);
			if(sffd1 == nil)
				fail(sprint("open %q: %r", statefile1));
			flushtime = int arg->arg();
		* =>	arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();
	scgiaddr := hd args;
	if(hashesfile != nil)
		readhashes();
	if(statefile0 != nil)
		readstate();

	(aok, aconn) := sys->announce(scgiaddr);
	if(aok < 0)
		fail(sprint("announce %q: %r", scgiaddr));

	spawn main(aconn);
}

main(aconn: Sys->Connection)
{
	spawn listener(aconn, lc := chan of ref Sys->FD);
	flushc := chan of int;
	verifyc = chan of (ref Peer, int);

	flushpid := -1;
	for(;;)
	alt {
	fd := <-lc =>
		{
			# note: we're doing one request at a time.
			# if we can block at all (to httpd (to client)), we should make sure it isn't for too long.
			serve(fd);
			if(flushpid < 0 && statefile0 != nil) {
				spawn flusher(flushtime, flushc);
				flushpid = <-flushc;
			}
		} exception e {
		"scgi:*" =>
			err := e[len "scgi:":];
			bresp := ref Bee.Dict (array[] of {Bee.makekey("failure reason", ref Bee.String (array of byte err))});
			sys->fprint(fd, "status: 200 ok\r\n\r\n%s", string bresp.pack());
		"http:*" =>
			sys->fprint(fd, "status: 500 fail\r\ncontent-type: text/plain; charset=utf-8\r\n\r\n%s\n", e[len "scgi:":]);
		}
		fd = nil;
		
	<-flushc =>
		writestate();
		flushpid = -1;

	(p, state) := <-verifyc =>
		p.dialtime = daytime->now();
		p.state = state;
		say(sprint("checked peer %s!%d, state %d", p.ip.text(), p.port, p.state));
	}
}

flusher(secs: int, c: chan of int)
{
	c <-= sys->pctl(0, nil);
	sys->sleep(secs*1000);
	c <-= 0;
}

listener(aconn: Sys->Connection, lc: chan of ref Sys->FD)
{
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
		lc <-= fd;
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

killer(pid: int, p: ref Peer, pidc: chan of int)
{
	pidc <-= sys->pctl(0, nil);
	sys->sleep(Dialtime);
	kill(pid);
	verifyc <-= (p, Bad);
}

checkpeer(p: ref Peer)
{
	spawn killer(sys->pctl(0, nil), p, pidc := chan of int);
	killerpid := <-pidc;
	addr := sprint("net!%s!%d", p.ip.text(), p.port);
	(ok, nil) := sys->dial(addr, nil);
	kill(killerpid);
	state := Good;
	if(ok < 0)
		state = Bad;
say(sprint("dial %q returned %d, %r", addr, ok));
	verifyc <-= (p, state);
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

	npeers := 0;
	case tr.event {
	"stopped" =>
		t.remove(tr.ip, tr.port, daytime->now()-Timeslack-interval);
say(sprint("removing peer %q", tr.ip.text()));

	* =>
		p := t.find(tr.ip, tr.port);
		if(p == nil) {
say(sprint("adding peer %q", tr.ip.text()));
			p = ref Peer (tr.peerid, tr.ip, tr.port, 0, 0, Trying);
			t.add(p);
		}
		now := p.mtime = daytime->now();
		if(p.dialtime < now-Timeslack-dialvalid) {
say(sprint("going to check peer"));
			p.state = Trying;
			spawn checkpeer(p);
		}
		npeers = t.stir(maxpeers);
	}
say(sprint("returning %d peers to %s!%d (%q)", npeers, tr.ip.text(), tr.port, tr.host));
	respondpeers(fd, tr, t.peers[:npeers]);
}

respondpeers(fd: ref Sys->FD, tr: ref Trackreq, a: array of ref Peer)
{
	peersdict: ref Bee;
	peers6dict: ref Bee;
	if(tr.compact) {
		n := maxpeers;
		if(n > len a)
			n = len a;
		r4 := array[6*n] of byte;
		r6 := array[18*n] of byte;
		h4 := 0;
		h6 := 0;
		for(i := 0; i < n; i++) {
			p := a[i];
			if(p.ip.isv4()) {
				r4[h4*6:] = p.ip.v4();
				p16(r4[h4*6+4:], p.port);
				h4++;
			} else {
				r6[h6*18:] = p.ip.v6();
				p16(r6[h6*18+16:], p.port);
				h6++;
			}
say(sprint("compact, %s!%d", p.ip.text(), p.port));
		}
		peersdict = ref Bee.String (r4[:h4*6]);
		peers6dict = ref Bee.String (r6[:h6*18]);
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
	extip: array of byte;
	if(tr.ip.isv4())
		extip = tr.ip.v4();
	else
		extip = tr.ip.v6();
	bpeerip := Bee.makekey("external ip", ref Bee.String (extip));
	respvals := array[] of {binterval, bpeers, bpeerip};
	if(tr.compact && peers6dict != nil) {
		bpeers6 := Bee.makekey("peers6", peers6dict);
		respvals = array[] of {binterval, bpeers, bpeers6, bpeerip};
	}
	bresp := ref Bee.Dict (respvals);
	buf := bresp.pack();
	if(sys->fprint(fd, "status: 200 ok\r\n\r\n") < 0 || sys->write(fd, buf, len buf) != len buf)
		httperror(sprint("write: %r"));
}

Tracked.stir(t: self ref Tracked, want: int): int
{
	valid := daytime->now()-Timeslack-interval;

	have := len t.peers;
	while(have > 0 && t.peers[have-1].state == Bad)
		have--;
say(sprint("stir, want %d, have %d, len peers %d", want, have, len t.peers));

	rand->init(sys->millisec()^sys->pctl(0, nil));
	i := 0;
	while(i < want && i < have) {
		j := i+rand->rand(have-1-i);
		(t.peers[i], t.peers[j]) = (t.peers[j], t.peers[i]);

		if(t.peers[i].mtime < valid) {
say(sprint("stir, peer no longer valid, removing %s!%d", t.peers[i].ip.text(), t.peers[i].port));
			have--;
			(t.peers[i], t.peers[have]) = (t.peers[have], t.peers[len t.peers-1]);
			t.peers = t.peers[:len t.peers-1];
			continue;
		}
		if(t.peers[i].state == Bad) {
say(sprint("stir, peer is bad, ignoring %s!%d", t.peers[i].ip.text(), t.peers[i].port));
			have--;
			(t.peers[i], t.peers[have]) = (t.peers[have], t.peers[i]);
			continue;
		}
say(sprint("stir, peer is good, returning %s!%d", t.peers[i].ip.text(), t.peers[i].port));
		i++;
	}
	return i;
}

Tracked.remove(t: self ref Tracked, ip: IPaddr, port: int, valid: int)
{
	for(i := 0; i < len t.peers; i++) {
		match := t.peers[i].port == port && t.peers[i].ip.eq(ip);
		old := t.peers[i].mtime < valid;
		if(match || old) {
			t.peers[i] = t.peers[len t.peers-1];
			t.peers = t.peers[:len t.peers-1];
			if(match)
				return;
		}
	}
}

Tracked.find(t: self ref Tracked, ip: IPaddr, port: int): ref Peer
{
	for(i := 0; i < len t.peers; i++)
		if(t.peers[i].port == port && t.peers[i].ip.eq(ip))
			return t.peers[i];
	return nil;
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


mtime(b: ref Iobuf): int
{
	s := b.gets('\n');
	if(s == nil)
		return -1;
	if(s[len s-1] == '\n')
		s = s[:len s-1];
	(i, rem) := str->toint(s, 10);
	if(rem != nil)
		return -1;
	return i;
}

parseerror(s: string)
{
	raise "parse:"+s;
}

unhexc(c: int): int
{
	if(c >= '0' && c <= '9')
		return c-'0';
	if(c >= 'a' && c <= 'f')
		return c-'a'+10;
	if(c >= 'A' && c <= 'F')
		return c-'A'+10;
	parseerror(sprint("bad hex char %c (%d)", c, c));
	return -1;  # not reached
}

unhex(hash: string): array of byte
{
	if(len hash != 40)
		parseerror(sprint("bad hex hash/peerid, expected 40 chars, saw %d", len hash));
	d := array[20] of byte;
	o := 0;
	for(i := 0; i < len hash; i += 2)
		d[o++] = byte ((unhexc(hash[i])<<4) | unhexc(hash[i+1]));
	return d;
}

parsestate(now: int, b: ref Iobuf): int
{
	tracked = tracked.new(64, nil);

	{
		# format:
		# mtime (already read)
		# peer $hash $peerid $ip $port $mtime $dialtime $state
		for(;;) {
			l := b.gets('\n');
			if(l == nil)
				break;
			if(l[len l-1] == '\n')
				l = l[:len l-1];
			toks := str->unquoted(l);
			if(toks == nil)
				parseerror("bad line in statefile");
			c := hd toks;
			toks = tl toks;
			case c {
			"peer" =>
				if(len toks != 7)
					parseerror(sprint("bad 'peer', expected 7 tokens, saw %d", len toks));
				hashhex := hd toks;
				peeridhex := hd tl toks;
				ipstr := hd tl tl toks;
				portstr := hd tl tl tl toks;
				mtimestr := hd tl tl tl tl toks;
				dialtime := int hd tl tl tl tl tl toks;
				state := int hd tl tl tl tl tl tl toks;
				case state {
				Trying or Bad or Good =>	;
				* =>	parseerror(sprint("bad state %d", state));
				}

				hash := unhex(hashhex);
				peerid := unhex(peeridhex);
				(ipok, iprem) := IPaddr.parse(ipstr);
				if(ipok != 0)
					parseerror(sprint("bad ip %#q", ipstr));
				(port, rem0) := str->toint(portstr, 10);
				if(rem0 != nil)
					parseerror("bad 'port'");
				(mtime, rem1) := str->toint(mtimestr, 10);
				if(rem1 != nil)
					parseerror("bad 'mtime'");
				if(mtime < now-Timeslack-interval)
					continue;  # stale peer
				t := tracked.find(hashhex);
				if(hashes != nil && t == nil)
					parseerror(sprint("peer for unknown torrent %#q", hashhex));
				if(t == nil) {
					t = ref Tracked (hash, hashhex, nil);
					tracked.add(hashhex, t);
				}
				if(t.find(iprem, port) != nil)
					parseerror(sprint("duplicate peer %s!%d", iprem.text(), port));
				t.add(ref Peer (peerid, iprem, port, mtime, dialtime, state));
			* =>
				say(sprint("bad token %#q in statefile", c));
				return -1;
			}
		}
	} exception e {
	"parse:*" =>
		err := e[len "parse:":];
		say("parsing state: "+err);
		return -1;
	}
	return 1;
}

readstate()
{
	now := daytime->now();
	sys->seek(sffd0, big 0, Sys->SEEKSTART);
	sys->seek(sffd1, big 0, Sys->SEEKSTART);

	b0 := bufio->fopen(sffd0, Bufio->OREAD);
	b1 := bufio->fopen(sffd1, Bufio->OREAD);
	mtime0 := mtime(b0);
	mtime1 := mtime(b1);
	statelast = 0;
	if(mtime0 < 0 && mtime1 < 0)
		return;
	(f, l) := (b0, b1);
	if(mtime1 > mtime0) {
		statelast = 1;
		(f, l) = (b1, b0);
		(mtime0, mtime1) = (mtime1, mtime0);
	}
	if(parsestate(now, f) < 0 && mtime1 >= 0) {
		statelast = 1-statelast;
		parsestate(now, l);
	}
}

writestate()
{
	fd := sffd0;
	if(statelast)
		fd = sffd1;

	if(sys->seek(fd, big 0, Sys->SEEKSTART) != big 0)
		return warn(sprint("seek statefile to 0: %r"));
	nulldir := sys->nulldir;
	nulldir.length = big 0;
	if(sys->fwstat(fd, nulldir) != 0)
		return warn(sprint("truncating statefile: %r"));
	now := daytime->now();
	b := bufio->fopen(fd, Bufio->OWRITE);
	b.puts(sprint("%d\n", now));
	a := tracked.items;
	for(i := 0; i < len a; i++)
		for(l := a[i]; l != nil; l = tl l) {
			(hashhex, tr) := hd l;
			# peer $hash $peerid $ip $port $mtime
			for(j := 0; j < len tr.peers; j++) {
				p := tr.peers[j];
				if(b.puts(sprint("peer %q %q %q %d %d %d %d\n", hashhex, hex(p.peerid), p.ip.text(), p.port, p.mtime, p.dialtime, p.state)) == Bufio->ERROR)
					return warn(sprint("writing statefile: %r"));
			}
		}
	if(b.flush() == Bufio->ERROR)
		return warn(sprint("writing statefile: %r"));
	statelast = 1-statelast;
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

kill(pid: int)
{
	fd := sys->open(sprint("/prog/%d/ctl", pid), Sys->OWRITE);
	sys->fprint(fd, "kill");
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
