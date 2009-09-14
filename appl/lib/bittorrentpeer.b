implement Bittorrentpeer;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "daytime.m";
	daytime: Daytime;
include "string.m";
	str: String;
include "keyring.m";
	kr: Keyring;
include "rand.m";
	rand: Rand;
include "styx.m";
	Rmsg, Tmsg: import Styx;
include "tables.m";
	tables: Tables;
	Table: import tables;
include "bitarray.m";
	bitarray: Bitarray;
	Bits, Bititer: import bitarray;
include "bittorrent.m";
	bt: Bittorrent;
	File, Torrent, Filex, Torrentx, Trackpeer: import bt;
include "util0.m";
	util: Util0;
	hex, hasstr, sizefmt, preadn, min, warn, rev, l2a: import util;
include "bittorrentpeer.m";

state: ref State;
peergen: int;

init(st: ref State)
{
	sys = load Sys Sys->PATH;
	daytime = load Daytime Daytime->PATH;
	str = load String String->PATH;
	kr = load Keyring Keyring->PATH;
	rand = load Rand Rand->PATH;
	rand->init(sys->pctl(0, nil)^sys->millisec());
	tables = load Tables Tables->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	bt = load Bittorrent Bittorrent->PATH;
	util = load Util0 Util0->PATH;
	util->init();

	state = st;

	lastmsec = sys->millisec();
	lastnow = big lastmsec;
}

lastmsec: int;
lastnow: big;
now(): big
{
	msec := sys->millisec();
	lastnow += big (msec-lastmsec);
	lastmsec = msec;
	return lastnow;
}


Req.rkey(piece, begin: int): big
{
	v := (big piece)<<32;
	v |= big begin;
	return v;
}

Req.key(r: self ref Req): big
{
	return Req.rkey(r.piece, r.begin);
}

Req.eq(r1, r2: ref Req): int
{
	return r1.piece == r2.piece &&
		r1.begin == r2.begin &&
		r1.length == r2.length;
}

Req.text(r: self ref Req): string
{
	return sprint("Req(piece %d, begin %d, length %d, flushed %d)", r.piece, r.begin, r.length, r.flushed);
}


Bigtab[T].new(n: int): ref Bigtab
{
	return ref Bigtab[T] (array[n] of list of (big, T), 0);
}

Bigtab[T].clear(t: self ref Bigtab)
{
	*t = *Bigtab.new(len t.items);
}

Bigtab[T].add(t: self ref Bigtab, k: big, v: T)
{
	i := int (k % big len t.items);
	t.items[i] = (k, v)::t.items[i];
	t.length++;
}

Bigtab[T].del(t: self ref Bigtab, k: big): T
{
	r: list of (big, T);
	i := int (k % big len t.items);
	v: T;
	for(l := t.items[i]; l != nil; l = tl l)
		if((hd l).t0 == k && v == nil) {
			v = (hd l).t1;
			t.length--;
		} else
			r = hd l::r;
	t.items[i] = r;
	return v;
}

Bigtab[T].find(t: self ref Bigtab, k: big): T
{
	i := int (k % big len t.items);
	for(l := t.items[i]; l != nil; l = tl l)
		if((hd l).t0 == k)
			return (hd l).t1;
	return nil;
}

Bigtab[T].all(t: self ref Bigtab): list of T
{
	r: list of T;
	for(i := 0; i < len t.items; i++)
		for(l := t.items[i]; l != nil; l = tl l)
			r = (hd l).t1::r;
	return r;
}



Reqs.new(): ref Reqs
{
	rr := ref Reqs;
	rr.q = rr.q.new();
	rr.tab = rr.tab.new(17);
	return rr;
}

Reqs.clear(rr: self ref Reqs)
{
	*rr = *Reqs.new();
}

Reqs.add(rr: self ref Reqs, r: ref Req)
{
	l := rr.q.append(r);
	rr.tab.add(r.key(), l);
}

Reqs.del(rr: self ref Reqs, r: ref Req): int
{
	return rr.delkey(r.key()) != nil;
}

Reqs.delkey(rr: self ref Reqs, key: big): ref Req
{
	l := rr.tab.del(key);
	if(l == nil)
		return nil;
	e := l.e;
	rr.q.unlink(l);
	return e;
}

Reqs.takefirst(rr: self ref Reqs): ref Req
{
	r := rr.q.unlink(rr.q.first);
	rr.tab.del(r.key());
	return r;
}

Reqs.drophead(rr: self ref Reqs, time, max: int)
{
	while(rr.q.first != nil && (rr.q.length > max || rr.q.first.e.flushed <= time)) {
		rr.tab.del(rr.q.first.e.key());
		rr.q.unlink(rr.q.first);
	}
}

Reqs.concat(rr: self ref Reqs, r: ref Reqs)
{
	for(f := r.q.first; f != nil; f = f.next)
		rr.add(f.e);
}


peeridfmt(d: array of byte): string
{
	s := "";
	for(i := 0; i < len d; i++)
		case c := int d[i] {
		'-' or
		'a' to 'z' or
		'A' to 'Z' or
		'0' to '9' =>
			s[len s] = c;
		* =>
			s += sprint("%02x", c);
		}
	return s;
}


Queue[T].new(): ref Queue[T]
{
	return ref Queue[T](nil, nil, 0);
}

Queue[T].take(q: self ref Queue): T
{
	if(q.first == nil)
		raise "take on empty queue";
	return q.unlink(q.first);
}

Queue[T].takeq(q: self ref Queue): ref Queue[T]
{
	e := q.take();
	nq := q.new();
	nq.append(e);
	return nq;
}

Queue[T].prepend(q: self ref Queue, e: T): ref Link[T]
{
	l := ref Link[T](nil, q.first, e);
	if(q.first != nil)
		q.first.prev = l;
	q.first = l;
	if(q.last == nil)
		q.last = l;
	q.length++;
	return l;
}

Queue[T].append(q: self ref Queue, e: T): ref Link[T]
{
	l := ref Link[T](q.last, nil, e);
	if(q.last != nil)
		q.last.next = l;
	q.last = l;
	if(q.first == nil)
		q.first = l;
	q.length++;
	return l;
}

Queue[T].empty(q: self ref Queue): int
{
	return q.length == 0;
}

Queue[T].unlink(q: self ref Queue, l: ref Link[T]): T
{
	prev := l.prev;
	next := l.next;
	if(prev != nil)
		prev.next = next;
	if(next != nil)
		next.prev = prev;
	if(q.first == l)
		q.first = next;
	if(q.last == l)
		q.last = prev;
	l.prev = l.next = nil;
	e := l.e;
	l.e = nil;
	q.length--;
	return e;
}


Peer.new(np: ref Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int, npieces: int): ref Peer
{
	outmsgc := chan of ref Queue[ref Bittorrent->Msg];
	msgseq := 0;
	writec := chan[4] of ref Chunkwrite;
	readc := chan of ref Req;
	now := daytime->now();
	return ref Peer (
		peergen++,
		np, fd, extensions, peerid, hex(peerid),
		Gunknown, 0, 0,
		outmsgc, Queue[ref Bittorrent->Msg].new(), Queue[ref Bittorrent->Msg].new(),
		Bits.new(npieces), Bits.new(npieces), Bits.new(npieces),
		RemoteChoking|LocalChoking,
		msgseq,
		Traffic.new(), Traffic.new(), Traffic.new(), Traffic.new(),
		Reqs.new(), Reqs.new(),
		Reqs.new(), Reqs.new(),
		0, 0, dialed, Chunk.new(), writec, readc,
		now, now, now);
}

Peer.remotechoking(p: self ref Peer): int
{
	return p.state & RemoteChoking;
}

Peer.remoteinterested(p: self ref Peer): int
{
	return p.state & RemoteInterested;
}

Peer.localchoking(p: self ref Peer): int
{
	return p.state & LocalChoking;
}

Peer.localinterested(p: self ref Peer): int
{
	return p.state & LocalInterested;
}

Peer.isdone(p: self ref Peer): int
{
	return p.rhave.isfull();
}

Peer.text(p: self ref Peer): string
{
	return sprint("Peer(id %d, addr %s, peerid %s)", p.id, p.np.addr, peeridfmt(p.peerid));
}


Chunk.new(): ref Chunk
{
	return ref Chunk (Queue[array of byte].new(), -1, 0, 0, 0);
}

Chunk.tryadd(c: self ref Chunk, piece: ref Piece, begin: int, buf: array of byte): int
{
	if(c.piece < 0) {
		c.bufs.append(buf);
		c.piece = piece.index;
		c.begin = begin;
		c.piecelength = piece.length;
		c.nbytes += len buf;
		return 1;
	}

	if(c.piece != piece.index || c.isfull())
		return 0;

	if(begin % Diskchunksize != 0 && begin == c.begin+c.nbytes) {
		c.bufs.append(buf);
	} else if(c.begin % Diskchunksize != 0 && begin == c.begin-len buf) {
		c.bufs.prepend(buf);
		c.begin = begin;
	} else
		return 0;
	c.nbytes += len buf;
	return 1;
}

Chunk.isempty(c: self ref Chunk): int
{
	return c.nbytes == 0;
}

Chunk.isfull(c: self ref Chunk): int
{
	return c.nbytes >= Diskchunksize || c.begin+c.nbytes == c.piecelength;
}

Chunk.overlaps(c: self ref Chunk, piece, begin, end: int): int
{
	bufstart := c.begin;
	bufend := c.begin+c.nbytes;
	return c.piece == piece && (bufstart >= begin && bufstart <= end || bufend >= begin && bufend <= end);
}

Chunk.flush(c: self ref Chunk): ref Chunkwrite
{
	bw := ref Chunkwrite (c.piece, c.begin, c.bufs);
	c.bufs = c.bufs.new();
	c.piece = c.begin = -1;
	c.nbytes = 0;
	return bw;
}


Newpeer.text(np: self ref Newpeer): string
{
	peerid := "nil";
	if(np.peerid != nil)
		peerid = peeridfmt(np.peerid);
	return sprint("Newpeer(addr %s, peerid %s)", np.addr, peerid);
}


newpeerready(n: ref Newpeers, np: ref Newpeer): int
{
	if(n.done && (np.state & Pseeding))
		return 0;
	return (np.state & (Pdialing|Pconnected|Plistener)) == 0 && np.time <= daytime->now();
}

newpeersfind(n: ref Newpeers, np: ref Newpeer): int
{
	for(i := 0; i < n.np; i++)
		if(n.p[i].ip == np.ip)
			return i;
	return -1;
}

newpeersfindip(n: ref Newpeers, ip: string): list of ref Newpeer
{
	l: list of ref Newpeer;
	for(i := 0; i < n.np; i++)
		if(n.p[i].ip == ip)
			l = n.p[i]::l;
	return l;
}

newpeersadd(n: ref Newpeers, np: ref Newpeer)
{
	if(n.np+1 >= len n.p)
		n.p = grow(n.p, 8);
	n.p[n.np++] = np;
}

newpeersdelindex(n: ref Newpeers, i: int)
{
	n.p[i:] = n.p[i+1:];
	n.p[--n.np] = nil;
}

newpeersdel(n: ref Newpeers, np: ref Newpeer)
{
	for(i := 0; i < n.np; i++)
		if(n.p[i] == np)
			return newpeersdelindex(n, i);
}

newpeercmp(a, b: ref Newpeer, done: int): int
{
	mask := Pdialing|Pconnected|Plistener;
	if(!done)
		mask |= Pseeding;
	if((a.state & mask) == (b.state & mask))
		return a.time-b.time;
	if((a.state & mask) == 0)
		return -1;
	return 1;
}

newpeerge(a, b: ref Newpeer, done: int): int
{
	return newpeercmp(a, b, done) >= 0;
}

newpeerssort(n: ref Newpeers)
{
	inssort(n.p[:n.np], newpeerge, n.done);
}

Newpeerslowat: con 1000;
Newpeershiwat: con 1100;
newpeerstrunc(n: ref Newpeers)
{
	if(n.np < Newpeershiwat)
		return;
	for(nn := n.np; nn-1 >= Newpeerslowat; nn--)
		if(n.p[nn-1].state & (Pdialing|Pconnected))
			break;
	if(nn == n.np)
		return;
	np := array[nn] of ref Newpeer;
	np[:] = n.p[:nn];
	n.p = np;
}

newpeertext(np: ref Newpeer): string
{
	return sprint("%s time %d waittime %d listener %d dialing %d connected %d", np.addr, np.time, np.waittime,
		(np.state & Plistener) != 0,
		(np.state & Pdialing) != 0,
		(np.state & Pconnected) != 0);
}

newpeersdump(n: ref Newpeers)
{
	say("newpeers:");
	for(i := 0; i < n.np; i++)
		say("\t"+newpeertext(n.p[i]));
	say("eonewpeers");
}

Newpeers.new(): ref Newpeers
{
	return ref Newpeers (nil, array[0] of ref Newpeer, 0, 0);
}

Newpeers.add(n: self ref Newpeers, tp: Trackpeer)
{
	return n.addmany(array[] of {tp});
}

Newpeers.addmany(n: self ref Newpeers, tps: array of Trackpeer)
{
	now := daytime->now();
next:
	for(i := 0; i < len tps; i++) {
		tp := tps[i];
		for(l := newpeersfindip(n, tp.ip); l != nil; l = tl l) {
			onp := hd l;
			if(!(onp.state & Plistener))
				continue next;
		}
		np := ref Newpeer (sprint("%s!%d", tp.ip, tp.port), tp.ip, tp.port, tp.peerid, now, 0, 0, nil, nil);
		newpeersadd(n, np);
	}
	newpeerssort(n);
	newpeerstrunc(n);
}

Newpeers.addlistener(n: self ref Newpeers, addr: string, peerid: array of byte): ref Newpeer
{
	(ip, portstr) := str->splitstrl(addr, "!");
	if(portstr != nil)
		portstr = portstr[1:];
	port := int portstr;
	now := daytime->now();
	np := ref Newpeer (addr, ip, port, peerid, now, 0, Plistener|Pconnected, nil, nil);
	newpeersadd(n, np);
	newpeerssort(n);
	newpeerstrunc(n);
	return np;
}

Newpeers.markself(n: self ref Newpeers, ip: string)
{
	if(hasstr(n.localips, ip))
		return;
	n.localips = ip::n.localips;
	for(l := newpeersfindip(n, ip); l != nil; l = tl l) {
		np := hd l;
		if(!(np.state & Pconnected) && !(np.state & Pdialing))
			newpeersdel(n, np);
	}
}

Newpeers.markdone(n: self ref Newpeers)
{
	n.done = 1;
	i := 0;
	while(i < n.np) {
		np := n.p[i];
		if((np.state & Pseeding) && (np.state & (Pdialing|Plistener) == 0))
			newpeersdelindex(n, i);
		else
			i++;
	}
}

Newpeers.peers(n: self ref Newpeers): ref List[ref Newpeer]
{
	first := p := ref List[ref Newpeer];
	for(i := 0; i < n.np; i++) {
		p.next = ref List[ref Newpeer](nil, n.p[i]);
		p = p.next;
	}
	return first.next;
}

Newpeers.faulty(n: self ref Newpeers): ref List[ref Newpeer]
{
	first := p := ref List[ref Newpeer];
	for(i := 0; i < n.np; i++) {
		if(n.p[i].banreason == nil)
			continue;
		p.next = ref List[ref Newpeer](nil, n.p[i]);
		p = p.next;
	}
	return first.next;
}

Newpeers.npeers(n: self ref Newpeers): int
{
	return n.np;
}

Newpeers.nfaulty(n: self ref Newpeers): int
{
	c := 0;
	for(i := 0; i < n.np; i++)
		if(n.p[i].banreason != nil)
			c++;
	return c;
}

Newpeers.ndialers(n: self ref Newpeers): int
{
	c := 0;
	for(i := 0; i < n.np; i++)
		if(n.p[i].state & Pdialing)
			c++;
	return c;
}

Newpeers.nready(n: self ref Newpeers): int
{
	c := 0;
	for(i := 0; i < n.np && newpeerready(n, n.p[i]); i++)
		c++;
	return c;
}

Newpeers.nseeding(n: self ref Newpeers): int
{
	c := 0;
	for(i := 0; i < n.np; i++)
		if(n.p[i].state & Pseeding)
			c++;
	return c;
}

Newpeers.nlisteners(n: self ref Newpeers): int
{
	c := 0;
	for(i := 0; i < n.np; i++)
		if(n.p[i].state & Plistener)
			c++;
	return c;
}

Newpeers.cantake(n: self ref Newpeers): int
{
#say(sprint("cantake, npeers %d", n.np));
	return n.np > 0 && newpeerready(n, n.p[0]);
}

Newpeers.take(n: self ref Newpeers): ref Newpeer
{
	if(!n.cantake())
		raise "newpeers.take, newpeer not ready";
	np := n.p[0];
	if(np.state & Pconnected)
		raise "newpeers.take, connected?";
	if(np.state & Plistener)
		raise "newpeers.take, listener?";
	np.state |= Pdialing;
	np.banreason = nil;
	newpeerssort(n);
	return np;
}

newpeerbackoff(np: ref Newpeer, backoff: int)
{
	if(backoff) {
		if(np.waittime == 0)
			np.waittime = 30;
		np.waittime *= 2;
	}
	np.time = daytime->now()+np.waittime;
}

Newpeers.dialfailed(n: self ref Newpeers, np: ref Newpeer, backoff: int, err: string)
{
	if((np.state & Pdialing) == 0)
		raise "dialfailed but not dialing?";
	np.state &= ~Pdialing;
	np.lasterror = err;
	newpeerbackoff(np, backoff);
	newpeerssort(n);
}

Newpeers.connected(n: self ref Newpeers, np: ref Newpeer)
{
	if((np.state & Pdialing) == 0)
		raise "connected but was not dialing?";
	np.state &= ~Pdialing;
	np.state |= Pconnected;
	np.lasterror = nil;
	newpeerssort(n);
}

Newpeers.disconnected(n: self ref Newpeers, np: ref Newpeer, err: string)
{
	if((np.state & Pconnected) == 0)
		raise "disconnected but were not connected?";
	np.state &= ~Pconnected;
	np.lasterror = err;
	newpeerbackoff(np, 1);
	newpeerssort(n);
}

Newpeers.disconnectfaulty(n: self ref Newpeers, np: ref Newpeer, err: string): int
{
	if(np.state & Pdialing)
		raise "newpeers.disconnectfaulty, for dialing peer";
	if((np.state & Pconnected) == 0)
		raise "newpeers.disconnectfaulty, but not connected";
	if(np.banreason != nil)
		raise "newpeers.disconnectfaulty, peer was already faulty";
	now := daytime->now();
	np.time = now+500;
	np.waittime = 0;
	np.banreason = err;
	np.lasterror = err;
	newpeerssort(n);
	return np.time;
}

Newpeers.seeding(nil: self ref Newpeers, np: ref Newpeer)
{
	np.state |= Pseeding;
}

Newpeers.isfaulty(n: self ref Newpeers, ip: string): int
{
	for(l := newpeersfindip(n, ip); l != nil; l = tl l)
		if((hd l).banreason != nil)
			return 1;
	return 0;
}


Traffic.new(): ref Traffic
{
	nslots: con (TrafficHistsecs*1000+(2**TrafficHistslotmsec2-1)) >> TrafficHistslotmsec2;
	tm := now();
	return ref Traffic(tm, 0, array[nslots] of {* => 0}, 0, big 0, 0, tm);
}

reclaim(t: ref Traffic, tm: big)
{
	diff := int ((tm>>TrafficHistslotmsec2) - (t.last>>TrafficHistslotmsec2));
	if(diff == 0)
		return;
	o := (t.lasti+1) % len t.bytes;
	for(i := 0; i < diff; i++) {
		t.winsum -= t.bytes[o];
		t.bytes[o] = 0;
		o = (o+1) % len t.bytes;
	}
	t.last = tm;
	t.lasti = (t.lasti+diff) % len t.bytes;
}

Traffic.add(t: self ref Traffic, nbytes, npkts: int)
{
	t.npackets += npkts;
	if(nbytes == 0)
		return;

	reclaim(t, now());
	t.bytes[t.lasti] += nbytes;
	t.winsum += nbytes;
	t.sum += big nbytes;
}

Traffic.rate(t: self ref Traffic): int
{
	tm := now();
	reclaim(t, tm);
	return int (big t.winsum*big 1000/big min(int (tm-t.time0), TrafficHistsecs*1000));
}

Traffic.total(t: self ref Traffic): big
{
	return t.sum;
}

Traffic.text(t: self ref Traffic): string
{
	return sprint("Traffic(rate %s, total %s)", sizefmt(big t.rate()), sizefmt(t.total()));
}


Pool[T].new(mode: int): ref Pool[T]
{
	return ref Pool[T](array[0] of T, array[0] of T, 0, mode);
}

Pool[T].clear(p: self ref Pool)
{
	*p = *Pool[T].new(p.mode);
}

Pool[T].fill(p: self ref Pool)
{
	case p.mode {
	PoolRandom =>
		return;
	PoolRotateRandom or
	PoolInorder =>
		n := len p.pool-len p.active;

		newa := array[len p.pool] of T;
		newa[:] = p.active;
		start := len p.active;
		for(i := 0; i < n; i++) {
			newa[start+i] = p.pool[p.poolnext];
			p.poolnext = (p.poolnext+1) % len p.pool;
		}
		p.active = newa;

		if(p.mode == PoolRotateRandom)
			randomize(p.active[len p.active-n:]);
	* =>
		raise sprint("bad mode for pool: %d", p.mode);
	}
}

Pool[T].take(p: self ref Pool): T
{
	if(len p.active == 0)
		return nil;

	case p.mode {
	PoolRandom =>
		return p.pool[rand->rand(len p.pool)];
	PoolRotateRandom or
	PoolInorder =>
		e := p.active[0];
		p.active = p.active[1:];
		return e;
	* =>
		raise sprint("bad mode for pool: %d", p.mode);
	}
}

Pool[T].pooladd(p: self ref Pool, e: T)
{
	newp := array[len p.pool+1] of T;
	newp[:] = p.pool;
	newp[len p.pool] = e;
}

Pool[T].pooladdunique(p: self ref Pool, e: T)
{
	if(!p.poolhas(e))
		p.pooladd(e);
}

Pool[T].poolhas(p: self ref Pool, e: T): int
{
	for(i := 0; i < len p.pool; i++)
		if(p.pool[i] == e)
			return 1;
	return 0;
}

Pool[T].pooldel(p: self ref Pool, e: T)
{
	i := 0;
	while(i < len p.pool) {
		if(p.pool[i] == e) {
			p.pool[i:] = p.pool[i+1:];
			p.pool = p.pool[:len p.pool-1];
		} else
			i++;
	}
}

poolmodes := array[] of {"random", "rotaterandom", "inorder"};
Pool[T].text(p: self ref Pool): string
{
	return sprint("Pool(npool %d, nactive %d, poolnext %d, mode %s)", len p.pool, len p.active, p.poolnext, poolmodes[p.mode]);
}


progresstags := array[] of {
"endofstate", "done", "started", "stopped", "newctl", "piece", "block", "pieces", "blocks", "filedone", "tracker", "error", "hashfail",
};
Progress.text(pp: self ref Progress): string
{
	s := progresstags[tagof pp];
	pick p := pp {
	Endofstate or
	Done or
	Started or
	Stopped or
	Newctl =>	;
	Piece =>	s += sprint(" %d %d %d", p.p, p.have, p.total);
	Block =>	s += sprint(" %d %d %d %d", p.p, p.b, p.have, p.total);
	Pieces =>	for(l := p.l; l != nil; l = tl l)
				s += " "+string hd l;
	Blocks =>	s += sprint(" %d", p.p);
			for(l := p.l; l != nil; l = tl l)
				s += " "+string hd l;
	Filedone =>	s += sprint(" %d %q %q", p.index, p.path, p.origpath);
	Tracker =>	s += sprint(" %d %d %d %q", p.interval, p.next, p.npeers, p.err);
	Error =>	s += sprint(" %q", p.msg);
	Hashfail =>	s += sprint(" %d", p.index);
	* =>	raise "missing case";
	}
	return s;
}

peereventtags := array[] of {
"endofstate", "dialing", "tracker", "new", "gone", "bad", "state", "piece", "pieces", "done",
};
eventstatestr0 := array[] of {"local", "remote"};
eventstatestr1 := array[] of {"choking", "unchoking", "interested", "uninterested"};
Peerevent.text(pp: self ref Peerevent): string
{
	s := peereventtags[tagof pp];
	pick p := pp {
	Endofstate =>	;
	Dialing =>	s += sprint(" %q", p.addr);
	Tracker =>	s += sprint(" %q", p.addr);
	New =>		s += sprint(" %q %d %s %d", p.addr, p.id, p.peeridhex, p.dialed);
	Gone =>		s += sprint(" %d", p.id);
	Bad =>		s += sprint(" %q %d %q %q", p.ip, p.time, hex(p.peerid), p.banreason);
	State =>	s += sprint(" %d %s %s", p.id, eventstatestr0[p.s>>2], eventstatestr1[p.s&3]);
	Piece => 	s += sprint(" %d %d", p.id, p.piece);
	Pieces =>	s += sprint(" %d", p.id);
			for(l := p.pieces; l != nil; l = tl l)
				s += " "+string hd l;
	Done =>		s += sprint(" %d", p.id);
	}
	return s;
}

Eventfid[T].new(fid: int): ref Eventfid[T]
{
	l := ref List[T];
	l.next = ref List[T];
	return ref Eventfid[T](fid, nil, l);
}

Eventfid[T].putread(ef: self ref Eventfid, r: ref Tmsg.Read)
{
	ef.r = r::ef.r;
}

Eventfid[T].read(ef: self ref Eventfid): ref Rmsg.Read
{
	if(ef.r == nil || ef.last.next.e == nil)
		return nil;

	ef.r = rev(ef.r);
	m := hd ef.r;
	ef.r = rev(tl ef.r);

	s := "";
	while(ef.last.next.e != nil) {
		p := ef.last.next.e;
		ns := p.text();
		# note: the following depends on all data being ascii!
		if(s != nil && len s+len ns+1 > m.count)
			break;
		if(ns != nil)
			ns[len ns] = '\n';
		s += ns;
		ef.last = ef.last.next;
	}
	if(len s > m.count)
		s = s[:m.count];
	return ref Rmsg.Read (m.tag, array of byte s);
}

Eventfid[T].flushtag(ef: self ref Eventfid, tag: int): int
{
	r: list of ref Tmsg.Read;
	for(l := ef.r; l != nil; l = tl l)
		if((hd l).tag != tag)
			r = hd l::r;
	if(len r == len ef.r)
		return 0;
	ef.r = rev(r);
	return 1;
}

grow[T](a: array of T, n: int): array of T
{
	na := array[len a+n] of T;
	na[:] = a;
	return na;
}

randomize[T](a: array of T)
{
	for(i := 0; i < len a; i++) {
		j := rand->rand(len a);
		(a[i], a[j]) = (a[j], a[i]);
	}
}

inssort[T](a: array of T, ge: ref fn(a, b: T, done: int): int, done: int)
{
	for(i := 1; i < len a; i++) {
		tmp := a[i];
		for(j := i; j > 0 && ge(a[j-1], tmp, done); j--)
			a[j] = a[j-1];
		a[j] = tmp;
	}
}

say(s: string)
{
	if(dflag)
		warn(s);
}
