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
	File, Torrent, Filex, Torrentx: import bt;
include "util0.m";
	util: Util0;
	hex, sizefmt, preadn, min, warn, rev, l2a, inssort: import util;
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


RReq.eq(r1, r2: ref RReq): int
{
	return r1.piece == r2.piece &&
		r1.begin == r2.begin &&
		r1.length == r2.length;
}

RReq.text(r: self ref RReq): string
{
	return sprint("RReq(piece %d, begin %d, length %d)", r.piece, r.begin, r.length);
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
	t.nelems++;
}

Bigtab[T].del(t: self ref Bigtab, k: big): T
{
	r: list of (big, T);
	i := int (k % big len t.items);
	v: T;
	for(l := t.items[i]; l != nil; l = tl l)
		if((hd l).t0 == k && v == nil) {
			v = (hd l).t1;
			t.nelems--;
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


RReqs.new(): ref RReqs
{
	rr := ref RReqs;
	rr.tab = rr.tab.new(17);
	rr.length = 0;
	return rr;
}

RReqs.clear(rr: self ref RReqs)
{
	*rr = *RReqs.new();
}

rrkey(r: ref RReq): big
{
	k := (big r.piece)<<32;;
	k |= big r.begin;
	return k;
}

RReqs.add(rr: self ref RReqs, r: ref RReq)
{
	l := ref Link[ref RReq](rr.last, nil, r);
	if(rr.first == nil) {
		rr.first = rr.last = l;
	} else {
		rr.last.next = l;
		rr.last = l;
	}
	rr.tab.add(rrkey(r), l);
	rr.length++;
}

RReqs.del(rr: self ref RReqs, r: ref RReq): int
{
	l := rr.tab.del(rrkey(r));
	if(l == nil)
		return 0;

	if(rr.first == l) {
		rr.first = rr.first.next;
		if(rr.first != nil)
			rr.first.prev = nil;
	} else {
		l.prev.next = l.next;
		if(l.next != nil)
			l.next.prev = l.prev;
	}
	if(rr.last == l)
		rr.last = l.prev;
	rr.length--;
	return 1;
}

RReqs.dropfirst(rr: self ref RReqs)
{
	rr.first.e = nil;
	if(rr.last == rr.first) {
		rr.first = rr.last = nil;
	} else {
		rr.first = rr.first.next;
		if(rr.first != nil)
			rr.first.prev = nil;
	}
	rr.length--;
}


isascii(d: array of byte): int
{
	for(i := 0; i < len d; i++)
		if(!str->in(int d[i], " -~"))
			return 0;
	return 1;
}

peeridfmt(d: array of byte): string
{
	if(isascii(d))
		return string d;
	return hex(d);
}


Queue[T].new(): ref Queue[T]
{
	return ref Queue[T];
}

Queue[T].take(q: self ref Queue): T
{
	if(q.first == nil)
		raise "take on empty queue";
	n := q.first;
	e := n.e;
	n.e = nil;
	if(q.first == q.last)
		q.first = q.last = nil;
	else
		q.first = q.first.next;
	return e;
}

Queue[T].takeq(q: self ref Queue): ref Queue[T]
{
	e := q.take();
	nq := q.new();
	nq.add(e);
	return nq;
}

Queue[T].prepend(q: self ref Queue, e: T)
{
	n := ref List[T];
	n.e = e;
	n.next = q.first;
	q.first = n;
	if(q.last == nil)
		q.last = n;
}

Queue[T].add(q: self ref Queue, e: T)
{
	n := ref List[T];
	n.e = e;
	if(q.first == nil) {
		q.first = q.last = n;
	} else {
		q.last.next = n;
		q.last = n;
	}
}

Queue[T].empty(q: self ref Queue): int
{
	return q.first == nil;
}


Peer.new(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int, npieces: int): ref Peer
{
	getmsgc := chan of ref Queue[ref Bittorrent->Msg];
	msgseq := 0;
	writec := chan[4] of ref Chunkwrite;
	readc := chan of ref RReq;
	return ref Peer (
		peergen++,
		np, fd, extensions, peerid, hex(peerid),
		0, 0, 0,
		getmsgc, Queue[ref Bittorrent->Msg].new(), Queue[ref Bittorrent->Msg].new(),
		Bits.new(npieces),
		Bits.new(npieces),
		RemoteChoking|LocalChoking,
		msgseq,
		Traffic.new(), Traffic.new(), Traffic.new(), Traffic.new(),
		Bigtab[ref LReq].new(17),
		RReqs.new(),
		0, dialed, Chunk.new(), writec, readc, -1);
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
	return sprint("Peer(id %d, addr %s)", p.id, p.np.addr);
}


Chunk.new(): ref Chunk
{
	return ref Chunk (Queue[array of byte].new(), -1, 0, 0, 0);
}

Chunk.tryadd(c: self ref Chunk, piece: ref Piece, begin: int, buf: array of byte): int
{
	if(c.piece < 0) {
		c.bufs.add(buf);
		c.piece = piece.index;
		c.begin = begin;
		c.piecelength = piece.length;
		c.nbytes += len buf;
		return 1;
	}

	if(c.piece != piece.index || c.isfull())
		return 0;

	if(begin % Diskchunksize != 0 && begin == c.begin+c.nbytes) {
		c.bufs.add(buf);
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


Newpeer.text(np: self Newpeer): string
{
	peerid := "nil";
	if(np.peerid != nil)
		peerid = peeridfmt(np.peerid);
	return sprint("Newpeer(addr %s, peerid %s)", np.addr, peerid);
}

Newpeers.del(n: self ref Newpeers, np: Newpeer)
{
	l: list of Newpeer;
	for(; n.l != nil; n.l = tl n.l)
		if((hd n.l).addr != np.addr)
			l = (hd n.l)::l;
	n.l = l;
}

Newpeers.add(n: self ref Newpeers, np: Newpeer)
{
	n.l = np::n.l;
}

Newpeers.take(n: self ref Newpeers): Newpeer
{
	np := hd n.l;
	n.l = tl n.l;
	return np;
}

Newpeers.all(n: self ref Newpeers): list of Newpeer
{
	return n.l;
}

Newpeers.empty(n: self ref Newpeers): int
{
	return n.l == nil;
}


LReq.key(r: self ref LReq): big
{
	v := (big r.piece)<<32;
	v |= big r.block;
	return v;
}

LReq.eq(r1, r2: ref LReq): int
{
	return r1.piece == r2.piece && r1.block == r2.block;
}

LReq.text(r: self ref LReq): string
{
	return sprint("LReq(piece %d, block %d)", r.piece, r.block);
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
	Bad =>		s += sprint(" %q %d %q", p.ip, p.mtime, p.err);
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

randomize[T](a: array of T)
{
	for(i := 0; i < len a; i++) {
		j := rand->rand(len a);
		(a[i], a[j]) = (a[j], a[i]);
	}
}

say(s: string)
{
	if(dflag)
		warn(s);
}
