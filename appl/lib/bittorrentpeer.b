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
	Bits: import bitarray;
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

randomize[T](a: array of T)
{
	for(i := 0; i < len a; i++) {
		j := rand->rand(len a);
		(a[i], a[j]) = (a[j], a[i]);
	}
}

Piece.new(index, length: int): ref Piece
{
	nblocks := (length+Blocksize-1)/Blocksize;
	return ref Piece(index, Bits.new(nblocks), Bits.new(nblocks), length, array[nblocks] of {* => (-1, -1)}, array[nblocks] of {* => 0});
}

Piece.orphan(p: self ref Piece): int
{
	for(i := 0; i < len p.busy; i++)
		if(p.busy[i].t0 >= 0 || p.busy[i].t1 >= 0)
			return 0;
	return 1;
}

Piece.isdone(p: self ref Piece): int
{
	return p.have.n == p.have.have;
}


Piece.text(p: self ref Piece): string
{
	return sprint("Piece(index %d, nblocks %d, haveblocks %d)", p.index, p.have.n, p.have.have);
}


Block.new(piece, begin, length: int): ref Block
{
	return ref Block(piece, begin, length);
}

Block.eq(b1, b2: ref Block): int
{
	return b1.piece == b2.piece &&
		b1.begin == b2.begin &&
		b1.length == b2.length;
}

Block.text(b: self ref Block): string
{
	return sprint("Block(piece %d, begin %d, length %d)", b.piece, b.begin, b.length);
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


peerstatestrs := array[] of {
"remotechoking",
"remoteinterested",
"localchoking",
"localinterested",
};
peerstatestr(state: int): string
{
	s := "";
	for(i := 0; i < 4; i++)
		if(state & (1<<i))
			s += ","+peerstatestrs[i];
	if(s != nil)
		s = s[1:];
	return s;
}


Newpeer.text(np: self Newpeer): string
{
	peerid := "nil";
	if(np.peerid != nil)
		peerid = peeridfmt(np.peerid);
	return sprint("Newpeer(addr %s, peerid %s)", np.addr, peerid);
}


Peer.new(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int, npieces: int): ref Peer
{
	getmsgc := chan of list of ref Bittorrent->Msg;
	msgseq := 0;
	writec := chan[4] of ref (int, int, array of byte);
	readc := chan of ref (int, int, int);
	return ref Peer(
		peergen++,
		np, fd, extensions, peerid, hex(peerid),
		0, getmsgc, nil, nil,
		Reqs.new(Blockqueuesize),
		Bits.new(npieces),
		RemoteChoking|LocalChoking,
		msgseq,
		Traffic.new(), Traffic.new(), Traffic.new(), Traffic.new(),
		nil, 0, dialed, Buf.new(), writec, readc, nil);
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
	return p.piecehave.isfull();
}

Peer.text(p: self ref Peer): string
{
	return sprint("Peer(id %d, addr %s)", p.id, p.np.addr);
}

Peer.fulltext(p: self ref Peer): string
{
	return sprint("Peer(id %d, addr %s, wantblocks %d, peerid %s>", p.id, p.np.addr, len p.wants, peeridfmt(p.peerid));
}


Buf.new(): ref Buf
{
	return ref Buf(array[0] of byte, -1, 0, 0);
}

Buf.tryadd(b: self ref Buf, piece: ref Piece, begin: int, buf: array of byte): int
{
	if(b.piece < 0) {
		b.data = array[len buf] of byte;
		b.data[:] = buf;
		b.piece = piece.index;
		b.pieceoff = begin;
		b.piecelength = piece.length;
		return 1;
	}

	if(piece.index != b.piece)
		return 0;

	if(b.isfull())
		return 0;

	# append data to buf, only if it won't cross disk chunk
	if(begin % Diskchunksize != 0 && begin == b.pieceoff+len b.data) {
		ndata := array[len b.data+len buf] of byte;
		ndata[:] = b.data;
		ndata[len b.data:] = buf;
		b.data = ndata;
		return 1;
	}

	# prepend data to buf, only if it won't cross disk chunk
	if(b.pieceoff % Diskchunksize != 0 && begin == b.pieceoff-len buf) {
		ndata := array[len buf+len b.data] of byte;
		ndata[:] = buf;
		ndata[len buf:] = b.data;
		b.data = ndata;
		b.pieceoff = begin;
		return 1;
	}

	return 0;
}

Buf.isfull(b: self ref Buf): int
{
	return len b.data >= Diskchunksize || b.pieceoff+len b.data == b.piecelength;
}

Buf.clear(b: self ref Buf)
{
	b.data = array[0] of byte;
	b.piece = -1;
	b.pieceoff = 0;
}

Buf.overlaps(b: self ref Buf, piece, begin, end: int): int
{
	bufstart := b.pieceoff;
	bufend := b.pieceoff+len b.data;
	return b.piece == piece && (bufstart >= begin && bufstart <= end || bufend >= begin && bufend <= end);
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


request(reqc: chan of ref (ref Piece, list of Req, chan of int), p: ref Piece, reqs: list of Req)
{
	donec := chan of int;
	reqc <-= ref (p, reqs, donec);
	<-donec;
}

schedbatches(reqc: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer, b: array of ref Batch)
{
	for(i := 0; needblocks(peer) && i < len b; i++)
		request(reqc, b[i].piece, b[i].unused());
}

schedpieces(reqc: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer, a: array of ref Piece)
{
	for(i := 0; needblocks(peer) && i < len a; i++)
		if((peer.piecehave).get(a[i].index)) {
			b := batches(a[i]);
			schedbatches(reqc, peer, b);
		}
}

batches(p: ref Piece): array of ref Batch
{
	nblocks := p.have.n;
	nbatches := (nblocks+Batchsize-1)/Batchsize;
	b := array[nbatches] of ref Batch;
	for(i := 0; i < len b; i++)
		b[i] = Batch.new(i*Batchsize, min(Batchsize, nblocks-i*Batchsize), p);
	return b;
}

getrandompiece(): ref Piece
{
	# will succeed, this is only called when few pieces are busy
	for(;;) {
		i := rand->rand(state.piecebusy.n);
		if(!state.piecebusy.get(i)) {
			p := Piece.new(i, state.t.piecelength(i));
			state.pieces.add(i, p);
			state.piecebusy.set(i);
			return p;
		}
	}
}

getrarestpiece(b: ref Bits)
{
	# first, determine which pieces are the rarest
	rarest: list of int;  # piece index
	min := -1;
	seen := 0;
	for(i := 0; i < b.n && seen < b.have; i++) {
		if(b.get(i)) {
			seen++;
			if(min < 0 || state.piececounts[i] < min) {
				rarest = nil;
				min = state.piececounts[i];
			}
			if(state.piececounts[i] <= min)
				rarest = i::rarest;
		}
	}
	if(rarest == nil)
		return;

	# next, pick a random element from the list
	skip := rand->rand(b.have);
	while(skip-- > 0)
		rarest = tl rarest;
	index := hd rarest;
	say(sprint("choose rarest piece %d", index));
}

needblocks(peer: ref Peer): int
{
	return peer.reqs.count() == 0 || peer.reqs.count()+Batchsize < peer.reqs.size();
}

inactiverare(): array of ref (int, int)
{
	a := array[state.piecebusy.n-state.piecebusy.have] of ref (int, int);
	say(sprint("inactiverare: %d pieces, %d busy, finding %d", state.piecebusy.n, state.piecebusy.have, len a));
	j := 0;
	for(i := 0; j < len a && i < state.piecebusy.n; i++)
		if(!state.piecebusy.get(i))
			a[j++] = ref (i, 0);

	randomize(a);
	return a;
}

rarepiececmp(p1, p2: ref Piece): int
{
	return state.piececounts[p1.index]-state.piececounts[p2.index];
}

piecesrareorphan(orphan: int): array of ref Piece
{
	r: list of ref Piece;
	for(l := tablist(state.pieces); l != nil; l = tl l) {
		p := hd l;
		if(p.orphan() && orphan)
			r = p::r;
		else if(!p.orphan() && !orphan)
			r = p::r;
	}

	a := l2a(r);
	inssort(a, rarepiececmp);
	return a;
}

progresscmp(p1, p2: ref Piece): int
{
	return p2.have.n-p1.have.n;
}

rarestfirst(): int
{
	return state.piecehave.have >= Piecesrandom;
}

schedule(reqc: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer)
{
	schedule0(reqc, peer);
	reqc <-= nil;
}

schedule0(reqc: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer)
{
	if(rarestfirst()) {
		say("schedule: doing rarest first");

		# attempt to work on last piece this peer was working on
		say("schedule: trying previous piece peer was working on");  # xxx should check if no other peer has taken over the piece?
		req := peer.reqs.last();
		if(req != nil) {
			piece := state.pieces.find(req.pieceindex);
			if(piece != nil)
				schedpieces(reqc, peer, array[] of {piece});
		}

		# find rarest orphan pieces to work on
		say("schedule: trying rarest orphans");
		a := piecesrareorphan(1);
		schedpieces(reqc, peer, a);
		if(!needblocks(peer))
			return;

		# find rarest inactive piece to work on
		say("schedule: trying inactive pieces");
		rare := inactiverare();
		for(i := 0; i < len rare; i++) {
			v := rare[i].t0;
			say(sprint("using new rare piece %d", v));
			p := Piece.new(v, state.t.piecelength(v));
			state.pieces.add(v, p);
			state.piecebusy.set(v);
			schedpieces(reqc, peer, array[] of {p});
			if(!needblocks(peer))
				return;
		}

		# find rarest active non-orphan piece to work on
		say("schedule: trying rarest non-orphans");
		a = piecesrareorphan(0);
		schedpieces(reqc, peer, a);
		if(!needblocks(peer))
			return;
		
	} else {
		say("schedule: doing random");

		# schedule requests for blocks of active pieces, most completed first:  we want whole pieces fast
		a := l2a(tablist(state.pieces));
		inssort(a, progresscmp);
		for(i := 0; i < len a; i++) {
			piece := a[i];
			say(sprint("schedule: looking at piece %d", piece.index));

			# skip piece if this peer doesn't have it
			if(!peer.piecehave.get(piece.index))
				continue;

			# divide piece into batches
			b := batches(piece);

			# request blocks from unused batches first
			say("schedule: trying unused batches from piece");
			schedbatches(reqc, peer, b);
			if(!needblocks(peer))
				return;

			# if more requests needed, start on partially used batches too, in reverse order (for fewer duplicate data)
			say("schedule: trying partially used batches from piece");
			for(k := len b-1; k >= 0; k--) {
				request(reqc, piece, b[k].usedpartial(peer));
				if(!needblocks(peer))
					return;
			}
		}

		# otherwise, get new random pieces to work on
		say("schedule: trying random pieces");
		while(needblocks(peer) && state.piecebusy.have < state.piecebusy.n) {
			say("schedule: getting another random piece...");
			piece := getrandompiece();
			b := batches(piece);
			schedbatches(reqc, peer, b);
		}
	}
}


Req.new(pieceindex, blockindex: int): Req
{
	return Req(pieceindex, blockindex, 0);
}

Req.eq(r1, r2: Req): int
{
	return r1.pieceindex == r2.pieceindex && r1.blockindex == r2.blockindex;
}

Req.text(r: self Req): string
{
	return sprint("Req(piece %d, block %d, begin %d)", r.pieceindex, r.blockindex, r.blockindex*Blocksize);
}


Reqs.new(size: int): ref Reqs
{
	return ref Reqs(array[size] of Req, 0, 0, nil);
}

Reqs.take(r: self ref Reqs, req: Req): int
{
	if(r.isempty())
		raise "take on empty list";

	# first, skip over cancelled requests if necessary
	f := r.first; 
	while(f != r.next && !Req.eq(r.a[f], req) && r.a[f].cancelled)
		f = (f+1) % len r.a;

	# then see if request was the one expected, if any
	if(f == r.next && !Req.eq(r.a[f], req))
		return 0;
	r.first = (f+1) % len r.a;
	return 1;
}

Reqs.peek(r: self ref Reqs): Req
{
	if(r.isempty())
		raise "peek on empty list";
	return r.a[r.first];
}

Reqs.cancel(r: self ref Reqs, req: Req)
{
	for(i := r.first; i != r.next; i = (i+1) % len r.a)
		if(Req.eq(req, r.a[i]))
			r.a[i].cancelled = 1;
}

Reqs.add(r: self ref Reqs, req: Req)
{
	if(r.isfull())
		raise "add on full list";
	r.a[r.next] = req;
	r.next = (r.next+1) % len r.a;
	r.lastreq = ref req;
}

Reqs.flush(r: self ref Reqs)
{
	r.first = r.next = 0;
}

Reqs.last(r: self ref Reqs): ref Req
{
	return r.lastreq;
}

Reqs.isempty(r: self ref Reqs): int
{
	return r.first == r.next;
}

Reqs.isfull(r: self ref Reqs): int
{
	return (r.next+1) % len r.a == r.first;
}

Reqs.count(r: self ref Reqs): int
{
	return (len r.a+r.next-r.first) % len r.a;
}

Reqs.size(r: self ref Reqs): int
{
	return len r.a;
}

Reqs.text(r: self ref Reqs): string
{
	return sprint("Reqs(first %d, next %d, size %d)", r.first, r.next, len r.a);
}


Batch.new(first, n: int, piece: ref Piece): ref Batch
{
	blocks := array[n] of int;
	for(i := 0; i < n; i++)
		blocks[i] = first+i;
	return ref Batch(blocks, piece);
}

Batch.unused(b: self ref Batch): list of Req
{
	reqs: list of Req;
	for(i := len b.blocks-1; i >= 0; i--) {
		busy := b.piece.busy[b.blocks[i]];
		if(busy.t0 < 0 && busy.t1 < 0)
			reqs = Req.new(b.piece.index, b.blocks[i])::reqs;
	}
	return reqs;
}

Batch.usedpartial(b: self ref Batch, peer: ref Peer): list of Req
{
	reqs: list of Req;
	for(i := len b.blocks-1; i >= 0; i--) {
		busy := b.piece.busy[b.blocks[i]];
		if(busy.t0 != peer.id && busy.t1 != peer.id && (busy.t0 < 0 || busy.t1 < 0))
			reqs = Req.new(b.piece.index, b.blocks[i])::reqs;
	}
	return reqs;
}

Batch.text(nil: self ref Batch): string
{
	return "<batch ...>";
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
	Bad =>		s += sprint(" %q %d", p.addr, p.mtime);
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

tablist[T](t: ref Table[T]): list of T
{
	r: list of T;
	for(i := 0; i < len t.items; i++)
		for(l := t.items[i]; l != nil; l = tl l)
			r = (hd l).t1::r;
	return r;
}

say(s: string)
{
	if(dflag)
		warn(s);
}
