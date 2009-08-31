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
include "ip.m";
	ipmod: IP;
	IPaddr: import ipmod;
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";
	bt: Bittorrent;
	Torrent: import bt;
include "util0.m";
	util: Util0;
	hex, sizefmt, preadn, min, warn, rev, l2a, inssort: import util;
include "bittorrentpeer.m";

peergen: int;
ip4mask := IPaddr(array[] of {
	byte 0, byte 0, byte 0, byte 0,
	byte 0, byte 0, byte 0, byte 0,
	byte 0, byte 0, byte 16rff, byte 16rff,
	byte 255, byte 255, byte 255, byte 0});
ip6mask := IPaddr(array[] of {
	byte 16rff, byte 16rff, byte 16rff, byte 16rff,
	byte 16rff, byte 16rff, byte 16rff, byte 16rff,
	byte 0, byte 0, byte 0, byte 0,
	byte 0, byte 0, byte 0, byte 0});

init()
{
	sys = load Sys Sys->PATH;
	daytime = load Daytime Daytime->PATH;
	str = load String String->PATH;
	kr = load Keyring Keyring->PATH;
	rand = load Rand Rand->PATH;
	rand->init(sys->pctl(0, nil)^sys->millisec());
	ipmod = load IP IP->PATH;
	ipmod->init();
	bitarray = load Bitarray Bitarray->PATH;
	bt = load Bittorrent Bittorrent->PATH;
	util = load Util0 Util0->PATH;
	util->init();
}

randomize[T](a: array of T)
{
	for(i := 0; i < len a; i++) {
		j := rand->rand(len a);
		(a[i], a[j]) = (a[j], a[i]);
	}
}

maskip(ipstr: string): string
{
        (ok, ip) := IPaddr.parse(ipstr);
        if(ok != 0)
                return ipstr;
	mask := ip4mask;
	if(!ip.isv4())
		mask = ip6mask;
        return ip.mask(mask).text();
}


Piece.new(index, length: int): ref Piece
{
	nblocks := (length+Blocksize-1)/Blocksize;
	return ref Piece(nil, 0, index, Bits.new(nblocks), Bits.new(nblocks), length, array[nblocks] of {* => (-1, -1)}, array[nblocks] of {* => 0});
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


Piece.hashadd(p: self ref Piece, buf: array of byte)
{
	p.hashstate = kr->sha1(buf, len buf, nil, p.hashstate);
	p.hashstateoff += len buf;
}

Piece.text(p: self ref Piece): string
{
	return sys->sprint("<piece %d have %s>", p.index, p.have.text());
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
	return sys->sprint("<block piece %d, begin %d, length %d>", b.piece, b.begin, b.length);
}


piecenew(t: ref Torrent, index: int): ref Piece
{
	p := Piece.new(index, t.piecelength(index));
	pieces = p::pieces;
	return p;
}

piecedel(p: ref Piece)
{
	new: list of ref Piece;
	for(l := pieces; l != nil; l = tl l)
		if(hd l != p)
			new = hd l::new;
	pieces = new;
}

piecefind(index: int): ref Piece
{
	for(l := pieces; l != nil; l = tl l)
		if((hd l).index == index)
			return hd l;
	return nil;
}


blockhave(l: list of ref Block, b: ref Block): int
{
	for(; l != nil; l = tl l)
		if(Block.eq(hd l, b))
			return 1;
	return 0;
}

blockdel(l: list of ref Block, b: ref Block): list of ref Block
{
	r: list of ref Block;
	for(; l != nil; l = tl l)
		if(!Block.eq(hd l, b))
			r = hd l::r;
	return rev(r);
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


Newpeer.text(np: self Newpeer): string
{
	peerid := "nil";
	if(np.peerid != nil)
		peerid = peeridfmt(np.peerid);
	return sprint("(newpeer %s peerid %s)", np.addr, peerid);
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


Peer.new(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int, npieces: int): ref Peer
{
	getmsgch := chan of list of ref Bittorrent->Msg;
	state := RemoteChoking|LocalChoking;
	msgseq := 0;
	writech := chan[4] of ref (int, int, array of byte);
	readch := chan of ref (int, int, int);
	return ref Peer(
		peergen++,
		np, fd, extensions, peerid, hex(peerid),
		0, getmsgch, nil, nil,
		Reqs.new(Blockqueuesize),
		Bits.new(npieces),
		state,
		msgseq,
		Traffic.new(), Traffic.new(), Traffic.new(), Traffic.new(),
		nil, 0, dialed, Buf.new(), writech, readch, nil);
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
	return sprint("<peer %s id %d>", p.np.addr, p.id);
}

Peer.fulltext(p: self ref Peer): string
{
	return sprint("<peer %s, id %d, wantblocks %d peerid %s>", p.np.text(), p.id, len p.wants, peeridfmt(p.peerid));
}


Buf.new(): ref Buf
{
	return ref Buf(array[0] of byte, -1, 0, 0);
}

Buf.tryadd(b: self ref Buf, piece: ref Piece, begin: int, buf: array of byte): int
{
	if(len b.data == 0) {
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
	return len b.data == Diskchunksize || b.pieceoff+len b.data == b.piecelength;
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


# trackerpeers

trackerpeerdel(np: Newpeer)
{
	n := trackerpeers;
	n = nil;
	for(; trackerpeers != nil; trackerpeers = tl trackerpeers) {
		e := hd trackerpeers;
		if(e.addr != np.addr)
			n = e::n;
	}
	trackerpeers = n;
}

trackerpeeradd(np: Newpeer)
{
	trackerpeers = np::trackerpeers;
}

trackerpeertake(): Newpeer
{
	np := hd trackerpeers;
	trackerpeers = tl trackerpeers;
	return np;
}


# peers

peerconnected(addr: string): int
{
	for(l := peers; l != nil; l = tl l) {
		e := hd l;
		if(e.np.addr == addr)
			return 1;
	}
	return 0;
}


peerdel(peer: ref Peer)
{
	npeers: list of ref Peer;
	for(; peers != nil; peers = tl peers)
		if(hd peers != peer)
			npeers = hd peers::npeers;
	peers = npeers;

	if(luckypeer == peer)
		luckypeer = nil;
}

peeradd(p: ref Peer)
{
	peerdel(p);
	peers = p::peers;
}

peerknownip(ip: string): int
{
	for(l := peers; l != nil; l = tl l)
		if((hd l).np.ip == ip)
			return 1;
	return 0;
}

peerhas(p: ref Peer): int
{
	for(l := peers; l != nil; l = tl l)
		if(p == hd l)
			return 1;
	return 0;
}

peersdialed(): int
{
	i := 0;
	for(l := peers; l != nil; l = tl l)
		if((hd l).dialed)
			i++;
	return i;
}

peerfind(id: int): ref Peer
{
	for(l := peers; l != nil; l = tl l)
		if((hd l).id == id)
			return hd l;
	return nil;
}

peersunchoked(): list of ref Peer
{
	r: list of ref Peer;
	for(l := peers; l != nil; l = tl l)
		if(!(hd l).localchoking())
			r = hd l::r;
	return r;
}

peersactive(): list of ref Peer
{
	r: list of ref Peer;
	for(l := peers; l != nil; l = tl l)
		if(!(hd l).localchoking() && (hd l).remoteinterested())
			r = hd l::r;
	return r;
}


request(reqch: chan of ref (ref Piece, list of Req, chan of int), p: ref Piece, reqs: list of Req)
{
	donech := chan of int;
	reqch <-= ref (p, reqs, donech);
	<-donech;
}

schedbatches(reqch: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer, b: array of ref Batch)
{
	for(i := 0; needblocks(peer) && i < len b; i++)
		request(reqch, b[i].piece, b[i].unused());
}

schedpieces(reqch: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer, a: array of ref Piece)
{
	for(i := 0; needblocks(peer) && i < len a; i++)
		if((peer.piecehave).get(a[i].index)) {
			b := batches(a[i]);
			schedbatches(reqch, peer, b);
		}
}


getrandompiece(t: ref Torrent): ref Piece
{
	# will succeed, this is only called when few pieces are busy
	for(;;) {
		i := rand->rand(piecebusy.n);
		if(!piecebusy.get(i)) {
			p := piecenew(t, i);
			piecebusy.set(i);
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
			if(min < 0 || piececounts[i] < min) {
				rarest = nil;
				min = piececounts[i];
			}
			if(piececounts[i] <= min)
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
	# we just use .t0 of the ref (int, int).  it's used here so we can use polymorphic functions
	a := array[piecebusy.n-piecebusy.have] of ref (int, int);
	say(sprint("inactiverare: %d pieces, %d busy, finding %d", piecebusy.n, piecebusy.have, len a));
	j := 0;
	for(i := 0; j < len a && i < piecebusy.n; i++)
		if(!piecebusy.get(i))
			a[j++] = ref (i, 0);

	randomize(a);
	return a;
}

rarepiececmp(p1, p2: ref Piece): int
{
	return piececounts[p1.index]-piececounts[p2.index];
}

piecesrareorphan(orphan: int): array of ref Piece
{
	r: list of ref Piece;
	for(l := pieces; l != nil; l = tl l) {
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
	return piecehave.have >= Piecesrandom;
}

schedule(t: ref Torrent, reqch: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer)
{
	schedule0(t, reqch, peer);
	reqch <-= nil;
}

schedule0(t: ref Torrent, reqch: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer)
{
	if(rarestfirst()) {
		say("schedule: doing rarest first");

		# attempt to work on last piece this peer was working on
		say("schedule: trying previous piece peer was working on");  # xxx should check if no other peer has taken over the piece?
		req := peer.reqs.last();
		if(req != nil) {
			piece := piecefind(req.pieceindex);
			if(piece != nil)
				schedpieces(reqch, peer, array[] of {piece});
		}

		# find rarest orphan pieces to work on
		say("schedule: trying rarest orphans");
		a := piecesrareorphan(1);
		schedpieces(reqch, peer, a);
		if(!needblocks(peer))
			return;

		# find rarest inactive piece to work on
		say("schedule: trying inactive pieces");
		rare := inactiverare();
		for(i := 0; i < len rare; i++) {
			v := rare[i].t0;  # it's a ref tuple to allow using polymorphic functions
			say(sprint("using new rare piece %d", v));
			p := piecenew(t, v);
			piecebusy.set(v);
			schedpieces(reqch, peer, array[] of {p});
			if(!needblocks(peer))
				return;
		}

		# find rarest active non-orphan piece to work on
		say("schedule: trying rarest non-orphans");
		a = piecesrareorphan(0);
		schedpieces(reqch, peer, a);
		if(!needblocks(peer))
			return;
		
	} else {
		say("schedule: doing random");

		# schedule requests for blocks of active pieces, most completed first:  we want whole pieces fast
		a := l2a(pieces);
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
			schedbatches(reqch, peer, b);
			if(!needblocks(peer))
				return;

			# if more requests needed, start on partially used batches too, in reverse order (for fewer duplicate data)
			say("schedule: trying partially used batches from piece");
			for(k := len b-1; k >= 0; k--) {
				request(reqch, piece, b[k].usedpartial(peer));
				if(!needblocks(peer))
					return;
			}
		}

		# otherwise, get new random pieces to work on
		say("schedule: trying random pieces");
		while(needblocks(peer) && piecebusy.have < piecebusy.n) {
			say("schedule: getting another random piece...");
			piece := getrandompiece(t);
			b := batches(piece);
			schedbatches(reqch, peer, b);
		}
	}
}


chunkreader(fds: list of ref (ref Sys->FD, big), reqch: chan of ref (int, big, chan of (array of byte, string)))
{
	for(;;) {
		req := <-reqch;
		if(req == nil)
			break;

		(n, off, chunkch) := *req;
		while(n > 0) {
			want := min(Diskchunksize, n);
			buf := array[want] of byte;
			err := bt->torrentpreadx(fds, buf, len buf, off);
			if(err != nil) {
				chunkch <-= (nil, err);
				return;
			}
			off += big len buf;
			n -= len buf;
			chunkch <-= (buf, nil);
		}
		chunkch <-= (nil, nil);
	}
}

piecehash(fds: list of ref (ref Sys->FD, big), piecelen: int, p: ref Piece): (array of byte, string)
{
	reqch := chan[1] of ref (int, big, chan of (array of byte, string));
	spawn chunkreader(fds, reqch);

	chunkch := chan of (array of byte, string);
	reqch <-= ref (p.length-p.hashstateoff, big p.index*big piecelen+big p.hashstateoff, chunkch);
	reqch <-= nil;

	state := p.hashstate;
	for(;;) {
		(buf, err) := <-chunkch;
		if(err != nil)
			return (nil, sprint("reading piece %d: %s", p.index, err));
		if(buf == nil)
			break;
		state = kr->sha1(buf, len buf, nil, state);
	}

	hash := array[Keyring->SHA1dlen] of byte;
	kr->sha1(nil, 0, hash, p.hashstate);
	return (hash, nil);
}

reader(t: ref Torrent, fds: list of ref (ref Sys->FD, big), c: chan of (array of byte, string))
{
	have := 0;
	buf := array[t.piecelen] of byte;

	for(; fds != nil; fds = tl fds) {
		(fd, size) := *hd fds;
		o := big 0;
		while(o < size) {
			want := t.piecelen-have;
			if(size-o < big want)
				want = int (size-o);
			nn := preadn(fd, buf[have:], want, o);
			if(nn <= 0) {
				c <-= (nil, sprint("reading: %r"));
				return;
			}
			have += nn;
			o += big nn;
			if(have == t.piecelen) {
				c <-= (buf, nil);
				buf = array[t.piecelen] of byte;
				have = 0;
			}
		}
	}
	if(have != 0)
		c <-= (buf[:have], nil);
}

torrenthash(fds: list of ref (ref Sys->FD, big), t: ref Torrent, haves: ref Bits): string
{
	spawn reader(t, fds, c := chan[2] of (array of byte, string));
	digest := array[kr->SHA1dlen] of byte;
	for(i := 0; i < len t.hashes; i++) {
		(buf, err) := <-c;
		if(err != nil)
			return err;
		kr->sha1(buf, len buf, digest, nil);
		if(hex(digest) == hex(t.hashes[i]))
			haves.set(i);
	}
	return nil;
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
	return sprint("<req piece=%d block=%d begin=%d>", r.pieceindex, r.blockindex, r.blockindex*Blocksize);
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
	first := r.first; 
	for(; first != r.next && !Req.eq(r.a[first], req); first = (first+1) % len r.a)
		if(!r.a[first].cancelled)
			return 0;

	# then see if request was the one expected, if any
	if(first == r.next && !Req.eq(r.a[first], req))
		return 0;
	r.first = (first+1) % len r.a;
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
	return sprint("<reqs first=%d next=%d size=%d>", r.first, r.next, len r.a);
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
	for(i := len b.blocks-1; i >= 0; i--)
		busy := b.piece.busy[b.blocks[i]];
		if(busy.t0 != peer.id && busy.t1 != peer.id && (busy.t0 < 0 || busy.t1 < 0))
			reqs = Req.new(b.piece.index, b.blocks[i])::reqs;
	return reqs;
}

Batch.text(nil: self ref Batch): string
{
	return "<batch ...>";
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


Traffic.new(): ref Traffic
{
	return ref Traffic(0, array[TrafficHistorysize] of {* => (0, 0)}, 0, big 0, 0, daytime->now());
}

Traffic.add(t: self ref Traffic, bytes: int)
{
	time := daytime->now();

	if(t.d[t.last].t0 != time) {
		reclaim(t, time);
		t.last = (t.last+1) % len t.d;
		t.winsum -= t.d[t.last].t1;
		t.d[t.last] = (time, 0);
	}
	t.d[t.last].t1 += bytes;
	t.winsum += bytes;
	t.sum += big bytes;
}

reclaim(t: ref Traffic, time: int)
{
	first := t.last+1;
	for(i := 0; t.d[pos := (first+i) % len t.d].t0 < time-TrafficHistorysize && i < TrafficHistorysize; i++) {
		t.winsum -= t.d[pos].t1;
		t.d[pos] = (0, 0);
	}
}

Traffic.packet(t: self ref Traffic)
{
	t.npackets++;
}

Traffic.rate(t: self ref Traffic): int
{
	time := daytime->now();
	reclaim(t, time);

	div := TrafficHistorysize;
	if(time-t.time0< TrafficHistorysize)
		div = time-t.time0;
	if(div == 0)
		div = 1;
	return t.winsum/div;
}

Traffic.total(t: self ref Traffic): big
{
	return t.sum;
}

Traffic.text(t: self ref Traffic): string
{
	return sprint("<rate %s/s total %s>", sizefmt(big t.rate()), sizefmt(t.total()));
}


Pool[T].new(mode: int): ref Pool[T]
{
	return ref Pool[T](array[0] of T, array[0] of T, 0, mode);
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
	return sprint("<rotation len active=%d len pool=%d poolnext=%d mode=%s>", len p.active, len p.pool, p.poolnext, poolmodes[p.mode]);
}


say(s: string)
{
	if(dflag)
		warn(s);
}
