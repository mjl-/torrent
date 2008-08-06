implement Peers;

include "torrentget.m";
include "string.m";

sys: Sys;
str: String;
rand: Rand;
bitarray: Bitarray;
misc: Misc;
rate: Rate;
requests: Requests;

Traffic: import rate;
Reqs: import requests;
Bits: import bitarray;


peergen:	int;


init(randmod: Rand)
{
	sys = load Sys Sys->PATH;
	str = load String String->PATH;
	rand = randmod;
	bitarray = load Bitarray Bitarray->PATH;
	misc = load Misc Misc->PATH;
	misc->init(rand);
	rate = load Rate Rate->PATH;	
	rate->init();
	requests = load Requests Requests->PATH;
	requests->init();
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
	return misc->hex(d);
}


Newpeer.text(np: self Newpeer): string
{
	peerid := "nil";
	if(np.peerid != nil)
		peerid = peeridfmt(np.peerid);
	return sys->sprint("(newpeer %s peerid %s)", np.addr, peerid);
}


Peer.new(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int, npieces: int): ref Peer
{
	outmsgs := chan of ref Bittorrent->Msg;
	state := RemoteChoking|LocalChoking;
	msgseq := 0;
	writech := chan[4] of ref (int, int, array of byte);
	return ref Peer(
		peergen++,
		np, fd, extensions, peerid, misc->hex(peerid),
		outmsgs, Reqs.new(Torrentget->Blockqueuesize),
		Bits.new(npieces),
		state,
		msgseq,
		Traffic.new(), Traffic.new(), Traffic.new(), Traffic.new(),
		nil, 0, 0, dialed, Buf.new(), writech, nil);
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
	return sys->sprint("<peer %s id %d>", p.np.addr, p.id);
}

Peer.fulltext(p: self ref Peer): string
{
	return sys->sprint("<peer %s, id %d, wantblocks %d peerid %s>", p.np.text(), p.id, len p.wants, peeridfmt(p.peerid));
}


Buf.new(): ref Buf
{
	return ref Buf(array[0] of byte, -1, 0, 0);
}

Buf.tryadd(b: self ref Buf, piece: ref Pieces->Piece, begin: int, buf: array of byte): int
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
	if(begin % Torrentget->Diskchunksize != 0 && begin == b.pieceoff+len b.data) {
		ndata := array[len b.data+len buf] of byte;
		ndata[:] = b.data;
		ndata[len b.data:] = buf;
		b.data = ndata;
		return 1;
	}

	# prepend data to buf, only if it won't cross disk chunk
	if(b.pieceoff % Torrentget->Diskchunksize != 0 && begin == b.pieceoff-len buf) {
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
	return len b.data == Torrentget->Diskchunksize || b.pieceoff+len b.data == b.piecelength;
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
