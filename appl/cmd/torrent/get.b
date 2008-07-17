implement Torrentget;

include "sys.m";
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "daytime.m";
include "string.m";
include "keyring.m";
include "security.m";
include "ip.m";
include "math.m";
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";

sys: Sys;
daytime: Daytime;
str: String;
keyring: Keyring;
random: Random;
ipmod: IP;
bittorrent: Bittorrent;

print, sprint, fprint, fildes: import sys;
Bee, Msg, Torrent: import bittorrent;
IPaddr: import ipmod;


Torrentget: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

Dflag: int;
nofix: int;

torrent: ref Torrent;
dstfds: list of ref (ref Sys->FD, big);  # fd, size
statefd: ref Sys->FD;
starttime: int;
totalleft: big;
listenport: int;
localpeerid: array of byte;
localpeeridhex: string;
trackerevent: string;
piececounts: array of int;  # for each piece, count of peers that have it
trafficup, trafficdown, trafficmetaup, trafficmetadown: ref Traffic;  # global traffic counters.  xxx uses same sliding window as traffic speed used for choking
diskchunkswritten: ref Bits;  # which disk chunks have been written to in the past and must not be written again
maxratio := 0.0;
maxdownload := big -1;
maxupload := big -1;

ip4maskstr:	con "255.255.255.0";
ip4mask:	IPaddr;

# piecekeeper
piecechan: chan of (int, int, int, chan of int);
Add, Remove, Request: con iota;

# tracker
trackkickchan:	chan of int;
trackreqchan:	chan of (big, big, big, int, string);  # up, down, left, listenport, event
trackchan:	chan of (int, array of (string, int, array of byte), string);  # interval, peers, error

Newpeer: adt {
	addr:	string;
	ip:	string;
	peerid:	array of byte;  # may be nil, for incoming connections or compact track responses

	text:	fn(np: self Newpeer): string;
};

# dialer/listener
canlistenchan: chan of int;
newpeerchan: chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);

# upload/download rate limiter
upchan, downchan: chan of (int, chan of int);


Piece: adt {
	index:	int;
	d:	array of byte;
	have:	ref Bits;

	isdone:	fn(p: self ref Piece): int;
	hash:	fn(p: self ref Piece): array of byte;
	text:	fn(p: self ref Piece): string;
};

Traffic: adt {
	last:	int;  # last element in `d' that may have been used
	d:	array of (int, int);  # time, bytes
	winsum:	int;
	sum:	big;
	npackets:	int;

	new:	fn(): ref Traffic;
	add:	fn(t: self ref Traffic, bytes: int);
	packet:	fn(t: self ref Traffic);
	rate:	fn(t: self ref Traffic): int;
	total:	fn(t: self ref Traffic): big;
	text:	fn(t: self ref Traffic): string;
};

Block: adt {
	piece, begin, length:	int;

	new:	fn(piece, begin, length: int): ref Block;
	eq:	fn(b1, b2: ref Block): int;
	text:	fn(b: self ref Block): string;
};

Req: adt {
	pieceindex, blockindex: int;

	eq:	fn(r1, r2: Req): int;
	text:	fn(r: self Req): string;
};

Reqs: adt {
	a:	array of Req;
	first, next:	int;
	lastreq:	ref Req;

	new:	fn(size: int): ref Reqs;
	take:	fn(r: self ref Reqs, req: Req): int;
	add:	fn(r: self ref Reqs, req: Req);
	peek:	fn(r: self ref Reqs): Req;
	flush:	fn(r: self ref Reqs);
	last:	fn(r: self ref Reqs): ref Req;
	isempty:	fn(r: self ref Reqs): int;
	isfull:		fn(r: self ref Reqs): int;
	text:	fn(r: self ref Reqs): string;
};

Peer: adt {
	id:	int;
	np:	Newpeer;
	fd:	ref Sys->FD;
	extensions, peerid: array of byte;
	peeridhex:	string;
	outmsgs:	chan of ref Msg;
	curpiece:	ref Piece;
	reqs:	ref Reqs;
	piecehave:	ref Bits;
	state:	int;
	msgseq:	int;
	up, down, metaup, metadown: ref Traffic;
	wants:	list of ref Block;
	netwriting:	int;
	lastunchoke:	int;
	dialed:	int;

	new:	fn(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int): ref Peer;
	remotechoking:	fn(p: self ref Peer): int;
	remoteinterested:	fn(p: self ref Peer): int;
	localchoking:	fn(p: self ref Peer): int;
	localinterested:	fn(p: self ref Peer): int;
	send:	fn(p: self ref Peer, m: ref Msg);
	isdone:	fn(p: self ref Peer): int;
	text:	fn(p: self ref Peer): string;
	fulltext:	fn(p: self ref Peer): string;
};


Dialersmax:	con 5;  # max number of dialer procs
Dialtimeout:	con 20;  # timeout for connecting to peer
Peersmax:	con 80;
Peersdialedmax:	con 40;
Piecesrandom:	con 4;  # count of first pieces in a download to pick at random instead of rarest-first
Blockqueuemax:	con 100;  # max number of Requests a peer can queue at our side without being considered bad
Blockqueuesize:	con 30;  # number of pending blocks to request to peer
Diskchunksize:	con 128*1024;  # do initial write to disk for any block/piece of this size, to prevent fragmenting the file system

Peeridlen:	con 20;

Listenhost:	con "*";
Listenport:	con 6881;
Listenportrange:	con 100;

Intervalmin:	con 30;
Intervalmax:	con 24*3600;
Intervalneed:	con 10;  # when we need more peers during startup period
Intervaldefault:	con 1800;
Intervalstartupperiod:	con 120;

Blocksize:	con 16*1024;  # block size of blocks we request
Blocksizemax:	con 32*1024;  # max block size allowed for incoming blocks
Unchokedmax:	con 4;
Seedunchokedmax:	con 4;
Stablesecs:	con 10;  # xxx related to TrafficHistorysize...
TrafficHistorysize:	con 10;
Ignorefaultyperiod:	con 300;

# Peer.state
RemoteChoking, RemoteInterested, LocalChoking, LocalInterested: con (1<<iota);


# progress/state
stopped := 0;
ndialers:	int;  # number of active dialers
trackerpeers:	list of Newpeer;  # peers we are not connected to
peers:	list of ref Peer;  # peers we are connected to
rotateips:	list of string;  # masked ip address
luckypeer:	ref Peer;  # current optimistic unchoked peer.  may be stale (not in peers)
peergen:	int;  # sequence number for peers
piecehave:	ref Bits;
piecebusy:	ref Bits;
faulty:		list of (string, int);  # ip, time
islistening:	int;  # whether listener() is listening

peerinmsgchan: chan of (ref Peer, ref Msg);
msgwrittenchan: chan of ref Peer;

# ticker
tickchan: chan of int;


init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	daytime = load Daytime Daytime->PATH;
	str = load String String->PATH;
	keyring = load Keyring Keyring->PATH;
	random = load Random Random->PATH;
	ipmod = load IP IP->PATH;
	ipmod->init();
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init(bitarray);

	arg->init(args);
	arg->setusage(arg->progname()+" [-Dn] [-m ratio] [-d maxdownload] [-u maxupload] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'D' =>	Dflag++;
		'n' =>	nofix = 1;
		'm' =>	maxratio = real arg->earg();
			if(maxratio <= 1.1)
				fail("invalid maximum ratio");
		'd' =>	maxdownload = bittorrent->byteparse(arg->earg());
			if(maxdownload < big 0)
				fail("invalid maximum download rate");
		'u' =>	maxupload = bittorrent->byteparse(arg->earg());
			if(maxupload < big 0)
				fail("invalid maximum upload rate");
		* =>
			fprint(fildes(2), "bad option: -%c\n", c);
			arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();

	err: string;
	(torrent, err) = Torrent.open(hd args);
	if(err != nil)
		fail(sprint("%s: %s", hd args, err));

	sys->pctl(Sys->NEWPGRP, nil);

	created: int;
	(dstfds, created, err) = torrent.openfiles(nofix, 0);
	if(err != nil)
		fail(err);

	if(created) {
		# all new files, we don't have pieces yet
		trackerevent = "started";
		say("no state file needed, all new files");
		piecehave = Bits.new(len torrent.piecehashes);
	} else {
		# attempt to read state of pieces from .torrent.state file
		statefd = sys->open(torrent.statepath, Sys->ORDWR);
		if(statefd != nil) {
			say("using .state file");
			(d, rerr) := readfd(statefd);
			if(rerr != nil)
				fail(sprint("%s: %s", torrent.statepath, rerr));
			(piecehave, err) = Bits.mk(len torrent.piecehashes, d);
			if(err != nil)
				fail(sprint("%s: invalid state", torrent.statepath));
		} else {
			# otherwise, read through all data
			say("starting to check all pieces in files...");
			piecehave = Bits.new(len torrent.piecehashes);
			for(i := 0; i < len torrent.piecehashes; i++) {
				(d, rerr) := bittorrent->pieceread(torrent, dstfds, i);
				if(rerr != nil)
					fail("verifying: "+rerr);
				hash := array[Keyring->SHA1dlen] of byte;
				keyring->sha1(d, len d, hash, nil);
				if(hex(hash) == hex(torrent.piecehashes[i]))
					piecehave.set(i);
			}
		}
	}

	if(isdone())
		say("already done!");

	if(statefd == nil) {
		say(sprint("creating statepath %q", torrent.statepath));
		statefd = sys->create(torrent.statepath, Sys->ORDWR, 8r666);
		if(statefd == nil)
			warn(sprint("failed to create state file (ignoring): %r"));
		else
			writestate();
	}

	ipok: int;
	(ipok, ip4mask) = IPaddr.parse(ip4maskstr);
	if(ipok != 0)
		fail(sprint("bad ip4 mask: %q", ip4maskstr));

	totalleft = torrent.length;
	localpeerid = bittorrent->genpeerid();
	localpeeridhex = hex(localpeerid);

	piecechan = chan of (int, int, int, chan of int);
	trackkickchan = chan of int;
	trackreqchan = chan of (big, big, big, int, string);
	trackchan = chan of (int, array of (string, int, array of byte), string);

	canlistenchan = chan of int;
	newpeerchan = chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);
	tickchan = chan of int;

	upchan = chan of (int, chan of int);
	downchan = chan of (int, chan of int);

	peers = nil;
	piecebusy = Bits.new(len torrent.piecehashes);

	peerinmsgchan = chan of (ref Peer, ref Msg);
	msgwrittenchan = chan of ref Peer;

	piececounts = array[len torrent.piecehashes] of {* => 0};

	trafficup = Traffic.new();
	trafficdown = Traffic.new();
	trafficmetaup = Traffic.new();
	trafficmetadown = Traffic.new();

	diskchunkswritten = Bits.new(int ((torrent.length+big Diskchunksize-big 1)/big Diskchunksize));
	seen := 0;
	for(i := 0; i < piecehave.n && seen < piecehave.have; i++) {
		if(piecehave.get(i)) {
			seen++;
			off := big i*big torrent.piecelen;
			offe := off+big torrent.piecelength(i);
			for(; off < offe; off += big Diskchunksize) {
				chunk := int (off/big Diskchunksize);
				diskchunkswritten.set(chunk);
				say(sprint("chunk %d has been written to, mark as such", chunk));
			}
		}
	}

	# start listener, for incoming connections
	ok := -1;
	conn: Sys->Connection;
	listenaddr: string;

	for(i = 0; i < Listenportrange; i++) {
		listenport = Listenport+i;
		listenaddr = sprint("net!%s!%d", Listenhost, listenport);
		(ok, conn) = sys->announce(listenaddr);
		if(ok == 0)
			break;
	}
	if(ok != 0) {
		say("could not listen on any port, incoming connections will not be possible...");
		listenport = 0;
	} else {
		say(sprint("listening on addr %s", listenaddr));
	}

	spawn listener(conn);
	spawn piecekeeper();
	spawn ticker();
	spawn track();
	spawn limiter(upchan, int maxupload);
	spawn limiter(downchan, int maxdownload);

	starttime = daytime->now();
	spawn trackkick(0);
	main();
}

isdone(): int
{
	return piecehave.n == piecehave.have;
}

writestate()
{
	d := piecehave.d;
	n := sys->pwrite(statefd, d, len d, big 0);
	if(n != len d)
		warn(sprint("writing state: %r"));
	else
		say("state written");
}

trackerpeerdel(np: Newpeer)
{
	n := trackerpeers;
	n = nil;
	for(; trackerpeers != nil; trackerpeers = tl trackerpeers) {
		e := hd trackerpeers;
		if(e.addr == np.addr)
			;
		else
			n = hd trackerpeers::n;
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


peerconnected(addr: string): int
{
	for(l := peers; l != nil; l = tl l) {
		e := hd l;
		if(e.np.addr == addr)
			return 1;
	}
	return 0;
}


peerdrop(peer: ref Peer)
{
	n := 0;
	for(i := 0; i < peer.piecehave.n && n < peer.piecehave.have; i++)
		if(peer.piecehave.get(i)) {
			piececounts[i]--;
			n++;
		}
}

peerdel(peer: ref Peer)
{
	npeers: list of ref Peer;
	for(; peers != nil; peers = tl peers)
		if(hd peers != peer)
			npeers = hd peers::npeers;
		else
			peerdrop(peer);
	peers = npeers;

	if(luckypeer == peer)
		luckypeer = nil;

	if(peer.dialed)
		dialpeers();
	else
		awaitpeer();
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

dialpeers()
{
	say(sprint("dialpeers, %d trackerpeers %d peers", len trackerpeers, len peers));

	while(trackerpeers != nil && ndialers < Dialersmax && len peers < Peersmax && peersdialed() < Peersdialedmax) {
		np := trackerpeertake();
		if(peerknownip(np.ip))
			continue;
		if(isfaulty(np.ip))
			continue;

		say("spawning dialproc for "+np.text());
		spawn dialer(np);
		ndialers++;
	}
}

awaitpeer()
{
	if(peersdialed() < Peersmax-Peersdialedmax && !islistening) {
		islistening = 1;
		spawn kicklistener();
	}
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
	if(!Req.eq(r.a[r.first], req))
		return 0;
	r.first = (r.first+1) % len r.a;
	return 1;
}

Reqs.peek(r: self ref Reqs): Req
{
	if(r.isempty())
		raise "peek on empty list";
	return r.a[r.first];
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

Reqs.text(r: self ref Reqs): string
{
	return sprint("<reqs first=%d next=%d size=%d>", r.first, r.next, len r.a);
}


Peer.new(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int): ref Peer
{
	outmsgs := chan of ref Msg;
	state := RemoteChoking|LocalChoking;
	msgseq := 0;
	return ref Peer(
		peergen++,
		np, fd, extensions, peerid, hex(peerid),
		outmsgs, nil, Reqs.new(Blockqueuesize),
		Bits.new(piecehave.n),
		state,
		msgseq,
		Traffic.new(), Traffic.new(), Traffic.new(), Traffic.new(),
		nil, 0, 0, dialed);
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

Peer.send(p: self ref Peer, msg: ref Msg)
{
	msize := msg.packedsize();
	dsize := 0;
	pick m := msg {
	Piece =>
		dsize = len m.d;
		p.up.add(dsize);
		trafficup.add(dsize);
		p.up.packet();
		trafficup.packet();
	* =>
		p.metaup.packet();
		trafficmetaup.packet();
	}
	p.metaup.add(msize-dsize);
	trafficmetaup.add(msize-dsize);
	p.outmsgs <-= msg;
}

Peer.text(p: self ref Peer): string
{
	return sprint("<peer %s id %d>", p.np.addr, p.id);
}

Peer.fulltext(p: self ref Peer): string
{
	return sprint("<peer %s, id %d, wantblocks %d peerid %s>", p.np.text(), p.id, len p.wants, peeridfmt(p.peerid));
}



Piece.isdone(p: self ref Piece): int
{
	return p.have.n == p.have.have;
}

Piece.hash(p: self ref Piece): array of byte
{
	hash := array[Keyring->SHA1dlen] of byte;
	keyring->sha1(p.d, len p.d, hash, nil);
	return hash;
}

Piece.text(p: self ref Piece): string
{
	return sprint("<piece %d have %s>", p.index, p.have.text());
}


Traffic.new(): ref Traffic
{
	return ref Traffic(0, array[TrafficHistorysize] of {* => (0, 0)}, 0, big 0, 0);
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
	if(time-starttime < TrafficHistorysize)
		div = time-starttime;
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
	return sprint("<rate %s/s total %s>", bittorrent->bytefmt(big t.rate()), bittorrent->bytefmt(t.total()));
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
	return sprint("<block piece %d, begin %d, length %d>", b.piece, b.begin, b.length);
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

blocktake(l: list of ref Block): (list of ref Block, ref Block)
{
	l = rev(l);
	b := hd l;
	return (rev(tl l), b);
}

blockreadsend(p: ref Peer)
{
	b: ref Block;
	(p.wants, b) = blocktake(p.wants);
	say(sprint("%s: sending to %s", b.text(), p.text()));

	(d, rerr) := bittorrent->blockread(torrent, dstfds, b.piece, b.begin, b.length);
	if(rerr != nil) {
		warn("reading block: "+rerr);
		return;
	}

	p.netwriting = 1;
	p.send(ref Msg.Piece(b.piece, b.begin, d));
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

cmp(n1, n2: int): int
{
	if(n1 == n2)
		return n1;
	return n2-n1;
}

peerratecmp(a1, a2: (ref Peer, int)): int
{
	(p1, r1) := a1;
	(p2, r2) := a2;
	n := cmp(r1, r2);
	if(n != 0)
		return n;
	if(p1.remoteinterested() == p2.remoteinterested())
		return 0;
	if(p1.remoteinterested())
		return -1;
	return 1;
}

# xxx should be done more generic
peersort(a: array of (ref Peer, int))
{ 
        for(i := 1; i < len a; i++) { 
                tmp := a[i]; 
                for(j := i; j > 0 && peerratecmp(a[j-1], tmp) > 0; j--) 
                        a[j] = a[j-1]; 
                a[j] = tmp; 
        } 
}

_nextoptimisticunchoke(regen: int): ref Peer
{
	# find next masked ip address to pick peer from (if still present)
	ipmasked: string;
	while(rotateips != nil) {
		(rotateips, ipmasked) = pickrandom(rotateips);

		# find peer from the address pool with oldest unchoke
		peer: ref Peer;
		for(l := peers; l != nil; l = tl l) {
			p := hd l;
			if(maskip(p.np.ip) == ipmasked && (peer == nil || p.lastunchoke < peer.lastunchoke))
				peer = p;
		}
		if(peer != nil)
			return peer;
	}

	if(!regen)
		return nil;

	# rotation list emptied, generate a new one
	for(l := peers; l != nil; l = tl l) {
		p := hd l;
		ip := maskip(p.np.ip);
		if(!has(rotateips, ip))
			rotateips = ip::rotateips;
	}

	return _nextoptimisticunchoke(0);
}

nextoptimisticunchoke(): ref Peer
{
	return _nextoptimisticunchoke(1);
}

choke(p: ref Peer)
{
	p.send(ref Msg.Choke());
	p.state |= LocalChoking;
}

unchoke(p: ref Peer)
{
	p.send(ref Msg.Unchoke());
	p.state &= ~LocalChoking;
	p.lastunchoke = daytime->now();
}

isfaulty(ip: string): int
{
	now := daytime->now();
	for(l := faulty; l != nil; l = tl l)
		if((hd l).t0 == ip && now < (hd l).t1+Ignorefaultyperiod)
			return 1;
	return 0;
}

setfaulty(ip: string)
{
	clearfaulty(nil);
	faulty = (ip, daytime->now())::faulty;
}

clearfaulty(ip: string)
{
	now := daytime->now();
	new: list of (string, int);
	for(l := faulty; l != nil; l = tl l)
		if((hd l).t0 != ip && (hd l).t1+Ignorefaultyperiod < now)
			new = hd l::new;
	faulty = new;
}


ticker()
{
	for(;;) {
		sys->sleep(10*1000);
		tickchan <-= 0;
	}
}

main()
{
	gen := 0;

	for(;;) alt {
	<-tickchan =>
		say(sprint("ticking, %d peers", len peers));
		for(l := peers; l != nil; l = tl l) {
			peer := hd l;
			say(sprint("%s: up %s, down %s, meta %s %s", peer.fulltext(), peer.up.text(), peer.down.text(), peer.metaup.text(), peer.metadown.text()));
		}

		say(sprint("total traffic:  up %s, down %s, meta %s %s", trafficup.text(), trafficdown.text(), trafficmetaup.text(), trafficmetadown.text()));
		
		ratiostr := "infinity";
		down := trafficdown.total();
		if(down != big 0)
			ratiostr = sprint("%.2f", real trafficup.total()/real down);
		say("ratio:  "+ratiostr);
		say(sprint("packets:  up %d, down %d, meta %d %d", trafficup.npackets, trafficdown.npackets, trafficmetaup.npackets, trafficmetadown.npackets));

		etasecs := eta();
		say(sprint("eta: %d: %s", etasecs, etastr(etasecs)));

		# do choking/unchoking algorithm round
		if(isdone()) {
			# we are seeding...

			if(gen % 3 == 2) {
				gen++;
				continue;
			}

			# find the peer that has been unchoked longest
			oldest: ref Peer;
			nunchoked := 0;
			for(l = peers; l != nil; l = tl l) {
				p := hd peers;
				if(!p.localchoking() && p.remoteinterested()) {
					nunchoked++;
					if(oldest == nil || p.lastunchoke < oldest.lastunchoke)
						oldest = p;
				}
			}

			# find all peers that we may want to unchoke randomly
			others: list of ref Peer;
			for(l = peers; l != nil; l = tl l) {
				p := hd peers;
				if(p.remoteinterested() && p.localchoking())
					others = p::others;
			}

			if(oldest != nil && nunchoked+len others >= Seedunchokedmax) {
				choke(oldest);
				nunchoked--;
			}

			while(others != nil && nunchoked < Seedunchokedmax) {
				p: ref Peer;
				(others, p) = pickrandom(others);
				unchoke(p);
				nunchoked++;
			}

			gen++;
		} else {
			# we are "leeching"...

			# new optimistic unchoke?
			if(gen % 3 == 0)
				luckypeer = nextoptimisticunchoke();

			# make sorted array of all peers, sorted by upload rate, then by interestedness
			allpeers := array[len peers] of (ref Peer, int);  # peer, rate
			i := 0;
			luckyindex := -1;
			for(l = peers; l != nil; l = tl l) {
				if(luckypeer != nil && hd peers == luckypeer)
					luckyindex = i;
				allpeers[i++] = (hd peers, (hd peers).down.rate());
			}
			peersort(allpeers);

			# determine N interested peers with highest upload rate
			nintr := 0;
			for(i = 0; nintr < Unchokedmax && i < len allpeers; i++)
				if(allpeers[i].t0.remoteinterested())
					nintr++;
			unchokeend := i;  # index of first peer to choke.  element before (if any) is slowest peer to unchoke

			# replace slowest of N by optimistic unchoke if lucky peer was not already going to be unchoked
			if(luckyindex >= 0 && luckyindex >= unchokeend && unchokeend-1 >= 0) {
				allpeers[luckyindex] = allpeers[unchokeend-1];
				allpeers[unchokeend-1] = (luckypeer, 0);
			}

			# now unchoke the N peers, and all non-interested peers that are faster.  choke all other peers if they weren't already.
			for(i = 0; i < len allpeers; i++) {
				(p, nil) := allpeers[i];
				if(p == nil)
					say(sprint("bad allpeers, len allpeers %d, len peers %d, i %d, unchokeend %d, luckyindex %d, nintr %d", len allpeers, len peers, i, unchokeend, luckyindex, nintr));
				if(i < unchokeend && p.localchoking())
					unchoke(p);
				else if(i >= unchokeend && !p.localchoking())
					choke(p);
			}
			gen++;
		}

	<-trackkickchan =>
		if(!stopped) {
			trackreqchan <-= (trafficup.total(), trafficdown.total(), totalleft, listenport, trackerevent);
			trackerevent = nil;
		}

	(interval, newpeers, trackerr) := <-trackchan =>
		if(trackerr != nil) {
			warn(sprint("tracker error: %s", trackerr));
		} else {
			say("main, new peers");
			for(i := 0; i < len newpeers; i++) {
				(ip, port, peerid) := newpeers[i];
				if(hex(peerid) == localpeeridhex)
					continue;  # skip self
				np := Newpeer(sprint("%s!%d", ip, port), ip, peerid);
				say("new: "+np.text());
				trackerpeerdel(np);
				if(!peerconnected(np.addr))
					trackerpeeradd(np);
				else
					say("already connected to "+np.text());
			}
		}
		dialpeers();

		# schedule next call to tracker
		if(interval < Intervalmin)
			interval = Intervalmin;
		if(interval > Intervalmax)
			interval = Intervalmax;
		if(daytime->now() < starttime+Intervalstartupperiod && !isdone() && len peers+len trackerpeers < Peersmax && interval > Intervalneed)
			interval = Intervalneed;

		say(sprint("next call to tracker will be in %d seconds", interval));
		spawn trackkick(interval);

	(dialed, np, peerfd, extensions, peerid, err) := <-newpeerchan =>
		if(!dialed)
			islistening = 0;

		if(err != nil) {
			warn(sprint("%s: %s", np.text(), err));
		} else if(hex(peerid) == localpeeridhex) {
			say("connected to self, dropping connection...");
		} else if(peerknownip(np.ip)) {
			say("new connection from known ip address, dropping new connection...");
		} else if(isfaulty(np.ip)) {
			say(sprint("connected to faulty ip %s, dropping connection...", np.ip));
		} else {
			peer := Peer.new(np, peerfd, extensions, peerid, dialed);
			spawn peernetreader(peer);
			spawn peernetwriter(peer);
			peeradd(peer);
			say("new peer "+peer.fulltext());

			ip := maskip(np.ip);
			if(!has(rotateips, ip))
				rotateips = ip::rotateips;

			if(piecehave.have == 0) {
				peer.send(ref Msg.Keepalive());
			} else {
				say("sending bitfield to peer: "+piecehave.text());
				d := array[len piecehave.d] of byte;
				d[:] = piecehave.d;
				peer.send(ref Msg.Bitfield(d));
			}

			if(len peersactive() < Unchokedmax) {
				say("unchoking rare new peer: "+peer.text());
				unchoke(peer);
			}
		}

		if(dialed) {
			ndialers--;
			dialpeers();
		} else
			awaitpeer();

	(peer, msg) := <-peerinmsgchan =>
		# xxx fix this code.  it can easily block now

		if(msg == nil) {
			warn("eof from peer "+peer.text());
			peerdel(peer);
			continue;
		}

		peer.msgseq++;

		msize := msg.packedsize();
		dsize := 0;
		pick m := msg {
		Piece =>
			dsize = len m.d;
			peer.down.add(dsize);
			trafficdown.add(dsize);
			peer.down.packet();
			trafficdown.packet();
		* =>
			peer.metadown.packet();
			trafficmetadown.packet();
		}
		peer.metadown.add(msize-dsize);
		trafficmetadown.add(msize-dsize);

		pick m := msg {
                Keepalive =>
			say("keepalive");

		Choke =>
			if(peer.remotechoking()) {
				say(sprint("%s choked us twice...", peer.text()));
				# xxx disconnect?
				continue;
			}

			say(sprint("%s choked us...", peer.text()));
			peer.state |= RemoteChoking;

		Unchoke =>
			if(!peer.remotechoking()) {
				say(sprint("%s unchoked us twice...", peer.text()));
				# xxx disconnect?
				continue;
			}

			say(sprint("%s unchoked us", peer.text()));
			peer.state &= ~RemoteChoking;

			if(peer.curpiece == nil)
				getpiece(peer);

			schedreq(peer);

		Interested =>
			if(peer.remoteinterested()) {
				say(sprint("%s is interested again...", peer.text()));
				# xxx disconnect?
				continue;
			}

			say(sprint("%s is interested", peer.text()));
			peer.state |= RemoteInterested;

			if(!peer.localchoking() && len peersactive() >= Unchokedmax && !isdone()) {
				# xxx choke slowest peer that is not the optimistic unchoke
			}

		Notinterested =>
			if(!peer.remoteinterested()) {
				say(sprint("%s is uninterested again...", peer.text()));
				# xxx disconnect?
				continue;
			}

			say(sprint("%s is no longer interested", peer.text()));
			peer.state &= ~RemoteInterested;

			# xxx if this peer was choked, unchoke another peer?

                Have =>
			if(m.index >= len torrent.piecehashes) {
				say(sprint("%s sent 'have' for invalid piece %d, disconnecting", peer.text(), m.index));
				peerdel(peer);
				continue;
			}
			if(peer.piecehave.get(m.index)) {
				say(sprint("%s already had piece %d", peer.text(), m.index));
				# xxx disconnect?
			} else
				piececounts[m.index]++;

			say(sprint("remote now has piece %d", m.index));
			peer.piecehave.set(m.index);

			interesting(peer);
			if(!peer.remotechoking() && peer.curpiece == nil) {
				getpiece(peer);
				schedreq(peer);
			}

                Bitfield =>
			if(peer.msgseq != 1) {
				say(sprint("%s sent bitfield after first message, disconnecting", peer.text()));
				peerdel(peer);
				continue;
			}

			err: string;
			(peer.piecehave, err) = Bits.mk(len torrent.piecehashes, m.d);
			if(err != nil) {
				say(sprint("%s sent bogus bitfield message: %s, disconnecting", peer.text(), err));
				peerdel(peer);
				continue;
			}
			say("remote sent bitfield, haves "+peer.piecehave.text());

			n := 0;
			for(i := 0; i < peer.piecehave.n && n < peer.piecehave.have; i++)
				if(peer.piecehave.get(i)) {
					piececounts[i]++;
					n++;
				}

			interesting(peer);

                Piece =>
			say(sprint("%s sent data for piece=%d begin=%d length=%d", peer.text(), m.index, m.begin, len m.d));

			piece := peer.curpiece;

			req := Req(m.index, m.begin/Blocksize);
			if(blocksize(req) != len m.d) {
				warn(sprint("%s sent bad size for block %s, disconnecting", peer.text(), req.text()));
				setfaulty(peer.np.ip);
				peerdel(peer);
				continue;
			}

			if(!peer.reqs.take(req)) {
				exp := "nothing";
				if(!peer.reqs.isempty())
					exp = peer.reqs.peek().text();
				warn(sprint("%s sent block %s, expected %s, disconnecting", peer.text(), req.text(), exp));
				setfaulty(peer.np.ip);
				peerdel(peer);
				continue;
			}

			piece.d[m.begin:] = m.d;
			piece.have.set(m.begin/Blocksize);
			totalleft -= big len m.d;

			if(piece.isdone()) {
				wanthash := hex(torrent.piecehashes[piece.index]);
				havehash := hex(piece.hash());
				if(wanthash != havehash) {
					say(sprint("%s from %s did not check out, want %s, have %s, disconnecting", piece.text(), peer.text(), wanthash, havehash));
					setfaulty(peer.np.ip);
					peerdel(peer);
					continue;
				}

				# before writing to disk, ensure data is written with at least Diskchunksize bytes at a time,
				# to prevent fragmentation.
				# xxx in the future we'll write blocks, not only whole pieces (now this doesn't make much sense, pieces will be larger than the 128kb disk chunk size).
				off := big piece.index*big torrent.piecelen;
				offe := off+big torrent.piecelength(piece.index);
				for(; off < offe; off += big Diskchunksize) {
					chunk := int (off/big Diskchunksize);
					if(diskchunkswritten.get(chunk))
						continue;

					say(sprint("writing to chunk %d", chunk));
					chunksize := Diskchunksize;
					if(off+big chunksize > torrent.length)
						chunksize = int (torrent.length-off);
					buf := array[chunksize] of {* => byte 0};
					err := bittorrent->torrentpwritex(dstfds, buf, len buf, off);
					if(err != nil)
						fail("writing disk chunk: "+err);  # xxx fail saner...
					diskchunkswritten.set(chunk);
				}

				err := bittorrent->piecewrite(torrent, dstfds, piece.index, piece.d);
				if(err != nil)
					fail("writing piece: "+err);  # xxx fail saner...

				piecehave.set(piece.index);
				writestate();
				say("piece now done: "+piece.text());
				say(sprint("pieces: have %s, busy %s", piecehave.text(), piecebusy.text()));

				for(l := peers; l != nil; l = tl l) {
					p := hd l;
					p.send(ref Msg.Have(piece.index));
				}

				piece = getpiece(peer);
			}
			schedreq(peer);

			if(isdone()) {
				trackerevent = "completed";
				spawn trackkick(0);
				npeers: list of ref Peer;
				for(l := peers; l != nil; l = tl l) {
					p := hd peers;
					if(p.isdone()) {
						say("done: dropping seed "+p.fulltext());
						peerdrop(p);
					} else {
						npeers = p::npeers;
						# we won't act on becoming interested while unchoked anymore
						if(!p.remoteinterested() && !p.localchoking())
							choke(p);
					}
				}
				peers = rev(npeers);
				print("DONE!\n");
			}

                Request =>
			b := Block.new(m.index, m.begin, m.length);
			say(sprint("%s sent request for %s", peer.text(), b.text()));

			if(m.length > Blocksizemax) {
				warn("requested block too large, disconnecting");
				peerdel(peer);
				continue;
			}

			if(peer.piecehave.get(m.index)) {
				warn("peer requested piece it already claimed to have");
				peerdel(peer);
				setfaulty(peer.np.ip);
				continue;
			}

			if(blockhave(peer.wants, b)) {
				say("peer already wanted block, skipping");
				continue;
			}
			if(len peer.wants >= Blockqueuemax) {
				say(sprint("peer scheduled one too many blocks, already has %d scheduled, disconnecting", len peer.wants));
				peerdel(peer);
				continue;
			}
			peer.wants = b::peer.wants;
			if(!peer.localchoking() && peer.remoteinterested() && !peer.netwriting && len peer.wants > 0)
				blockreadsend(peer);

		Cancel =>
			b := Block.new(m.index, m.begin, m.length);
			say(sprint("%s sent cancel for %s", peer.text(), b.text()));
			nwants := blockdel(peer.wants, b);
			if(len nwants == len peer.wants) {
				say("peer did not want block before, skipping");
				continue;
			}
			peer.wants = nwants;
		}

	peer := <-msgwrittenchan =>
		say(sprint("%s: message written", peer.text()));

		if(!stopped && isdone()) {
			ratio := ratio();
			if(ratio >= 1.1 && ratio >= maxratio) {
				say(sprint("stopping due to max ratio achieved (%.2f)", ratio));
				stopped = 1;

				# disconnect from all peers and don't do further tracker requests
				peers = nil;
				luckypeer = nil;
				rotateips = nil;
			}
		}

		peer.netwriting = 0;
		if(!peer.localchoking() && peer.remoteinterested() && len peer.wants > 0)
			blockreadsend(peer);
	}
}

trackkick(n: int)
{
	sys->sleep(n*1000);
	trackkickchan <-= 1;
}

track()
{
	for(;;) {
		(up, down, left, lport, event) := <-trackreqchan;

		say("getting new tracker info");
		(interval, newpeers, nil, err) := bittorrent->trackerget(torrent, localpeerid, up, down, left, lport, event);
		if(err != nil)
			say("trackerget: "+err);
		else
			say("trackget okay");
		trackchan <-= (interval, newpeers, err);
	}
}


kicklistener()
{
	canlistenchan <-= 1;
}

listener(aconn: Sys->Connection)
{
	<-canlistenchan;
	for(;;) {
		(ok, conn) := sys->listen(aconn);
		if(ok != 0) {
			warn(sprint("listen: %r"));
			continue;
		}

		remote := conn.dir+"/remote";
		(remaddr, rerr) := readfile(remote);
		if(rerr != nil) {
			warn(sprint("reading %s: %s", remote, rerr));
			continue;
		}
		remaddr = str->splitstrl(remaddr, "\n").t0;

		f := conn.dir+"/data";
		fd := sys->open(f, Sys->ORDWR);
		if(fd == nil) {
			warn(sprint("new connection, open %s: %r", f));
			continue;
		}

		(extensions, peerid, err) := handshake(fd);
		
		np := Newpeer(remaddr, str->splitstrl(remaddr, "!").t0, nil);
		if(err != nil)
			say("error handshaking incoming connection: "+err);
		newpeerchan <-= (0, np, fd, extensions, peerid, err);
		<-canlistenchan;
	}
}


# we are allowed to pass `max' bytes per second.
# xxx do more fine-grained allocations.  detect contention and allow smaller pieces?  never give full bandwidth away in one request?
limiter(ch: chan of (int, chan of int), max: int)
{
	allow := max;  # remaining bandwidth in current second
	cur := sys->millisec();  # start of current second

	for(;;) {
		(want, respch) := <-ch;
		if(max < 0) {
			# no rate limiting, let traffic pass
			respch <-= want;
			continue;
		}

		now := sys->millisec();
		if(now < cur)
			cur = now;  # wrap-around, stretch time

		if(now-1000 > cur) {
			# new second, new bandwidth
			cur = now-(now-cur)%1000;
			allow = max;
		}

		if(allow == 0) {
			# we already used all our bandwidth for this second, wait till next one
			sys->sleep(cur+1000-now);
			cur = sys->millisec();
			allow = max;
		}

		get := want;
		if(get > allow)
			get = allow;
		allow -= get;
		respch <-= get;
	}
}


piecekeeper()
{
	awaiting: list of (int, int);  # piece, peer

nextreq:
	for(;;) {
		(reqtype, pieceindex, peer, reqch) := <-piecechan;
		case reqtype {
		Add =>
			# we are currently expecting pieceindex from peer
			awaiting = (pieceindex, peer)::awaiting;
		Remove =>
			# we are no longer expecting pieceindex from peer.  if pieceindex is -1, we no longer expect anything from peer
			new := awaiting;
			new = nil;
			for(; awaiting != nil; awaiting = tl awaiting)
				if((pieceindex == -1 || (hd awaiting).t0 == pieceindex) && (hd awaiting).t1 == peer)
					;
				else
					new = hd awaiting::new;
		Request =>
			# peer proc asks whether we are expecting piece from peer
			for(l := awaiting; l != nil; l = tl l)
				if((hd l).t0 == pieceindex && (hd l).t1 == peer) {
					reqch <-= 1;
					continue nextreq;
				}
			reqch <-= 0;
		}
	}
}


dialkiller(pidch: chan of int, pid: int, np: Newpeer)
{
	pidch <-= sys->pctl(0, nil);
	sys->sleep(Dialtimeout*1000);
	kill(pid);
	newpeerchan <-= (1, np, nil, nil, nil, sprint("dial/handshake %s: timeout", np.addr));
}

dialer(np: Newpeer)
{
	pid := sys->pctl(0, nil);
	spawn dialkiller(pidch := chan of int, pid, np);
	killerpid := <-pidch;
	
	addr := sprint("net!%s", np.addr);
	(ok, conn) := sys->dial(addr, nil);
	if(ok < 0) {
		kill(killerpid);
		newpeerchan <-= (1, np, nil, nil, nil, sprint("dial %s: %r", np.addr));
		return;
	}

	say("dialed "+addr);
	fd := conn.dfd;

	(extensions, peerid, err) := handshake(fd);
	if(err != nil)
		fd = nil;
	kill(killerpid);
	newpeerchan <-= (1, np, fd, extensions, peerid, err);
}

handshake(fd: ref Sys->FD): (array of byte, array of byte, string)
{
	d := array[20+8+20+20] of byte;
	i := 0;
	d[i++] = byte 19;
	d[i:] = array of byte "BitTorrent protocol";
	i += 19;
	d[i:] = array[8] of {* => byte '\0'};
	i += 8;
	d[i:] = torrent.hash;
	i += 20;
	d[i:] = localpeerid;
	i += 20;
	if(i != len d)
		fail("bad peer header, internal error");

	n := netwrite(fd, d, len d);
	if(n != len d)
		return (nil, nil, sprint("writing peer header: %r"));

	rd := array[len d] of byte;
	n = netread(fd, rd, len rd);
	if(n < 0)
		return (nil, nil, sprint("reading peer header: %r"));
	if(n != len rd)
		return (nil, nil, sprint("short read on peer header (%d)", n));

	extensions := rd[20:20+8];
	peerid := rd[20+8+20:];

	return (extensions, peerid, nil);
}

wantpeerpieces(p: ref Peer): ref Bits
{
	b := Bits.union(array[] of {piecehave, piecebusy});
	b.invert();
	b = Bits.and(array[] of {p.piecehave, b});

	say("pieces peer has and we are interested in: "+b.text());
	return b;
}

interesting(p: ref Peer)
{
	if(p.localinterested())
		return;

	b := wantpeerpieces(p);
	if(!b.isempty()) {
		say("we are now interested in "+p.text());
		p.state |= LocalInterested;
		p.send(ref Msg.Interested());
	}
}



setpiece(peer: ref Peer, index: int)
{
	piecelen := torrent.piecelength(index);
	nblocks := (piecelen+Blocksize-1)/Blocksize;
	piece := ref Piece(index, array[piecelen] of byte, Bits.new(nblocks));

	say(sprint("assigned %s to %s", piece.text(), peer.text()));
	peer.curpiece = piece;
	piecebusy.set(index);
}


getrandompiece(peer: ref Peer, b: ref Bits)
{
	skip := random->randomint(Random->NotQuiteRandom) % b.have;

	for(i := 0; i < b.n; i++)
		if(b.get(i)) {
			if(skip <= 0) {
				say(sprint("choose random piece %d", i));
				return setpiece(peer, i);
			}
			skip--;
		}
}

getrarestpiece(peer: ref Peer, b: ref Bits)
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
	skip := random->randomint(Random->NotQuiteRandom) % b.have;
	while(skip-- > 0)
		rarest = tl rarest;
	index := hd rarest;
	say(sprint("choose rarest piece %d", index));
	setpiece(peer, index);
}

getpiece(peer: ref Peer): ref Piece
{
	peer.curpiece = nil;
	b := wantpeerpieces(peer);
	if(b.isempty()) {
		say("no piece to get from "+peer.text());
		return nil;
	}

	if(piecehave.have < Piecesrandom)
		getrandompiece(peer, b);
	else
		getrarestpiece(peer, b);
	return peer.curpiece;  # should always be non-nil...
}

blocksize(req: Req): int
{
	# first a quick check
	if(req.pieceindex < len torrent.piecehashes-1)
		return Blocksize;

	# otherwise, the full check
	if(big req.pieceindex*big torrent.piecelen + big (req.blockindex+1)*big Blocksize > torrent.length)
		return int (torrent.length % big Blocksize);
	return Blocksize;
}

schedreq(peer: ref Peer)
{
	piece := peer.curpiece;

	if(piece == nil)
		return;

	while(!peer.reqs.isfull()) {
		req: Req;
		last := peer.reqs.last();
		if(last == nil || last.pieceindex != piece.index)
			req = Req(piece.index, 0);
		else if((last.blockindex+1)*Blocksize < torrent.piecelength(last.pieceindex))
			req = Req(piece.index, last.blockindex+1);
		else
			return;
		say(sprint("requesting next block, %s", req.text()));
		peer.reqs.add(req);
		peer.send(ref Msg.Request(req.pieceindex, req.blockindex*Blocksize, blocksize(req)));
	}
}


netread(fd: ref Sys->FD, buf: array of byte, n: int): int
{
	read := 0;
	while(n > 0) {
		downchan <-= (n, respch := chan of int);
		can := <-respch;
		nn := sys->readn(fd, buf, can);
		if(nn < 0)
			return nn;
		if(nn != can)
			return read+nn;
		n -= can;
		buf = buf[can:];
		read += can;
	}
	return read;
}

netwrite(fd: ref Sys->FD, buf: array of byte, n: int): int
{
	wrote := 0;
	while(n > 0) {
		upchan <-= (n, respch := chan of int);
		can := <-respch;
		nn := sys->write(fd, buf, can);
		if(nn < 0)
			return nn;
		if(nn != can)
			return wrote+nn;
		n -= can;
		buf = buf[can:];
		wrote += can;
	}
	return wrote;
}


# copied from bittorrent.b Msg.read
msgread(fd: ref Sys->FD): (ref Msg, string)
{
	buf := array[4] of byte;
	n := netread(fd, buf, len buf);
	if(n < 0)
		return (nil, sprint("reading: %r"));
	if(n < len buf)
		return (nil, sprint("short read"));
	(size, nil) := bittorrent->g32(buf, 0);
	buf = array[size] of byte;
say(sprint("msg.read: have size=%d", size));


	n = netread(fd, buf, len buf);
	if(n < 0)
		return (nil, sprint("reading: %r"));
	if(n < len buf)
		return (nil, sprint("short read"));

	return Msg.unpack(buf);
}


peernetreader(peer: ref Peer)
{
	for(;;) {
		(m, err) := msgread(peer.fd);
		if(err != nil)
			fail("reading msg: "+err);  # xxx return error to main
		fprint(fildes(2), "<< %s\n", m.text());
		peerinmsgchan <-= (peer, m);
	}
}

peernetwriter(peer: ref Peer)
{
	for(;;) {
		m := <- peer.outmsgs;
		if(m == nil)
			return;
		fprint(fildes(2), ">> %s\n", m.text());
		d := m.pack();
		n := netwrite(peer.fd, d, len d);
		if(n != len d)
			fail(sprint("writing msg: %r"));
		if(tagof m == tagof Msg.Piece)
			msgwrittenchan <-= peer;
	}
}


pickrandom[T](l: list of T): (list of T, T)
{
	if(len l == 0)
		return (l, nil);

	elem: T;
	new: list of T;
	skip := random->randomint(Random->NotQuiteRandom) % len l;
	for(; l != nil; l = tl l) {
		if(skip == 0)
			elem = hd l;
		else
			new = hd l::new;
		skip--;
	}
	return (rev(new), elem);
}

maskip(ipstr: string): string
{
	(ok, ip) := IPaddr.parse(ipstr);
	if(ok != 0)
		return ipstr;
	return ip.mask(ip4mask).text();
}

ratio(): real
{
	up := trafficup.total();
	down := trafficdown.total();
	if(down == big 0)
		return Math->Infinity;
	return real up/real down;
}

etastr(secs: int): string
{
	if(secs < 0)
		return "stalled";
	else if(secs < 60*60)
		return sprint("%3dm %3ds", secs / 60, secs % 60);
	else if(secs < 24*60*60)
		return sprint("%3dh %3dm", secs / (60*60), secs % (60*60) / 60);
	else if(secs < 366*24*60*60)
		return sprint("%3dd %3dh", secs / (24*60*60), secs % (24*60*60) / (60*60));
	else
		return "  > year";
}

eta(): int
{
	rate := trafficdown.rate();
	if(rate <= 0)
		return -1;
	return int (totalleft / big rate);
}

readfile(f: string): (string, string)
{
	fd := sys->open(f, Sys->OREAD);
	if(fd == nil)
		return (nil, sprint("open: %r"));
	(d, err) := readfd(fd);
	if(err != nil)
		return (nil, err);
	return (string d, err);
}

readfd(fd: ref Sys->FD): (array of byte, string)
{
	n := sys->readn(fd, buf := array[32*1024] of byte, len buf);
	if(n < 0)
		return (nil, sprint("read: %r"));
	return (buf[:n], nil);
}

hex(d: array of byte): string
{
	s := "";
	for(i := 0; i < len d; i++)
		s += sprint("%02x", int d[i]);
	return s;
}

progctl(pid: int, s: string)
{
	path := sprint("/prog/%d/ctl", pid);
	fd := sys->open(path, Sys->OWRITE);
	if(fd != nil)
		fprint(fd, "%s", s);
}

killgrp(pid: int)
{
	progctl(pid, "killgrp");
}

kill(pid: int)
{
	progctl(pid, "kill");
}

has[T](l: list of T, e: T): int
{
	for(; l != nil; l = tl l)
		if(hd l == e)
			return 1;
	return 0;
}

rev[T](l: list of T): list of T
{
	r: list of T;
	for(; l != nil; l = tl l)
		r = hd l::r;
	return r;
}

fail(s: string)
{
	warn(s);
	raise "fail:"+s;
}

warn(s: string)
{
	fprint(fildes(2), "%s\n", s);
}

say(s: string)
{
	if(Dflag)
		fprint(fildes(2), "%s\n", s);
}
