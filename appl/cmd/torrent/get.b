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
include "rand.m";
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
rand: Rand;
ipmod: IP;
bittorrent: Bittorrent;

print, sprint, fprint, fildes: import sys;
Bee, Msg, Torrent: import bittorrent;
IPaddr: import ipmod;
DigestState: import keyring;


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
maxratio := 0.0;
maxdownload := big -1;
maxupload := big -1;
pieces: list of ref Piece;  # only active pieces

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
	hashstate:	ref DigestState;
	hashstateoff:	int;
	index:	int;
	have:	ref Bits;
	length:	int;
	busy:	array of (int, int);  # peerid, peerid

	new:	fn(index: int): ref Piece;
	isdone:	fn(p: self ref Piece): int;
	orphan:	fn(p: self ref Piece): int;
	hashadd:	fn(p: self ref Piece, buf: array of byte);
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
	pieceindex, blockindex, cancelled: int;

	new:	fn(pieceindex, blockindex: int): Req;
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
	cancel:	fn(r: self ref Reqs, req: Req);
	flush:	fn(r: self ref Reqs);
	last:	fn(r: self ref Reqs): ref Req;
	isempty:	fn(r: self ref Reqs): int;
	isfull:		fn(r: self ref Reqs): int;
	count:	fn(r: self ref Reqs): int;
	size:	fn(r: self ref Reqs): int;
	text:	fn(r: self ref Reqs): string;
};

Batch: adt {
	blocks:	array of int;
	piece:	ref Piece;

	new:	fn(first, n: int, piece: ref Piece): ref Batch;
	unused:	fn(b: self ref Batch): list of Req;
	usedpartial:	fn(b: self ref Batch, peer: ref Peer): list of Req;
	text:	fn(b: self ref Batch): string;
};

Buf: adt {
	data:	array of byte;
	piece:	int;
	pieceoff:	int;

	new:	fn(): ref Buf;
	tryadd:	fn(b: self ref Buf, index, begin: int, buf: array of byte): int;
	isfull:	fn(b: self ref Buf): int;
	clear:	fn(b: self ref Buf);
	overlaps:	fn(b: self ref Buf, piece, begin, end: int): int;
};

Peer: adt {
	id:	int;
	np:	Newpeer;
	fd:	ref Sys->FD;
	extensions, peerid: array of byte;
	peeridhex:	string;
	outmsgs:	chan of ref Msg;
	reqs:	ref Reqs;
	piecehave:	ref Bits;
	state:	int;
	msgseq:	int;
	up, down, metaup, metadown: ref Traffic;
	wants:	list of ref Block;
	netwriting:	int;
	lastunchoke:	int;
	dialed:	int;
	buf:	ref Buf;

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

Rotation: adt[T] {
	a:	array of T;

	new:	fn(): ref Rotation[T];
	takerandom:	fn(r: self ref Rotation): T;
	add:	fn(r: self ref Rotation, e: T);
	addunique:	fn(r: self ref Rotation, e: T);
	has:	fn(r: self ref Rotation, e: T): int;
	text:	fn(r: self ref Rotation): string;
};


Dialersmax:	con 5;  # max number of dialer procs
Dialtimeout:	con 20;  # timeout for connecting to peer
Peersmax:	con 80;
Peersdialedmax:	con 40;
Piecesrandom:	con 4;  # count of first pieces in a download to pick at random instead of rarest-first
Blocksize:	con 16*1024;  # block size of blocks we request
Blockqueuemax:	con 100;  # max number of Requests a peer can queue at our side without being considered bad
Blockqueuesize:	con 30;  # number of pending blocks to request to peer
Diskchunksize:	con 128*1024;  # do initial write to disk for any block/piece of this size, to prevent fragmenting the file system
Batchsize:	con Diskchunksize/Blocksize;
Netiounit:	con 1500-20-20;  # typical network data io unit, ethernet-ip-tcp

Peeridlen:	con 20;

Listenhost:	con "*";
Listenport:	con 6881;
Listenportrange:	con 100;

Intervalmin:	con 30;
Intervalmax:	con 24*3600;
Intervalneed:	con 10;  # when we need more peers during startup period
Intervaldefault:	con 1800;
Intervalstartupperiod:	con 120;

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
rotateips:	ref Rotation[string];  # masked ip address
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
	rand = load Rand Rand->PATH;
	rand->init(random->randomint(Random->ReallyRandom));
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
			if(maxupload < big (10*1024))
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

	rotateips = Rotation[string].new();

	# start listener, for incoming connections
	ok := -1;
	conn: Sys->Connection;
	listenaddr: string;

	for(i := 0; i < Listenportrange; i++) {
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

rarestfirst(): int
{
	return piecehave.have >= Piecesrandom;
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

	for(l := pieces; l != nil; l = tl l) {
		piece := hd l;
		for(i := 0; i < len piece.busy; i++) {
			if(piece.busy[i].t0 == peer.id)
				piece.busy[i].t0 = -1;
			if(piece.busy[i].t1 == peer.id)
				piece.busy[i].t1 = -1;
		}
	}

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

peerfind(id: int): ref Peer
{
	for(l := peers; l != nil; l = tl l)
		if((hd l).id == id)
			return hd l;
	return nil;
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


Buf.new(): ref Buf
{
	return ref Buf(array[0] of byte, -1, 0);
}

Buf.tryadd(b: self ref Buf, index, begin: int, buf: array of byte): int
{
	if(len b.data == 0) {
		b.data = array[len buf] of byte;
		b.data[:] = buf;
		b.piece = index;
		b.pieceoff = begin;
		return 1;
	}

	if(index != b.piece)
		return 0;

	if(b.isfull())
		return 0;

	# append data to buf, only if it won't cross disk chunk
	if(begin % Diskchunksize != 0 && begin == b.pieceoff+len b.data) {
		say("buf: appending data to buffer");
		ndata := array[len b.data+len buf] of byte;
		ndata[:] = b.data;
		ndata[len b.data:] = buf;
		b.data = ndata;
		return 1;
	}

	# prepend data to buf, only if it won't cross disk chunk
	if(b.pieceoff % Diskchunksize != 0 && begin == b.pieceoff-len buf) {
		say("buf: prepending data to buffer");
		ndata := array[len buf+len b.data] of byte;
		ndata[:] = buf;
		ndata[len buf:] = b.data;
		b.data = ndata;
		b.pieceoff = begin;
		return 1;
	}

	return 0;
}

bufflush(b: ref Buf)
{
	say(sprint("buf: writing chunk to disk, pieceoff %d, len data %d", b.pieceoff, len b.data));
	off := big b.piece*big torrent.piecelen + big b.pieceoff;
	err := bittorrent->torrentpwritex(dstfds, b.data, len b.data, off);
	if(err != nil)
		fail("writing piece: "+err);  # xxx fail saner...
	b.clear();
}

Buf.isfull(b: self ref Buf): int
{
	piecelength := torrent.piecelength(b.piece);
	return len b.data == Diskchunksize || b.pieceoff+len b.data == piecelength;
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

Peer.new(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int): ref Peer
{
	outmsgs := chan of ref Msg;
	state := RemoteChoking|LocalChoking;
	msgseq := 0;
	return ref Peer(
		peergen++,
		np, fd, extensions, peerid, hex(peerid),
		outmsgs, Reqs.new(Blockqueuesize),
		Bits.new(piecehave.n),
		state,
		msgseq,
		Traffic.new(), Traffic.new(), Traffic.new(), Traffic.new(),
		nil, 0, 0, dialed, Buf.new());
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


Rotation[T].new(): ref Rotation[T]
{
	return ref Rotation[T](array[0] of T);
}

Rotation[T].takerandom(r: self ref Rotation): T
{
	if(len r.a == 0)
		return nil;

	i := rand->rand(len r.a);
	res := r.a[i];
	r.a[i] = r.a[len r.a-1];
	r.a = r.a[:len r.a-1];
	return res;
}

Rotation[T].add(r: self ref Rotation, e: T)
{
	newa := array[len r.a+1] of T;
	newa[:] = r.a;
	newa[len r.a] = e;
}

Rotation[T].addunique(r: self ref Rotation, e: T)
{
	if(!r.has(e))
		r.add(e);
}

Rotation[T].has(r: self ref Rotation, e: T): int
{
	for(i := 0; i < len r.a; i++)
		if(r.a[i] == e)
			return 1;
	return 0;
}


Rotation[T].text(r: self ref Rotation): string
{
	return sprint("<rotation len a=%d>", len r.a);
}


Piece.new(index: int): ref Piece
{
	length := torrent.piecelength(index);
	nblocks := (length+Blocksize-1)/Blocksize;
	return ref Piece(nil, 0, index, Bits.new(nblocks), length, array[nblocks] of {* => (-1, -1)});
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
	p.hashstate = keyring->sha1(buf, len buf, nil, p.hashstate);
	p.hashstateoff += len buf;
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

peerratecmp(a1, a2: ref (ref Peer, int)): int
{
	(p1, r1) := *a1;
	(p2, r2) := *a2;
	n := cmp(r1, r2);
	if(n != 0)
		return n;
	if(p1.remoteinterested() == p2.remoteinterested())
		return 0;
	if(p1.remoteinterested())
		return -1;
	return 1;
}

_nextoptimisticunchoke(regen: int): ref Peer
{
	# find next masked ip address to pick peer from (if still present)
	while((ipmasked := rotateips.takerandom()) != nil) {
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
	for(l := peers; l != nil; l = tl l)
		rotateips.addunique(maskip((hd l).np.ip));

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

batchflushcomplete(piece: ref Piece, block: int): int
{
	b := batches(piece)[block/Batchsize];
	for(i := 0; i < len b.blocks; i++)
		if(!piece.have.get(b.blocks[i]))
			return 0;

	start := (block/Batchsize)*Batchsize*Blocksize;
	end := start+Batchsize*Blocksize;
	for(l := peers; l != nil; l = tl l) {
		peer := hd l;
		if(peer.buf.overlaps(piece.index, start, end))
			bufflush(peer.buf);
	}
	return 1;
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
				if(p.remoteinterested() && p.localchoking() && p != oldest)
					others = p::others;
			}

			# choke oldest unchoked peer if we want to reuse its slot
			if(oldest != nil && nunchoked+len others >= Seedunchokedmax) {
				choke(oldest);
				nunchoked--;
			}

			othersa := l2a(others);
			randomize(othersa);
			for(i := 0; i < len othersa && nunchoked+i < Seedunchokedmax; i++)
				unchoke(othersa[i]);

			gen++;
		} else {
			# we are "leeching"...

			# new optimistic unchoke?
			if(gen % 3 == 0)
				luckypeer = nextoptimisticunchoke();

			# make sorted array of all peers, sorted by upload rate, then by interestedness
			allpeers := array[len peers] of ref (ref Peer, int);  # peer, rate
			i := 0;
			luckyindex := -1;
			for(l = peers; l != nil; l = tl l) {
				if(luckypeer != nil && hd peers == luckypeer)
					luckyindex = i;
				allpeers[i++] = ref (hd peers, (hd peers).down.rate());
			}
			sort(allpeers, peerratecmp);

			# determine N interested peers with highest upload rate
			nintr := 0;
			for(i = 0; nintr < Unchokedmax && i < len allpeers; i++)
				if(allpeers[i].t0.remoteinterested())
					nintr++;
			unchokeend := i;  # index of first peer to choke.  element before (if any) is slowest peer to unchoke

			# replace slowest of N by optimistic unchoke if lucky peer was not already going to be unchoked
			if(luckyindex >= 0 && luckyindex >= unchokeend && unchokeend-1 >= 0) {
				allpeers[luckyindex] = allpeers[unchokeend-1];
				allpeers[unchokeend-1] = ref (luckypeer, 0);
			}

			# now unchoke the N peers, and all non-interested peers that are faster.  choke all other peers if they weren't already.
			for(i = 0; i < len allpeers; i++) {
				(p, nil) := *allpeers[i];
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

			rotateips.addunique(maskip(np.ip));

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

			schedule(peer);

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
			if(!peer.remotechoking())
				schedule(peer);

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

			piece := piecefind(m.index);
			if(piece == nil)
				fail(sprint("could not find piece %d", m.index));

			req := Req.new(m.index, m.begin/Blocksize);
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

			blockindex := m.begin/Blocksize;

			if(piece.have.get(blockindex)) {
				say("already have this block, skipping...");
				continue;
			}

			# possibly send cancel to other peer
			busy := piece.busy[blockindex];
			busypeer: ref Peer;
			if(busy.t0 >= 0 && busy.t0 != peer.id)
				busypeer = peerfind(busy.t0);
			else if(busy.t1 >= 0 && busy.t1 != peer.id)
				busypeer = peerfind(busy.t1);
			if(busypeer != nil) {
				peer.reqs.cancel(Req.new(m.index, m.begin/Blocksize));
				peer.send(ref Msg.Cancel(m.index, m.begin, len m.d));
			}

			# add block to buffer.  if new block does not fit in, flush the old data.
			if(!peer.buf.tryadd(m.index, m.begin, m.d)) {
				bufflush(peer.buf);
				if(!peer.buf.tryadd(m.index, m.begin, m.d))
					fail("tryadd failed...");
			}

			# flush now, rather then delaying it
			if(peer.buf.isfull())
				bufflush(peer.buf);

			# if this completes the batch, flush it for all peers that have blocks belonging to it
			batchflushcomplete(piece, blockindex);

			# progress with piece
			if(piece.hashstateoff == m.begin)
				piece.hashadd(m.d);

			piece.have.set(m.begin/Blocksize);
			totalleft -= big len m.d;

			if(piece.isdone()) {
				# flush all bufs about this piece, also from other peers, to disk.  to make hash-checking read right data.
				for(l := peers; l != nil; l = tl l) {
					p := hd l;
					if(p.buf.piece == m.index) {
						say("flushing buf of other peer before hash check");
						bufflush(p.buf);
					}
				}

				wanthash := hex(torrent.piecehashes[piece.index]);
				(piecehash, herr) := piecehash(piece);
				if(herr != nil)
					fail("verifying hash: "+herr);
				havehash := hex(piecehash);
				if(wanthash != havehash) {
					say(sprint("%s from %s did not check out, want %s, have %s, disconnecting", piece.text(), peer.text(), wanthash, havehash));
					setfaulty(peer.np.ip);
					peerdel(peer);
					# xxx what do to with other peers?
					continue;
				}

				piecehave.set(piece.index);
				piecedel(piecefind(piece.index));

				writestate();
				say("piece now done: "+piece.text());
				say(sprint("pieces: have %s, busy %s", piecehave.text(), piecebusy.text()));

				for(l = peers; l != nil; l = tl l) {
					p := hd l;
					p.send(ref Msg.Have(piece.index));
				}
			}
			schedule(peer);

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


min(a, b: int): int
{
	if(a < b)
		return a;
	return b;
}

# we are allowed to pass `max' bytes per second.
limiter(ch: chan of (int, chan of int), max: int)
{
	maxallow := min(max, Netiounit);

	for(;;) {
		(want, respch) := <-ch;
		if(max <= 0) {
			# no rate limiting, let traffic pass
			respch <-= want;
			continue;
		}

		give := min(maxallow, want);
		respch <-= give;
		sys->sleep(980*give/max);  # don't give out more bandwidth until this portion has run out
	}
}


piecenew(index: int): ref Piece
{
	p := Piece.new(index);
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

piecehash(p: ref Piece): (array of byte, string)
{
	while(p.hashstateoff < p.length) {
		size := min(Diskchunksize, p.length-p.hashstateoff);
		buf := array[size] of byte;
		err := bittorrent->torrentpreadx(dstfds, buf, len buf, big p.index*big torrent.piecelen+big p.hashstateoff);
		if(err != nil)
			return (nil, err);
		p.hashadd(buf);
	}
	
	hash := array[Keyring->SHA1dlen] of byte;
	keyring->sha1(nil, 0, hash, p.hashstate);
	return (hash, nil);
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


getrandompiece(): ref Piece
{
	# will succeed, this is only called when few pieces are busy
	for(;;) {
		i := rand->rand(piecebusy.n);
		if(!piecebusy.get(i)) {
			p := piecenew(i);
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


batches(p: ref Piece): array of ref Batch
{
	nblocks := p.have.n;
	nbatches := (nblocks+Batchsize-1)/Batchsize;
	b := array[nbatches] of ref Batch;
	for(i := 0; i < len b; i++)
		b[i] = Batch.new(i*Batchsize, min(Batchsize, nblocks-i*Batchsize), p);
	return b;
}

needblocks(peer: ref Peer): int
{
	r := peer.reqs.count() == 0 || peer.reqs.count()+Batchsize < peer.reqs.size();
	if(r)
		say("need more blocks...");
	else
		say("no more blocks needed");
	return r;
}

request(peer: ref Peer, piece: ref Piece, reqs: list of Req)
{
	for(; reqs != nil; reqs = tl reqs) {
		req := hd reqs;
		busy := piece.busy[req.blockindex];
		if(busy.t0 < 0)
			piece.busy[req.blockindex].t0 = peer.id;
		else if(busy.t1 < 0)
			piece.busy[req.blockindex].t1 = peer.id;
		else
			fail("both slots busy...");

		# xxx send in one packet?
		say("request: requesting "+req.text());
		peer.reqs.add(req);
		peer.send(ref Msg.Request(req.pieceindex, req.blockindex*Blocksize, blocksize(req)));
	}
}

schedbatches(peer: ref Peer, b: array of ref Batch)
{
	for(i := 0; needblocks(peer) && i < len b; i++)
		request(peer, b[i].piece, b[i].unused());
}


progresscmp(p1, p2: ref Piece): int
{
	return p2.have.n-p1.have.n;
}

schedpieces(peer: ref Peer, a: array of ref Piece)
{
	for(i := 0; needblocks(peer) && i < len a; i++)
		if(peer.piecehave.get(a[i].index)) {
			b := batches(a[i]);
			schedbatches(peer, b);
		}
}


inactiverare(): array of int
{
	a := array[piecebusy.n-piecebusy.have] of int;
	say(sprint("inactiverare: %d pieces, %d busy, finding %d", piecebusy.n, piecebusy.have, len a));
	j := 0;
	for(i := 0; j < len a && i < piecebusy.n; i++)
		if(!piecebusy.get(i))
			a[j++] = i;

	randomizeint(a);
	return a;
}

randomizeint(a: array of int)
{
	for(i := 0; i < len a; i++) {
		newi := rand->rand(len a);
		tmp := a[i];
		a[i] = a[newi];
		a[newi] = tmp;
	}
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
	sort(a, rarepiececmp);
	return a;
}

schedule(peer: ref Peer)
{
	if(!needblocks(peer))
		return;

	if(rarestfirst()) {
		say("schedule: doing rarest first");

		# attempt to work on last piece this peer was working on
		say("schedule: trying previous piece peer was working on");  # xxx should check if no other peer has taken over the piece?
		req := peer.reqs.last();
		if(req != nil) {
			piece := piecefind(req.pieceindex);
			if(piece != nil)
				schedpieces(peer, array[] of {piece});
		}

		# find rarest orphan pieces to work on
		say("schedule: trying rarest orphans");
		a := piecesrareorphan(1);
		schedpieces(peer, a);
		if(!needblocks(peer))
			return;

		# find rarest inactive piece to work on
		say("schedule: trying inactive pieces");
		rare := inactiverare();
		for(i := 0; i < len rare; i++) {
			say(sprint("using new rare piece %d", rare[i]));
			p := piecenew(rare[i]);
			piecebusy.set(rare[i]);
			schedpieces(peer, array[] of {p});
			if(!needblocks(peer))
				return;
		}

		# find rarest active non-orphan piece to work on
		say("schedule: trying rarest non-orphans");
		a = piecesrareorphan(0);
		schedpieces(peer, a);
		if(!needblocks(peer))
			return;
		
	} else {
		say("schedule: doing random");

		# schedule requests for blocks of active pieces, most completed first:  we want whole pieces fast
		a := l2a(pieces);
		sort(a, progresscmp);
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
			schedbatches(peer, b);
			if(!needblocks(peer))
				return;

			# if more requests needed, start on partially used batches too, in reverse order (for fewer duplicate data)
			say("schedule: trying partially used batches from piece");
			for(k := len b-1; k >= 0; k--) {
				request(peer, piece, b[k].usedpartial(peer));
				if(!needblocks(peer))
					return;
			}
		}

		# otherwise, get new random pieces to work on
		say("schedule: trying random pieces");
		while(needblocks(peer) && piecebusy.have < piecebusy.n) {
			say("schedule: getting another random piece...");
			piece := getrandompiece();
			b := batches(piece);
			schedbatches(peer, b);
		}
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

l2a[T](l: list of T): array of T
{
	a := array[len l] of T;
	i := 0;
	for(; l != nil; l = tl l)
		a[i++] = hd l;
	return a;
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
