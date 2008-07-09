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
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";

sys: Sys;
daytime: Daytime;
str: String;
keyring: Keyring;
bittorrent: Bittorrent;

print, sprint, fprint, fildes: import sys;
Bee, Msg, Torrent: import bittorrent;


Torrentget: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

Dflag: int;
nofix: int;

torrent: ref Torrent;
dstfds: list of ref (ref Sys->FD, big);  # fd, size
statefd: ref Sys->FD;
starttime: int;
totalupload := big 0;
totaldownload := big 0;
totalleft: big;
listenport: int;
localpeerid: array of byte;
trackerevent: string;

# piecekeeper
piecechan: chan of (int, int, int, chan of int);
Add, Remove, Request: con iota;

# tracker
trackkickchan:	chan of int;
trackreqchan:	chan of (big, big, big, int, string);  # up, down, left, listenport, event
trackchan:	chan of (int, array of (string, int, array of byte), string);  # interval, peers, error

Newpeer: adt {
	addr:	string;
	peerid:	array of byte;  # may be nil, for incoming connections or compact track responses

	text:	fn(np: self Newpeer): string;
};

# dialer/listener
newpeerchan: chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);

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

	new:	fn(): ref Traffic;
	add:	fn(t: self ref Traffic, bytes: int);
	rate:	fn(t: self ref Traffic): int;
	total:	fn(t: self ref Traffic): big;
	text:	fn(t: self ref Traffic): string;
};

Peer: adt {
	id:	int;
	np:	Newpeer;
	fd:	ref Sys->FD;
	extensions, peerid: array of byte;
	outmsgs:	chan of ref Msg;
	curpiece:	ref Piece;
	piecehave:	ref Bits;
	state:	int;
	msgseq:	int;
	up, down, metaup, metadown: ref Traffic;

	new:	fn(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte): ref Peer;
	remotechoked:	fn(p: self ref Peer): int;
	remoteinterested:	fn(p: self ref Peer): int;
	localchoked:	fn(p: self ref Peer): int;
	localinterested:	fn(p: self ref Peer): int;
	send:	fn(p: self ref Peer, m: ref Msg);
	text:	fn(p: self ref Peer): string;
	fulltext:	fn(p: self ref Peer): string;
};


Dialersmax: con 5;  # max number of dialer procs
Dialtimeout: con 20;  # timeout for connecting to peer
Peersmax: con 40;

Peeridlen:	con 20;

Listenhost:	con "*";
Listenport:	con 6881;
Listenportrange:	con 100;

Intervalmin:	con 30;
Intervalmax:	con 24*3600;
Intervalneed:	con 10;  # when we need more peers during startup period
Intervaldefault:	con 1800;
Intervalstartupperiod:	con 120;

Blocklength:	con 16*1024;
WantUnchokedCount:	con 4;
Stablesecs:	con 10;  # xxx related to TrafficHistorysize...
Minscheduled:	con 6;
Maxdialedpeers:	con 20;
TrafficHistorysize:	con 10;

# Peer.state
RemoteChoked, RemoteInterested, LocalChoked, LocalInterested: con (1<<iota);


# progress/state
ndialers: int;  # number of active dialers
trackerpeers: list of Newpeer;  # peers we are not connected to
peers: list of ref Peer;  # peers we are connected to
peergen: int;  # sequence number for peers
piecehave: ref Bits;
piecebusy: ref Bits;

peerinmsgchan: chan of (ref Peer, ref Msg);

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
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init(bitarray);

	arg->init(args);
	arg->setusage(arg->progname()+" [-Dn] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'D' =>	Dflag++;
		'n' =>	nofix = 1;
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

	if(statefd == nil) {
		say(sprint("creating statepath %q", torrent.statepath));
		statefd = sys->create(torrent.statepath, Sys->ORDWR, 8r666);
		if(statefd == nil)
			warn(sprint("failed to create state file (ignoring): %r"));
		else
			writestate();
	}

	totalleft = torrent.length;
	localpeerid = bittorrent->genpeerid();

	piecechan = chan of (int, int, int, chan of int);
	trackkickchan = chan of int;
	trackreqchan = chan of (big, big, big, int, string);
	trackchan = chan of (int, array of (string, int, array of byte), string);

	newpeerchan = chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);
	tickchan = chan of int;

	peers = nil;
	piecebusy = Bits.new(len torrent.piecehashes);

	peerinmsgchan = chan of (ref Peer, ref Msg);

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
		if(str->in(int d[i], " -~"))
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

peerdel(peer: ref Peer)
{
	npeers: list of ref Peer;
	for(; peers != nil; peers = tl peers) {
		if(hd peers != peer)
			npeers = hd peers::npeers;
	}
	peers = npeers;
}

peeradd(p: ref Peer)
{
	peerdel(p);
	peers = p::peers;
}


dialpeers()
{
	say(sprint("dialpeers, %d trackerpeers %d peers", len trackerpeers, len peers));

	while(trackerpeers != nil && ndialers < Dialersmax && len peers < Peersmax) {
		np := trackerpeertake();
		say("spawning dialproc for "+np.text());
		spawn dialer(np);
		ndialers++;
	}
}



Peer.new(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte): ref Peer
{
	outmsgs := chan of ref Msg;
	state := RemoteChoked|LocalChoked;
	msgseq := 0;
	return ref Peer(peergen++, np, fd, extensions, peerid, outmsgs, nil, Bits.new(piecehave.n), state, msgseq, Traffic.new(), Traffic.new(), Traffic.new(), Traffic.new());
}

Peer.remotechoked(p: self ref Peer): int
{
	return p.state & RemoteChoked;
}

Peer.remoteinterested(p: self ref Peer): int
{
	return p.state & RemoteInterested;
}

Peer.localchoked(p: self ref Peer): int
{
	return p.state & LocalChoked;
}

Peer.localinterested(p: self ref Peer): int
{
	return p.state & LocalInterested;
}

Peer.send(p: self ref Peer, msg: ref Msg)
{
	msize := msg.packedsize();
	dsize := 0;
	pick m := msg {
	Piece =>
		dsize = len m.d;
		p.up.add(dsize);
	}
	p.metaup.add(msize-dsize);
	p.outmsgs <-= msg;
}

Peer.text(p: self ref Peer): string
{
	return sprint("<peer %s id %d>", p.np.addr, p.id);
}

Peer.fulltext(p: self ref Peer): string
{
	return sprint("<peer %s, id %d, peerid %s>", p.np.text(), p.id, peeridfmt(p.peerid));
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
	return ref Traffic(0, array[TrafficHistorysize] of {* => (0, 0)}, 0, big 0);
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

ticker()
{
	for(;;) {
		sys->sleep(10*1000);
		tickchan <-= 0;
	}
}

main()
{
	for(;;) alt {
	<-tickchan =>
		say(sprint("ticking, %d peers", len peers));
		for(l := peers; l != nil; l = tl l) {
			peer := hd l;
			say(sprint("%s: up %s, down %s, meta %s %s", peer.text(), peer.up.text(), peer.down.text(), peer.metaup.text(), peer.metadown.text()));
		}

	<-trackkickchan =>
		trackreqchan <-= (totalupload, totaldownload, totalleft, listenport, trackerevent);
		trackerevent = nil;

	(interval, newpeers, trackerr) := <-trackchan =>
		if(trackerr != nil) {
			warn(sprint("tracker error: %s", trackerr));
		} else {
			say("main, new peers");
			for(i := 0; i < len newpeers; i++) {
				(ip, port, peerid) := newpeers[i];
				np := Newpeer(sprint("%s!%d", ip, port), peerid);
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
		if(err != nil) {
			warn(sprint("%s: %s", np.text(), err));
		} else {
			peer := Peer.new(np, peerfd, extensions, peerid);
			spawn peernetreader(peer);
			spawn peernetwriter(peer);
			peeradd(peer);
			say("new peer "+peer.fulltext());

			# xxx should send our bitfield instead
			peer.send(ref Msg.Keepalive());
		}
		if(dialed) {
			ndialers--;
			dialpeers();
		}

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
			totaldownload += big dsize;
		}
		peer.metadown.add(msize-dsize);

		pick m := msg {
                Keepalive =>
			say("keepalive");

		Choke =>
			if(peer.remotechoked()) {
				say(sprint("%s choked us twice...", peer.text()));
				continue;
			}

			say(sprint("%s choked us...", peer.text()));
			peer.state |= RemoteChoked;

		Unchoke =>
			if(!peer.remotechoked()) {
				say(sprint("%s unchoked us twice...", peer.text()));
				continue;
			}

			say(sprint("%s unchoked us", peer.text()));
			peer.state &= ~RemoteChoked;

			if(peer.curpiece == nil)
				getpiece(peer);

			schedreq(peer);

		Interested =>
			if(peer.remoteinterested()) {
				say(sprint("%s is interested again...", peer.text()));
				continue;
			}

			say(sprint("%s is interested", peer.text()));
			peer.state |= RemoteInterested;

		Notinterested =>
			if(!peer.remoteinterested()) {
				say(sprint("%s is uninterested again...", peer.text()));
				continue;
			}

			say(sprint("%s is no longer interested", peer.text()));
			peer.state &= ~RemoteInterested;

                Have =>
			if(m.index >= len torrent.piecehashes) {
				say(sprint("%s sent 'have' for invalid piece %d", peer.text(), m.index));
				# xxx close connection?
				continue;
			}
			if(peer.piecehave.get(m.index))
				say(sprint("%s already had piece %d", peer.text(), m.index));

			say(sprint("remote now has piece %d", m.index));
			peer.piecehave.set(m.index);

			interesting(peer);
			if(!peer.remotechoked() && peer.curpiece == nil) {
				getpiece(peer);
				schedreq(peer);
			}

                Bitfield =>
			if(peer.msgseq != 1) {
				say(sprint("%s sent bitfield after first message, ignoring...", peer.text()));
				continue;
			}

			err: string;
			(peer.piecehave, err) = Bits.mk(len torrent.piecehashes, m.d);
			if(err != nil) {
				say(sprint("%s sent bogus bitfield message: %s", peer.text(), err));
				# xxx
				continue;
			}
			say("remote sent bitfield, haves "+peer.piecehave.text());

			interesting(peer);

                Piece =>
			# xxx check if block isn't too large

			say(sprint("%s sent data for piece=%d begin=%d length=%d", peer.text(), m.index, m.begin, len m.d));

			piece := peer.curpiece;

			(begin, length) := nextblock(piece);
			if(m.begin != begin || len m.d != length)
				fail(sprint("%s sent bad begin (have %d, want %d) or length (%d, %d)", peer.text(), m.begin, begin, len m.d, length));

			piece.d[m.begin:] = m.d;
			piece.have.set(m.begin/Blocklength);
			totalleft -= big len m.d;

			if(piece.isdone()) {
				wanthash := hex(torrent.piecehashes[piece.index]);
				havehash := hex(piece.hash());
				if(wanthash != havehash) {
					say(sprint("%s from %s did not check out, want %s, have %s", piece.text(), peer.text(), wanthash, havehash));
					# xxx disconnect peer?
					continue;
				}

				err := bittorrent->piecewrite(torrent, dstfds, piece.index, piece.d);
				if(err != nil)
					fail("writing piece: "+err);

				piecehave.set(piece.index);
				writestate();
				say("piece now done: "+piece.text());
				say(sprint("pieces: have %s, busy %s", piecehave.text(), piecebusy.text()));

				piece = getpiece(peer);
			}
			schedreq(peer);

			if(isdone()) {
				trackerevent = "completed";
				spawn trackkick(0);
				print("DONE!\n");
			}

                Request =>
			say(sprint("remote sent request, ignoring"));

		Cancel =>
			say(sprint("remote sent cancel for piece=%d block=%d length=%d", m.index, m.begin, m.length));

		}
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

listener(aconn: Sys->Connection)
{
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

		f := conn.dir+"/data";
		fd := sys->open(f, Sys->ORDWR);
		if(fd == nil) {
			warn(sprint("new connection, open %s: %r", f));
			continue;
		}

		(extensions, peerid, err) := handshake(fd);
		np := Newpeer(remaddr, nil);
		if(err != nil)
			say("error handshaking incoming connection: "+err);
		newpeerchan <-= (0, np, fd, extensions, peerid, err);
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

	n := sys->write(fd, d, len d);
	if(n != len d)
		return (nil, nil, sprint("writing peer header: %r"));

	rd := array[len d] of byte;
	n = sys->readn(fd, rd, len rd);
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

setpiece(peer: ref Peer, index: int): ref Piece
{
	piecelen := torrent.piecelength(index);
	nblocks := (piecelen+Blocklength-1)/Blocklength;
	piece := ref Piece(index, array[piecelen] of byte, Bits.new(nblocks));

	say(sprint("assigned %s to %s", piece.text(), peer.text()));
	peer.curpiece = piece;
	piecebusy.set(index);

	return piece;
}

getpiece(peer: ref Peer): ref Piece
{
	b := wantpeerpieces(peer);
	if(!b.isempty()) {
		for(i := 0; i < b.n; i++)
			if(b.get(i))
				return setpiece(peer, i);
	}

	say("no piece to get from "+peer.text());
	peer.curpiece = nil;
	return nil;
}

schedreq(peer: ref Peer)
{
	piece := peer.curpiece;

	if(piece != nil) {
		(begin, length) := nextblock(piece);
		say(sprint("requesting next block, begin %d, length %d, %s", begin, length, piece.text()));
		peer.send(ref Msg.Request(piece.index, begin, length));
	}
}


nextblock(p: ref Piece): (int, int)
{
	# request sequentially, may change
	begin := Blocklength*p.have.have;
	length := Blocklength;
	if(len p.d-begin < length)
		length = len p.d-begin;
	return (begin, length);
}


peernetreader(peer: ref Peer)
{
	for(;;) {
		(m, err) := Msg.read(peer.fd);
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
		n := sys->write(peer.fd, d, len d);
		if(n != len d)
			fail(sprint("writing msg: %r"));
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
