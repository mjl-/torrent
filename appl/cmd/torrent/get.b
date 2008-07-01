implement Torrentget;

include "sys.m";
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "keyring.m";
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";

sys: Sys;
keyring: Keyring;
bittorrent: Bittorrent;

print, sprint, fprint, fildes: import sys;
Bee, Msg, Torrent: import bittorrent;


Torrentget: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

Dflag: int;

torrent: ref Torrent;
dstfd: ref Sys->FD;

# piecekeeper
piecechan: chan of (int, int, int, chan of int);
Add, Remove, Request: con iota;

# tracker
trackchan: chan of (array of Trackerpeer, string);

Trackerpeer: adt {
	ip:	string;
	port:	int;
	peerid:	array of byte;

	text:	fn(tp: self Trackerpeer): string;
};

# dialer
peerdialchan: chan of (Trackerpeer, ref Sys->FD, array of byte, array of byte, string);

Piece: adt {
	index:	int;
	d:	array of byte;
	have:	ref Bits;

	isdone:	fn(p: self ref Piece): int;
	hash:	fn(p: self ref Piece): array of byte;
	text:	fn(p: self ref Piece): string;
};

Peer: adt {
	id:	int;
	tp:	Trackerpeer;
	fd:	ref Sys->FD;
	extensions, peerid: array of byte;
	outmsgs:	chan of ref Msg;
	curpiece:	ref Piece;
	piecehave:	ref Bits;
	state:	int;
	msgseq:	int;

	new:	fn(tp: Trackerpeer, fd: ref Sys->FD, extensions, peerid: array of byte): ref Peer;
	remotechoked:	fn(p: self ref Peer): int;
	remoteinterested:	fn(p: self ref Peer): int;
	localchoked:	fn(p: self ref Peer): int;
	localinterested:	fn(p: self ref Peer): int;
	text:	fn(p: self ref Peer): string;
	fulltext:	fn(p: self ref Peer): string;
};


Dialersmax: con 5;  # max number of dialer procs
Dialtimeout: con 20;  # timeout for connecting to peer
Peersmax: con 40;

Peeridlen:	con 20;
Listenport:	con 6881;
MinInterval:	con 30;
Blocklength:	con 1<<14;
DefaultInterval:	con 1800;
WantUnchokedCount:	con 4;
Stablesecs:	con 10;
Minscheduled:	con 6;
Maxdialedpeers:	con 20;

# Peer.state
RemoteChoked, RemoteInterested, LocalChoked, LocalInterested: con (1<<iota);


# progress/state
ndialers: int;  # number of active dialers
trackerpeers: list of Trackerpeer;  # peers we are not connected to
peers: list of ref Peer;  # peers we are connected to
peergen: int;  # sequence number for peers
piecehave: ref Bits;
piecebusy: ref Bits;


peerinmsgchan: chan of (ref Peer, ref Msg);


init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	keyring = load Keyring Keyring->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init(bitarray);

	arg->init(args);
	arg->setusage(arg->progname()+" [-D] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'D' =>	Dflag++;
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

	f := "torrentdata";
	dstfd = sys->create(f, Sys->OWRITE, 8r666);
	if(dstfd == nil)
		fail(sprint("create %s: %r", f));

	sys->pctl(Sys->NEWPGRP, nil);

	piecechan = chan of (int, int, int, chan of int);
	trackchan = chan of (array of Trackerpeer, string);
	peerdialchan = chan of (Trackerpeer, ref Sys->FD, array of byte, array of byte, string);

	peers = nil;
	piecehave = Bits.new(len torrent.piecehashes);
	piecebusy = Bits.new(len torrent.piecehashes);

	peerinmsgchan = chan of (ref Peer, ref Msg);

	spawn piecekeeper();
	spawn track();
	main();
}

trackerpeerdel(tp: Trackerpeer)
{
	n := trackerpeers;
	n = nil;
	for(; trackerpeers != nil; trackerpeers = tl trackerpeers) {
		e := hd trackerpeers;
		if(e.ip == tp.ip && e.port == tp.port)
			;
		else
			n = hd trackerpeers::n;
	}
	trackerpeers = n;
}

trackerpeeradd(tp: Trackerpeer)
{
	trackerpeers = tp::trackerpeers;
}

trackerpeertake(): Trackerpeer
{
	tp := hd trackerpeers;
	trackerpeers = tl trackerpeers;
	return tp;
}


Trackerpeer.text(tp: self Trackerpeer): string
{
	return sprint("(trackerpeer %s!%d peerid %s)", tp.ip, tp.port, string tp.peerid);
}


peerconnected(ip: string, port: int): int
{
	for(l := peers; l != nil; l = tl l) {
		e := hd l;
		if(e.tp.ip == ip && e.tp.port == port)
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
		tp := trackerpeertake();
		say("spawning dialproc for "+tp.text());
		spawn dialer(tp);
		ndialers++;
	}
}



Peer.new(tp: Trackerpeer, fd: ref Sys->FD, extensions, peerid: array of byte): ref Peer
{
	outmsgs := chan of ref Msg;
	state := RemoteChoked|LocalChoked;
	msgseq := 0;
	return ref Peer(peergen++, tp, fd, extensions, peerid, outmsgs, nil, Bits.new(piecehave.n), state, msgseq);
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

Peer.text(p: self ref Peer): string
{
	return sprint("<peer %s!%d id %d>", p.tp.ip, p.tp.port, p.id);
}

Peer.fulltext(p: self ref Peer): string
{
	return sprint("<peer %s, id %d, peerid %s>", p.tp.text(), p.id, string p.peerid);
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


main()
{
	for(;;) alt {
	(newpeers, trackerr) := <-trackchan =>
		if(trackerr != nil) {
			warn(sprint("tracker error: %s", trackerr));
		} else {
			say("main, new peers");
			for(i := 0; i < len newpeers; i++) {
				tp := Trackerpeer newpeers[i];
				say("new: "+tp.text());
				trackerpeerdel(tp);
				if(!peerconnected(tp.ip, tp.port))
					trackerpeeradd(tp);
				else
					say("already connected to "+tp.text());
			}
		}
		dialpeers();

	(tp, peerfd, extensions, peerid, dialerr) := <-peerdialchan =>
		if(dialerr != nil) {
			warn(sprint("dial peer %s: %s", string tp.peerid, dialerr));
		} else {
			peer := Peer.new(tp, peerfd, extensions, peerid);
			spawn peernetreader(peer);
			spawn peernetwriter(peer);
			peeradd(peer);
			say("dialed peer "+peer.fulltext());

			# xxx should send our bitfield instead
			peer.outmsgs <-= ref Msg.Keepalive();
		}
		ndialers--;
		dialpeers();

	(peer, msg) := <-peerinmsgchan =>
		# xxx fix this code.  it can easily block now
		if(msg == nil) {
			warn("eof from peer "+peer.text());
			peerdel(peer);
			continue;
		}

		peer.msgseq++;

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
			# xxx check if we are expecting piece
			# xxx check if block isn't too large

			say(sprint("%s sent data for piece=%d begin=%d length=%d", peer.text(), m.index, m.begin, len m.d));

			piece := peer.curpiece;

			(begin, length) := nextblock(piece);
			if(m.begin != begin || len m.d != length)
				fail(sprint("%s sent bad begin (have %d, want %d) or length (%d, %d)", peer.text(), m.begin, begin, len m.d, length));

			piece.d[m.begin:] = m.d;
			piece.have.set(m.begin/Blocklength);

			if(piece.isdone()) {
				wanthash := hex(torrent.piecehashes[piece.index]);
				havehash := hex(piece.hash());
				if(wanthash != havehash) {
					say(sprint("%s from %s did not check out, want %s, have %s", piece.text(), peer.text(), wanthash, havehash));
					# xxx disconnect peer?
					continue;
				}
				piecehave.set(piece.index);
				say("piece now done: "+piece.text());
				say(sprint("pieces: have %s, busy %s", piecehave.text(), piecebusy.text()));

				n := sys->pwrite(dstfd, piece.d, len piece.d, big piece.index * big torrent.piecelen);
				if(n != len piece.d)
					fail(sprint("writing piece: %r"));

				piece = getpiece(peer);
			}
			schedreq(peer);

			if(piecehave.n == piecehave.have)
				print("DONE!\n");

                Request =>
			say(sprint("remote sent request, ignoring"));

		Cancel =>
			say(sprint("remote sent cancel for piece=%d block=%d length=%d", m.index, m.begin, m.length));

		}
	}
}

track()
{
	for(;;) {
		say("getting new tracker info");
		(interval, newpeers, nil, terr) := bittorrent->trackerget(torrent, nil);
		if(terr != nil)
			say("trackerget: "+terr);
		else
			say("trackget okay");
		trackchan <-= (newpeers, terr);

		# xxx find something sane here
		# it should be possible to make us wake up here, e.g. by main
		if(interval < 60)
			interval = 60;
		say(sprint("track, sleeping for %d seconds", interval));
		sys->sleep(interval*1000);
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


dialer(tp: Trackerpeer)
{
	err := _dialer(tp);
	if(err != nil)
		peerdialchan <-= (tp, nil, nil, nil, err);
	# otherwise, _dialer sent success
}

_dialer(tp: Trackerpeer): string
{
	# xxx use timeout
	addr := sprint("net!%s!%d", tp.ip, tp.port);
	(ok, conn) := sys->dial(addr, nil);
	if(ok < 0)
		return sprint("dial %s: %r", addr);

	say("dialed "+addr);
	fd := conn.dfd;

	d := array[20+8+20+20] of byte;
	i := 0;
	d[i++] = byte 19;
	d[i:] = array of byte "BitTorrent protocol";
	i += 19;
	d[i:] = array[8] of {* => byte '\0'};
	i += 8;
	d[i:] = torrent.hash;
	i += 20;
	d[i:] = torrent.peerid;
	i += 20;
	if(i != len d)
		fail("bad peer header, internal error");

	n := sys->write(fd, d, len d);
	if(n != len d)
		return sprint("writing peer header: %r");

	rd := array[len d] of byte;
	n = sys->readn(fd, rd, len rd);
	if(n < 0)
		return sprint("reading peer header: %r");
	if(n != len rd)
		return sprint("short read on peer header (%d)", n);

	extensions := rd[20:20+8];
	peerid := rd[20+8+20:];

	peerdialchan <-= (tp, fd, extensions, peerid, nil);
	return nil;
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
		p.outmsgs <-= ref Msg.Interested();
	}
}

setpiece(peer: ref Peer, index: int): ref Piece
{
	piecelen := torrent.piecelen;
	if(index+1 == len torrent.piecehashes) {
		piecelen = int (torrent.length % big torrent.piecelen);
		if(piecelen == 0)
			piecelen = torrent.piecelen;
	}
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
		peer.outmsgs <-= ref Msg.Request(piece.index, begin, length);
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
			fail(sprint("reading msg: %r"));  # xxx return error to main
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


hex(d: array of byte): string
{
	s := "";
	for(i := 0; i < len d; i++)
		s += sprint("%02x", int d[i]);
	return s;
}

killgrp(pid: int)
{
	path := sprint("/prog/%d/ctl", pid);
	fd := sys->open(path, Sys->OWRITE);
	if(fd != nil)
		fprint(fd, "killgrp");
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
