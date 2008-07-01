implement Torrentget;

include "sys.m";
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";

sys: Sys;
bittorrent: Bittorrent;

print, sprint, fprint, fildes: import sys;
Bee, Piece, Msg, Torrent, Bitelength: import bittorrent;


Torrentget: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

Dflag: int;

torrent: ref Torrent;
fd: ref Sys->FD;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
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

	(interval, peers, nil, terr) := bittorrent->trackerget(torrent, nil);
	if(terr != nil)
		fail("trackerget: "+terr);
	say("trackget okay");

	f := "torrentdata";
	fd = sys->create(f, Sys->OWRITE, 16r666);
	if(fd == nil)
		fail(sprint("create %s: %r", f));

	print("interval=%d\n", interval);
	print("number of peers=%d\n", len peers);
	for(i := 0; i < len peers; i++) {
		(ip, port, peerid) := peers[i];
		print("ip=%s port=%d rpeerid=...\n", ip, port);
		perr := dopeer(ip, port, peerid);
		if(perr != nil)
			say(sprint("peer: %s", perr));
		sys->sleep(1*1000);
	}
	say("done");
}

dopeer(ip: string, port: int, peerid: array of byte): string
{
	addr := sprint("net!%s!%d", ip, port);
	(ok, conn) := sys->dial(addr, nil);
	if(ok < 0)
		return sprint("dial %s: %r", addr);

	pfd := conn.dfd;

	d := array[20+8+20+20] of byte;
	i := 0;
	d[i++] = byte 16r13;
	d[i:] = array of byte "Bittorrent protocol";
	i += 19;
	d[i:] = array[8] of {* => byte '\0'};
	i += 8;
	d[i:] = torrent.hash;
	i += 20;
	d[i:] = torrent.peerid;
	i += 20;
	if(i != len d)
		fail("bad peer header");

	n := sys->write(pfd, d, len d);
	if(n != len d)
		return sprint("writing peer header: %r");

	rd := array[len d] of byte;
	n = sys->readn(pfd, rd, len rd);
	if(n != len rd)
		return sprint("reading peer header: %r");

	refd := array[len d] of byte;
	refd[:] = d;
	refd[20+8+20:] = peerid;
	for(j := 0; j < len rd; j++)
		if(rd[j] != refd[j])
			return sprint("bad header, i=%d", j);

	inmsg := chan of ref Msg;
	outmsg := chan of ref Msg;
	spawn preader(pfd, inmsg);
	spawn pwriter(pfd, outmsg);
	outmsg <-= ref Msg.Keepalive();
	outmsg <-= ref Msg.Interested();

	piece := torrent.mkpiece(0);
	(begin, length) := nextbite(piece);

	for(;;) alt {
	msg := <-inmsg =>
		if(msg == nil)
			return "eof from remote";
		pick m := msg {
                Keepalive =>
			say("keepalive");
		Choke =>
			say("we are choked");
		Unchoke =>
			say("we are unchoked");
			outmsg <-= ref Msg.Request(piece.index, begin, length);
		Interested =>
			say("remote is interested");
		Notinterested =>
			say("not interested");
                Have =>
			say(sprint("remote now has index=%d", m.index));
                Bitfield =>
			say(sprint("remote sent bitfield"));
			say("assuming it has all pieces..."); # xxx
                Piece =>
			say(sprint("remote sent data for piece=%d begin=%d length=%d", m.index, m.begin, m.length));
			if(m.begin != begin || m.length != length)
				fail(sprint("remote sent bad begin (have %d, want %d) or length (%d, %d)", m.begin, begin, m.length, length));
			piece.d[m.begin:] = m.d;
			piece.have.set(m.begin/torrent.piecelen);

			if(piecedone(piece)) {
				n = sys->pwrite(fd, piece.d, len piece.d, big piece.index * big torrent.piecelen);
				if(n != len piece.d)
					fail(sprint("writeing piece: %r"));
				if(piece.index+1 == len torrent.piecehashes)
					fail(sprint("done!!!"));
				piece = torrent.mkpiece(piece.index+1);
			}
			(begin, length) = nextbite(piece);

			outmsg <-= ref Msg.Request(piece.index, begin, length);

                Request =>
			say(sprint("remote sent request, ignoring"));
		Cancel =>
			say(sprint("remote sent cancel for piece=%d bite=%d length=%d", m.index, m.begin, m.length));
		}
	}
}

piecedone(p: ref Piece): int
{
	return p.have.n == p.have.have;
}

nextbite(p: ref Piece): (int, int)
{
	# request sequentially, may change
	begin := Bitelength*p.have.have;
	length := Bitelength;
	if(len p.d-begin < length)
		length = len p.d-begin;
	return (begin, length);
}

preader(pfd: ref Sys->FD, mchan: chan of ref Msg)
{
	(m, err) := Msg.read(pfd);
	if(err != nil)
		fail(sprint("reading msg: %r"));
	fprint(fildes(2), "<< %s\n", m.text());
	mchan <-= m;
}

pwriter(pfd: ref Sys->FD, mchan: chan of ref Msg)
{
	for(;;) {
		m := <- mchan;
		if(m == nil)
			return;
		fprint(fildes(2), ">> %s\n", m.text());
		d := m.pack();
		n := sys->write(pfd, d, len d);
		if(n != len d)
			fail(sprint("writing msg: %r"));
	}
}


fail(s: string)
{
	fprint(fildes(2), "%s\n", s);
	raise "fail:"+s;
}

say(s: string)
{
	if(Dflag)
		fprint(fildes(2), "%s\n", s);
}
