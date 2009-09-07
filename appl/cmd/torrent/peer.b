implement Torrentpeer;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "daytime.m";
	daytime: Daytime;
include "string.m";
	str: String;
include "keyring.m";
	kr: Keyring;
include "ip.m";
	ipmod: IP;
	IPaddr: import ipmod;
include "math.m";
include "styx.m";
	styx: Styx;
	Tmsg, Rmsg: import styx;
include "styxservers.m";
	styxservers: Styxservers;
	Styxserver, Fid, Navigator, Navop: import styxservers;
include "tables.m";
	tables: Tables;
	Strhash, Table: import tables;
include "util0.m";
	util: Util0;
	pid, kill, killgrp, hex, join, min, rev, l2a, g32i, readfile, readfd, inssort, sizefmt, sizeparse: import util;
include "bitarray.m";
	bitarray: Bitarray;
	Bits, Bititer: import bitarray;
include "bittorrent.m";
	bt: Bittorrent;
	Bee, Msg, File, Torrent, Filex, Torrentx: import bt;
include "../../lib/bittorrentpeer.m";
	btp: Bittorrentpeer;
	State, Pool, Traffic, Piece, Pieces, Rare, Peer, Newpeer, Newpeers, Buf, LReq, LReqs, RReq, RReqs, Batch, List, Progress, Peerevent, Eventfid: import btp;
	Slocal, Sremote, Schoking, Sunchoking, Sinterested, Suninterested: import Bittorrentpeer;

Torrentpeer: module {
	init:	fn(nil: ref Draw->Context, nil: list of string);
};


Peerbox: adt {
	peers:		list of ref Peer;  # peers we are connected to
	ips,						# newpeer.ip
	addrs:		ref Tables->Strhash[ref Peer];  # newpeer.addr
	ids:		ref Tables->Table[ref Peer];	# peer.id
	ndialed:	int;
	nseeding:	int;
	lucky:		ref Peer;

	new:		fn(): ref Peerbox;
	add:		fn(b: self ref Peerbox, p: ref Peer);
	del:		fn(b: self ref Peerbox, p: ref Peer);
	findip:		fn(b: self ref Peerbox, ip: string): ref Peer;
	findaddr:	fn(b: self ref Peerbox, addr: string): ref Peer;
	findid:		fn(b: self ref Peerbox, id: int): ref Peer;
	nactive:		fn(b: self ref Peerbox): int;
	oldestunchoke:		fn(b: self ref Peerbox, ipmasked: string): ref Peer;
	localunchokable:	fn(b: self ref Peerbox): list of ref Peer;
	nunchokedinterested:	fn(b: self ref Peerbox): int;
	longestunchoked:	fn(b: self ref Peerbox): ref Peer;
};


# state
state:		ref State;
newpeers:	ref Newpeers;  # peers we are not connected to
peerbox:	ref Peerbox;
stopped:	int;
ndialers:	int;	# number of active dialers
rotateips:	ref Pool[string];  # masked ip address
faulty:		list of (string, int, string);  # ip, time, error
selfips:	ref Strhash[string];
islistening := 1;	# whether listener() is listening
totalleft:	big;
trackerevent:	string;
trackkickpid := -1;
trafficup,
trafficdown,
trafficmetaup,
trafficmetadown:	ref Traffic;

# config
dflag: int;
nofix: int;
torrentpath:	string;
time0:	int;
listenport:	int;
localpeerid:	array of byte;
localpeeridhex:	string;
ip4mask,
ip6mask:	IPaddr;
maxratio	:= 0.0;
maxdownrate	:= big -1;
maxuprate	:= big -1;
maxdowntotal	:= big -1;
maxuptotal	:= big -1;


# tracker
trackkickc:	chan of int;
trackreqc:	chan of (big, big, big, int, string);  # up, down, left, listenport, event
trackc:		chan of (int, array of (string, int, array of byte), string);  # interval, peers, error

# dialer/listener
canlistenc:	chan of int;
newpeerc:	chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);

# upload/download rate limiter
upc, downc:	chan of (int, chan of int);

# peer interaction
peerinmsgc:	chan of (ref Peer, ref Msg, chan of list of ref (int, int, array of byte), string);
peererrc:	chan of (ref Peer, string);
wantmsgc:	chan of ref Peer;
diskwritec:	chan of ref (int, int, array of byte);
diskwrittenc:	chan of (int, int, int, string);
diskreadc:	chan of (ref Peer, int, int, array of byte, string);
mainwrites:	list of ref (int, int, array of byte);

# round ticker
roundc:		chan of int;


Dialersmax:	con 5;  # max number of dialer procs
Dialtimeout:	con 20;  # timeout for connecting to peer
Peersmax:	con 80;
Peersdialedmax:	con 40;
Piecesrandom:	con 4;  # count of first pieces in a download to pick at random instead of rarest-first
Blockqueuemax:	con 100;  # max number of Requests a peer can queue at our side without being considered bad
Blockqueuesize:	con 30;  # number of pending blocks to request to peer
Diskchunksize:	con 128*1024;  # do initial write to disk for any block/piece of this size, to prevent fragmenting the file system
Batchsize:	con Diskchunksize/btp->Blocksize;
Netiounit:	con 1500-20-20;  # typical network data io unit, ethernet-ip-tcp

Listenhost:	con "*";
Listenport:	con 6881;
Listenportrange:	con 100;

Intervalmin:	con 30;
Intervalneed:	con 45;
Intervalmax:	con 24*3600;
Intervaldefault:	con 1800;
Intervalstartupperiod:	con 120;

Blocksizemax:	con 32*1024;  # max block size allowed for incoming blocks
Unchokedmax:	con 4;
Seedunchokedmax:	con 4;
Ignorefaultyperiod:	con 300;


progresslast:	ref List[ref Progress];
progressfids:	ref Table[ref Eventfid[ref Progress]];
peereventlast:	ref List[ref Peerevent];
peerfids:	ref Table[ref Eventfid[ref Peerevent]];
trackereventlast: ref Progress.Tracker;
nexttrack:	int;

Qroot, Qctl, Qinfo, Qstate, Qfiles, Qprogress, Qpeerevents, Qpeers, Qpeerstracker, Qpeersbad: con iota;
Qfirst:	con Qctl;
Qlast:	con Qpeersbad;
tab := array[] of {
	(Qroot,		".",		Sys->DMDIR|8r555),
	(Qctl,		"ctl",		8r666),
	(Qinfo,		"info",		8r444),
	(Qstate,	"state",	8r444),
	(Qfiles,	"files",	8r444),
	(Qprogress,	"progress",	8r444),
	(Qpeerevents,	"peerevents",	8r444),
	(Qpeers,	"peers",	8r444),
	(Qpeerstracker,	"peerstracker",	8r444),
	(Qpeersbad,	"peersbad",	8r444),
};
srv:	ref Styxserver;
msgc:	chan of ref Tmsg;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	daytime = load Daytime Daytime->PATH;
	str = load String String->PATH;
	kr = load Keyring Keyring->PATH;
	ipmod = load IP IP->PATH;
	ipmod->init();
	styx = load Styx Styx->PATH;
	styx->init();
	styxservers = load Styxservers Styxservers->PATH;
	styxservers->init(styx);
	tables = load Tables Tables->PATH;
	util = load Util0 Util0->PATH;
	util->init();
	bitarray = load Bitarray Bitarray->PATH;
	bt = load Bittorrent Bittorrent->PATH;
	bt->init();
	btp = load Bittorrentpeer Bittorrentpeer->PATH;
	state = ref State;
	btp->init(state);

	ok: int;
	(ok, ip4mask) = IPaddr.parsemask("255.255.255.0");
	if(ok != 0)
		fail("bad ip4 mask");
	(ok, ip6mask) = IPaddr.parsemask("/48");
	if(ok != 0)
		fail("bad ip6 mask");

	selfips = selfips.new(4, nil);

	# note that noone has any business with *last.e
	progressfids = progressfids.new(4, nil);
	progresslast = ref List[ref Progress];
	progresslast.next = ref List[ref Progress];
	peerfids = peerfids.new(4, nil);
	peereventlast = ref List[ref Peerevent];
	peereventlast.next = ref List[ref Peerevent];

	sys->pctl(Sys->NEWPGRP, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-dns] [-m ratio] [-r maxuprate] [-R maxdownrate] [-t maxuptotal] [-T maxdowntotal] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	bt->dflag = btp->dflag = dflag++;
		'n' =>	nofix = 1;
		'm' =>	maxratio = real arg->earg();
			if(maxratio <= 1.1)
				fail("invalid maximum ratio");
		'r' =>	maxuprate = sizeparse(arg->earg());
			if(maxuprate < big (10*1024))
				fail("invalid maximum uprate rate");
		'R' =>	maxdownrate = sizeparse(arg->earg());
			if(maxdownrate < big 0)
				fail("invalid maximum downrate rate");
		's' =>	stopped = 1;
		't' =>	maxuptotal = sizeparse(arg->earg());
			if(maxuptotal < big (10*1024))
				fail("invalid maximum uptotal total");
		'T' =>	maxdowntotal = sizeparse(arg->earg());
			if(maxdowntotal < big 0)
				fail("invalid maximum downtotal total");
		* =>
			arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();

	torrentpath = hd args;
	err: string;
	(state.t, err) = Torrent.open(torrentpath);
	if(err != nil)
		fail(sprint("%s: %s", torrentpath, err));

	created: int;
	(state.tx, created, err) = Torrentx.open(state.t, torrentpath, nofix, 0);
	if(err != nil)
		fail(err);

	state.pieces = ref Pieces;
	if(created) {
		# all new files, we don't have pieces yet
		trackerevent = "started";
		say("no state file needed, all new files");
		state.pieces.have = Bits.new(state.t.piececount);
	} else {
		# attempt to read state of pieces from .torrent.state file
		state.tx.statefd = sys->open(state.tx.statepath, Sys->ORDWR);
		if(state.tx.statefd != nil) {
			say("using .state file");
			d := readfd(state.tx.statefd, 128*1024);
			if(d == nil)
				fail(sprint("%r"));
			(state.pieces.have, err) = Bits.mk(state.t.piececount, d);
			if(err != nil)
				fail(sprint("%s: invalid state", state.tx.statepath));
		} else {
			# otherwise, read through all data
			say("starting to check all pieces in files...");
			state.pieces.have = Bits.new(state.t.piececount);
			bt->torrenthash(state.tx, state.pieces.have);
		}
	}
	state.pieces.busy = state.pieces.have.clone();
	state.pieces.active = state.pieces.active.new(8, nil);
	state.pieces.count = array[state.t.piececount] of {* => 0};
	state.pieces.rare = Rare.new();

	peerbox = Peerbox.new();
	newpeers = ref Newpeers;

	if(done())
		say("already done!");

	if(state.tx.statefd == nil) {
		say(sprint("creating statepath %q", state.tx.statepath));
		state.tx.statefd = sys->create(state.tx.statepath, Sys->ORDWR, 8r666);
		if(state.tx.statefd == nil)
			warn(0, sprint("failed to create state file (ignoring): %r"));
		else
			writestate();
	}

	totalhave := big state.pieces.have.have*big state.t.piecelen;
	if(state.pieces.have.get(state.t.piececount-1)) {
		totalhave -= big state.t.piecelen;
		totalhave += big state.t.piecelength(state.t.piececount-1);
	}
	totalleft = state.t.length-totalhave;
	localpeerid = bt->genpeerid();
	localpeeridhex = hex(localpeerid);

	trackkickc = chan of int;
	trackreqc = chan of (big, big, big, int, string);
	trackc = chan of (int, array of (string, int, array of byte), string);

	canlistenc = chan of int;
	newpeerc = chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);
	roundc = chan of int;

	upc = chan of (int, chan of int);
	downc = chan of (int, chan of int);

	peerinmsgc = chan of (ref Peer, ref Msg, chan of list of ref (int, int, array of byte), string);
	peererrc = chan of (ref Peer, string);
	wantmsgc = chan of ref Peer;
	diskwritec = chan[4] of ref (int, int, array of byte);
	diskwrittenc = chan of (int, int, int, string);
	diskreadc = chan of (ref Peer, int, int, array of byte, string);

	trafficup = Traffic.new();
	trafficdown = Traffic.new();
	trafficmetaup = Traffic.new();
	trafficmetadown = Traffic.new();

	rotateips = Pool[string].new(btp->PoolRotateRandom);

	# start listener, for incoming connections
	ok = -1;
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
	} else
		say(sprint("listening on addr %s", listenaddr));

	spawn listener(conn);
	spawn roundticker();
	spawn track();
	spawn limiter(upc, int maxuprate);
	spawn limiter(downc, int maxdownrate);
	spawn diskwriter(diskwritec);

	navc := chan of ref Navop;
	spawn navigator(navc);

	nav := Navigator.new(navc);
	(msgc, srv) = Styxserver.new(sys->fildes(0), nav, big Qroot);

	spawn main();

	time0 = daytime->now();
	if(!stopped)
		trackkick(0);
}

dostyx(mm: ref Tmsg)
{
	pick m := mm {
	Open =>
		(fid, nil, nil, err) := srv.canopen(m);
		if(fid == nil)
			return replyerror(m, err);
		q := int fid.path&16rff;
		case q {
		Qprogress =>
			ef := Eventfid[ref Progress].new(m.fid);
			progressfids.add(ef.fid, ef);
			putprogressstate(ef.last);

		Qpeerevents =>
			ef := Eventfid[ref Peerevent].new(m.fid);
			peerfids.add(m.fid, ef);
			putpeerstate(ef.last);
		}

	Read =>
		fid := srv.getfid(m.fid);
		if(fid.qtype & Sys->QTDIR)
			return srv.default(m);
		q := int fid.path&16rff;

		case q {
		Qctl =>
			if(m.offset == big 0) {
				s := "";
				s += sprint("listenport %d\n", listenport);
				s += sprint("localpeerid %s\n", localpeeridhex);
				s += sprint("maxratio %.2f\n", maxratio);
				s += sprint("maxuprate %bd\n", maxuprate);
				s += sprint("maxdownrate %bd\n", maxdownrate);
				s += sprint("maxuptotal %bd\n", maxuptotal);
				s += sprint("maxdowntotal %bd\n", maxdowntotal);
				s += sprint("debugpeer %d\n", dflag);
				s += sprint("debuglib %d\n", bt->dflag);
				s += sprint("debugpeerlib %d\n", btp->dflag);
				fid.data = array of byte s;
			}
			srv.reply(styxservers->readbytes(m, fid.data));
		Qinfo =>
			if(m.offset == big 0) {
				t := state.t;
				s := "";
				s += sprint("fs 0\n");
				s += sprint("torrentpath %q\n", torrentpath);
				s += sprint("infohash %s\n", hex(t.infohash));
				s += sprint("announce %q\n", t.announce);
				s += sprint("piecelen %d\n", t.piecelen);
				s += sprint("piececount %d\n", t.piececount);
				s += sprint("length %bd\n", t.length);
				fid.data = array of byte s;
			}
			srv.reply(styxservers->readbytes(m, fid.data));
		Qstate =>
			if(m.offset == big 0) {
				s := "";
				s += sprint("stopped %d\n", stopped);
				s += sprint("totalleft %bd\n", totalleft);
				s += sprint("totalup %bd\n", trafficup.total());
				s += sprint("totaldown %bd\n", trafficdown.total());
				s += sprint("rateup %d\n", trafficup.rate());
				s += sprint("ratedown %d\n", trafficdown.rate());
				s += sprint("eta %d\n", eta());
				s += sprint("peers %d\n", len peerbox.peers);
				s += sprint("seeds %d\n", peerbox.nseeding);
				s += sprint("trackerpeers %d\n", len newpeers.all());
				fid.data = array of byte s;
			}
			srv.reply(styxservers->readbytes(m, fid.data));

		Qfiles =>
			s := "";
			for(i := 0; i < len state.tx.files; i++) {
				fx := state.tx.files[i];
				s += sprint("%q %q %bd %d %d\n", fx.path, fx.f.path, fx.f.length, fx.pfirst, fx.plast);
			}
			srv.reply(styxservers->readstr(m, s));

		Qprogress =>
			pf := progressfids.find(m.fid);
			pf.putread(m);
			while((rm := pf.read()) != nil)
				srv.reply(rm);

		Qpeerevents =>
			pf := peerfids.find(m.fid);
			pf.putread(m);
			while((rm := pf.read()) != nil)
				srv.reply(rm);

		Qpeers =>
			if(m.offset == big 0) {
				s := "";
				for(l := peerbox.peers; l != nil; l = tl l)
					s += peerline(hd l);
				fid.data = array of byte s;
			}
			srv.reply(styxservers->readbytes(m, fid.data));

		Qpeerstracker =>
			if(m.offset == big 0) {
				s := "";
				for(l := newpeers.all(); l != nil; l = tl l)
					s += sprint("%q %q\n", (hd l).addr, hex((hd l).peerid));
				fid.data = array of byte s;
			}
			srv.reply(styxservers->readbytes(m, fid.data));

		Qpeersbad =>
			if(m.offset == big 0) {
				s := "";
				for(l := faulty; l != nil; l = tl l)
					s += sprint("%q %d %q\n", (hd l).t0, (hd l).t1, (hd l).t2);
				fid.data = array of byte s;
			}
			srv.reply(styxservers->readbytes(m, fid.data));

		* =>
			raise "missing case";
		}
		return;

	Write =>
		(fid, err) := srv.canwrite(m);
		if(fid == nil)
			return replyerror(m, err);
		q := int fid.path&16rff;

		case q {
		Qroot =>
			raise "should not happen";
		Qctl =>
			return ctl(m);
		}

	Flush =>
		for(l := tablist(progressfids); l != nil; l = tl l)
			if((hd l).flushtag(m.oldtag))
				return srv.default(mm);
		for(ll := tablist(peerfids); ll != nil; ll = tl ll)
			if((hd ll).flushtag(m.oldtag))
				return srv.default(mm);

	Clunk or
	Remove =>
		fid := srv.getfid(m.fid);
		if(fid != nil && fid.isopen) {
			q := int fid.path&16rff;
			case q {
			Qprogress =>
				if(!progressfids.del(fid.fid))
					raise "missing progressfid";
			Qpeerevents =>
				if(!peerfids.del(fid.fid))
					raise "missing peerfid";
			}
		}
	}
	srv.default(mm);
}

commands0 := array[] of {"stop", "start", "track"};
commands1 := array[] of {"disconnect", "debug", "maxratio", "maxdowntotal", "maxuptotal", "maxdownrate", "maxuprate"};
ctl(m: ref Tmsg.Write)
{
	s := string m.data;
	if(s != nil && s[len s-1] == '\n')
		s = s[:len s-1];
	l := str->unquoted(s);
	if(l == nil)
		return replyerror(m, "missing command");
	cmd := hd l;
	l = tl l;

	ok := 0;
	for(i := 0; !ok && i < len commands0; i++)
		if(commands0[i] == cmd) {
			if(len l != 0)
				return replyerror(m, styxservers->Ebadarg);
			ok = 1;
		}
	for(i = 0; !ok && i < len commands1; i++)
		if(commands1[i] == cmd) {
			if(len l != 1)
				return replyerror(m, styxservers->Ebadarg);
			ok = 1;
		}
	if(!ok)
		return replyerror(m, "unknown command");

	case cmd {
	"stop" =>
		stop();
	"start" =>
		start();
	"disconnect" =>
		(id, rem) := str->toint(hd l, 10);
		if(rem != nil)
			return replyerror(m, styxservers->Ebadarg);
		p := peerbox.findid(id);
		if(p == nil)
			return replyerror(m, "no such peer");
		peerdrop(p, 0, nil);
	"track" =>
		trackkick(0);
	"debug" =>
		case hd l {
		"peer" =>	dflag = !dflag;
		"lib" =>	bt->dflag = !bt->dflag;
		"peerlib" =>	btp->dflag = !btp->dflag;
		}
		putprogress(ref Progress.Newctl);
	"maxratio" =>
		(v, rem) := str->toreal(hd l, 10);
		if(rem != nil || v <= 1.1)
			return replyerror(m, styxservers->Ebadarg);
		maxratio = v; # xxx might want to immediately rethink whether we want to stop
		putprogress(ref Progress.Newctl);
	"maxdowntotal" or
	"maxuptotal" or
	"maxdownrate" or
	"maxuprate" =>
		v := sizeparse(hd l);
		if(v < big 0)
			return replyerror(m, styxservers->Ebadarg);
		case cmd {
		"maxdowntotal" =>	maxdowntotal = v;
		"maxuptotal" =>		maxuptotal = v;
		"maxdownrate" =>	maxdownrate = v;
		"maxuprate" =>		maxuprate = v;
		}
		putprogress(ref Progress.Newctl);
	* =>
		raise "missing case";
	}
	srv.reply(ref Rmsg.Write (m.tag, len m.data));
	return;
}

putlist[T](fids: ref Table[ref Eventfid[T]], last: ref List[T], e: T): ref List[T]
for {
T =>	text:	fn(t: self T): string;
}
{
	l := last.next;
	l.e = e;
	l.next = ref List[T];
	for(t := tablist(fids); t != nil; t = tl t)
		while((rm := (hd t).read()) != nil)
			srv.reply(rm);
	return l;
}

putprogress(p: ref Progress)
{
	progresslast = putlist(progressfids, progresslast, p);
}

putevent(p: ref Peerevent)
{
	peereventlast = putlist(peerfids, peereventlast, p);
}

next[T](l: ref List[T], e: T): ref List[T]
{
	l.next.e = e;
	l.next.next = ref List[T];
	return l.next;
}

putprogressstate(l: ref List[ref Progress])
{
	if(stopped)
		l = next(l, ref Progress.Stopped);
	else
		l = next(l, ref Progress.Started);
	if(done())
		l = next(l, ref Progress.Done);
	else {
		it := state.pieces.have.iter();
		r: list of int;
		i: int;
		do {
			i = it.next();
			if(i < 0 || len r == 20) {
				l = next(l, ref Progress.Pieces (r));
				r = nil;
			}
			r = i::r;
		} while(i >= 0);
		for(fl := filesdone(-1); fl != nil; fl = tl fl) {
			f := hd fl;
			l = next(l, ref Progress.Filedone (f.index, f.path, f.f.path));
		}
	}
	t := trackereventlast;
	if(t == nil)
		t = ref Progress.Tracker (-1, -1, -1, nil);
	t.next = nexttrack-daytime->now();
	l = next(l, t);
	l = next(l, ref Progress.Endofstate);
	l.next = progresslast.next;
}

putpeerstate(l: ref List[ref Peerevent])
{
	for(f := faulty; f != nil; f = tl f)
		l = next(l, ref Peerevent.Bad ((hd f).t0, (hd f).t1, (hd f).t2));
	for(t := newpeers.all(); t != nil; t = tl t)
		l = next(l, ref Peerevent.Tracker ((hd t).addr));
	for(pl := peerbox.peers; pl != nil; pl = tl pl) {
		p := hd pl;
		l = next(l, ref Peerevent.New (p.np.addr, p.id, p.peeridhex, p.dialed));
		if(p.isdone())
			l = next(l, ref Peerevent.Done (p.id));
		else
			for(ll := peereventpieces(p.id, p.rhave); ll != nil; ll = tl ll)
				l = next(l, hd ll);
		if(p.localchoking())
			l = next(l, ref Peerevent.State (p.id, Slocal|Schoking));
		if(p.localinterested())
			l = next(l, ref Peerevent.State (p.id, Slocal|Sinterested));
		if(p.remotechoking())
			l = next(l, ref Peerevent.State (p.id, Sremote|Schoking));
		if(p.remoteinterested())
			l = next(l, ref Peerevent.State (p.id, Sremote|Sinterested));
	}
	l = next(l, ref Peerevent.Endofstate);
	l.next = peereventlast.next;
}

peereventpieces(id: int, b: ref Bits): list of ref Peerevent.Pieces
{
	l: list of ref Peerevent.Pieces;
	r: list of int;
	i: int;
	it := b.iter();
	do {
		i = it.next();
		if(i < 0 || len r == 20) {
			l = ref Peerevent.Pieces (id, r)::l;
			r = nil;
		}
		r = i::r;
	} while(i >= 0);
	return l;
}

# addr hex id direction localchoking localinterested remotechoking remoteinterested lastunchoke npiecehave "up" total rate "down" total rate "metaup" total rate "metadown" total rate
peerline(p: ref Peer): string
{
	direction := "listened";
	if(p.dialed)
		direction = "dialed";
	s := "";
	s += sprint("%q %q %d %s", p.np.addr, p.peeridhex, p.id, direction);
	s += sprint(" %s %s", chokestr(p.localchoking()), intereststr(p.localinterested()));
	s += sprint(" %s %s", chokestr(p.remotechoking()), intereststr(p.remoteinterested()));
	s += sprint(" %d", p.lastunchoke);
	s += sprint(" %d", p.rhave.have);
	s += sprint(" up %bd %d", p.up.total(), p.up.rate());
	s += sprint(" down %bd %d", p.down.total(), p.down.rate());
	s += sprint(" metaup %bd %d", p.metaup.total(), p.metaup.rate());
	s += sprint(" metadown %bd %d", p.metadown.total(), p.metadown.rate());
	s += "\n";
	return s;
}


navigator(c: chan of ref Navop)
{
	for(;;)
		navigate(<-c);
}

navigate(oo: ref Navop)
{
	q := int oo.path&16rff;
	pick o := oo {
	Stat =>
		o.reply <-= (dir(q), nil);

	Walk =>
		if(o.name == "..") {
			o.reply <-= (dir(Qroot), nil);
			return;
		}
		case q {
		Qroot =>
			for(i := Qfirst; i <= Qlast; i++)
				if(tab[i].t1 == o.name) {
					o.reply <-= (dir(tab[i].t0), nil);
					return;
				}
			o.reply <-= (nil, styxservers->Enotfound);
		* =>
			o.reply <-= (nil, styxservers->Enotdir);
		}
	Readdir =>
		case q {
		Qroot =>
			n := Qlast+1-Qfirst;
			have := 0;
			for(i := 0; have < o.count && o.offset+i < n; i++)
				case Qfirst+i {
				Qfirst to Qlast =>
					o.reply <-= (dir(Qfirst+i), nil);
					have++;
				* =>
					raise "missing case";
				}
		* =>
			raise "missing case";
		}
		o.reply <-= (nil, nil);
	}
}

dir(q: int): ref Sys->Dir
{
	(nil, name, perm) := tab[q];
	d := ref sys->zerodir;
	d.name = name;
	d.uid = d.gid = "torrent";
	d.qid.path = big q;
	if(perm&Sys->DMDIR)
		d.qid.qtype = Sys->QTDIR;
	else
		d.qid.qtype = Sys->QTFILE;
	d.mtime = d.atime = time0;
	d.mode = perm;
	return d;
}

replyerror(m: ref Tmsg, s: string)
{
	srv.reply(ref Rmsg.Error(m.tag, s));
}


done(): int
{
	return state.pieces.have.n == state.pieces.have.have;
}

stop()
{
	if(stopped)
		return;

	stopped = 1;
	putprogress(ref Progress.Stopped);

	# disconnect from all peers and don't do further tracker requests
	peerbox.lucky = nil;
	for(l := peerbox.peers; l != nil; l = tl l)
		peerdrop(hd l, 0, nil);
}

start()
{
	if(!stopped)
		return;

	stopped = 0;
	putprogress(ref Progress.Started);

	spawn trackkick(0);
	# when we have new peers, scheduling will begin as normal
}

writestate()
{
	d := state.pieces.have.d;
	if(sys->pwrite(state.tx.statefd, d, len d, big 0) != len d)
		warn(1, sprint("writing state: %r"));
	else
		say("state written");
}

peerdrop(p: ref Peer, faulty: int, err: string)
{
	if(err != nil)
		warn(1, err);
	if(faulty)
		setfaulty(p.np.ip, err);

	state.pieces.delpeer(p);

	if(p.dialed)
		dialpeers();
	else
		awaitpeer();
	peerbox.del(p);
	putevent(ref Peerevent.Gone (p.id));

	# peernetreader,peernetwriter
	for(pids := p.pids; pids != nil; pids = tl pids)
		kill(hd pids);
	spawn stopreadc(p.readc);
	spawn stopwritec(p.writec);
}

stopreadc(c: chan of ref RReq)
{
	c <-= nil;
}

stopwritec(c: chan of ref (int, int, array of byte))
{
	c <-= nil;
}

dialpeers()
{
	say(sprint("dialpeers, %d newpeers %d peers", len newpeers.all(), len peerbox.peers));
	while(!newpeers.empty() && ndialers < Dialersmax && len peerbox.peers < Peersmax && peerbox.ndialed < Peersdialedmax) {
		np := newpeers.take();
		if(selfips.find(np.ip) != nil)
			continue;
		if(peerbox.findip(np.ip) != nil)
			continue;
		if(isfaulty(np.ip))
			continue;

		say("spawning dialproc for "+np.text());
		putevent(ref Peerevent.Dialing (np.addr));
		spawn dialer(np);
		ndialers++;
	}
}

awaitpeer()
{
	if(peerbox.ndialed < Peersmax-Peersdialedmax && !islistening) {
say("awaitpeer, kicking listener");
		islistening = 1;
		canlistenc <-= 1;
	}
}

peersendmany(p: ref Peer, l: list of ref Msg)
{
	for(; l != nil; l = tl l)
		peersend0(p, hd l);
	peergive(p);
}

peersend0(p: ref Peer, mm: ref Msg)
{
	pick m := mm {
	Piece =>	p.datamsgs = rev(m::rev(p.datamsgs));
	* =>		p.metamsgs = rev(m::rev(p.metamsgs));
	}
}

peersend(p: ref Peer, m: ref Msg)
{
	peersend0(p, m);
	peergive(p);
}

account(p: ref Peer, l: list of ref Msg)
{
	for(; l != nil; l = tl l) {
		mm := hd l;
		msize := mm.packedsize();
		pick m := mm {
		Piece =>
			dsize := len m.d;
			p.up.add(dsize, 1);
			p.metaup.add(msize-dsize, 0);
			trafficup.add(dsize, 1);
			trafficmetaup.add(msize-dsize, 0);
		* =>
			p.metaup.add(msize, 1);
			trafficmetaup.add(msize, 1);
		}
	}
}

peergive(p: ref Peer)
{
	if(!p.getmsg)
		return;

	if(p.metamsgs != nil) {
		p.getmsgc <-= p.metamsgs;
		account(p, p.metamsgs);
		p.metamsgs = nil;
		p.getmsg = 0;
	} else if(p.datamsgs != nil && !p.localchoking() && p.remoteinterested()) {
		m := hd p.datamsgs;
		p.datamsgs = tl p.datamsgs;
		account(p, m::nil);
		p.getmsgc <-= m::nil;
		p.getmsg = 0;
	}
}

blocksize(req: ref LReq): int
{
	# first a quick check
	if(req.piece < state.t.piececount-1)
		return btp->Blocksize;

	# otherwise, the full check
	if(big req.piece*big state.t.piecelen + big (req.block+1)*big btp->Blocksize > state.t.length)
		return int (state.t.length % big btp->Blocksize);
	return btp->Blocksize;
}

readrreq(p: ref Peer)
{
	if(p.rreqs.length == 0)
		return;

	alt {
	p.readc <-= p.rreqs.first.e =>
		if(dflag) say("readrreq: another rreq");
		p.rreqs.dropfirst();
	* =>
		if(dflag) say("readrreq: diskreader busy, did get another not request");
	}
}

request(p: ref Peer, pc: ref Piece, reqs: list of ref LReq)
{
	msgs: list of ref Msg;
	for(; reqs != nil; reqs = tl reqs) {
		req := hd reqs;
		if(pc.busy[req.block].t0 < 0)
			pc.busy[req.block].t0 = p.id;
		else if(pc.busy[req.block].t1 < 0)
			pc.busy[req.block].t1 = p.id;
		else
			raise "both slots busy";
		if(pc.busy[req.block].t0 >= 0 && pc.busy[req.block].t1 >= 0) {
			pc.nhalfbusy--;
			pc.nfullbusy++;
		} else
			pc.nhalfbusy++;

		if(dflag) say(sprint("peer %d: %s", p.id, req.text()));
		p.lreqs.add(req);
		msgs = ref Msg.Request(req.piece, req.block*btp->Blocksize, blocksize(req))::msgs;
	}
	if(msgs != nil)
		peersendmany(p, rev(msgs));
}

schedule(p: ref Peer)
{
	if(!p.localinterested() || p.remotechoking() || !btp->needblocks(p))
		return;

	reqc := chan of ref (ref Piece, list of ref LReq, chan of int);
	spawn btp->schedule(reqc, p);
	while((r := <-reqc) != nil) {
		(pc, reqs, donec) := *r;
		request(p, pc, reqs);
		donec <-= 0;
	}
}


choke(p: ref Peer)
{
	peersend(p, ref Msg.Choke);
	p.state |= btp->LocalChoking;
	putevent(ref Peerevent.State (p.id, Slocal|Schoking));
}

unchoke(p: ref Peer)
{
	peersend(p, ref Msg.Unchoke);
	p.state &= ~btp->LocalChoking;
	putevent(ref Peerevent.State (p.id, Slocal|Sunchoking));
	p.lastunchoke = daytime->now();
}

interested(p: ref Peer)
{
	say("we are now interested in "+p.text());
	peersend(p, ref Msg.Interested);
	p.state |= btp->LocalInterested;
	putevent(ref Peerevent.State (p.id, Slocal|Sinterested));
}

uninterested(p: ref Peer)
{
	say("we are no longer interested in "+p.text());
	peersend(p, ref Msg.Notinterested);
	p.state &= ~btp->LocalInterested;
	putevent(ref Peerevent.State (p.id, Slocal|Suninterested));
}

interesting(p: ref Peer)
{
	# xxx we should call this more often.  a peer may have a piece we don't have yet, but we may have assigned all remaining blocks to (multiple) other peers, or we may be in paranoid mode
	if(p.localinterested() && p.lreqs.length == 0 && p.lwant.have == 0)
		uninterested(p);
	else if(!p.localinterested() && p.lwant.have > 0)
		interested(p);
}


isfaulty(ip: string): int
{
	now := daytime->now();
	for(l := faulty; l != nil; l = tl l)
		if((hd l).t0 == ip && now < (hd l).t1+Ignorefaultyperiod)
			return 1;
	return 0;
}

setfaulty(ip, err: string)
{
	clearfaulty(nil);
	now := daytime->now();
	faulty = (ip, now, err)::faulty;
	putevent(ref Peerevent.Bad (ip, now, err));
}

clearfaulty(ip: string)
{
	now := daytime->now();
	new: list of (string, int, string);
	for(l := faulty; l != nil; l = tl l)
		if((hd l).t0 != ip && (hd l).t1+Ignorefaultyperiod < now)
			new = hd l::new;
	faulty = new;
}


peerbufflush(b: ref Buf): ref (int, int, array of byte)
{
	if(dflag) say(sprint("peerbufflush: writing chunk to disk, pieceoff %d, len data %d", b.pieceoff, len b.data));
	tmp := ref (b.piece, b.pieceoff, b.data);
	b.clear();
	return tmp;
}

mainbufflush(b: ref Buf)
{
	if(dflag) say(sprint("mainbufflush: writing chunk to disk, pieceoff %d, len data %d", b.pieceoff, len b.data));
	mainwrites = rev(ref (b.piece, b.pieceoff, b.data)::rev(mainwrites));
	b.clear();
}

batchflushcomplete(pc: ref Piece, block: int): int
{
	b := btp->batches(pc)[block/btp->Batchsize];
	for(i := 0; i < len b.blocks; i++)
		if(!pc.have.get(b.blocks[i]))
			return 0;

	start := (block/btp->Batchsize)*btp->Batchsize*btp->Blocksize;
	end := start+btp->Batchsize*btp->Blocksize;
	for(l := peerbox.peers; l != nil; l = tl l) {
		p := hd l;
		if(p.buf.overlaps(pc.index, start, end))
			mainbufflush(p.buf);
	}
	return 1;
}


nextoptimisticunchoke(): ref Peer
{
	rotateips.fill(); # xxx replace by markstart, and then lazily rotate

	# find next masked ip address to pick peer from (if still present)
	for(;;) {
		ipmasked := rotateips.take();
		if(ipmasked == nil)
			break;
		p := peerbox.oldestunchoke(ipmasked);
		if(p != nil)
			return p;
		rotateips.pooldel(ipmasked);
	}
	return nil;
}

chokingupload(gen: int)
{
	if(gen % 3 == 2)
		return;

	oldest := peerbox.longestunchoked();
	nunchoked := peerbox.nunchokedinterested();
	others := peerbox.localunchokable();

	if(oldest != nil && nunchoked+len others >= Seedunchokedmax) {
		choke(oldest);
		nunchoked--;
	}

	othersa := l2a(others);
	btp->randomize(othersa);
	for(i := 0; i < len othersa && nunchoked+i < Seedunchokedmax; i++)
		unchoke(othersa[i]);
}

peerratecmp(a1, a2: ref (ref Peer, int)): int
{
	(p1, r1) := *a1;
	(p2, r2) := *a2;
	n := r2-r1;
	if(n != 0)
		return n;
	if(p1.remoteinterested() == p2.remoteinterested())
		return 0;
	if(p1.remoteinterested())
		return -1;
	return 1;
}

chokingdownload(gen: int)
{
	# new optimistic unchoke?
	if(gen % 3 == 0)
		peerbox.lucky = nextoptimisticunchoke();

	# make sorted array of all peers, sorted by upload rate, then by interestedness
	allpeers := array[len peerbox.peers] of ref (ref Peer, int);  # peer, rate
	i := 0;
	luckyindex := -1;
	for(l := peerbox.peers; l != nil; l = tl l) {
		if(peerbox.lucky != nil && hd l == peerbox.lucky)
			luckyindex = i;
		allpeers[i++] = ref (hd l, (hd l).down.rate());
	}
	inssort(allpeers, peerratecmp);

	# determine N interested peers with highest upload rate
	nintr := 0;
	for(i = 0; nintr < Unchokedmax && i < len allpeers; i++)
		if(allpeers[i].t0.remoteinterested())
			nintr++;
	unchokeend := i;  # index of first peer to choke.  element before (if any) is slowest peer to unchoke

	# replace slowest of N by optimistic unchoke if lucky peer was not already going to be unchoked
	if(luckyindex >= 0 && luckyindex >= unchokeend && unchokeend-1 >= 0) {
		allpeers[luckyindex] = allpeers[unchokeend-1];
		allpeers[unchokeend-1] = ref (peerbox.lucky, 0);
	}

	# now unchoke the N peers, and all non-interested peers that are faster.  choke all other peers if they weren't already.
	for(i = 0; i < len allpeers; i++) {
		(p, nil) := *allpeers[i];
		if(p == nil)
			say(sprint("bad allpeers, len allpeers %d, len peers %d, i %d, unchokeend %d, luckyindex %d, nintr %d", len allpeers, len peerbox.peers, i, unchokeend, luckyindex, nintr));
		if(i < unchokeend && p.localchoking())
			unchoke(p);
		else if(i >= unchokeend && !p.localchoking())
			choke(p);
	}
}

# xxx fix this code to never either block entirely or delay on disk/net i/o
peermsg(p: ref Peer, mm: ref Msg, needwritec: chan of list of ref (int, int, array of byte))
{
	p.msgseq++;
if(dflag) say(sprint("<- %s: %s", p.np.ip, mm.text()));

	msize := mm.packedsize();
	pick m := mm {
	Piece =>
		# xxx count pieces we did not request as overhead (meta)?
		dsize := len m.d;
		p.down.add(dsize, 1);
		p.metadown.add(msize-dsize, 0);
		trafficdown.add(dsize, 1);
		trafficmetadown.add(msize-dsize, 0);
	* =>
		p.metadown.add(msize, 1);
		trafficmetadown.add(msize, 1);
	}

	pick m := mm {
	Keepalive =>
		;

	Choke =>
		if(p.remotechoking())
			return peerdrop(p, 1, "double choke");

		say(sprint("%s choked us...", p.text()));
		p.state |= btp->RemoteChoking;
		putevent(ref Peerevent.State (p.id, Sremote|Schoking));
		# xxx clear any pending requests
		

	Unchoke =>
		if(!p.remotechoking())
			return peerdrop(p, 1, sprint("double unchoke"));

		say(sprint("%s unchoked us", p.text()));
		p.state &= ~btp->RemoteChoking;
		putevent(ref Peerevent.State (p.id, Sremote|Sunchoking));
		schedule(p);

	Interested =>
		if(0 && p.remoteinterested())
			return peerdrop(p, 1, "double interested");

		say(sprint("%s is interested", p.text()));
		p.state |= btp->RemoteInterested;
		putevent(ref Peerevent.State (p.id, Sremote|Sinterested));

		if(!p.localchoking() && peerbox.nactive() >= Unchokedmax && !done()) {
			# xxx choke slowest peer that is not the optimistic unchoke
		}

	Notinterested =>
		if(!p.remoteinterested())
			return peerdrop(p, 1, "double uninterested");

		say(sprint("%s is no longer interested", p.text()));
		p.state &= ~btp->RemoteInterested;
		putevent(ref Peerevent.State (p.id, Sremote|Suninterested));
		p.rreqs.clear();
		# if peer was unchoked, we'll unchoke another during next round

	Have =>
		if(m.index >= state.t.piececount)
			return peerdrop(p, 1, sprint("%s sent 'have' for invalid piece %d, disconnecting", p.text(), m.index));

		# xxx it seems some clients (rtorrent) send "have" for pieces that were in the bitfield message.
		if(0 && p.rhave.get(m.index))
			return peerdrop(p, 1, sprint("peer already had piece %d (has %d/%d)", m.index, p.rhave.have, p.rhave.n));

		putevent(ref Peerevent.Piece (p.id, m.index));
		if(dflag) say(sprint("remote now has piece %d", m.index));
		p.rhave.set(m.index);
		if(!state.pieces.have.get(m.index))
			p.lwant.set(m.index);
		if(p.isdone())
			peerbox.nseeding++;
		state.pieces.addpeerpiece(p, m.index);

		interesting(p);
		schedule(p);

	Bitfield =>
		if(p.msgseq != 1)
			return peerdrop(p, 1, sprint("%s sent bitfield after first message, disconnecting", p.text()));

		err: string;
		(p.rhave, err) = Bits.mk(state.t.piececount, m.d);
		if(err != nil)
			return peerdrop(p, 1, sprint("%s sent bogus bitfield message: %s, disconnecting", p.text(), err));
		p.lwant = Bits.nand(p.rhave, state.pieces.have);

		state.pieces.addpeerpieces(p);

		if(p.isdone()) {
			putevent(ref Peerevent.Done (p.id));
			peerbox.nseeding++;
		} else
			for(l := peereventpieces(p.id, p.rhave); l != nil; l = tl l)
				putevent(hd l);

		interesting(p);

	Piece =>
		if(dflag) say(sprint("peer %d sent block piece=%d begin=%d length=%d", p.id, m.index, m.begin, len m.d));

		block := m.begin/btp->Blocksize;
		req := ref LReq (m.index, block, 0);
		if(blocksize(req) != len m.d)
			return peerdrop(p, 1, sprint("%s sent bad size for block %s, disconnecting", p.text(), req.text()));

		piece := state.pieces.active.find(m.index);
		if(piece == nil)
			return peerdrop(p, 1, sprint("got data for inactive piece %d", m.index));

		if(!p.lreqs.take(req)) {
			exp := "nothing";
			if(p.lreqs.length > 0)
				exp = p.lreqs.first.e.text();
			return peerdrop(p, 1, sprint("%s sent block %s, expected %s, disconnecting", p.text(), req.text(), exp));
		}

		if(piece.have.get(block)) {
			needwritec <-= nil;
			return say("already have this block, skipping...");
		}

		if(m.begin == piece.hashoff) {
			piece.hash = kr->sha1(m.d, len m.d, nil, piece.hash);
			piece.hashoff += len m.d;
		}

		# possibly send cancel to other peer
		(b0, b1) := piece.busy[block];
		busypeer: ref Peer;
		if(b0 >= 0 && b0 != p.id) {
			busypeer = peerbox.findid(b0);
			piece.nfullbusy--;
			piece.busy[block].t0 = -1;
		} else if(b1 >= 0 && b1 != p.id) {
			busypeer = peerbox.findid(b1);
			piece.nfullbusy--;
			piece.busy[block].t1 = -1;
		} else
			piece.nhalfbusy--;
		if(busypeer != nil) {
			p.lreqs.cancel(ref LReq (m.index, block, 0));
			peersend(p, ref Msg.Cancel(m.index, m.begin, len m.d));
		}

		needwrites: list of ref (int, int, array of byte);

		# add block to buffer.  if new block does not fit in, flush the old data.
		if(!p.buf.tryadd(piece, m.begin, m.d)) {
			needwrite := peerbufflush(p.buf);
			if(needwrite != nil)
				needwrites = needwrite::needwrites;
			if(!p.buf.tryadd(piece, m.begin, m.d))
				raise "tryadd failed...";
		}

		# flush now, rather then delaying it
		if(p.buf.isfull()) {
			needwrite := peerbufflush(p.buf);
			if(needwrite != nil)
				needwrites = needwrite::needwrites;
		} else  # if this completes the batch, flush it for all peers that have blocks belonging to it
			batchflushcomplete(piece, block);

		needwritec <-= rev(needwrites);

		piece.have.set(block);
		piece.done[block] = p.id;
		putprogress(ref Progress.Block (piece.index, block, piece.have.have, piece.have.n));

		if(piece.isdone()) {
			# flush all bufs about this piece, also from other peers, to disk.  to make hash-checking read right data.
			for(l := peerbox.peers; l != nil; l = tl l) {
				op := hd l;
				if(op.buf.piece == m.index) {
					say("flushing buf of other peer before hash check");
					mainbufflush(op.buf);
				}
			}
		}
		schedule(p);

	Request =>
		rr := ref RReq (m.index, m.begin, m.length);
		if(dflag) say(sprint("%s sent request for %s", p.text(), rr.text()));

		if(m.length > Blocksizemax)
			return peerdrop(p, 0, "requested block too large, disconnecting");
		if(p.rhave.get(m.index))
			return peerdrop(p, 1, "peer requested piece it already claimed to have");
		if(p.rreqs.length >= Blockqueuemax)
			return peerdrop(p, 0, sprint("peer scheduled one too many blocks, already has %d scheduled, disconnecting", p.rreqs.length));

		p.rreqs.add(rr);
		if(!p.localchoking() && p.remoteinterested())
			readrreq(p);

	Cancel =>
		rr := ref RReq (m.index, m.begin, m.length);
		if(dflag) say(sprint("%s sent cancel for %s", p.text(), rr.text()));
		if(!p.rreqs.del(rr))
			say("peer did not want block before, skipping");
	}
}


gen: int;
bogusc: chan of ref (int, int, array of byte);
main0()
{
	# the alt statement doesn't do conditional sending/receiving on a channel.
	# so, before we start the alt, we evaluate the condition.
	# if true, we use the real channel we want to send on.
	# if false, we use a bogus channel without receiver, so the case is never taken.
	curwritec := bogusc;
	curmainwrite: ref (int, int, array of byte);
	if(mainwrites != nil) {
		curmainwrite = hd mainwrites;
		curwritec = diskwritec;
	}

	alt {
	mm := <-msgc =>
		if(mm == nil)
			fail("styx eof");
		pick m := mm {
		Readerror =>
			fail("styx read error: "+m.error);
		}
		dostyx(mm);

	<-roundc =>
		if(stopped)
			return;
		say(sprint("ticking, %d peers", len peerbox.peers));

		# do choking/unchoking algorithm round
		if(done())
			chokingupload(gen);
		else
			chokingdownload(gen);
		gen++;

	<-trackkickc =>
		trackkickpid = -1;
		if(stopped)
			return;
		trackreqc <-= (trafficup.total(), trafficdown.total(), totalleft, listenport, trackerevent);
		trackerevent = nil;

	(interval, nps, trackerr) := <-trackc =>
		if(stopped)
			return;

		# schedule next call to tracker
		if(interval < Intervalmin)
			interval = Intervalmin;
		if(interval > Intervalmax)
			interval = Intervalmax;
		now := daytime->now();
		if(now < time0+Intervalstartupperiod && !done() && len peerbox.peers+len newpeers.all() < Peersmax && interval > Intervalneed)
			interval = Intervalneed;

		say(sprint("next call to tracker will be in %d seconds", interval));
		trackkick(interval);
		nexttrack = now+interval;
		trackereventlast = ref Progress.Tracker (interval, interval, len nps, trackerr);
		putprogress(trackereventlast);

		if(trackerr != nil)
			return warn(0, sprint("tracker error: %s", trackerr));

		say("main, new peers");
		for(i := 0; i < len nps; i++) {
			(ip, port, peerid) := nps[i];
			if(hex(peerid) == localpeeridhex)
				continue;  # skip self
			np := Newpeer(sprint("%s!%d", ip, port), ip, peerid);
			say("new: "+np.text());
			newpeers.del(np);
			if(peerbox.findaddr(np.addr) == nil) {
				newpeers.add(np);
				putevent(ref Peerevent.Tracker (np.addr));
			} else
				say("already connected to "+np.text());
		}
		dialpeers();

	(dialed, np, peerfd, extensions, peerid, err) := <-newpeerc =>
		if(!dialed)
			islistening = 0;
		if(stopped)
			return;

		if(err != nil) {
			warn(1, sprint("%s: %s", np.text(), err));
		} else if(hex(peerid) == localpeeridhex) {
			say("connected to self, dropping connection...");
			tcpdir := str->splitstrr(sys->fd2path(peerfd), "/").t0;
			localip := readip(tcpdir+"local");
			remoteip := readip(tcpdir+"remote");
			if(localip == remoteip && localip != nil && selfips.find(localip) == nil)
				selfips.add(localip, localip);
		} else if(peerbox.findip(np.ip) != nil) {
			say("new connection from known ip address, dropping new connection...");
		} else if(isfaulty(np.ip)) {
			say(sprint("connected to faulty ip %s, dropping connection...", np.ip));
		} else {
			p := Peer.new(np, peerfd, extensions, peerid, dialed, state.t.piececount);
			pidc := chan of int;
			spawn peernetreader(pidc, p);
			spawn peernetwriter(pidc, p);
			spawn diskwriter(p.writec);
			spawn diskreader(p);
			p.pids = <-pidc::<-pidc::p.pids;

			peerbox.add(p);
			say("new peer "+p.fulltext());
			putevent(ref Peerevent.New (p.np.addr, p.id, p.peeridhex, p.dialed));

			rotateips.pooladdunique(maskip(np.ip));

			if(state.pieces.have.have == 0)
				peersend(p, ref Msg.Keepalive);
			else
				peersend(p, ref Msg.Bitfield(state.pieces.have.bytes()));

			if(peerbox.nactive() < Unchokedmax) {
				say("unchoking rare new peer: "+p.text());
				unchoke(p);
			}
		}
		if(dialed) {
			ndialers--;
			dialpeers();
		} else
			awaitpeer();

	(p, msg, needwritec, err) := <-peerinmsgc =>
		if(stopped)
			return;
		if(err != nil)
			return peerdrop(p, 0, "read error from peer: "+err);
		if(msg == nil)
			return peerdrop(p, 0, "eof from peer "+p.text());
		peermsg(p, msg, needwritec);

	(p, err) := <-peererrc =>
		peerdrop(p, 0, "peer error: "+err);

	(pieceindex, begin, length, err) := <-diskwrittenc =>
		if(stopped)
			return;
		if(err != nil)
			return warnstop(sprint("error writing piece %d, begin %d, length %d: %s", pieceindex, begin, length, err));

		piece := state.pieces.active.find(pieceindex);
		if(piece == nil)
			raise sprint("data written for inactive piece %d", pieceindex);

		# mark written blocks as such
		first := begin/btp->Blocksize; 
		n := (length+btp->Blocksize-1)/btp->Blocksize;
		for(i := 0; i < n; i++)
			piece.written.set(first+i);

		if(!piece.written.isfull())
			return;

		say(sprint("last parts of piece %d have been written, verifying...", pieceindex));

		need := state.t.piecelength(piece.index)-piece.hashoff;
		if(need > 0) {
			# xxx should do this in separate prog?
			buf := array[need] of byte;
			herr := state.tx.preadx(buf, len buf, big piece.index*big state.t.piecelen+big piece.hashoff);
			if(herr != nil)
				return warnstop("verifying hash: "+herr);
			piece.hash = kr->sha1(buf, len buf, nil, piece.hash);
		}
		wanthash := hex(state.t.hashes[piece.index]);
		kr->sha1(nil, 0, digest := array[kr->SHA1dlen] of byte, piece.hash);
		havehash := hex(digest);
		if(wanthash != havehash) {
			# xxx blame peers
			putprogress(ref Progress.Hashfail (piece.index));
			warn(1, sprint("%s did not check out, want %s, have %s, disconnecting", piece.text(), wanthash, havehash));
			piece.have.clearall();
			for(i = 0; i < len piece.busy; i++)
				if(piece.busy[i].t0 >= 0 || piece.busy[i].t1 >= 0)
					raise sprint("piece %d should be complete, but block %d is busy", piece.index, i);

			# xxx what do to with other peers?
			return;
		}

		state.pieces.havepiece(piece.index);
		totalleft -= big state.t.piecelength(piece.index);
		putprogress(ref Progress.Piece (piece.index, state.pieces.have.have, state.pieces.have.n));
		for(fl := filesdone(piece.index); fl != nil; fl = tl fl) {
			f := hd fl;
			putprogress(ref Progress.Filedone (f.index, f.path, f.f.path));
		}

		# this could have been the last piece this peer had, making us no longer interested
		for(l := peerbox.peers; l != nil; l = tl l) {
			pp := hd l;
			if(pp.lwant.get(piece.index)) {
				pp.lwant.clear(piece.index);
				interesting(pp);
			}
		}

		writestate();
		say("piece now done: "+piece.text());

		for(l = peerbox.peers; l != nil; l = tl l)
			peersend(hd l, ref Msg.Have(piece.index));

		if(done()) {
			trackerevent = "completed";
			trackkick(0);
			npeers: list of ref Peer;
			for(l = peerbox.peers; l != nil; l = tl l) {
				p := hd l;
				if(p.isdone()) {
					say("done: dropping seed "+p.fulltext());
					peerdrop(p, 0, nil);
				} else {
					npeers = p::npeers;
					# we won't act on becoming interested while unchoked anymore
					if(!p.remoteinterested() && !p.localchoking())
						choke(p);
				}
			}
			peerbox.peers = rev(npeers);
			warn(0, "DONE!");
			putprogress(ref Progress.Done);

			if(!stopped) {
				ratio := ratio();
				if(ratio >= 1.1 && ratio >= maxratio) {
					say(sprint("stopping due to max ratio achieved (%.2f)", ratio));
					return stop();
				}
			}
		}

	curwritec <-= curmainwrite =>
		mainwrites = tl mainwrites;

	p := <-wantmsgc =>
		p.getmsg = 1;
		if(stopped)
			return;
		peergive(p);
		if(!p.localchoking() && p.remoteinterested() && p.getmsg)
			readrreq(p);

	(p, piece, begin, buf, err) := <-diskreadc =>
		if(stopped)
			return;
		if(err != nil)
			return warnstop(sprint("error writing piece %d, begin %d, length %d: %s", piece, begin, len buf, err));
		if(peerbox.findid(p.id) == nil)
			return;
		peersend(p, ref Msg.Piece(piece, begin, buf));
	}
}

main()
{
warn(1, sprint("main pid %d", pid()));
	gen = 0;
	bogusc = chan of ref (int, int, array of byte);
	for(;;)
		main0();
}

roundticker()
{
	for(;;) {
		sys->sleep(10*1000);
		roundc <-= 0;
	}
}

trackkicktime: int;
trackkick(n: int)
{
	ntrackkicktime := daytime->now()+n;
	if(trackkickpid >= 0) {
		if(ntrackkicktime > trackkicktime)
			return;
		kill(trackkickpid);
	}
	spawn trackkick0(n, pidc := chan of int);
	trackkickpid = <-pidc;
	trackkicktime = ntrackkicktime;
}

trackkick0(n: int, pidc: chan of int)
{
	pidc <-= pid();
	sys->sleep(n*1000);
	trackkickc <-= 1;
}

track()
{
	for(;;) {
		(up, down, left, lport, event) := <-trackreqc;

		say("getting new tracker info");
		(interval, nps, nil, err) := bt->trackerget(state.t, localpeerid, up, down, left, lport, event);
		if(err != nil)
			say("trackerget: "+err);
		else
			say("trackget okay");
		trackc <-= (interval, nps, err);
	}
}


listener(aconn: Sys->Connection)
{
	for(;;) {
		(ok, conn) := sys->listen(aconn);
		if(ok != 0) {
			warn(0, sprint("listen: %r"));
			continue;
		}

		rembuf := readfile(conn.dir+"/remote", 128);
		if(rembuf == nil) {
			warn(0, sprint("%r"));
			continue;
		}
		remaddr := str->splitstrl(string rembuf, "\n").t0;

		f := conn.dir+"/data";
		fd := sys->open(f, Sys->ORDWR);
		if(fd == nil) {
			warn(0, sprint("new connection, open %s: %r", f));
			continue;
		}

		(extensions, peerid, err) := handshake(fd);
		if(err != nil)
			say("error handshaking incoming connection: "+err);
		np := Newpeer(remaddr, str->splitstrl(remaddr, "!").t0, nil);
		newpeerc <-= (0, np, fd, extensions, peerid, err);
		<-canlistenc;
	}
}


# we are allowed to pass `max' bytes per second.
limiter(c: chan of (int, chan of int), max: int)
{
	maxallow := min(max, Netiounit);

	for(;;) {
		(want, respc) := <-c;
		if(max <= 0) {
			# no rate limiting, let traffic pass
			respc <-= want;
			continue;
		}

		give := min(maxallow, want);
		respc <-= give;
		sys->sleep(980*give/max);  # don't give out more bandwidth until this portion has run out
	}
}


dialkill(pidc: chan of int, ppid: int, np: Newpeer)
{
	pidc <-= pid();
	sys->sleep(Dialtimeout*1000);
	kill(ppid);
	newpeerc <-= (1, np, nil, nil, nil, sprint("dial/handshake %s: timeout", np.addr));
}

dialer(np: Newpeer)
{
	ppid := pid();
	spawn dialkill(pidc := chan of int, ppid, np);
	killerpid := <-pidc;
	
	addr := sprint("net!%s", np.addr);
	(ok, conn) := sys->dial(addr, nil);
	if(ok < 0) {
		kill(killerpid);
		newpeerc <-= (1, np, nil, nil, nil, sprint("dial %s: %r", np.addr));
		return;
	}

	say("dialed "+addr);
	fd := conn.dfd;

	(extensions, peerid, err) := handshake(fd);
	if(err != nil)
		fd = nil;
	kill(killerpid);
	newpeerc <-= (1, np, fd, extensions, peerid, err);
}

handshake(fd: ref Sys->FD): (array of byte, array of byte, string)
{
	d := array[20+8+20+20] of byte;
	i := 0;
	d[i++] = byte 19;
	d[i:] = array of byte "BitTorrent protocol";
	i += 19;
	d[i:] = array[8] of {* => byte 0};
	i += 8;
	d[i:] = state.t.infohash;
	i += 20;
	d[i:] = localpeerid;
	i += 20;
	if(i != len d)
		raise "bad peer header, internal error";

	n := netwrite(fd, d, len d);
	if(n != len d)
		return (nil, nil, sprint("writing peer header: %r"));

	rd := array[len d] of byte;
	n = netread(fd, rd, len rd);
	if(n < 0)
		return (nil, nil, sprint("reading peer header: %r"));
	if(n != len rd)
		return (nil, nil, sprint("short read on peer header (%d)", n));

	if(rd[0] != byte 19 || string rd[1:1+19] != "BitTorrent protocol")
		return (nil, nil, sprint("peer does not speak bittorrent protocol"));

	extensions := rd[20:20+8];
	hash := rd[20+8:20+8+20];
	peerid := rd[20+8+20:];

	if(hex(hash) != hex(state.t.infohash))
		return (nil, nil, sprint("peer wants torrent hash %s, not %s", hex(hash), hex(state.t.infohash)));

	return (extensions, peerid, nil);
}


# read, going through limiter
netread(fd: ref Sys->FD, buf: array of byte, n: int): int
{
	read := 0;
	while(n > 0) {
		downc <-= (n, respc := chan of int);
		can := <-respc;
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

# write, going through limiter
netwrite(fd: ref Sys->FD, buf: array of byte, n: int): int
{
	wrote := 0;
	while(n > 0) {
		upc <-= (n, respc := chan of int);
		can := <-respc;
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


msgread(fd: ref Sys->FD): (ref Msg, string)
{
	buf := array[4] of byte;
	n := netread(fd, buf, len buf);
	if(n < 0)
		return (nil, sprint("reading: %r"));
	if(n < len buf)
		return (nil, sprint("short read"));
	(size, nil) := g32i(buf, 0);
	buf = array[size] of byte;

	n = netread(fd, buf, len buf);
	if(n < 0)
		return (nil, sprint("reading: %r"));
	if(n < len buf)
		return (nil, sprint("short read"));

	return Msg.unpack(buf);
}


peernetreader(pidc: chan of int, p: ref Peer)
{
	pidc <-= pid();

	needwritec := chan of list of ref (int, int, array of byte);
	for(;;) {
		(m, err) := msgread(p.fd);
		peerinmsgc <-= (p, m, needwritec, err);
		if(err != nil || m == nil)
			break;

		if(tagof m == tagof Msg.Piece)
			for(l := <-needwritec; l != nil; l = tl l)
				p.writec <-= hd l;  # will block if disk is slow, slowing down peer as well
	}
}

peernetwriter(pidc: chan of int, p: ref Peer)
{
	pidc <-= pid();

	for(;;) {
		wantmsgc <-= p;
		ml := <-p.getmsgc;
		if(ml == nil) {
			say("peernetwriter: stopping...");
			return;
		}

		dlen := 0;
		for(l := ml; l != nil; l = tl l)
			dlen += (hd l).packedsize();
		d := array[dlen] of byte;
		o := 0;
		for(; ml != nil; ml = tl ml) {
			m := hd ml;
			size := m.packedsize();
			m.packbuf(d[o:o+size]);
			o += size;
if(dflag) say(sprint("-> %s: %s", p.np.ip, m.text()));
		}
		n := netwrite(p.fd, d, len d);
		if(n != len d) {
			peererrc <-= (p, sprint("write: %r"));
			return;
		}
	}
}

diskwriter(reqc: chan of ref (int, int, array of byte))
{
	for(;;) {
		req := <-reqc;
		if(req == nil) {
			say("diskwriter: stopping...");
			break;
		}
		(piece, begin, buf) := *req;

		off := big piece*big state.t.piecelen + big begin;
		err := state.tx.pwritex(buf, len buf, off);
		diskwrittenc <-= (piece, begin, len buf, err);
	}
}

diskreader(p: ref Peer)
{
	for(;;) {
		rr := <-p.readc;
		if(rr == nil) {
			say("diskreader: stopping...");
			break;
		}
		off := big rr.piece*big state.t.piecelen + big rr.begin;
		err := state.tx.preadx(buf := array[rr.length] of byte, len buf, off);
		diskreadc <-= (p, rr.piece, rr.begin, buf, err);
	}
}


Peerbox.new(): ref Peerbox
{
	b := ref Peerbox;
	b.ips = b.ips.new(32, nil);
	b.addrs = b.addrs.new(32, nil);
	b.ids = b.ids.new(32, nil);
	b.ndialed = 0;
	b.nseeding = 0;
	return b;
}

Peerbox.add(b: self ref Peerbox, p: ref Peer)
{
	b.peers = p::b.peers;
	b.ips.add(p.np.ip, p);
	b.addrs.add(p.np.addr, p);
	b.ids.add(p.id, p);
	if(p.dialed)
		b.ndialed++;
	if(p.isdone())
		b.nseeding++;
}

Peerbox.del(b: self ref Peerbox, p: ref Peer)
{
	r: list of ref Peer;
	for(l := b.peers; l != nil; l = tl l)
		if((hd l) != p)
			r = hd l::r;
	b.peers = r;
	if(b.lucky == p)
		b.lucky = nil;
	b.ips.del(p.np.ip);
	b.addrs.del(p.np.addr);
	b.ids.del(p.id);
	if(p.dialed)
		b.ndialed--;
	if(p.isdone())
		b.nseeding--;
}

Peerbox.findip(b: self ref Peerbox, ip: string): ref Peer
{
	return b.ips.find(ip);
}

Peerbox.findaddr(b: self ref Peerbox, addr: string): ref Peer
{
	return b.addrs.find(addr);
}

Peerbox.findid(b: self ref Peerbox, id: int): ref Peer
{
	return b.ids.find(id);
}

# can be kept up to date by operations in future, not having to recalculate all the time
Peerbox.nactive(b: self ref Peerbox): int
{
	n := 0;
	for(l := b.peers; l != nil; l = tl l)
		if(!(hd l).localchoking() && (hd l).remoteinterested())
			n++;
	return n;

}

Peerbox.oldestunchoke(b: self ref Peerbox, ipmasked: string): ref Peer
{
	oldest: ref Peer;
	for(l := b.peers; l != nil; l = tl l) {
		p := hd l;
		if(maskip(p.np.ip) == ipmasked && (oldest == nil || p.lastunchoke < oldest.lastunchoke))
			oldest = p;
	}
	return oldest;
}

Peerbox.localunchokable(b: self ref Peerbox): list of ref Peer
{
	r: list of ref Peer;
	for(l := b.peers; l != nil; l = tl l) {
		p := hd l;
		if(p.remoteinterested() && p.localchoking())
			r = p::r;
	}
	return r;
}

Peerbox.nunchokedinterested(b: self ref Peerbox): int
{
	n := 0;
	for(l := b.peers; l != nil; l = tl l) {
		p := hd l;
		if(!p.localchoking() && p.remoteinterested())
			n++;
	}
	return n;
}

Peerbox.longestunchoked(b: self ref Peerbox): ref Peer
{
	oldest: ref Peer;
	for(l := b.peers; l != nil; l = tl l) {
		p := hd l;
		if(!p.localchoking() && p.remoteinterested()) {
			if(oldest == nil || p.lastunchoke < oldest.lastunchoke)
				oldest = p;
		}
	}
	return oldest;
}


filedone(f: ref Filex): int
{
	for(i := f.pfirst; i <= f.plast; i++)
		if(!state.pieces.have.get(i))
			return 0;
	return 1;
}

# return all files currently done when pindex == -1 
filesdone(pindex: int): list of ref Filex
{
	l: list of ref Filex;
	for(i := 0; i < len state.tx.files; i++) {
		f := state.tx.files[i];
		if(pindex >= 0 && pindex > f.plast)
			break;
		if((pindex < 0 || f.pfirst >= pindex && pindex <= f.plast) && filedone(f))
			l = f::l;
	}
	return rev(l);
}

ratio(): real
{
	up := trafficup.total();
	down := trafficdown.total();
	if(down == big 0)
		return Math->Infinity;
	return real up/real down;
}

haveeta := 0;
preveta: int;
lasteta: int;
eta(): int
{
	if(done())
		return 0;
	ms := sys->millisec();
	if(!haveeta || ms-preveta >= 1000) {
		r := trafficdown.rate();
		if(r <= 0)
			return -1;
		neweta := int (totalleft/big r);
		if(haveeta)
			lasteta = (lasteta*8+neweta*2)/10;
		else
			lasteta = neweta;
		haveeta = 1;
		preveta = ms;
	}
	return lasteta;
}

readip(f: string): string
{
	d := readfile(f, 128);
	if(d == nil)
		return nil;
	ipstr := str->splitstrl(string d, "!").t0;
	(ok, ipa) := IPaddr.parse(ipstr);
	if(ok != 0)
		return nil;
	return ipa.text();
}

chokestr(choked: int): string
{
	if(choked)
		return "choked";
	return "unchoked";
}

intereststr(i: int): string
{
	if(i)
		return "interested";
	return "uninterested";
}

tablist[T](t: ref Table[T]): list of T
{
	r: list of T;
	for(i := 0; i < len t.items; i++)
		for(l := t.items[i]; l != nil; l = tl l)
			r = (hd l).t1::r;
	return r;
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

sha1(d: array of byte): array of byte
{
	digest := array[kr->SHA1dlen] of byte;
	kr->sha1(d, len d, digest, nil);
	return digest;
}

warn(put: int, s: string)
{
	sys->fprint(sys->fildes(2), "%s\n", s);
	if(put)
		putprogress(ref Progress.Error (s));
}

warnstop(s: string)
{
	warn(1, s);
	stop();
}

say(s: string)
{
	if(dflag)
		warn(0, s);
}

fail(s: string)
{
	warn(0, s);
	killgrp(pid());
	raise "fail:"+s;
}
