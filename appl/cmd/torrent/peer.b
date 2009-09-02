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
	pid, kill, killgrp, hex, min, warn, rev, l2a, g32i, readfile, readfd, inssort, sizefmt, sizeparse: import util;
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";
	bt: Bittorrent;
	Bee, Msg, File, Torrent, Filex, Torrentx: import bt;
include "../../lib/bittorrentpeer.m";
	btp: Bittorrentpeer;
	State, Pool, Traffic, Piece, Block, Peer, Newpeer, Newpeers, Buf, Req, Reqs, Batch, List, Progress, Peerevent, Eventfid: import btp;
	Slocal, Sremote, Schoking, Sunchoking, Sinterested, Suninterested: import Bittorrentpeer;

Torrentpeer: module {
	init:	fn(nil: ref Draw->Context, nil: list of string);
};


dflag: int;
nofix: int;

state:	ref State;
newpeers:	ref Newpeers;  # peers we are not connected to
peers:		list of ref Peer;  # peers we are connected to
peeripmap,					# peer.id
peeraddrmap:	ref Tables->Strhash[ref Peer];  # newpeer.addr
peeridmap:	ref Tables->Table[ref Peer];	# newpeer.ip
peersdialed:	int;
peersseeding:	int;
luckypeer:	ref Peer;

torrentpath:	string;
time0:	int;
totalleft:	big;
listenport:	int;
localpeerid:	array of byte;
localpeeridhex:	string;
trackerevent:	string;
trafficup,
trafficdown,
trafficmetaup,
trafficmetadown:	ref Traffic;  # global traffic counters.  xxx uses same sliding window as traffic speed used for choking
maxratio	:= 0.0;
maxdownrate	:= big -1;
maxuprate	:= big -1;
maxdowntotal	:= big -1;
maxuptotal	:= big -1;
ip4mask,
ip6mask:	IPaddr;

# tracker
trackkickc:	chan of int;
trackreqc:	chan of (big, big, big, int, string);  # up, down, left, listenport, event
trackc:		chan of (int, array of (string, int, array of byte), string);  # interval, peers, error

# dialer/listener
canlistenc:	chan of int;
newpeerc:	chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);

# upload/download rate limiter
upc, downc:	chan of (int, chan of int);

# progress/state
stopped := 0;
ndialers:	int;  # number of active dialers
rotateips:	ref Pool[string];  # masked ip address
faulty:		list of (string, int);  # ip, time
islistening:	int;  # whether listener() is listening

peerinmsgc:	chan of (ref Peer, ref Msg, chan of list of ref (int, int, array of byte), string);
peererrc:	chan of (ref Peer, string);
wantmsgc:	chan of ref Peer;
diskwritec:	chan of ref (int, int, array of byte);
diskwrittenc:	chan of (int, int, int, string);
diskreadc:	chan of (ref Peer, int, int, array of byte, string);
mainwrites:	list of ref (int, int, array of byte);

# ticker
tickc:		chan of int;


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
Intervalmax:	con 24*3600;
Intervalneed:	con 10;  # when we need more peers during startup period
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

	# note that noone has any business with *last.e
	progressfids = progressfids.new(4, nil);
	progresslast = ref List[ref Progress];
	progresslast.next = ref List[ref Progress];
	peerfids = peerfids.new(4, nil);
	peereventlast = ref List[ref Peerevent];
	peereventlast.next = ref List[ref Peerevent];

	sys->pctl(Sys->NEWPGRP, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-dn] [-m ratio] [-r maxuprate] [-R maxdownrate] [-t maxuptotal] [-T maxdowntotal] torrentfile");
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

	if(created) {
		# all new files, we don't have pieces yet
		trackerevent = "started";
		say("no state file needed, all new files");
		state.piecehave = Bits.new(state.t.piececount);
	} else {
		# attempt to read state of pieces from .torrent.state file
		state.tx.statefd = sys->open(state.tx.statepath, Sys->ORDWR);
		if(state.tx.statefd != nil) {
			say("using .state file");
			d := readfd(state.tx.statefd, 128*1024);
			if(d == nil)
				fail(sprint("%r"));
			(state.piecehave, err) = Bits.mk(state.t.piececount, d);
			if(err != nil)
				fail(sprint("%s: invalid state", state.tx.statepath));
		} else {
			# otherwise, read through all data
			say("starting to check all pieces in files...");
			state.piecehave = Bits.new(state.t.piececount);
			bt->torrenthash(state.tx, state.piecehave);
		}
	}
	state.piecebusy = Bits.new(state.t.piececount);
	state.piececounts = array[state.t.piececount] of {* => 0};
	state.pieces = state.pieces.new(8, nil);

	peeripmap = peeripmap.new(32, nil);
	peeraddrmap = peeraddrmap.new(32, nil);
	peeridmap = peeridmap.new(32, nil);
	newpeers = ref Newpeers;

	if(isdone())
		say("already done!");

	if(state.tx.statefd == nil) {
		say(sprint("creating statepath %q", state.tx.statepath));
		state.tx.statefd = sys->create(state.tx.statepath, Sys->ORDWR, 8r666);
		if(state.tx.statefd == nil)
			warn(sprint("failed to create state file (ignoring): %r"));
		else
			writestate();
	}

	totalleft = state.t.length-big state.piecehave.have*big state.t.piecelen;
	if(state.piecehave.get(state.t.piececount-1))
		totalleft -= big (state.t.piecelen-state.t.piecelength(state.t.piececount-1));
	localpeerid = bt->genpeerid();
	localpeeridhex = hex(localpeerid);

	trackkickc = chan of int;
	trackreqc = chan of (big, big, big, int, string);
	trackc = chan of (int, array of (string, int, array of byte), string);

	canlistenc = chan of int;
	newpeerc = chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);
	tickc = chan of int;

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
	spawn ticker();
	spawn track();
	spawn limiter(upc, int maxuprate);
	spawn limiter(downc, int maxdownrate);
	spawn diskwriter(diskwritec);

	time0 = daytime->now();
	spawn trackkick(0);

	navc := chan of ref Navop;
	spawn navigator(navc);

	nav := Navigator.new(navc);
	(msgc, srv) = Styxserver.new(sys->fildes(0), nav, big Qroot);

	spawn main();
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
				s += sprint("peers %d\n", len peers);
				s += sprint("seeds %d\n", peersseeding);
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
				for(l := peers; l != nil; l = tl l)
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
					s += sprint("%q %d\n", (hd l).t0, (hd l).t1);
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
		return replyerror(m, "xxx implement");
		if(!stopped)
			stop();
	"start" =>
		return replyerror(m, "xxx implement");
		if(stopped)
			return replyerror(m, "xxx implement");
	"disconnect" =>
		(id, rem) := str->toint(hd l, 10);
		if(rem != nil)
			return replyerror(m, styxservers->Ebadarg);
		p := peeridmap.find(id);
		if(p == nil)
			return replyerror(m, "no such peer");
		peerdrop(p, 0, nil);
	"track" =>
		spawn trackkick(0);
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
	if(isdone())
		l = next(l, ref Progress.Done);
	else {
		lp := state.piecehave.all();
		while(lp != nil) {
			r: list of int;
			for(i := 0; i < 20 && lp != nil; i++) {
				r = hd lp::r;
				lp = tl lp;
			}
			l = next(l, ref Progress.Pieces (r));
		}
		for(pl := tablist(state.pieces); pl != nil; pl = tl pl) {
			p := hd pl;
			lb := p.have.all();
			if(lb != nil)
				l = next(l, ref Progress.Blocks (p.index, lb));
		}
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
		l = next(l, ref Peerevent.Bad ((hd f).t0, (hd f).t1));
	for(t := newpeers.all(); t != nil; t = tl t)
		l = next(l, ref Peerevent.Tracker ((hd t).addr));
	for(pl := peers; pl != nil; pl = tl pl) {
		p := hd pl;
		l = next(l, ref Peerevent.New (p.np.addr, p.id, p.peeridhex, p.dialed));
		if(p.isdone())
			l = next(l, ref Peerevent.Done (p.id));
		else if(p.piecehave.have != 0) {
			for(ll := peereventpieces(p.id, p.piecehave.all()); ll != nil; ll = tl ll)
				l = next(l, hd ll);
		}
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

peereventpieces(id: int, lp: list of int): list of ref Peerevent.Pieces
{
	l: list of ref Peerevent.Pieces;
	while(lp != nil) {
		r: list of int;
		for(i := 0; i < 20 && lp != nil; i++) {
			r = hd lp::r;
			lp = tl lp;
		}
		l = ref Peerevent.Pieces (id, r)::l;
	}
	return l;
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
	s += sprint(" %d", p.piecehave.have);
	s += sprint(" up %bd %d", p.up.total(), p.up.rate());
	s += sprint(" down %bd %d", p.up.total(), p.up.rate());
	s += sprint(" metaup %bd %d", p.metaup.total(), p.metaup.rate());
	s += sprint(" metadown %bd %d", p.up.total(), p.up.rate());
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
	say(sprint("have navop, tag %d", tagof oo));
	q := int oo.path&16rff;
	pick o := oo {
	Stat =>
		o.reply <-= (dir(int o.path, 0), nil);

	Walk =>
		if(o.name == "..") {
			o.reply <-= (dir(Qroot, 0), nil);
			return;
		}
		case q {
		Qroot =>
			for(i := Qfirst; i <= Qlast; i++)
				if(tab[i].t1 == o.name) {
					o.reply <-= (dir(tab[i].t0, time0), nil);
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
					o.reply <-= (dir(Qfirst+i, 0), nil);
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

dir(path, mtime: int): ref Sys->Dir
{
	q := path&16rff;
	(nil, name, perm) := tab[q];
	d := ref sys->zerodir;
	d.name = name;
	d.uid = d.gid = "torrent";
	d.qid.path = big path;
	if(perm&Sys->DMDIR)
		d.qid.qtype = Sys->QTDIR;
	else
		d.qid.qtype = Sys->QTFILE;
	d.mtime = d.atime = mtime;
	d.mode = perm;
	return d;
}

replyerror(m: ref Tmsg, s: string)
{
	srv.reply(ref Rmsg.Error(m.tag, s));
}


isdone(): int
{
	return state.piecehave.n == state.piecehave.have;
}

stop()
{
	# xxx probably more to do here.  we also want a pause() and a start()
	stopped = 1;
	putprogress(ref Progress.Stopped);

	# disconnect from all peers and don't do further tracker requests
	luckypeer = nil;
	rotateips = nil;
	for(l := peers; l != nil; l = tl l)
		peerdrop(hd l, 0, nil);
}

writestate()
{
	d := state.piecehave.d;
	if(sys->pwrite(state.tx.statefd, d, len d, big 0) != len d)
		warn(sprint("writing state: %r"));
	else
		say("state written");
}

peerdrop(peer: ref Peer, faulty: int, err: string)
{
	if(err != nil)
		warn(err);
	if(faulty)
		setfaulty(peer.np.ip);

	n := 0;
	for(i := 0; i < peer.piecehave.n && n < peer.piecehave.have; i++)
		if(peer.piecehave.get(i)) {
			state.piececounts[i]--;
			n++;
		}

	for(l := tablist(state.pieces); l != nil; l = tl l) {
		piece := hd l;
		for(i = 0; i < len piece.busy; i++) {
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

	peerdel(peer);

	# peernetreader+peernetwriter
	for(pids := peer.pids; pids != nil; pids = tl pids)
		kill(hd pids);

	spawn stopreadc(peer.readc);
	spawn stopwritec(peer.writec);
}

peerdel(p: ref Peer)
{
	r: list of ref Peer;
	for(l := peers; l != nil; l = tl l)
		if((hd l) != p)
			r = hd l::r;
	peers = r;
	if(luckypeer == p)
		luckypeer = nil;
	peeridmap.del(p.id);
	peeripmap.del(p.np.ip);
	peeraddrmap.del(p.np.addr);
	if(p.dialed)
		peersdialed--;
	if(p.isdone())
		peersseeding--;
	putevent(ref Peerevent.Gone (p.id));
}

peersactive(): int
{
	n := 0;
	for(l := peers; l != nil; l = tl l)
		if(!(hd l).localchoking() && (hd l).remoteinterested())
			n++;
	return n;

}

stopreadc(c: chan of ref (int, int, int))
{
	c <-= nil;
}

stopwritec(c: chan of ref (int, int, array of byte))
{
	c <-= nil;
}

dialpeers()
{
	say(sprint("dialpeers, %d newpeers %d peers", len newpeers.all(), len peers));

	while(!newpeers.empty() && ndialers < Dialersmax && len peers < Peersmax && peersdialed < Peersdialedmax) {
		np := newpeers.take();
		if(peeripmap.find(np.ip) != nil)
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
	if(peersdialed < Peersmax-Peersdialedmax && !islistening) {
		islistening = 1;
		spawn kicklistener();
	}
}

peersendmany(p: ref Peer, msgs: list of ref Msg)
{
	for(; msgs != nil; msgs = tl msgs)
		peersend0(p, hd msgs);
	peergive(p);
}

peersend0(p: ref Peer, msg: ref Msg)
{
	pick m := msg {
	Piece =>
		p.datamsgs = rev(m::rev(p.datamsgs));
	* =>
		p.metamsgs = rev(m::rev(p.metamsgs));
	}
}

peersend(p: ref Peer, msg: ref Msg)
{
	peersend0(p, msg);
	peergive(p);
}

account(p: ref Peer, l: list of ref Msg)
{
	for(; l != nil; l = tl l) {
		mm := hd l;
		say(sprint("sending message: %s", mm.text()));
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

blocksize(req: Req): int
{
	# first a quick check
	if(req.pieceindex < state.t.piececount-1)
		return btp->Blocksize;

	# otherwise, the full check
	if(big req.pieceindex*big state.t.piecelen + big (req.blockindex+1)*big btp->Blocksize > state.t.length)
		return int (state.t.length % big btp->Blocksize);
	return btp->Blocksize;
}

readblock(p: ref Peer)
{
	if(p.wants == nil)
		return;

	alt {
	p.readc <-= hd p.wants =>
		say("readblock: requested another block");
		p.wants = tl p.wants;
	* =>
		say("readblock: diskreader busy, did not request another block");
	}
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

request(peer: ref Peer, piece: ref Piece, reqs: list of Req)
{
	msgs: list of ref Msg;
	for(; reqs != nil; reqs = tl reqs) {
		req := hd reqs;
		busy := piece.busy[req.blockindex];
		if(busy.t0 < 0)
			piece.busy[req.blockindex].t0 = peer.id;
		else if(busy.t1 < 0)
			piece.busy[req.blockindex].t1 = peer.id;
		else
			raise "both slots busy...";

		say("request: requesting "+req.text());
		peer.reqs.add(req);
		msgs = ref Msg.Request(req.pieceindex, req.blockindex*btp->Blocksize, blocksize(req))::msgs;
	}
	if(msgs != nil)
		peersendmany(peer, rev(msgs));
}

schedule(p: ref Peer)
{
	if(!btp->needblocks(p))
		return;

	reqc := chan of ref (ref Piece, list of Req, chan of int);
	spawn btp->schedule(reqc, p);
	while((r := <-reqc) != nil) {
		(piece, reqs, donec) := *r;
		request(p, piece, reqs);
		donec <-= 0;
	}
}


choke(p: ref Peer)
{
	peersend(p, ref Msg.Choke());
	p.state |= btp->LocalChoking;
	putevent(ref Peerevent.State (p.id, Slocal|Schoking));
}

unchoke(p: ref Peer)
{
	peersend(p, ref Msg.Unchoke());
	p.state &= ~btp->LocalChoking;
	putevent(ref Peerevent.State (p.id, Slocal|Sunchoking));
	p.lastunchoke = daytime->now();
}

wantpeerpieces(p: ref Peer): ref Bits
{
	b := state.piecehave.clone();
	b.invert();
	b = Bits.and(array[] of {p.piecehave, b});

	say("pieces peer has and we are interested in: "+b.text());
	return b;
}

interesting(p: ref Peer)
{
	# xxx we should call this more often.  a peer may have a piece we don't have yet, but we may have assigned all remaining blocks to (multiple) other peers, or we may be in paranoid mode
	if(p.localinterested()) {
		if(p.reqs.isempty() && wantpeerpieces(p).isempty()) {
			say("we are no longer interested in "+p.text());
			p.state &= ~btp->LocalInterested;
			putevent(ref Peerevent.State (p.id, Slocal|Suninterested));
			peersend(p, ref Msg.Notinterested());
		}
	} else {
		if(!wantpeerpieces(p).isempty()) {
			say("we are now interested in "+p.text());
			p.state |= btp->LocalInterested;
			putevent(ref Peerevent.State (p.id, Slocal|Sinterested));
			peersend(p, ref Msg.Interested());
		}
	}
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
	now := daytime->now();
	faulty = (ip, now)::faulty;
	putevent(ref Peerevent.Bad (ip, now));
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


peerbufflush(b: ref Buf): ref (int, int, array of byte)
{
	say(sprint("buf: writing chunk to disk, pieceoff %d, len data %d", b.pieceoff, len b.data));
	say("letting peers diskwriter handle write");
	tmp := ref (b.piece, b.pieceoff, b.data);
	b.clear();
	return tmp;
}

mainbufflush(b: ref Buf)
{
	say(sprint("buf: writing chunk to disk, pieceoff %d, len data %d", b.pieceoff, len b.data));
	mainwrites = rev(ref (b.piece, b.pieceoff, b.data)::rev(mainwrites));
	say("letting mains diskwriter handle write");
	b.clear();
}

batchflushcomplete(piece: ref Piece, block: int): int
{
	b := btp->batches(piece)[block/btp->Batchsize];
	for(i := 0; i < len b.blocks; i++)
		if(!piece.have.get(b.blocks[i]))
			return 0;

	start := (block/btp->Batchsize)*btp->Batchsize*btp->Blocksize;
	end := start+btp->Batchsize*btp->Blocksize;
	for(l := peers; l != nil; l = tl l) {
		peer := hd l;
		if(peer.buf.overlaps(piece.index, start, end))
			mainbufflush(peer.buf);
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

		# find peer from the address pool with oldest unchoke
		peer: ref Peer;
		for(l := peers; l != nil; l = tl l) {
			p := hd l;
			if(maskip(p.np.ip) == ipmasked && (peer == nil || p.lastunchoke < peer.lastunchoke))
				peer = p;
		}
		if(peer != nil)
			return peer;
		else
			rotateips.pooldel(ipmasked);
	}
	return nil;
}

chokingupload(gen: int)
{
	if(gen % 3 == 2)
		return;

	# find the peer that has been unchoked longest
	oldest: ref Peer;
	nunchoked := 0;
	for(l := peers; l != nil; l = tl l) {
		p := hd l;
		if(!p.localchoking() && p.remoteinterested()) {
			nunchoked++;
			if(oldest == nil || p.lastunchoke < oldest.lastunchoke)
				oldest = p;
		}
	}

	# find all peers that we may want to unchoke randomly
	others: list of ref Peer;
	for(l = peers; l != nil; l = tl l) {
		p := hd l;
		if(p.remoteinterested() && p.localchoking() && p != oldest)
			others = p::others;
	}

	# choke oldest unchoked peer if we want to reuse its slot
	if(oldest != nil && nunchoked+len others >= Seedunchokedmax) {
		choke(oldest);
		nunchoked--;
	}

	othersa := l2a(others);
	btp->randomize(othersa);
	for(i := 0; i < len othersa && nunchoked+i < Seedunchokedmax; i++)
		unchoke(othersa[i]);
}

chokingdownload(gen: int)
{
	# new optimistic unchoke?
	if(gen % 3 == 0)
		luckypeer = nextoptimisticunchoke();

	# make sorted array of all peers, sorted by upload rate, then by interestedness
	allpeers := array[len peers] of ref (ref Peer, int);  # peer, rate
	i := 0;
	luckyindex := -1;
	for(l := peers; l != nil; l = tl l) {
		if(luckypeer != nil && hd l == luckypeer)
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
}

handleinmsg(peer: ref Peer, msg: ref Msg, needwritec: chan of list of ref (int, int, array of byte))
{
	handleinmsg0(peer, msg, needwritec);
	if(tagof msg == tagof Msg.Piece)
		needwritec <-= nil;
}

# xxx fix this code to never either block entirely or delay on disk/net i/o
handleinmsg0(peer: ref Peer, msg: ref Msg, needwritec: chan of list of ref (int, int, array of byte))
{
	peer.msgseq++;

	msize := msg.packedsize();
	pick m := msg {
	Piece =>
		# xxx count pieces we did not request as overhead (meta)?
		dsize := len m.d;
		peer.down.add(dsize, 1);
		trafficdown.add(dsize, 1);
		peer.metadown.add(msize-dsize, 0);
		trafficmetadown.add(msize-dsize, 0);
	* =>
		peer.metadown.add(msize, 1);
		trafficmetadown.add(msize, 1);
	}

	pick m := msg {
	Keepalive =>
		say("keepalive");

	Choke =>
		if(peer.remotechoking())
			return say(sprint("%s choked us twice...", peer.text())); # xxx disconnect?

		say(sprint("%s choked us...", peer.text()));
		peer.state |= btp->RemoteChoking;
		putevent(ref Peerevent.State (peer.id, Sremote|Schoking));

	Unchoke =>
		if(!peer.remotechoking())
			return say(sprint("%s unchoked us twice...", peer.text())); # xxx disconnect?

		say(sprint("%s unchoked us", peer.text()));
		peer.state &= ~btp->RemoteChoking;
		putevent(ref Peerevent.State (peer.id, Sremote|Sunchoking));

		schedule(peer);

	Interested =>
		if(peer.remoteinterested())
			return say(sprint("%s is interested again...", peer.text())); # xxx disconnect?

		say(sprint("%s is interested", peer.text()));
		peer.state |= btp->RemoteInterested;
		putevent(ref Peerevent.State (peer.id, Sremote|Sinterested));

		if(!peer.localchoking() && peersactive() >= Unchokedmax && !isdone()) {
			# xxx choke slowest peer that is not the optimistic unchoke
		}

	Notinterested =>
		if(!peer.remoteinterested())
			return say(sprint("%s is uninterested again...", peer.text())); # xxx disconnect?

		say(sprint("%s is no longer interested", peer.text()));
		peer.state &= ~btp->RemoteInterested;
		putevent(ref Peerevent.State (peer.id, Sremote|Suninterested));
		peer.wants = nil;

		# if peer was unchoked, we'll unchoke another during next round

	Have =>
		if(m.index >= state.t.piececount)
			return peerdrop(peer, 1, sprint("%s sent 'have' for invalid piece %d, disconnecting", peer.text(), m.index));

		if(peer.piecehave.get(m.index)) {
			say(sprint("%s already had piece %d", peer.text(), m.index)); # xxx disconnect?
		} else {
			state.piececounts[m.index]++;
			putevent(ref Peerevent.Piece (peer.id, m.index));
			say(sprint("remote now has piece %d", m.index));
			peer.piecehave.set(m.index);
			if(peer.isdone())
				peersseeding++;
		}

		interesting(peer);
		if(!peer.remotechoking())
			schedule(peer);

	Bitfield =>
		if(peer.msgseq != 1)
			return peerdrop(peer, 1, sprint("%s sent bitfield after first message, disconnecting", peer.text()));

		err: string;
		(peer.piecehave, err) = Bits.mk(state.t.piececount, m.d);
		if(err != nil)
			return peerdrop(peer, 1, sprint("%s sent bogus bitfield message: %s, disconnecting", peer.text(), err));

		say("remote sent bitfield, haves "+peer.piecehave.text());

		n := 0;
		for(i := 0; i < peer.piecehave.n && n < peer.piecehave.have; i++)
			if(peer.piecehave.get(i)) {
				state.piececounts[i]++;
				n++;
			}

		if(peer.isdone()) {
			putevent(ref Peerevent.Done (peer.id));
			peersseeding++;
		} else
			for(l := peereventpieces(peer.id, peer.piecehave.all()); l != nil; l = tl l)
				putevent(hd l);

		interesting(peer);

	Piece =>
		say(sprint("%s sent data for piece=%d begin=%d length=%d", peer.text(), m.index, m.begin, len m.d));

		req := Req.new(m.index, m.begin/btp->Blocksize);
		if(blocksize(req) != len m.d)
			return peerdrop(peer, 1, sprint("%s sent bad size for block %s, disconnecting", peer.text(), req.text()));

		piece := state.pieces.find(m.index);
		if(piece == nil)
			return peerdrop(peer, 1, sprint("got data for inactive piece %d", m.index));

		if(!peer.reqs.take(req)) {
			exp := "nothing";
			if(!peer.reqs.isempty())
				exp = peer.reqs.peek().text();
			return peerdrop(peer, 1, sprint("%s sent block %s, expected %s, disconnecting", peer.text(), req.text(), exp));
		}

		blockindex := m.begin/btp->Blocksize;

		if(piece.have.get(blockindex))
			return say("already have this block, skipping...");

		# possibly send cancel to other peer
		busy := piece.busy[blockindex];
		busypeer: ref Peer;
		if(busy.t0 >= 0 && busy.t0 != peer.id) {
			busypeer = peeridmap.find(busy.t0);
			piece.busy[blockindex].t0 = -1;
		} else if(busy.t1 >= 0 && busy.t1 != peer.id) {
			busypeer = peeridmap.find(busy.t1);
			piece.busy[blockindex].t1 = -1;
		}
		if(busypeer != nil) {
			peer.reqs.cancel(Req.new(m.index, m.begin/btp->Blocksize));
			peersend(peer, ref Msg.Cancel(m.index, m.begin, len m.d));
		}

		needwrites: list of ref (int, int, array of byte);

		# add block to buffer.  if new block does not fit in, flush the old data.
		if(!peer.buf.tryadd(piece, m.begin, m.d)) {
			needwrite := peerbufflush(peer.buf);
			if(needwrite != nil)
				needwrites = needwrite::needwrites;
			if(!peer.buf.tryadd(piece, m.begin, m.d))
				raise "tryadd failed...";
		}

		# flush now, rather then delaying it
		if(peer.buf.isfull()) {
			needwrite := peerbufflush(peer.buf);
			if(needwrite != nil)
				needwrites = needwrite::needwrites;
		} else  # if this completes the batch, flush it for all peers that have blocks belonging to it
			batchflushcomplete(piece, blockindex);

		if(needwrites != nil) {
			say(sprint("sending %d needwrites to peernetreader", len needwrites));
			needwritec <-= rev(needwrites);
		}

		piece.have.set(blockindex);
		piece.done[blockindex] = peer.id;
		putprogress(ref Progress.Block (piece.index, blockindex, piece.have.have, piece.have.n));

		if(piece.isdone()) {
			# flush all bufs about this piece, also from other peers, to disk.  to make hash-checking read right data.
			for(l := peers; l != nil; l = tl l) {
				p := hd l;
				if(p.buf.piece == m.index) {
					say("flushing buf of other peer before hash check");
					mainbufflush(p.buf);
				}
			}
		}
		schedule(peer);  # xxx will this work?  piece.have != piece.written may cause trouble

	Request =>
		b := Block.new(m.index, m.begin, m.length);
		say(sprint("%s sent request for %s", peer.text(), b.text()));

		if(m.length > Blocksizemax)
			return peerdrop(peer, 0, "requested block too large, disconnecting");

		if(peer.piecehave.get(m.index))
			return peerdrop(peer, 1, "peer requested piece it already claimed to have");

		if(blockhave(peer.wants, b))
			return say("peer already wanted block, skipping");

		if(len peer.wants >= btp->Blockqueuemax)
			return peerdrop(peer, 0, sprint("peer scheduled one too many blocks, already has %d scheduled, disconnecting", len peer.wants));

		peer.wants = rev(b::rev(peer.wants));
		if(!peer.localchoking() && peer.remoteinterested() && len peer.wants > 0)
			readblock(peer);

	Cancel =>
		b := Block.new(m.index, m.begin, m.length);
		say(sprint("%s sent cancel for %s", peer.text(), b.text()));
		nwants := blockdel(peer.wants, b);
		if(len nwants == len peer.wants)
			return say("peer did not want block before, skipping");
		peer.wants = nwants;
	}
}


ticker()
{
	for(;;) {
		sys->sleep(10*1000);
		tickc <-= 0;
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

	<-tickc =>
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
		if(isdone())
			chokingupload(gen);
		else
			chokingdownload(gen);
		gen++;

	<-trackkickc =>
		if(!stopped) {
			trackreqc <-= (trafficup.total(), trafficdown.total(), totalleft, listenport, trackerevent);
			trackerevent = nil;
		}

	(interval, nps, trackerr) := <-trackc =>
		# schedule next call to tracker
		if(interval < Intervalmin)
			interval = Intervalmin;
		if(interval > Intervalmax)
			interval = Intervalmax;
		now := daytime->now();
		if(now < time0+Intervalstartupperiod && !isdone() && len peers+len newpeers.all() < Peersmax && interval > Intervalneed)
			interval = Intervalneed;

		say(sprint("next call to tracker will be in %d seconds", interval));
		spawn trackkick(interval);
		nexttrack = now+interval;
		trackereventlast = ref Progress.Tracker (interval, interval, len nps, trackerr);
		putprogress(trackereventlast);

		if(trackerr != nil)
			return warn(sprint("tracker error: %s", trackerr));

		say("main, new peers");
		for(i := 0; i < len nps; i++) {
			(ip, port, peerid) := nps[i];
			if(hex(peerid) == localpeeridhex)
				continue;  # skip self
			np := Newpeer(sprint("%s!%d", ip, port), ip, peerid);
			say("new: "+np.text());
			newpeers.del(np);
			if(peeraddrmap.find(np.addr) == nil) {
				newpeers.add(np);
				putevent(ref Peerevent.Tracker (np.addr));
			} else
				say("already connected to "+np.text());
		}
		dialpeers();

	(dialed, np, peerfd, extensions, peerid, err) := <-newpeerc =>
		if(!dialed)
			islistening = 0;

		if(err != nil)
			return warn(sprint("%s: %s", np.text(), err));
		if(hex(peerid) == localpeeridhex)
			return say("connected to self, dropping connection...");
		if(peeripmap.find(np.ip) != nil)
			return say("new connection from known ip address, dropping new connection...");
		if(isfaulty(np.ip))
			return say(sprint("connected to faulty ip %s, dropping connection...", np.ip));

		peer := Peer.new(np, peerfd, extensions, peerid, dialed, state.t.piececount);
		pidc := chan of int;
		spawn peernetreader(pidc, peer);
		spawn peernetwriter(pidc, peer);
		spawn diskwriter(peer.writec);
		spawn diskreader(peer);
		peer.pids = <-pidc::<-pidc::peer.pids;

		peers = peer::peers;
		peeridmap.add(peer.id, peer);
		peeripmap.add(peer.np.ip, peer);
		peeraddrmap.add(peer.np.addr, peer);
		if(peer.dialed)
			peersdialed++;
		if(peer.isdone())
			peersseeding++;

		say("new peer "+peer.fulltext());
		putevent(ref Peerevent.New (peer.np.addr, peer.id, peer.peeridhex, peer.dialed));

		rotateips.pooladdunique(maskip(np.ip));

		if(state.piecehave.have == 0) {
			peersend(peer, ref Msg.Keepalive());
		} else {
			say("sending bitfield to peer: "+state.piecehave.text());
			peersend(peer, ref Msg.Bitfield(state.piecehave.bytes()));
		}

		if(peersactive() < Unchokedmax) {
			say("unchoking rare new peer: "+peer.text());
			unchoke(peer);
		}

		if(dialed) {
			ndialers--;
			dialpeers();
		} else
			awaitpeer();

	(peer, msg, needwritec, err) := <-peerinmsgc =>
		if(err != nil)
			return peerdrop(peer, 0, "read error from peer: "+err);
		if(msg == nil)
			return peerdrop(peer, 0, "eof from peer "+peer.text());
		handleinmsg(peer, msg, needwritec);

	(peer, err) := <-peererrc =>
		peerdrop(peer, 0, "peer error: "+err);

	(pieceindex, begin, length, err) := <-diskwrittenc =>
		if(err != nil)
			raise sprint("error writing piece %d, begin %d, length %d: %s", pieceindex, begin, length, err);

		piece := state.pieces.find(pieceindex);
		if(piece == nil)
			raise sprint("data written for inactive piece %d", pieceindex);

		# mark written blocks as such
		first := begin/btp->Blocksize; 
		n := (length+btp->Blocksize-1)/btp->Blocksize;
		for(i := 0; i < n; i++)
			piece.written.set(first+i);

		if(!piece.written.isfull())
			return;

		say("last parts of piece have been written, verifying...");

		wanthash := hex(state.t.hashes[piece.index]);
		(buf, herr) := state.tx.pieceread(piece.index);
		if(herr != nil)
			fail("verifying hash: "+herr); # xxx should stop() instead
		havehash := hex(sha1(buf));
		if(wanthash != havehash) {
			# xxx blame peers
			say(sprint("%s did not check out, want %s, have %s, disconnecting", piece.text(), wanthash, havehash));
			piece.have.clearall();
			if(dflag) {
				for(i = 0; i < len piece.busy; i++)
					if(piece.busy[i].t0 >= 0 || piece.busy[i].t1 >= 0)
						raise sprint("piece %d should be complete, but block %d is busy", piece.index, i);
			}
			
			# xxx what do to with other peers?
			return;
		}

		totalleft -= big state.t.piecelength(piece.index);
		state.piecehave.set(piece.index);
		state.pieces.del(piece.index);
		putprogress(ref Progress.Piece (piece.index, state.piecehave.have, state.piecehave.n));
		for(fl := filesdone(piece.index); fl != nil; fl = tl fl) {
			f := hd fl;
			putprogress(ref Progress.Filedone (f.index, f.path, f.f.path));
		}

		# this could have been the last piece this peer had, making us no longer interested
		for(l := peers; l != nil; l = tl l)
			interesting(hd l);

		writestate();
		say("piece now done: "+piece.text());
		say(sprint("pieces: have %s, busy %s", state.piecehave.text(), state.piecebusy.text()));

		for(l = peers; l != nil; l = tl l)
			peersend(hd l, ref Msg.Have(piece.index));

		if(isdone()) {
			trackerevent = "completed";
			spawn trackkick(0);
			npeers: list of ref Peer;
			for(l = peers; l != nil; l = tl l) {
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
			peers = rev(npeers);
			sys->print("DONE!\n");
			putprogress(ref Progress.Done);
		}

	curwritec <-= curmainwrite =>
		say("queued mainwrite to diskwriter");
		mainwrites = tl mainwrites;

	peer := <-wantmsgc =>
		say(sprint("%s: wants message", peer.text()));
		peer.getmsg = 1;

		if(!stopped && isdone()) {
			ratio := ratio();
			if(ratio >= 1.1 && ratio >= maxratio) {
				say(sprint("stopping due to max ratio achieved (%.2f)", ratio));
				stop();
				return;
			}
		}

		peergive(peer);
		if(!peer.localchoking() && peer.remoteinterested() && peer.getmsg && len peer.wants > 0)
			readblock(peer);

	(peer, pieceindex, begin, buf, err) := <-diskreadc =>
		if(err != nil)
			raise sprint("error writing piece %d, begin %d, length %d: %s", pieceindex, begin, len buf, err);

		say("have block from disk");
		peersend(peer, ref Msg.Piece(pieceindex, begin, buf));
		# xxx calculate if we need to request another block already
	}
}

main()
{
	gen = 0;
	bogusc = chan of ref (int, int, array of byte);
	for(;;)
		main0();
}

trackkick(n: int)
{
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


kicklistener()
{
	canlistenc <-= 1;
}

listener(aconn: Sys->Connection)
{
	<-canlistenc;
	for(;;) {
		(ok, conn) := sys->listen(aconn);
		if(ok != 0) {
			warn(sprint("listen: %r"));
			continue;
		}

		rembuf := readfile(conn.dir+"/remote", 128);
		if(rembuf == nil) {
			warn(sprint("%r"));
			continue;
		}
		remaddr := str->splitstrl(string rembuf, "\n").t0;

		f := conn.dir+"/data";
		fd := sys->open(f, Sys->ORDWR);
		if(fd == nil) {
			warn(sprint("new connection, open %s: %r", f));
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
	d[i:] = array[8] of {* => byte '\0'};
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


# copied from bittorrent.b Msg.read
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


peernetreader(pidc: chan of int, peer: ref Peer)
{
	pidc <-= pid();

	needwritec := chan of list of ref (int, int, array of byte);
	for(;;) {
		(m, err) := msgread(peer.fd);
		peerinmsgc <-= (peer, m, needwritec, err);
		if(err != nil || m == nil)
			break;

		if(tagof m == tagof Msg.Piece) {
			needwrites := <-needwritec;
			say(sprint("peernetreader: have %d needwrites", len needwrites));
			if(needwrites != nil) {
				for(l := needwrites; l != nil; l = tl l)
					peer.writec <-= hd l;  # will block if disk is slow, slowing down peer as well
				<-needwritec;
			}
		}
	}
}

peernetwriter(pidc: chan of int, peer: ref Peer)
{
	pidc <-= pid();

	for(;;) {
		wantmsgc <-= peer;
		ml := <-peer.getmsgc;
		if(ml == nil) {
			say("peernetwriter: stopping...");
			return;
		}

		dlen := 0;
		for(l := ml; l != nil; l = tl l)
			dlen += (hd l).packedsize();
		d := array[dlen] of byte;
		o := 0;
		say(sprint("peernetwriter: dlen %d", dlen));
		for(; ml != nil; ml = tl ml) {
			m := hd ml;
			size := m.packedsize();
			say(sprint("peernetwriter: len d %d, o %d, size %d", len d, o, size));
			m.packbuf(d[o:o+size]);
			o += size;
		}
		n := netwrite(peer.fd, d, len d);
		if(n != len d) {
			peererrc <-= (peer, sprint("write: %r"));
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

diskreader(peer: ref Peer)
{
	for(;;) {
		req := <-peer.readc;
		if(req == nil) {
			say("diskreader: stopping...");
			break;
		}
		(piece, begin, length) := *req;
		off := big piece*big state.t.piecelen + big begin;
		err := state.tx.preadx(buf := array[length] of byte, len buf, off);
		diskreadc <-= (peer, piece, begin, buf, err);
	}
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


filedone(f: ref Filex): int
{
	for(i := f.pfirst; i <= f.plast; i++)
		if(!state.piecehave.get(i))
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

etastr(n: int): string
{
	Min: con 60;
	Hour: con 60*Min;
	Day: con 24*Hour;
	Year: con 366*Day;
	s := "";
	if(n < 0)
		s = "∞";
	else if(n < Hour)
		s = sprint("%3dm %3ds", n/Min, n%Min);
	else if(n < Day)
		s = sprint("%3dh %3dm", n/Hour, n%Hour/Min);
	else if(n < Year)
		s = sprint("%3dd %3dh", n/Day, n%Day/Hour);
	else
		s = " >= year";
	return s;
}

eta(): int
{
	r := trafficdown.rate();
	if(r <= 0)
		return -1;
	return int (totalleft/big r);
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

say(s: string)
{
	if(dflag)
		warn(s);
}

fail(s: string)
{
	warn(s);
	killgrp(pid());
	raise "fail:"+s;
}
