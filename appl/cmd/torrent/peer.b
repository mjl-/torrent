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
include "rand.m";
	rand: Rand;
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
	pid, kill, killgrp, hex, join, max, min, rev, l2a, g32i, readfile, readfd, inssort, sizefmt, sizeparse: import util;
include "bitarray.m";
	bitarray: Bitarray;
	Bits, Bititer: import bitarray;
include "bittorrent.m";
	bt: Bittorrent;
	Bee, Msg, File, Torrent, Filex, Torrentx: import bt;
include "../../lib/bittorrentpeer.m";
	btp: Bittorrentpeer;
	State, Pool, Traffic, Piece, Peer, Newpeer, Newpeers, Bigtab, Queue, Chunk, Chunkwrite, LReq, RReq, RReqs, List, Progress, Peerevent, Eventfid: import btp;
	Slocal, Sremote, Schoking, Sunchoking, Sinterested, Suninterested: import Bittorrentpeer;

Torrentpeer: module {
	init:	fn(nil: ref Draw->Context, nil: list of string);
};

Int: adt {
	i:	int;
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
peerinmsgc:	chan of (ref Peer, ref Msg, chan of int, chan of ref Queue[ref Chunkwrite], string);
peererrc:	chan of (ref Peer, string);
wantmsgc:	chan of ref Peer;
diskwritec:	chan of ref Chunkwrite;
diskwrittenc:	chan of (int, int, int, string);
diskreadc:	chan of (ref Peer, int, int, array of byte, string);
mainwrites:	ref Queue[ref Chunkwrite];

# round ticker
roundc:		chan of int;


Dialersmax:	con 10;  # max number of dialer procs
Dialtimeout:	con 30;  # timeout for connecting to peer
Peersmax:	con 80;
Peersdialedmax:	con 40;
Blockqueuemax:	con 100;  # max number of Requests a peer can queue at our side without being considered bad
Blockqueuesize:	con 50;	# number of pending blocks to request to peer
Batchsize:	con 8;
Netiounit:	con 1500-20-20;  # typical network data io unit, ethernet-ip-tcp

Listenhost:	con "*";
Listenport:	con 6881;
Listenportrange:	con 100;

Intervalmin:	con 30;
Intervalneed:	con 45;
Intervalmax:	con 24*3600;
Intervaldefault:	con 1800;
Intervalstartupperiod:	con 120;

Blocksize:	con 16*1024;
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
	rand = load Rand Rand->PATH;
	rand->init(sys->millisec()^sys->pctl(0, nil));
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
	mainwrites = mainwrites.new();

	# note that no one has any business with *last.e
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

	if(created) {
		# all new files, we don't have pieces yet
		trackerevent = "started";
		say("no state file needed, all new files");
		state.have = Bits.new(state.t.piececount);
	} else {
		# attempt to read state of pieces from .torrent.state file
		state.tx.statefd = sys->open(state.tx.statepath, Sys->ORDWR);
		if(state.tx.statefd != nil) {
			say("using .state file");
			d := readfd(state.tx.statefd, 128*1024);
			if(d == nil)
				fail(sprint("%r"));
			(state.have, err) = Bits.mk(state.t.piececount, d);
			if(err != nil)
				fail(sprint("%s: invalid state", state.tx.statepath));
		} else {
			# otherwise, read through all data
			say("starting to check all pieces in files...");
			state.have = Bits.new(state.t.piececount);
			bt->torrenthash(state.tx, state.have);
		}
	}
	state.active = state.active.new(8, nil);
	state.orphans = state.active.new(8, nil);

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

	totalhave := big state.have.have*big state.t.piecelen;
	if(state.have.get(state.t.piececount-1)) {
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

	peerinmsgc = chan of (ref Peer, ref Msg, chan of int, chan of ref Queue[ref Chunkwrite], string);
	peererrc = chan of (ref Peer, string);
	wantmsgc = chan of ref Peer;
	diskwritec = chan[4] of ref Chunkwrite;
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
		it := state.have.iter();
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


done(): int
{
	return state.have.n == state.have.have;
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
	d := state.have.d;
	if(sys->pwrite(state.tx.statefd, d, len d, big 0) != len d)
		warn(1, sprint("writing state: %r"));
	else
		say("state written");
}

peerdrop(p: ref Peer, faulty: int, err: string)
{
	if(err != nil) {
		err = p.text()+": "+err;
		warn(1, err);
	}
	if(faulty)
		setfaulty(p.np.ip, err);

	if(p.dialed)
		dialpeers();
	else
		awaitpeer();

	if(!p.chunk.isempty())
		mainwrites.add(p.chunk.flush());
	peerbox.del(p);
	putevent(ref Peerevent.Gone (p.id));

	kill(p.netwritepid);
	spawn stopreadc(p.readc);
	spawn stopwritec(p.writec);

	undolreqs(p);
}

stopreadc(c: chan of ref RReq)
{
	c <-= nil;
}

stopwritec(c: chan of ref Chunkwrite)
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
		islistening = 1;
		canlistenc <-= 1;
	}
}

peersend0(p: ref Peer, mm: ref Msg)
{
	pick m := mm {
	Piece =>	p.datamsgs.add(m);
	* =>		p.metamsgs.add(m);
	}
}

peersend(p: ref Peer, m: ref Msg)
{
	peersend0(p, m);
	peergive(p);
}

account(p: ref Peer, q: ref Queue[ref Msg])
{
	for(f := q.first; f != nil; f = f.next) {
		msize := f.e.packedsize();
		pick m := f.e {
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

	if(!p.metamsgs.empty()) {
		p.getmsgc <-= p.metamsgs;
		account(p, p.metamsgs);
		p.metamsgs = ref Queue[ref Msg];
		p.getmsg = 0;
	} else if(!p.datamsgs.empty() && !p.localchoking() && p.remoteinterested()) {
		m := p.datamsgs.takeq();
		account(p, m);
		p.getmsgc <-= m;
		p.getmsg = 0;
	}
}

blocksize(req: ref LReq): int
{
	# first a quick check
	if(req.piece < state.t.piececount-1)
		return Blocksize;

	# otherwise, the full check
	if(big req.piece*big state.t.piecelen + big (req.block+1)*big Blocksize > state.t.length)
		return int (state.t.length % big Blocksize);
	return Blocksize;
}

readrreq(p: ref Peer)
{
	alt {
	p.readc <-= p.rreqs.first.e =>
		if(dflag) say("readrreq: another rreq");
		p.rreqs.dropfirst();
	* =>
		if(dflag) say("readrreq: diskreader busy, did get another not request");
	}
}


peerlwant(li: list of int, on: int)
{
	if(li == nil)
		return;
	for(l := peerbox.peers; l != nil; l = tl l) {
		pp := hd l;
		for(ll := li; ll != nil; ll = tl ll)
			if(on)
				pp.lwantinact.set(hd ll);
			else
				pp.lwantinact.clear(hd ll);
		interesting(pp);
	}
}

undolreqs(p: ref Peer)
{
	li: list of int;
	for(l := p.lreqs.all(); l != nil; l = tl l) {
		lr := hd l;
		if(hasint(li, lr.piece))
			continue;
		li = lr.piece::li;

		pc := state.active.find(lr.piece);
		if(pc == nil)
			raise "peer had lreq for inactive piece?";
		state.active.del(lr.piece);
		if(!pc.have.isempty()) {
			pc.requested = pc.have.clone();
			state.orphans.add(pc.index, pc);
		}
	}
	peerlwant(li, 1);
	p.lreqs.clear();
}


blocksleft(): int
{
	blocksperpiece := state.t.piecelen/Blocksize;
	n := (state.have.n-state.have.have)*blocksperpiece;
	if(state.have.get(state.t.piececount-1))
		n += (state.t.piecelen-state.t.piecelength(state.t.piececount-1))/Blocksize;
	return n;
}

# we like sending requests in batches so we don't need to incur tcp/ip overhead (and perhaps padding) for each request
needlreq(p: ref Peer): int
{
	return p.lreqs.nelems == 0 || (p.lreqs.nelems+Batchsize < Blockqueuesize && p.lreqs.nelems < blocksleft()/max(10, len peerbox.peers));
}

schedulepiece(p: ref Peer, pc: ref Piece)
{
	n := Blockqueuesize-p.lreqs.nelems;
	it := pc.requested.clone().inviter();
	while(n-- > 0 && (i := it.next()) >= 0) {
		pc.requested.set(i);
		req := ref LReq (pc.index, i);
		if(dflag) say(sprint("peer %d: %s", p.id, req.text()));
		p.lreqs.add(req.key(), req);
		m := ref Msg.Request(req.piece, req.block*Blocksize, blocksize(req));
		p.metamsgs.add(m);
		p.lastpiece = pc.index;
		pc.peer = p.id;
	}
}

pieceprogressge(a, b: ref Piece): int
{
	return a.have.have < b.have.have;
}

scheduleorphans(p: ref Peer)
{
	orphs := l2a(tablist(state.orphans));
	inssort(orphs, pieceprogressge);
	li: list of int;
	for(i := 0; needlreq(p) && i < len orphs; i++) {
		pc := orphs[i];
		if(!p.rhave.get(pc.index))
			continue;
		state.orphans.del(pc.index);
		state.active.add(pc.index, pc);
		li = pc.index::li;
		schedulepiece(p, pc);
	}
	peerlwant(li, 0);
}

schedule(p: ref Peer)
{
	if(!p.localinterested() || p.remotechoking())
		return;
if(dflag) say("schedule for "+p.text());
	schedule0(p);
	peergive(p);
}


schedule0(p: ref Peer)
{
	# attempt to work on last piece this peer was working on
	if(!needlreq(p))
		return;
	if(p.lastpiece >= 0) {
		if(dflag) say("schedule: trying previous piece peer was working on");
		pc := state.active.find(p.lastpiece);
		if(pc != nil && pc.peer == p.id)
			schedulepiece(p, pc);
		if(!needlreq(p))
			return;
	}

	# orphan pieces
	if(p.good) {
		if(dflag) say("schedule: orphans for good peer");
		scheduleorphans(p);
		if(!needlreq(p))
			return;
	}

	# random new piece
	if(dflag) say("schedule: trying inactive random pieces");
	li: list of int;
	while(needlreq(p) && p.lwantinact.have > 0) {
		i := p.lwantinact.rand();
		if(dflag) say(sprint("using new random piece %d", i));
		length := state.t.piecelength(i);
		nblocks := (length+Blocksize-1)/Blocksize;
		pc := ref Piece (i, Bits.new(nblocks), Bits.new(nblocks), Bits.new(nblocks), length, 0, nil, nil, 0);
		state.active.add(i, pc);
		p.lwantinact.clear(i);
		li = i::li;
		schedulepiece(p, pc);
	}
	peerlwant(li, 0);
	if(!needlreq(p))
		return;

	# orphan pieces for non-good peers
	if(!p.good) {
		if(dflag) say("schedule: orphans for non-good peer");
		scheduleorphans(p);
	}
}


choke(p: ref Peer)
{
	peersend(p, ref Msg.Choke);
	p.state |= btp->LocalChoking;
	putevent(ref Peerevent.State (p.id, Slocal|Schoking));
	p.rreqs.clear();
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
	if(p.lreqs.nelems != 0)
		raise "we became interested but already had blocks requested?";

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
	if(p.localinterested() && p.lwantinact.have == 0 && p.lreqs.nelems == 0)
		uninterested(p);
	else if(!p.localinterested() && p.lwantinact.have > 0)
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
	randomize(othersa);
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

peermsg(p: ref Peer, mm: ref Msg, needwritec: chan of ref Queue[ref Chunkwrite], sent: ref Int)
{
if(dflag) say(sprint("<- %s: %s", p.np.ip, mm.text()));
	p.msgseq++;
	msize := mm.packedsize();
	pick m := mm {
	Piece =>
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

		p.state |= btp->RemoteChoking;
		putevent(ref Peerevent.State (p.id, Sremote|Schoking));

		if(!p.chunk.isempty()) {
			needwrites := Queue[ref Chunkwrite].new();
			needwrites.add(p.chunk.flush());
			needwritec <-= needwrites;
			sent.i = 1;
		}
		undolreqs(p);

	Unchoke =>
		if(!p.remotechoking())
			return peerdrop(p, 1, "double unchoke");

		p.state &= ~btp->RemoteChoking;
		putevent(ref Peerevent.State (p.id, Sremote|Sunchoking));
		schedule(p);

	Interested =>
		if(0 && p.remoteinterested())
			return peerdrop(p, 1, "double interested");

		p.state |= btp->RemoteInterested;
		putevent(ref Peerevent.State (p.id, Sremote|Sinterested));

		if(!p.localchoking() && peerbox.nactive() >= Unchokedmax && !done()) {
			# xxx choke slowest peer that is not the optimistic unchoke
		}

	Notinterested =>
		if(!p.remoteinterested())
			return peerdrop(p, 1, "double uninterested");

		p.state &= ~btp->RemoteInterested;
		putevent(ref Peerevent.State (p.id, Sremote|Suninterested));
		if(!p.localchoking() && p.rreqs.length > 0)
			choke(p);
		# if peer was unchoked, we'll unchoke another during next round

	Have =>
		if(m.index >= state.t.piececount)
			return peerdrop(p, 1, sprint("sent 'have' for invalid piece %d", m.index));

		# xxx it seems some clients (rtorrent) send "have" for pieces that were in the bitfield message.
		if(0 && p.rhave.get(m.index))
			return peerdrop(p, 1, sprint("already had piece %d (has %d/%d)", m.index, p.rhave.have, p.rhave.n));

		putevent(ref Peerevent.Piece (p.id, m.index));
		p.rhave.set(m.index);
		if(!state.have.get(m.index) && state.active.find(m.index) == nil)
			p.lwantinact.set(m.index);
		if(p.isdone())
			peerbox.nseeding++;

		interesting(p);
		schedule(p);

	Bitfield =>
		if(p.msgseq != 1)
			return peerdrop(p, 1, "sent bitfield after first message");

		(rhave, err) := Bits.mk(state.t.piececount, m.d);
		if(err != nil)
			return peerdrop(p, 1, sprint("sent bogus bitfield message: %s", err));
		p.rhave = rhave;
		p.lwantinact = Bits.nand(p.rhave, state.have);
		for(l := tablist(state.active); l != nil; l = tl l)
			p.lwantinact.clear((hd l).index);

		if(p.isdone()) {
			putevent(ref Peerevent.Done (p.id));
			peerbox.nseeding++;
		} else
			for(ll := peereventpieces(p.id, p.rhave); ll != nil; ll = tl ll)
				putevent(hd ll);

		interesting(p);

	Piece =>
		block := m.begin/Blocksize;
		lr := ref LReq (m.index, block);
		if(blocksize(lr) != len m.d)
			return peerdrop(p, 1, sprint("sent bad size %d for block %s", len m.d, lr.text()));

		pc := state.active.find(m.index);
		if(pc == nil)
			return peerdrop(p, 1, sprint("sent block for non-active piece %d", m.index));
		if(p.lreqs.del(lr.key()) == nil)
			return peerdrop(p, 1, "sent unrequested block "+lr.text());

		if(pc.have.get(block))
			raise "duplicate block";

		if(m.begin == pc.hashoff) {
			pc.hash = kr->sha1(m.d, len m.d, nil, pc.hash);
			pc.hashoff += len m.d;
		}

		pc.have.set(block);
		if(!hasint(pc.peers, p.id))
			pc.peers = p.id::pc.peers;
		putprogress(ref Progress.Block (pc.index, block, pc.have.have, pc.have.n));

		needwrites := Queue[ref Chunkwrite].new();
		if(!p.chunk.tryadd(pc, m.begin, m.d)) {
			needwrites.add(p.chunk.flush());
			if(!p.chunk.tryadd(pc, m.begin, m.d))
				raise "tryadd";
		}
		if(p.chunk.isfull() || (pc.have.isfull() && !p.chunk.isempty()))
			needwrites.add(p.chunk.flush());
		needwritec <-= needwrites;
		sent.i = 1;
		schedule(p);

	Request =>
		rr := ref RReq (m.index, m.begin, m.length);
		if(m.begin != 0 && (m.begin & ((32*1024)-1)))
			return peerdrop(p, 0, "bad begin, "+rr.text());
		if(m.length != 16*1024 && m.length != 32*1024 && (big m.index*big state.t.piecelen+big m.begin+big m.length) != state.t.length)
			return peerdrop(p, 0, "bad length, "+rr.text());
		if(p.rhave.get(m.index))
			return peerdrop(p, 1, "requested piece it already claimed to have");
		if(p.rreqs.length >= Blockqueuemax)
			return peerdrop(p, 0, sprint("scheduled one too many block, already has %d scheduled", p.rreqs.length));

		p.rreqs.add(rr);
		if(!p.localchoking() && p.remoteinterested())
			readrreq(p);

	Cancel =>
		rr := ref RReq (m.index, m.begin, m.length);
		if(!p.rreqs.del(rr))
			return peerdrop(p, 1, "peer cancelled unrequested block "+rr.text());

	* =>
		raise "missing case";
	}
}


roundgen: int;
bogusc: chan of ref Chunkwrite;
main0()
{
	# the alt statement doesn't do conditional sending/receiving on a channel.
	# so, before we start the alt, we evaluate the condition.
	# if true, we use the real channel we want to send on.
	# if false, we use a bogus channel without receiver, so the case is never taken.
	curwritec := bogusc;
	curmainwrite: ref Chunkwrite;
	if(!mainwrites.empty()) {
		curmainwrite = mainwrites.first.e;
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

		if(done())
			chokingupload(roundgen);
		else
			chokingdownload(roundgen);
		roundgen++;

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
			spawn peernetreader(p);
			spawn peernetwriter(pidc, p);
			spawn diskwriter(p.writec);
			spawn diskreader(p);
			p.netwritepid = <-pidc;

			peerbox.add(p);
			say(sprint("new peer %s, peerid %s", p.text(), btp->peeridfmt(p.peerid)));
			putevent(ref Peerevent.New (p.np.addr, p.id, p.peeridhex, p.dialed));

			rotateips.pooladdunique(maskip(np.ip));

			if(state.have.have == 0)
				peersend(p, ref Msg.Keepalive);
			else
				peersend(p, ref Msg.Bitfield(state.have.bytes()));

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

	(p, msg, stopc, needwritec, err) := <-peerinmsgc =>
		if((p = peerbox.findid(p.id)) == nil || stopped || msg == nil || err != nil) {
			stopc <-= 1;
			if(p == nil)
				{}
			else if(err != nil)
				peerdrop(p, 0, "read error from peer: "+err);
			else if(msg == nil)
				peerdrop(p, 0, "eof from peer "+p.text());
			return;
		}
		stopc <-= 0;
		sent := ref Int (0);
		peermsg(p, msg, needwritec, sent);
		if(!sent.i)
			needwritec <-= nil;

	(p, err) := <-peererrc =>
		peerdrop(p, 0, err);

	(piece, begin, length, err) := <-diskwrittenc =>
		if(err != nil)
			return warnstop(sprint("error writing piece %d, begin %d, length %d: %s", piece, begin, length, err));

		pc := state.active.find(piece);
		if(pc == nil)
			pc = state.orphans.find(piece);
		if(pc == nil)
			raise sprint("data written for non-active piece %d", piece);

		# mark written blocks as such
		first := begin/Blocksize; 
		end := (begin+length+Blocksize-1)/Blocksize;
		while(first < end) {
			if(!pc.have.get(first))
				raise "written block we don't have";
			pc.written.set(first++);
		}

		if(!pc.written.isfull())
			return;
		if(state.have.get(pc.index))
			raise "already have piece";

		say(sprint("last parts of piece %d have been written, verifying...", piece));

		state.active.del(pc.index);
		state.orphans.del(pc.index);

		need := state.t.piecelength(pc.index)-pc.hashoff;
		if(need > 0) {
			# xxx should do this in separate prog?
			buf := array[need] of byte;
			herr := state.tx.preadx(buf, len buf, big pc.index*big state.t.piecelen+big pc.hashoff);
			if(herr != nil)
				return warnstop("verifying hash: "+herr);  # xxx have to correct more state, e.g. marking piece inactive again
			pc.hash = kr->sha1(buf, len buf, nil, pc.hash);
		}
		wanthash := hex(state.t.hashes[pc.index]);
		kr->sha1(nil, 0, digest := array[kr->SHA1dlen] of byte, pc.hash);
		havehash := hex(digest);
		if(wanthash != havehash) {
			putprogress(ref Progress.Hashfail (pc.index));
			say(sprint("piece %d did not check out, want %s, have %s", pc.index, wanthash, havehash));

			if(stopped)
				return;
			if(len pc.peers == 1) {
				pp := peerbox.findid(hd pc.peers);
				if(pp != nil)
					peerdrop(pp, 1, sprint("gave us faulty piece %d", pc.index));
			} else {
				for(l := pc.peers; l != nil; l = tl l) {
					pp := peerbox.findid(hd l);
					if(pp != nil && !pp.good)
						peerdrop(pp, 1, sprint("might have given us faulty piece %d", pc.index));
				}
			}
			peerlwant(pc.index::nil, 1);
			return;
		}
		state.have.set(pc.index);
		totalleft -= big state.t.piecelength(pc.index);
		putprogress(ref Progress.Piece (pc.index, state.have.have, state.have.n));
		for(fl := filesdone(pc.index); fl != nil; fl = tl fl) {
			f := hd fl;
			putprogress(ref Progress.Filedone (f.index, f.path, f.f.path));
		}

		writestate();
		say(sprint("piece %d now done", pc.index));

		if(done()) {
			warn(0, "DONE!");
			putprogress(ref Progress.Done);
		}

		if(stopped)
			return;

		if(len pc.peers == 1) {
			pp := peerbox.findid(hd pc.peers);
			if(pp != nil)
				pp.good = 1;
		}
		for(l := peerbox.peers; l != nil; l = tl l)
			peersend(hd l, ref Msg.Have(pc.index));

		if(done()) {
			trackerevent = "completed";
			trackkick(0);
			for(l = peerbox.peers; l != nil; l = tl l) {
				p := hd l;
				if(p.isdone()) {
					say("done: dropping seed "+p.text());
					peerdrop(p, 0, nil);
				} else {
					# we won't act on becoming interested while unchoked anymore
					if(!p.localchoking() && !p.remoteinterested())
						choke(p);
				}
			}
			if(!stopped) {
				ratio := ratio();
				if(ratio >= 1.1 && ratio >= maxratio) {
					say(sprint("stopping due to max ratio achieved (%.2f)", ratio));
					return stop();
				}
			}
		}

	curwritec <-= curmainwrite =>
		mainwrites.take();

	p := <-wantmsgc =>
		p.getmsg = 1;
		if(stopped)
			return;
		peergive(p);
		if(p.rreqs.length > 0 && p.getmsg && !p.localchoking() && p.remoteinterested())
			readrreq(p);

	(p, piece, begin, buf, err) := <-diskreadc =>
		if(stopped)
			return;
		if(err != nil)
			return warnstop(sprint("error reading piece %d, begin %d, length %d: %s", piece, begin, len buf, err));
		if(peerbox.findid(p.id) == nil)
			return;
		peersend(p, ref Msg.Piece(piece, begin, buf));
	}
}

main()
{
warn(1, sprint("main pid %d", pid()));
	roundgen = 0;
	bogusc = chan of ref Chunkwrite;
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
		if(ok != 0)
			return warn(1, sprint("listen: %r"));

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
	spawn dialkill(pidc := chan of int, pid(), np);
	killerpid := <-pidc;
	
	addr := sprint("net!%s", np.addr);
	(ok, conn) := sys->dial(addr, nil);
	if(ok < 0) {
		newpeerc <-= (1, np, nil, nil, nil, sprint("dial %s: %r", np.addr));
		kill(killerpid);
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
	if(n == 0)
		return (nil, "eof");
	if(n < len buf)
		return (nil, "short read");
	(size, nil) := g32i(buf, 0);
	buf = array[size] of byte;

	n = netread(fd, buf, len buf);
	if(n < 0)
		return (nil, sprint("reading: %r"));
	if(n < len buf)
		return (nil, "short read");

	return Msg.unpack(buf);
}


peernetreader(p: ref Peer)
{
	needwritec := chan[1] of ref Queue[ref Chunkwrite];
	stopc := chan[1] of int;
	for(;;) {
		(m, err) := msgread(p.fd);
		peerinmsgc <-= (p, m, stopc, needwritec, err);
		if(<-stopc)
			break;

		q := <-needwritec;
		if(q != nil)
			for(f := q.first; f != nil; f = f.next)
				p.writec <-= f.e;  # will block if disk is slow, slowing down peer as well
	}
}

peernetwriter(pidc: chan of int, p: ref Peer)
{
	pidc <-= pid();

	for(;;) {
		wantmsgc <-= p;
		mq := <-p.getmsgc;
		if(mq == nil)
			break;

		dlen := 0;
		for(f := mq.first; f != nil; f = f.next)
			dlen += f.e.packedsize();
		d := array[dlen] of byte;
		o := 0;
		for(f = mq.first; f != nil; f = f.next) {
			m := f.e;
			size := m.packedsize();
			m.packbuf(d[o:o+size]);
			o += size;
if(dflag) say(sprint("-> %s: %s", p.np.ip, m.text()));
		}
		n := netwrite(p.fd, d, len d);
		if(n != len d) {
			peererrc <-= (p, sprint("write: %r"));
			break;
		}
	}
	say("peernetwriter: stopping...");
}

diskwriter(reqc: chan of ref Chunkwrite)
{
	for(;;) {
		cw := <-reqc;
		if(cw == nil)
			break;

		off := big cw.piece*big state.t.piecelen + big cw.begin;
		chunkoff := 0;
		err: string;
		for(f := cw.bufs.first; err == nil && f != nil; f = f.next) {
			buf := f.e;
			err = state.tx.pwritex(buf, len buf, off+big chunkoff);
			chunkoff += len buf;
		}
		diskwrittenc <-= (cw.piece, cw.begin, chunkoff, err);
	}
	say("diskwriter: stopping...");
}

diskreader(p: ref Peer)
{
	for(;;) {
		rr := <-p.readc;
		if(rr == nil)
			break;
		off := big rr.piece*big state.t.piecelen + big rr.begin;
		err := state.tx.preadx(buf := array[rr.length] of byte, len buf, off);
		diskreadc <-= (p, rr.piece, rr.begin, buf, err);
	}
	say("diskreader: stopping...");
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
		if(!state.have.get(i))
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

hasint(l: list of int, v: int): int
{
	for(; l != nil; l = tl l)
		if(hd l == v)
			return 1;
	return 0;
}

randomize[T](a: array of T)
{
	for(i := 0; i < len a; i++) {
		j := rand->rand(len a);
		(a[i], a[j]) = (a[j], a[i]);
	}
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
