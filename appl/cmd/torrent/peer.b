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
	pid, kill, killgrp, writefile, hex, join, max, min, rev, l2a, g32i, readfile, readfd, inssort, sizefmt, sizeparse: import util;
include "bitarray.m";
	bitarray: Bitarray;
	Bits, Bititer: import bitarray;
include "bittorrent.m";
	bt: Bittorrent;
	Bee, Msg, File, Torrent, Filex, Torrentx, Trackreq, Track, Trackpeer: import bt;
include "../../lib/bittorrentpeer.m";
	btp: Bittorrentpeer;
	State, Traffic, Piecepeer, Piece, Peer, Newpeer, Newpeers, Bigtab, Queue, Chunk, Chunkwrite, Req, Reqs, Read, Link, List, Progress, Peerevent, Eventfid: import btp;
	Slocal, Sremote, Schoking, Sunchoking, Sinterested, Suninterested: import btp;
	Gfaulty, Gunknown, Ghalfgood, Ggood: import btp;

Torrentpeer: module {
	init:	fn(nil: ref Draw->Context, nil: list of string);
};

Int: adt {
	i:	int;
};

Peerbox: adt {
	peers:		list of ref Peer;	# peers we are connected to
	ips,					# newpeer.ip
	addrs:		ref Strhash[ref Peer];  # newpeer.addr
	ids:		ref Table[ref Peer];	# peer.id
	maskips:	array of string;	# unique masked ip addresses for peers, for round robin peer selection
	maskippeers:	ref Strhash[list of ref Peer];	# maps masked ip address to peers
	ndialed:	int;
	nseeding:	int;
	lucky:		ref Peer;
	lastchange:	int;
	fnow:		ref Table[ref Peer];
	fdelay:		ref Table[ref Link[ref Peer]];
	fdelayq:	ref Queue[ref Peer];

	new:		fn(): ref Peerbox;
	add:		fn(b: self ref Peerbox, p: ref Peer);
	del:		fn(b: self ref Peerbox, p: ref Peer);
	findip:		fn(b: self ref Peerbox, ip: string): ref Peer;
	findaddr:	fn(b: self ref Peerbox, addr: string): ref Peer;
	findid:		fn(b: self ref Peerbox, id: int): ref Peer;
	nactive:		fn(b: self ref Peerbox): int;
	droppable:		fn(b: self ref Peerbox): ref Peer;
	ninteresting,
	ngiving:	fn(b: self ref Peerbox): int;

	maskipused:	fn(b: self ref Peerbox, i: int, maskip: string);
	oldestunchoke:	fn(b: self ref Peerbox): ref Peer;

	listflushnow:	fn(b: self ref Peerbox): list of ref Peer;
	peekflushdelay:	fn(b: self ref Peerbox): ref Peer;
	addflushnow,
	addflushdelay:	fn(b: self ref Peerbox, p: ref Peer);
	haveflushdelay:	fn(b: self ref Peerbox): int;
	delflush:	fn(b: self ref Peerbox, p: ref Peer);
};

# state
state:		ref State;
newpeers:	ref Newpeers;  # peers we are not connected to
peerbox:	ref Peerbox;
stopped := 1;
dialtoken := 1;
dialtokenc: chan of int;
delaytoken := 1;
delaytokenc: chan of int;
tracktoken := 1;
islistening := 0;	# whether listener() is listening
nhashfails := 0;
totalleft:	big;
trackerevent:	string;
trackkickpid := -1;
trafficup,
trafficdown,
trafficmetaup,
trafficmetadown:	ref Traffic;

# config
dflag: int;
sflag: int;
nofix: int;
torrentpath:	string;
time0:	int;
listenport:	int;
localpeerid:	array of byte;
localpeeridhex:	string;
trackerkey:	string;
ip4mask,
ip6mask:	IPaddr;
maxratio	:= 0.0;
maxuprate	:= -1;
maxdownrate	:= -1;
maxuptotal	:= big -1;
maxdowntotal	:= big -1;


# tracker
trackkickc:	chan of int;
trackreqc:	chan of ref Trackreq;
trackc:		chan of (ref Track, string);

# dialer/listener
canlistenc:	chan of int;
newpeerc:	chan of (string, string, int, ref Newpeer, ref Sys->FD, array of byte, array of byte, string);

# upload/download rate limiter
upc, downc:	chan of (int, chan of int);
maxupc, maxdownc:	chan of int;

# peer interaction
peerinmsgc:	chan of (ref Peer, ref Msg, chan of int, chan of ref Queue[ref Chunkwrite], string);
peererrc:	chan of (ref Peer, string);
wantmsgc:	chan of ref Peer;
diskwritec:	chan of ref Chunkwrite;
diskwrittenc:	chan of (int, int, int, string);
diskreadc:	chan of (ref Peer, list of ref Read, string);
mainwrites:	ref Queue[ref Chunkwrite];

roundc:		chan of int;
swarmc:		chan of int;
verifyc:	chan of (ref Piece, string);

Dialinterval:	con 700;	# msec to wait between dials of peers
Dialersmax:	con 15;		# max number of dialer procs
Dialtimeout:	con 20*1000;	# timeout for connecting to peer
Peerslowmax:	con 80;
Peershighmax:	con Peerslowmax+5;
Blockqueuemax:	con 100;	# max number of Requests a peer can queue at our side without being considered bad
Blockqueuesize:	con 100;	# number of pending blocks to request to peer
Batchsize:	con 8;		# number of requests to send in one go
Netiounit:	con 1500-20-20-20;  # typical network data io unit, ethernet-ip-tcp-slack
Keepalivesend:	con 2*60-20;
Keepaliverecv:	con 4*60;

Listenhost:	con "*";
Listenport:	con 6881;
Listenportrange:	con 100;

Intervaldefault:	con 1800;
Intervalneed:		con 5*60;
ntracks := 0;
Trackfast:	con 3;		# number of fast trackings to do at startup
Trackfastsecs:	con 120;	# seconds between initial fast trackings

Blocksize:	con 16*1024;
Unchokedmax:	con 4;
Seedunchokedmax:	con 4;


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

	mainwrites = mainwrites.new();

	# note that no one has any business with *last.e
	progressfids = progressfids.new(4, nil);
	progresslast = ref List[ref Progress];
	progresslast.next = ref List[ref Progress];
	peerfids = peerfids.new(4, nil);
	peereventlast = ref List[ref Peerevent];
	peereventlast.next = ref List[ref Peerevent];

	sys->pctl(Sys->NEWPGRP, nil);
	writefile(sprint("/prog/%d/ctl", pid()), 0, array of byte "restricted");

	arg->init(args);
	arg->setusage(arg->progname()+" [-dns] [-m ratio] [-r maxuprate] [-R maxdownrate] [-t maxuptotal] [-T maxdowntotal] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	bt->dflag = btp->dflag = dflag++;
		'n' =>	nofix = 1;
		'm' =>	maxratio = real arg->earg();
		'r' =>	maxuprate = int sizeparse(arg->earg());
			if(maxuprate < 0)
				fail("invalid maximum uprate rate");
		'R' =>	maxdownrate = int sizeparse(arg->earg());
			if(maxdownrate < 0)
				fail("invalid maximum downrate rate");
		's' =>	sflag++;
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
	if(err == nil && (state.t.piecelen/Blocksize)*Blocksize != state.t.piecelen)
		err = sprint("%s: piece length not multiple of blocksize", torrentpath);
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
	state.orphans = state.orphans.new(8, nil);

	peerbox = Peerbox.new();
	newpeers = Newpeers.new();

	if(done())
		say("already done!");
	newpeers.markdone();

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
	trackerkey = hex(genrandom(8));

	dialtokenc = chan of int;
	delaytokenc = chan of int;

	trackkickc = chan of int;
	trackreqc = chan of ref Trackreq;
	trackc = chan of (ref Track, string);

	canlistenc = chan of int;
	newpeerc = chan of (string, string, int, ref Newpeer, ref Sys->FD, array of byte, array of byte, string);
	roundc = chan of int;
	swarmc = chan of int;
	verifyc = chan of (ref Piece, string);

	upc = chan of (int, chan of int);
	downc = chan of (int, chan of int);

	peerinmsgc = chan of (ref Peer, ref Msg, chan of int, chan of ref Queue[ref Chunkwrite], string);
	peererrc = chan of (ref Peer, string);
	wantmsgc = chan of ref Peer;
	diskwritec = chan[4] of ref Chunkwrite;
	diskwrittenc = chan of (int, int, int, string);
	diskreadc = chan of (ref Peer, list of ref Read, string);

	trafficup = Traffic.new();
	trafficdown = Traffic.new();
	trafficmetaup = Traffic.new();
	trafficmetadown = Traffic.new();

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
	spawn swarmticker();
	spawn track();
	spawn limiter(upc, maxupc = chan[1] of int, maxuprate);
	spawn limiter(downc, maxdownc = chan[1] of int, maxdownrate);
	spawn diskwriter(diskwritec);

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
				s += sprint("maxuprate %d\n", maxuprate);
				s += sprint("maxdownrate %d\n", maxdownrate);
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
				s += sprint("announces %q\n", aafmt(t.announces));
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
				s += sprint("metarateup %d\n", trafficmetaup.rate());
				s += sprint("metaratedown %d\n", trafficmetadown.rate());
				s += sprint("eta %d\n", eta());
				s += sprint("hashfails %d\n", nhashfails);
				s += sprint("peers %d\n", len peerbox.peers);
				s += sprint("seeds %d\n", peerbox.nseeding);
				s += sprint("dialed %d\n", peerbox.ndialed);
				s += sprint("interesting %d\n", peerbox.ninteresting());
				s += sprint("giving %d\n", peerbox.ngiving());
				s += sprint("knownpeers %d\n", newpeers.npeers());
				s += sprint("knownseeds %d\n", newpeers.nseeding());
				s += sprint("unusedpeers %d\n", newpeers.nready());
				s += sprint("listenpeers %d\n", newpeers.nlisteners());
				s += sprint("faultypeers %d\n", newpeers.nfaulty());
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
				for(f := newpeers.peers(); f != nil; f = f.next) {
					n := f.e;
					st := "";
					if(n.state & btp->Plistener) st += "l";
					if(n.state & btp->Pdialing) st += "d";
					if(n.state & btp->Pconnected) st += "c";
					if(n.state & btp->Pseeding) st += "s";
					s += sprint("%q %q %d %d %s\n", n.addr, hex(n.peerid), n.time, n.waittime, st);
				}
				fid.data = array of byte s;
			}
			srv.reply(styxservers->readbytes(m, fid.data));

		Qpeersbad =>
			if(m.offset == big 0) {
				s := "";
				for(f := newpeers.faulty(); f != nil; f = f.next)
					s += sprint("%q %d %q %q\n", f.e.addr, f.e.time, hex(f.e.peerid), f.e.banreason);
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
commands1 := array[] of {"disconnect", "debug", "maxratio", "maxuptotal", "maxdowntotal", "maxuprate", "maxdownrate"};
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
		stop("ctl");
	"start" =>
		start();
	"disconnect" =>
		(id, rem) := str->toint(hd l, 10);
		if(rem != nil)
			return replyerror(m, styxservers->Ebadarg);
		p := peerbox.findid(id);
		if(p == nil)
			return replyerror(m, "no such peer");
		peerdrop(p, 0, "disconnect ctl");
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
		if(rem != nil)
			return replyerror(m, styxservers->Ebadarg);
		maxratio = v;
		putprogress(ref Progress.Newctl);
		ratio := ratio();
		if(done() && maxratio > 0.0 && ratio >= maxratio)
			stop(sprint("max ratio reached (%.2f)", ratio));
	"maxuptotal" or
	"maxdowntotal" or
	"maxuprate" or
	"maxdownrate" =>
		v := sizeparse(hd l);
		if(v < big 0 && hd l != "-1")
			return replyerror(m, styxservers->Ebadarg);
		case cmd {
		"maxuptotal" =>		maxuptotal = v;
		"maxdowntotal" =>	maxdowntotal = v;
		"maxuprate" =>		maxupc <-= maxuprate = int v;
		"maxdownrate" =>	maxdownc <-= maxdownrate = int v;
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
		t = ref Progress.Tracker (-1, -1, -1, nil, nil);
	t.next = nexttrack-daytime->now();
	l = next(l, t);
	l = next(l, ref Progress.Endofstate);
	l.next = progresslast.next;
}

putpeerstate(l: ref List[ref Peerevent])
{
	for(f := newpeers.faulty(); f != nil; f = f.next)
		l = next(l, ref Peerevent.Bad (f.e.addr, f.e.time, f.e.peerid, f.e.banreason));
	for(t := newpeers.peers(); t != nil; t = t.next)
		l = next(l, ref Peerevent.Tracker (t.e.addr));
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

# id addr hex direction localstate remotestate lastunchoke npiecehave "up" total rate "down" total rate "metaup" total rate "metadown" total rate
peerline(p: ref Peer): string
{
	direction := "in";
	if(p.dialed)
		direction = "out";
	s := "";
	s += sprint("%d %q %q %s", p.id, p.np.addr, p.peeridhex, direction);
	s += sprint(" %q", statestr(p.localchoking(), p.localinterested()));
	s += sprint(" %q", statestr(p.remotechoking(), p.remoteinterested()));
	s += sprint(" %d", p.lastunchokemsg);
	s += sprint(" %d", p.rhave.have);
	s += sprint(" %d", p.createtime);
	s += sprint(" up %bd %d", p.up.total(), p.up.rate());
	s += sprint(" down %bd %d", p.down.total(), p.down.rate());
	s += sprint(" metaup %bd %d", p.metaup.total(), p.metaup.rate());
	s += sprint(" metadown %bd %d", p.metadown.total(), p.metadown.rate());
	s += sprint(" reqs %d %d", p.lreqs.q.length, p.rreqs.q.length);
	s += "\n";
	return s;
}

statestr(choking, interested: int): string
{
	s := "";
	if(choking)
		s[len s] = 'c';
	if(interested)
		s[len s] = 'i';
	return s;
}


done(): int
{
	return state.have.have == state.have.total;
}

stop(reason: string)
{
	if(stopped)
		return;

	say("stopping: "+reason);

	tracknow("stopped");
	putprogress(ref Progress.Stopped);

	stopped = 1;
	peerbox.lucky = nil;
	for(l := peerbox.peers; l != nil; l = tl l)
		peerdrop(hd l, 0, "stopping");

	trafficup = Traffic.new();
	trafficdown = Traffic.new();
	trafficmetaup = Traffic.new();
	trafficmetadown = Traffic.new();
}

start()
{
	if(!stopped)
		return;

	time0 = daytime->now();
	stopped = 0;
	putprogress(ref Progress.Started);

	trackkick(0);
	if(!islistening) {
		canlistenc <-= 1;
		islistening = 1;
	}
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

peerflush()
{
	for(l := peerbox.listflushnow(); l != nil; l = tl l)
		peergive(hd l);
	if(delaytoken && peerbox.haveflushdelay()) {
		delaytoken = 0;
		spawn givedelaytoken(100+randvar(20));
	}
}

peerdrop(p: ref Peer, faulty: int, err: string)
{
	warn(1, err+", "+p.text());
	if(faulty) {
		nexttime := newpeers.disconnectfaulty(p.np, err);
		putevent(ref Peerevent.Bad (p.np.ip, nexttime, p.np.peerid, err));
	} else
		newpeers.disconnected(p.np, err);

	if(!p.chunk.isempty())
		mainwrites.append(p.chunk.flush());
	peerbox.del(p);
	putevent(ref Peerevent.Gone (p.id));

	spawn stopreadc(p.readc);
	spawn stopwritec(p.writec);

	undolreqs(p);

	if(faulty || stopped)
		hangup(p.fd);

	diallisten();
}

stopreadc(c: chan of (big, list of ref Read, int))
{
	c <-= (big 0, nil, 0);
}

stopwritec(c: chan of ref Chunkwrite)
{
	c <-= nil;
}

givedialtoken()
{
	sys->sleep(Dialinterval);
	dialtokenc <-= 1;
}

givedelaytoken(ms: int)
{ 
	sys->sleep(ms);
	delaytokenc <-= 1;
}

diallisten()
{
	if(dialtoken && newpeers.cantake() && newpeers.ndialers() < Dialersmax && (peerbox.ndialed < Peerslowmax/2 || len peerbox.peers < Peerslowmax)) {
		np := newpeers.take();
		say("spawning dialer for "+np.text());
		putevent(ref Peerevent.Dialing (np.addr));
		dialtoken = 0;
		spawn dialer(np);
		spawn givedialtoken();
	}

	if(!islistening && (len peerbox.peers-peerbox.ndialed < Peerslowmax/2 || len peerbox.peers < Peerslowmax)) {
		islistening = 1;
		canlistenc <-= 1;
	}
}

peersend(p: ref Peer, m: ref Msg)
{
	p.metamsgs.append(m);

	case tagof m {
	tagof Msg.Choke or
	tagof Msg.Notinterested or
	tagof Msg.Have or
	tagof Msg.Keepalive =>
		peerbox.addflushdelay(p);
	* =>
		peerbox.addflushnow(p);
	}
}

account0(p: ref Peer, mm: ref Msg)
{
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

account(p: ref Peer, q: ref Queue[ref Msg])
{
	for(f := q.first; f != nil; f = f.next)
		account0(p, f.e);
}

getpiece(q: ref Queue[ref Read]): ref Read.Piece
{
	for(f := q.first; f != nil; f = f.next)
		pick e := f.e {
		Piece =>
			q.unlink(f);
			return e;
		}
	return nil;
}

peergive(p: ref Peer)
{
	peerbox.delflush(p);
	if(!p.wantmsg)
		return;

	if(!p.metamsgs.empty()) {
		p.outmsgc <-= p.metamsgs;
		account(p, p.metamsgs);
		p.metamsgs = ref Queue[ref Msg];
		p.wantmsg = 0;
		p.lastsend = daytime->now();
	} else {
		readrreqs(p);
		rr := getpiece(p.reads);
		if(rr == nil)
			return;
		mq := Queue[ref Msg].new();
		mq.append(rr.m);
		account(p, mq);
		p.outmsgc <-= mq;
		p.wantmsg = 0;
		p.lastpiecemsg = p.lastsend = daytime->now();
		readrreqs(p);
	}
}

blocksize(piece, begin: int): int
{
	# first a quick check
	if(piece < state.t.piececount-1)
		return Blocksize;

	# otherwise, the full check
	if(big piece*big state.t.piecelen + big begin + big Blocksize > state.t.length)
		return int (state.t.length % big Blocksize);
	return Blocksize;
}

clearrreq(p: ref Peer)
{
	p.rreqs.clear();
	for(l := p.reading.all(); l != nil; l = tl l)
		(hd l).cancelled = 1;
	f := p.reads.first;
	while(f != nil) {
		next := f.next;
		if(tagof f.e == tagof Read.Piece)
			p.reads.unlink(f);
		f = next;
	}
}

roundup(v, up: int): int
{
	return (v+up-1) & ~(up-1);
}

readrreqs(p: ref Peer)
{
	while(p.rreqs.q.length != 0 && p.reads.length != 0 && tagof p.reads.first.e == tagof Read.Token)
		readrreq(p);
}

readrreq(p: ref Peer)
{
	token := p.reads.take();

	if(dflag) say("readrreq: reading more");
	n := max(Blocksize, min(roundup(p.up.rate(), Blocksize), 128*1024));
	h := 0;
	fr: ref Req;
	reads: list of ref Read;
	do {
		req := p.rreqs.takefirst();
		if(fr == nil)
			fr = req;
		r := ref Read.Piece (0, req, nil);
		reads = r::reads;
		h += req.length;
		p.reading.add(req.key(), r);
	} while(h < n && p.rreqs.q.length > 0 && p.rreqs.q.first.e.piece == fr.piece && p.rreqs.q.first.e.begin == fr.begin+h);
	reads = token::reads;

	off := big fr.piece*big state.t.piecelen+big fr.begin;
	p.readc <-= (off, rev(reads), h);
}


# we thought we didn't need piece anymore, but we do now (hash check failed)
peerlwant(i: int)
{
	for(l := peerbox.peers; l != nil; l = tl l) {
		p := hd l;
		if(!p.rhave.get(i))
			continue;

		if(p.lwant.get(i))
			raise "peerlwant, piece already set in lwant";
		if(p.canschedule.get(i))
			raise "peerlwant, piece already set in canschedule";
		p.lwant.set(i);
		p.canschedule.set(i);
		interesting(p);
		schedule(p);
	}
}

# mark piece as unwanted from peers.
# we have all blocks of the piece (still need to verify it, but if that fails, we mark them as wanted again using peerlwant)
peernolwant(i: int)
{
	for(l := peerbox.peers; l != nil; l = tl l) {
		p := hd l;
		if(!p.rhave.get(i))
			continue;

		p.lwant.clear(i);
		p.canschedule.clear(i);
		interesting(p);
	}
}

# block came in, cancel requests for block sent to other peers.
cancel(pc: ref Piece, block: int)
{
	begin := block*Blocksize;
	length := blocksize(pc.index, begin);
	for(l := tablist(pc.peers); l != nil; l = tl l) {
		pcp := hd l;
		if(!pcp.requested.get(block))
			continue;

		p := peerbox.findid(pcp.peerid);
		if(p == nil)
			continue;
		peersend(p, ref Msg.Cancel(pc.index, begin, length));
		pcp.requested.clear(block);

		r := ref Req (pc.index, begin, length);
		if(!p.lreqs.del(r))
			raise "cancel, peer had block requested, but no lreq for it?";
	}
}

# peer is choking us, or we are dropping it.
# we won't be getting the blocks pending from it.
# so mark them such that they will be scheduled again.
undolreqs(p: ref Peer)
{
	li: list of int;
	for(f := p.lreqs.q.first; f != nil; f = f.next) {
		lr := f.e;
		if(hasint(li, lr.piece))
			continue;
		li = lr.piece::li;

		pc := state.active.find(lr.piece);
		if(pc == nil)
			raise "peer had lreq for inactive piece?";
		pcp := pc.peers.find(p.id);
		if(pcp == nil)
			raise "undolreqs, have lreq for piece for which we are not in peer list?";
		pc.peers.del(p.id);
		if(p.lwant.get(lr.piece)) {
			if(pc.have.isfull())
				raise "pc.have.isfull() but peer still had lwant set for piece?";
			p.canschedule.set(lr.piece);
		}
		if(len tablist(pc.peers) == 0) {
			state.active.del(lr.piece);
			if(!pc.have.isempty()) {
				pc.needrequest = pc.have.clone();
				pc.needrequest.invert();
				state.orphans.add(pc.index, pc);
if(dflag) say(sprint("schedule peer %d: turning active piece %d (have %d) into orphan", p.id, pc.index, pc.have.have));
			}
		} else {
			pc.needrequest = pc.have.clone();
			pc.needrequest.invert();
			for(l := tablist(pc.peers); l != nil; l = tl l)
				pc.needrequest.clearbits((hd l).requested);
		}
	}
	p.lreqs = p.lreqs.new();
}


# we like sending requests in batches so we don't need to incur tcp/ip overhead (and perhaps padding) for each request.
# we also only want to queue more requests if remote has been given us pieces, to prevent tying up many pieces there.
needlreqs(p: ref Peer): int
{
	n := Batchsize;
	r := p.down.rate();
	if(r > 0)
		n += r/(8*1024);
	return min(Blockqueuesize, ((n-p.lreqs.q.length+Batchsize-1)/Batchsize)*Batchsize);
}

requestblock(p: ref Peer, pc: ref Piece, pcp: ref Piecepeer, block: int)
{
	if(!pcp.canrequest.get(block))
		raise "requestblock, cannot request";
	if(pcp.requested.get(block))
		raise "requestblock, already requested";
	pc.needrequest.clear(block);
	pcp.canrequest.clear(block);
	pcp.requested.set(block);
	begin := block*Blocksize;
	lr := ref Req (pc.index, begin, blocksize(pc.index, begin));
if(dflag) say(sprint("schedule peer %d: %s", p.id, lr.text()));
	p.lreqs.add(lr);
	peersend(p, ref Msg.Request(lr.piece, lr.begin, lr.length));
	p.lastrequestmsg = daytime->now();
	p.lastpiece = pc.index;
}

schedulepiece(p: ref Peer, pc: ref Piece, nblocks: int): int
{
	if(!p.rhave.get(pc.index))
		raise "scheduling piece that peer does not have";
	if(!p.canschedule.get(pc.index))
		raise "scheduling piece not in peer's canschedule";
	if(!p.lwant.get(pc.index))
		raise "scheduleing piece not in peer's lwant";

	pcp := pc.peers.find(p.id);
	if(pcp == nil) {
if(dflag) say(sprint("schedule peer %d: first block for peer on piece %d, making Piecepeer", p.id, pc.index));
		canrequest := pc.have.clone();
		canrequest.invert();
		pcp = ref Piecepeer (p.id, canrequest, Bits.new(pc.have.total));
		pc.peers.add(p.id, pcp);
	}

if(dflag) say(sprint("schedule peer %d: pc.needrequest %d, pcp.canrequest %d", p.id, pc.needrequest.have, pcp.canrequest.have));
	onblocks := nblocks;
	it := pc.needrequest.clone().iter();
	for(; nblocks > 0 && (i := it.next()) >= 0; nblocks--)
		requestblock(p, pc, pcp, i);
	for(; nblocks > 0 && !pcp.canrequest.isempty(); nblocks--)
		requestblock(p, pc, pcp, pcp.canrequest.rand());
if(dflag) say(sprint("schedule peer %d: scheduled %d blocks (%d allowed) for piece %d", p.id, onblocks-nblocks, onblocks, pc.index));
	return nblocks;
}

pieceprogressge(a, b: ref Piece): int
{
	return a.have.have < b.have.have;
}

scheduleorphans(p: ref Peer, nblocks: int): int
{
	orphs := l2a(tablist(state.orphans));
if(dflag) say(sprint("schedule peer %d: %d orphan pieces to look at", p.id, len orphs));
	inssort(orphs, pieceprogressge);
	li: list of int;
	for(i := 0; i < len orphs; i++) {
		pc := orphs[i];
		if(!p.rhave.get(pc.index))
			continue;
		if(!p.lwant.get(pc.index))
			raise "orphan, and rhave, but not lwant?";
		if(!p.canschedule.get(pc.index))
			continue;
		nnblocks := schedulepiece(p, pc, nblocks);
		if(nblocks > nnblocks) {
if(dflag) say(sprint("schedule peer %d: making orphan piece %d active (have %d, written %d)", p.id, pc.index, pc.have.have, pc.written.have));
			state.orphans.del(pc.index);
			state.active.add(pc.index, pc);
			li = pc.index::li;
			nblocks = nnblocks;
		} else {
			if(dflag) say(sprint("schedule peer %d: no blocks in piece %d to schedule", p.id, pc.index));
			break;
		}
	}
	return nblocks;
}

schedule(p: ref Peer)
{
if(dflag) say(sprint("schedule peer %d", p.id));
	if(!p.localinterested() || p.remotechoking())
		return;
	nblocks := needlreqs(p);
	if(nblocks <= 0)
		return;
	schedule0(p, nblocks);
}


schedule0(p: ref Peer, nblocks: int)
{
if(dflag) say(sprint("schedule peer %d: starting", p.id));

	# attempt to work on last piece this peer was working on
	if(p.lastpiece >= 0) {
		pc := state.active.find(p.lastpiece);
		if(pc != nil && p.canschedule.get(p.lastpiece) && pc.peers.find(p.id) != nil) {
			if(dflag) say(sprint("schedule peer %d: previous piece peer", p.id));
			nblocks = schedulepiece(p, pc, nblocks);
			if(nblocks <= 0)
				return;
		}
if(dflag) say(sprint("schedule peer %d: done with previous piece", p.id));
	}

	# orphan pieces
	if(p.good >= Ggood) {
		if(dflag) say(sprint("schedule peer %d: orphans for good peer", p.id));
		nblocks = scheduleorphans(p, nblocks);
if(dflag) say(sprint("schedule peer %d: done with orphan pieces", p.id));
		if(nblocks <= 0)
			return;
	}

	# random new piece
if(dflag) say(sprint("schedule peer %d: looking at new pieces.  rhave %d, lwant %d, canschedule %d", p.id, p.rhave.have, p.lwant.have, p.canschedule.have));
	# xxx selection should be less inefficient
	canstart := p.canschedule.clone();
	for(l := tablist(state.active); l != nil; l = tl l)
		canstart.clear((hd l).index);
	for(l = tablist(state.orphans); l != nil; l = tl l)
		canstart.clear((hd l).index);
	while(canstart.have > 0) {
		i := canstart.rand();
		if(!p.lwant.get(i))
			raise "canstart.rand() returned piece not in peer's lwant";
if(dflag) say(sprint("schedule peer %d: using new random piece %d", p.id, i));
		canstart.clear(i);
		length := state.t.piecelength(i);
		nb := (length+Blocksize-1)/Blocksize;
		needrequest := Bits.new(nb);
		needrequest.setall();
		pc := ref Piece (i, Bits.new(nb), Bits.new(nb), needrequest, length, Table[ref Piecepeer].new(5, nil), nil, nil, 0);
		if(state.orphans.find(i) != nil)
			raise "just created active piece while orphan exists";
		if(state.active.find(i) != nil)
			raise "just created active piece while active piece already exists";
		nnblocks := schedulepiece(p, pc, nblocks);
		if(nnblocks < nblocks)
			state.active.add(i, pc);
		nblocks = nnblocks;
		if(nblocks <= 0)
			return;
	}

	# orphan pieces for non-good peers
	if(p.good < Ggood) {
		if(dflag) say(sprint("schedule peer %d: orphans for non-good peer", p.id));
		nblocks = scheduleorphans(p, nblocks);
		if(nblocks <= 0)
			return;
	}

	# finally, try active pieces.  xxx could sort this by least peers busy, or most done
	nblocks = min(nblocks, max(0, 2+p.down.rate()/(32*1024)-p.lreqs.q.length));
	if(nblocks <= 0)
		return;
	l = tablist(state.active);
if(dflag) say(sprint("schedule peer %d: looking at %d active pieces", p.id, len l));
	for(; l != nil; l = tl l) {
		pc := hd l;
		if(!p.canschedule.get(pc.index)) {
if(dflag) say(sprint("schedule peer %d: cannot schedule on active piece %d, want %d, canschedule %d", p.id, pc.index, p.lwant.get(pc.index) != 0, p.canschedule.get(pc.index) != 0));
			continue;
		}
		if(dflag) say(sprint("schedule peer %d: co-scheduling on active piece %d", p.id, pc.index));
		nblocks = schedulepiece(p, pc, nblocks);
		if(nblocks <= 0)
			return;
	}
}

choke(p: ref Peer)
{
	peersend(p, ref Msg.Choke);
	p.state |= btp->LocalChoking;
	putevent(ref Peerevent.State (p.id, Slocal|Schoking));
	p.lastchokemsg = daytime->now();

	clearrreq(p);
}

unchoke(p: ref Peer)
{
	peersend(p, ref Msg.Unchoke);
	p.state &= ~btp->LocalChoking;
	putevent(ref Peerevent.State (p.id, Slocal|Sunchoking));
	p.lastunchokemsg = daytime->now();
}

interested(p: ref Peer)
{
	if(p.lreqs.q.length != 0)
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
	if(p.localinterested() && p.lwant.have == 0)
		uninterested(p);
	else if(!p.localinterested() && p.lwant.have > 0)
		interested(p);
}

unchokecmp(a, b: ref Peer): int
{
	if(a.remoteinterested() == b.remoteinterested())
		return a.lastunchokemsg-b.lastunchokemsg;
	if(a.remoteinterested())
		return -1;
	return 1;
}

unchokege(a, b: ref Peer): int
{
	return unchokecmp(a, b) >= 0;
}

nextoptimisticunchoke(): ref Peer
{
	for(i := 0; i < len peerbox.maskips; i++) {
		maskip := peerbox.maskips[i];
		peers := l2a(peerbox.maskippeers.find(maskip));
		inssort(peers, unchokege);
		p := peers[0];
		if(p.isdone() || !p.remoteinterested())
			continue;
		if(!p.localchoking())
			break;
		return p;
	}
	return nil;
}

chokingupload(nil: int)
{
	for(i := 0; i < len peerbox.maskips; i++) {
		maskip := peerbox.maskips[i];
		peers := l2a(peerbox.maskippeers.find(maskip));
		inssort(peers, unchokege);
		p := peers[0];
		if(!p.remoteinterested())
			continue;
		if(!p.localchoking())
			break;

		unchoke(p);
		peerbox.maskipused(i, maskip);
		pp := peerbox.oldestunchoke();
		if(pp != nil)
			choke(pp);
		return;
	}
}

peerratecmp(a1, a2: ref (ref Peer, int)): int
{
	(p1, r1) := *a1;
	(p2, r2) := *a2;
	if(p1.remoteinterested() == p2.remoteinterested())
		return r2-r1;
	if(p1.remoteinterested())
		return -1;
	return 1;
}

peerratege(a1, a2: ref (ref Peer, int)): int
{
	return peerratecmp(a1, a2) >= 0;
}

chokingdownload(gen: int)
{
	if(gen % 3 == 0)
		peerbox.lucky = nextoptimisticunchoke();

	all := array[len peerbox.peers] of ref (ref Peer, int);  # peer, rate
	i := 0;
	for(l := peerbox.peers; l != nil; l = tl l) {
		p := hd l;
		downrate := p.down.rate();
		if(downrate > 0)
			downrate = max(1, downrate-p.metadown.rate());
		all[i++] = ref (p, downrate);
	}
	inssort(all, peerratege);

	if(peerbox.lucky != nil) {
		if(peerbox.lucky.localchoking())
			unchoke(peerbox.lucky);
	}
	nunchoked := 0;
	for(i = 0; i < len all; i++) {
		(p, nil) := *all[i];
		if(p == peerbox.lucky)
			continue;
		if(nunchoked < Unchokedmax-1) {
			if(p.remoteinterested() && p.localchoking() && !p.isdone())
				unchoke(p);
			nunchoked++;
		} else if(!p.localchoking())
			choke(p);
	}
}

peermsg(p: ref Peer, mm: ref Msg, needwritec: chan of ref Queue[ref Chunkwrite], sent: ref Int)
{
if(dflag) say(sprint("<- peer %d: %s", p.id, mm.text()));
	p.msgseq++;
	msize := mm.packedsize();
	pick m := mm {
	Piece =>
		# handled at the case for Piece below
		;
	* =>
		p.metadown.add(msize, 1);
		trafficmetadown.add(msize, 1);
	}

	pick m := mm {
	Keepalive =>
		;

	Choke =>
		if(p.remotechoking())
			pwarn(p, 1, "double choke");

		p.state |= btp->RemoteChoking;
		putevent(ref Peerevent.State (p.id, Sremote|Schoking));

		if(!p.chunk.isempty()) {
			needwrites := Queue[ref Chunkwrite].new();
			needwrites.append(p.chunk.flush());
			needwritec <-= needwrites;
			sent.i = 1;
		}
		undolreqs(p);

	Unchoke =>
		if(!p.remotechoking())
			pwarn(p, 1, "double unchoke");

		p.unchokeblocks = 0;
		p.state &= ~btp->RemoteChoking;
		putevent(ref Peerevent.State (p.id, Sremote|Sunchoking));
		schedule(p);

	Interested =>
		if(p.remoteinterested())
			pwarn(p, 1, "double interested");

		p.state |= btp->RemoteInterested;
		putevent(ref Peerevent.State (p.id, Sremote|Sinterested));

		if(peerbox.nactive() < Unchokedmax && !p.isdone()) {
			say("unchoking rare new peer: "+p.text());
			unchoke(p);
		}

	Notinterested =>
		if(!p.remoteinterested())
			pwarn(p, 1, "double uninterested");

		p.state &= ~btp->RemoteInterested;
		putevent(ref Peerevent.State (p.id, Sremote|Suninterested));
		if(!p.localchoking())
			choke(p);
		clearrreq(p);

	Have =>
		if(m.index >= state.t.piececount)
			return peerdrop(p, 1, sprint("'have' for invalid piece %d", m.index));
		if(p.rhave.get(m.index))
			return peerdrop(p, 1, sprint("already had piece %d (has %d/%d)", m.index, p.rhave.have, p.rhave.total));
		if(dflag) say(sprint("peer %d has %d/%d pieces, has piece %d", p.id, p.rhave.have, p.rhave.total, m.index));

		putevent(ref Peerevent.Piece (p.id, m.index));
		p.rhave.set(m.index);
		if(!state.have.get(m.index)) {
			p.lwant.set(m.index);
			pc := state.active.find(m.index);
			if(pc == nil || !pc.have.isfull())
				p.canschedule.set(m.index);
		}
		if(p.isdone()) {
			peerbox.nseeding++;
			newpeers.seeding(p.np);
		}

		interesting(p);
		schedule(p);

	Bitfield =>
		if(p.msgseq != 1)
			return peerdrop(p, 1, "bitfield after first message");

		(rhave, err) := Bits.mk(state.t.piececount, m.d);
		if(err != nil)
			return peerdrop(p, 1, sprint("bogus bitfield message: %s", err));
		p.rhave = rhave;
		p.lwant = Bits.nand(p.rhave, state.have);
		p.canschedule = p.lwant.clone();
		for(l := tablist(state.active); l != nil; l = tl l)
			if((hd l).have.isfull()) {
				p.lwant.clear((hd l).index);
				p.canschedule.clear((hd l).index);
			}
		if(dflag) say(sprint("peer %d has %d/%d pieces, from bitfield", p.id, p.rhave.have, p.rhave.total));

		if(p.isdone()) {
			putevent(ref Peerevent.Done (p.id));
			peerbox.nseeding++;
			newpeers.seeding(p.np);
		} else
			for(ll := peereventpieces(p.id, p.rhave); ll != nil; ll = tl ll)
				putevent(hd ll);

		interesting(p);

	Piece =>
		block := m.begin/Blocksize;

		if(blocksize(m.index, m.begin) != len m.d)
			return peerdrop(p, 1, sprint("bad size %d for piece %d, begin %d", len m.d, m.index, m.begin));

		key := Req.rkey(m.index, m.begin, len m.d);
		if(p.lreqs.delkey(key) == nil) {
			p.metadown.add(msize, 0);
			trafficmetadown.add(msize, 0);
			return;
		}

		dsize := len m.d;
		p.down.add(dsize, 1);
		p.metadown.add(msize-dsize, 0);
		trafficdown.add(dsize, 1);
		trafficmetadown.add(msize-dsize, 0);

		pc := state.active.find(m.index);
		if(pc == nil)
			return peerdrop(p, 0, sprint("block for non-active and unfinished piece %d", m.index));
		if(pc.have.get(block))
			raise "duplicate block";

		pcp := pc.peers.find(p.id);
		if(pcp == nil)
			raise "block from peer we had request for, but is not in Piece.peers";
		if(!pcp.requested.get(block))
			raise "block from peer we had request for, but not in Piecepeer.requested";
		if(pcp.canrequest.get(block))
			raise "have block from peer, and was requested, but can still request?";
		if(pc.needrequest.get(block))
			raise "have block, but it is still marked needrequest";
		pc.have.set(block);
		pcp.requested.clear(block);
		cancel(pc, block);
		for(l := tablist(pc.peers); l != nil; l = tl l)
			(hd l).canrequest.clear(block);
		if(!hasint(pc.peersgiven, p.id))
			pc.peersgiven = p.id::pc.peersgiven;
		putprogress(ref Progress.Block (p.id, pc.index, block, pc.have.have, pc.have.total));

		p.unchokeblocks++;

		if(pc.have.isfull())
			peernolwant(pc.index);

		if(m.begin == pc.hashoff) {
			pc.hash = kr->sha1(m.d, len m.d, nil, pc.hash);
			pc.hashoff += len m.d;
		}

		needwrites := Queue[ref Chunkwrite].new();
		if(!p.chunk.tryadd(pc, m.begin, m.d)) {
			needwrites.append(p.chunk.flush());
			if(!p.chunk.tryadd(pc, m.begin, m.d))
				raise "tryadd";
		}
		if(p.chunk.isfull())
			needwrites.append(p.chunk.flush());
		if(pc.have.isfull()) {
			for(l = tablist(pc.peers); l != nil; l = tl l) {
				pp := peerbox.findid((hd l).peerid);
				if(pp != nil && !pp.chunk.isempty())
					mainwrites.append(pp.chunk.flush());
			}
		}
		needwritec <-= needwrites;
		sent.i = 1;
		schedule(p);

	Request =>
		rr := ref Req (m.index, m.begin, m.length);
		if(m.begin != 0 && (m.begin & ((16*1024)-1)))
			return peerdrop(p, 1, "bad begin, "+rr.text());
		if(m.length != 16*1024 && m.length != 32*1024 && (big m.index*big state.t.piecelen+big m.begin+big m.length) != state.t.length)
			return peerdrop(p, 1, "bad length, "+rr.text());
		if(p.rhave.get(m.index))
			return peerdrop(p, 1, "requested piece it already claimed to have");
		if(p.rreqs.q.length >= Blockqueuemax)
			return peerdrop(p, 0, sprint("scheduled one too many block, already has %d scheduled", p.rreqs.q.length));
		if(!p.remoteinterested())
			return peerdrop(p, 1, "remote sent request while not interested in us");

		if(p.localchoking()) {
			if(p.lastchokemsg < daytime->now()-3*60)
				return peerdrop(p, 1, sprint("remote sent request even though last choke was long ago (now %d, lastchokemsg %d)", daytime->now(), p.lastchokemsg));
		} else {
			p.rreqs.add(rr);
			readrreqs(p);
		}

	Cancel =>
		rr := ref Req (m.index, m.begin, m.length);
		p.rreqs.del(rr);
		r := p.reading.find(rr.key());
		if(r != nil)
			r.cancelled = 1;
	Nextread:
		for(f := p.reads.first; f != nil; f = f.next)
			pick e := f.e {
			Piece =>
				if(Req.eq(rr, e.r)) {
					p.reads.unlink(f);
					break Nextread;
				}
			}

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
if(dflag > 2) say("<-msgc");
		if(mm == nil)
			fail("styx eof");
		pick m := mm {
		Readerror =>
			fail("styx read error: "+m.error);
		}
		dostyx(mm);

	<-roundc =>
if(dflag > 2) say("<-roundc");
		if(stopped)
			return;
		say(sprint("ticking, %d peers", len peerbox.peers));

		if(done())
			chokingupload(roundgen);
		else
			chokingdownload(roundgen);
		roundgen++;

	<-swarmc =>
if(dflag > 2) say("<-swarmc");
		if(stopped)
			return;
		now := daytime->now();
		sendtime := now-Keepalivesend;
		recvtime := now-Keepaliverecv;
		for(l := peerbox.peers; l != nil; l = tl l) {
			p := hd l;
			if(p.lastrecv < recvtime) {
				peerdrop(p, 0, "idle connection");
			} else if(p.lreqs.q.length > 0 && now-p.lastrequestmsg > 2*60 && now-p.lastpiecemsg > 2*60 && p.localinterested() && !p.remotechoking()) {
				# assume requests got lost in choke message race, resend them.
				for(f := p.lreqs.q.first; f != nil; f = f.next)
					peersend(p, ref Msg.Request (f.e.piece, f.e.begin, f.e.length));
			} else if(p.lastsend < sendtime) {
				peersend(p, ref Msg.Keepalive);
			}
		}
		if(len peerbox.peers >= Peerslowmax && now-peerbox.lastchange > 5*60)
			peerdrop(peerbox.droppable(), 0, "swarm circulation");
		diallisten();

	<-trackkickc =>
if(dflag > 2) say("<-trackkickc");
		if(!tracktoken)
			raise "trackkick but no tracktoken?";
		trackkickpid = -1;
		if(stopped) {
			trackerevent = nil;
			return;
		}
		trackreqc <-= ref Trackreq (state.t, localpeerid, trafficup.total(), trafficdown.total(), totalleft, listenport, trackerevent, trackerkey);
		trackerevent = nil;
		tracktoken = 0;

	(tr, err) := <-trackc =>
if(dflag > 2) say("<-trackc");
		tracktoken = 1;
		if(stopped)
			return;

		ntracks++;

		# schedule next call to tracker
		interval := Intervaldefault;
		if(tr != nil)
			interval = tr.interval;
		if(!newpeers.cantake() && len peerbox.peers < Peerslowmax/4 && trafficdown.rate() <= 0)
			interval = min(interval, Intervalneed);
		if(ntracks < Trackfast)
			interval = Trackfastsecs;
		if(tr != nil && tr.mininterval > 0)
			interval = max(tr.mininterval, interval);

		say(sprint("next call to tracker will be in %d seconds", interval));
		trackkick(interval);
		nexttrack = daytime->now()+interval;

		npeers := 0;
		tracker: string;
		if(tr != nil) {
			npeers = len tr.peers;
			tracker = tr.tracker;
		}
		trackereventlast = ref Progress.Tracker (interval, interval, npeers, tracker, err);
		putprogress(trackereventlast);

		if(err != nil)
			return warn(0, sprint("tracker error: %s", err));

		newpeers.addmany(tr.peers);
		for(i := 0; i < len tr.peers; i++)
			putevent(ref Peerevent.Tracker (sprint("%s!%d", tr.peers[i].ip, tr.peers[i].port)));
		diallisten();

	<-dialtokenc =>
if(dflag > 2) say("<-dialtokenc");
		if(dialtoken)
			raise "already had dial token?";
		dialtoken = 1;
		if(stopped)
			return;
		diallisten();

	<-delaytokenc =>
if(dflag > 2) say("<-delaytokenc");
		if(delaytoken)
			raise "already had delay token?";
		delaytoken = 1;
		if(stopped)
			return;
		p := peerbox.peekflushdelay();
		if(p != nil)
			peergive(p);
		if(peerbox.haveflushdelay()) {
			delaytoken = 0;
			spawn givedelaytoken(100+randvar(20));
		}

	(addr, ip, nil, np, peerfd, extensions, peerid, err) := <-newpeerc =>
if(dflag > 2) say("<-newpeerc");
		dialed := np != nil;
		if(!dialed)
			islistening = 0;
		if(stopped) {
			if(dialed)
				newpeers.dialfailed(np, 0, "");
			return;
		}

		if(err != nil) {
			warn(1, sprint("%s: %s", addr, err));
			if(dialed)
				newpeers.dialfailed(np, 1, err);
		} else if(hex(peerid) == localpeeridhex) {
			tcpdir := str->splitstrr(sys->fd2path(peerfd), "/").t0;
			localip := readip(tcpdir+"local");
			remoteip := readip(tcpdir+"remote");
			if(localip == remoteip && localip != nil)
				newpeers.markself(localip);
			else if(dialed)
				newpeers.dialfailed(np, 1, "saw our own peerid");
		} else if(peerbox.findip(ip) != nil) {
			say("new connection from ip address we're connected to");
			if(dialed)
				newpeers.dialfailed(np, 0, "already connected");
		} else if(newpeers.isfaulty(ip)) {
			say(sprint("connected to faulty ip %s", ip));
			if(dialed) {
				newpeers.connected(np);
				newpeers.disconnectfaulty(np, "connected to faulty ourselves");
			}
		} else {
			if(dialed)
				newpeers.connected(np);
			else
				np = newpeers.addlistener(addr, peerid);

			if(len peerbox.peers+1 > Peershighmax)
				peerdrop(peerbox.droppable(), 0, "too many peers");

			np.peerid = peerid;
			p := Peer.new(np, peerfd, extensions, peerid, dialed, state.t.piececount, maskip(np.ip));
			spawn peernetreader(p);
			spawn peernetwriter(p);
			spawn diskwriter(p.writec);
			spawn diskreader(p);

			peerbox.add(p);
			say(sprint("new peer %s", p.text()));
			putevent(ref Peerevent.New (p.np.addr, p.id, p.peeridhex, p.dialed));
			putevent(ref Peerevent.State (p.id, Slocal|Schoking));
			putevent(ref Peerevent.State (p.id, Sremote|Schoking));

			if(state.have.have == 0)
				peersend(p, ref Msg.Keepalive);
			else
				peersend(p, ref Msg.Bitfield(state.have.bytes()));
		}
		diallisten();

	(p, msg, stopc, needwritec, err) := <-peerinmsgc =>
if(dflag > 2) say("<-peerinmsgc");
		if((p = peerbox.findid(p.id)) == nil || stopped || msg == nil || err != nil) {
			stopc <-= 1;
			if(p == nil)
				{}
			else if(err != nil)
				peerdrop(p, 0, err);
			else if(msg == nil)
				peerdrop(p, 0, "eof");
			return;
		}
		stopc <-= 0;
		sent := ref Int (0);
		peermsg(p, msg, needwritec, sent);
		p.lastrecv = daytime->now();
		if(!sent.i)
			needwritec <-= nil;

	(p, err) := <-peererrc =>
if(dflag > 2) say("<-peererrc");
		p = peerbox.findid(p.id);
		if(p != nil)
			peerdrop(p, 0, err);

	(piece, begin, length, err) := <-diskwrittenc =>
if(dflag > 2) say("<-diskwrittenc");
		if(err != nil)
			return warnstop(sprint("error writing piece %d, begin %d, length %d: %s", piece, begin, length, err));

		pc := state.active.find(piece);
		if(pc == nil)
			pc = state.orphans.find(piece);
		if(pc == nil)
			raise sprint("data written for inactive piece %d", piece);

		# mark written blocks as such
		end := begin+length;
		for(off := begin; off < end; off += Blocksize) {
			block := off/Blocksize;
			if(!pc.have.get(block))
				raise "written block we don't have?";
			pc.written.set(block);
		}

		if(!pc.written.isfull())
			return;
		if(state.have.get(pc.index))
			raise "already have piece";

		say(sprint("last parts of piece %d have been written, verifying...", piece));

		state.active.del(pc.index);
		state.orphans.del(pc.index);

		# note: we haven't verified the piece, and do not yet advertise this piece.
		# marking it now prevents us from scheduling the piece on peers that connect while we are verifying.
		state.have.set(pc.index);
		spawn verifyhash(pc);

	(pc, err) := <-verifyc =>
if(dflag > 2) say("<-verifyc");
		if(err == nil) {
			wanthash := hex(state.t.hashes[pc.index]);
			kr->sha1(nil, 0, digest := array[kr->SHA1dlen] of byte, pc.hash);
			havehash := hex(digest);
			if(wanthash != havehash) {
				err = sprint("piece %d did not check out, want %s, have %s", pc.index, wanthash, havehash);
				putprogress(ref Progress.Hashfail (pc.index));
				nhashfails++;
			}
		}
		if(err != nil) {
			say(err);
			state.have.clear(pc.index);
			if(len pc.peersgiven == 1) {
				pp := peerbox.findid(hd pc.peersgiven);
				if(pp != nil)
					peerdrop(pp, 1, sprint("gave us faulty piece %d", pc.index));
			} else {
				for(l := pc.peersgiven; l != nil; l = tl l) {
					pp := peerbox.findid(hd l);
					if(pp != nil && pp.good < Ggood)
						peerdrop(pp, 1, sprint("might have given us faulty piece %d", pc.index));
				}
			}
			peerlwant(pc.index);
			return;
		}
		totalleft -= big state.t.piecelength(pc.index);
		putprogress(ref Progress.Piece (pc.index, state.have.have, state.have.total));
		for(fl := filesdone(pc.index); fl != nil; fl = tl fl) {
			f := hd fl;
			putprogress(ref Progress.Filedone (f.index, f.path, f.f.path));
		}

		writestate();
		say(sprint("piece %d now done", pc.index));

		if(done())
			putprogress(ref Progress.Done);

		if(len pc.peersgiven == 1) {
			pp := peerbox.findid(hd pc.peersgiven);
			if(pp != nil)
				pp.good = Ggood;
		} else {
			for(l := pc.peersgiven; l != nil; l = tl l) {
				pp := peerbox.findid(hd l);
				if(pp != nil)
					pp.good = max(pp.good, Ghalfgood);
			}
		}
		for(l := peerbox.peers; l != nil; l = tl l)
			peersend(hd l, ref Msg.Have(pc.index));

		if(!stopped && done()) {
			tracknow("completed");
			newpeers.markdone();
			for(l = peerbox.peers; l != nil; l = tl l) {
				p := hd l;
				if(p.isdone()) {
					say("done: dropping seed "+p.text());
					peerdrop(p, 0, "fellow seeder");
				}
			}
			ratio := ratio();
			if(maxratio > 0.0 && ratio >= maxratio)
				return stop(sprint("max ratio reached (%.2f)", ratio));
		}

	curwritec <-= curmainwrite =>
if(dflag > 2) say("<-curmainwrite");
		mainwrites.take();

	p := <-wantmsgc =>
if(dflag > 2) say(sprint("<-wantmsgc, peer %d", p.id));
		p.wantmsg = 1;
		if(stopped) {
			hangup(p.fd);
			if(p.wantmsg) {
				p.outmsgc <-= nil;
				p.wantmsg = 0;
			}
			return;
		}
		if(peerbox.findid(p.id) == nil) {
			if(!p.metamsgs.empty()) {
				peergive(p);
			} else {
				hangup(p.fd);
				p.outmsgc <-= nil;
				p.wantmsg = 0;
			}
			return;
		}
		peergive(p);

	(p, reads, err) := <-diskreadc =>
if(dflag > 2) say("<-diskreadc");
		if(stopped)
			return;
		if(err != nil)
			return warnstop(sprint("error reading piece: %s", err));
		if(peerbox.findid(p.id) == nil)
			return;

		for(l := reads; l != nil; l = tl l) {
			pick e := hd l {
			Piece =>
				p.reading.del(e.r.key());
				if(e.cancelled)
					continue;
			}
			p.reads.append(hd l);
		}
		peergive(p);
	}
}

main()
{
warn(1, sprint("main pid %d", pid()));
	roundgen = 0;
	bogusc = chan of ref Chunkwrite;
	if(!sflag)
		start();
	for(;;) {
		main0();
		peerflush();
		if(maxdowntotal >= big 0 && trafficdown.total() >= maxdowntotal)
			stop(sprint("maxdowntotal reached (max %bd, reached %bd)", maxdowntotal, trafficdown.total()));
		if(maxuptotal >= big 0 && trafficup.total() >= maxuptotal)
			stop(sprint("maxuptotal reached (max %bd, reached %bd)", maxuptotal, trafficup.total()));
	}
}

verifyhash(pc: ref Piece)
{
	need := state.t.piecelength(pc.index)-pc.hashoff;
	if(need > 0) {
		buf := array[need] of byte;
		herr := state.tx.preadx(buf, len buf, big pc.index*big state.t.piecelen+big pc.hashoff);
		if(herr != nil) {
			verifyc <-= (pc, "verifying hash: "+herr);
			return;
		}
		pc.hash = kr->sha1(buf, len buf, nil, pc.hash);
	}
	verifyc <-= (pc, nil);
}

roundticker()
{
	for(;;) {
		sys->sleep(10*1000);
		roundc <-= 0;
	}
}

swarmticker()
{
	r := load Rand Rand->PATH;
	r->init(sys->millisec()^pid());
	for(;;) {
		sys->sleep(10*1000+r->rand(6*1000)-3*1000);
		swarmc <-= 0;
	}
}


trackkicktime: int;
trackkick(n: int)
{
	if(!tracktoken)
		return;
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

tracknow(event: string)
{
	tr := ref Trackreq (state.t, localpeerid, trafficup.total(), trafficdown.total(), totalleft, listenport, event, trackerkey);
	spawn tracknow0(tr);
}

tracknow0(tr: ref Trackreq)
{
	bt->trackerget(tr);
}

track()
{
	for(;;) {
		tr := <-trackreqc;
		trackc <-= bt->trackerget(tr);
	}
}


listener(aconn: Sys->Connection)
{
	<-canlistenc;
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
		(ip, port) := ipport(remaddr);

		f := conn.dir+"/data";
		fd := sys->open(f, Sys->ORDWR);
		if(fd == nil) {
			warn(0, sprint("new connection, open %s: %r", f));
			continue;
		}

		(extensions, peerid, err) := handshake(fd);
		if(err != nil)
			say("error handshaking incoming connection: "+err);
		newpeerc <-= (remaddr, ip, port, nil, fd, extensions, peerid, err);
		<-canlistenc;
	}
}


# pass `max' bytes per second.
limiter(c: chan of (int, chan of int), maxc: chan of int, max: int)
{
	maxallow := min(max, Netiounit);

	last := sys->millisec();
	for(;;) alt {
	max = <-maxc  =>
		maxallow = min(max, Netiounit);
		
	(want, respc) := <-c =>
		if(max <= 0) {
			respc <-= want;
			continue;
		}

		give := min(maxallow, want);
		respc <-= give;

		msec := 1000*give/max;
		t0 := sys->millisec();
		msec -= util->max(0, min(1000, t0-last));
		if(msec > 0)
			sys->sleep(msec);
		last = t0+msec;
	}
}


dialkill(timeout: int, pidc: chan of int, ppid: int, np: ref Newpeer)
{
	pidc <-= pid();
	sys->sleep(timeout);
	kill(ppid);
	newpeerc <-= (np.addr, np.ip, np.port, np, nil, nil, nil, "dial/handshake: timeout");
}

dialer(np: ref Newpeer)
{
	timeout := Dialtimeout+randvar(6*1000);
	spawn dialkill(timeout, pidc := chan of int, pid(), np);
	killerpid := <-pidc;
	
	addr := sprint("net!%s", np.addr);
	(ok, conn) := sys->dial(addr, nil);
	if(ok < 0) {
		err := sprint("dial: %r");
		kill(killerpid);
		newpeerc <-= (np.addr, np.ip, np.port, np, nil, nil, nil, err);
		return;
	}

	say("dialed "+addr);
	fd := conn.dfd;

	(extensions, peerid, err) := handshake(fd);
	if(err != nil)
		fd = nil;
	kill(killerpid);
	newpeerc <-= (np.addr, np.ip, np.port, np, fd, extensions, peerid, err);
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
		return (nil, nil, sprint("peer does not speak bittorrent protocol (%q)", netdatafmt(rd)));

	extensions := rd[20:20+8];
	hash := rd[20+8:20+8+20];
	peerid := rd[20+8+20:];

	if(hex(hash) != hex(state.t.infohash))
		return (nil, nil, sprint("peer wants torrent hash %s, not %s", hex(hash), hex(state.t.infohash)));

	return (extensions, peerid, nil);
}

netdatafmt(d: array of byte): string
{
	if(len d > 64)
		d = d[:64];
	s := "";
	for(i := 0; i < len d; i++)
		case c := int d[i] {
		' ' to '~' =>
			s[len s] = c;
		* =>
			s += sprint("%%%02x", c);
		}
	return s;
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

peernetwriter(p: ref Peer)
{
	for(;;) {
		wantmsgc <-= p;
		mq := <-p.outmsgc;
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
if(dflag) say(sprint("-> peer %d: %s", p.id, m.text()));
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
		(off, reads, n) := <-p.readc;
		if(reads == nil)
			break;
		
		err := state.tx.preadx(buf := array[n] of byte, len buf, off);
		if(err == nil) {
			o := 0;
			for(l := reads; l != nil; l = tl l)
				pick e := hd l {
				Piece =>
					e.m = ref Msg.Piece (e.r.piece, e.r.begin, buf[o:o+e.r.length]);
					o += e.r.length;
				}
			if(o != n)
				raise "not all data from reads used";
		}
		diskreadc <-= (p, reads, err);
	}
	say("diskreader: stopping...");
}


Peerbox.new(): ref Peerbox
{
	b := ref Peerbox;
	b.ips = b.ips.new(31, nil);
	b.addrs = b.addrs.new(31, nil);
	b.ids = b.ids.new(31, nil);
	b.maskips = array[0] of string;
	b.maskippeers = b.maskippeers.new(31, nil);
	b.ndialed = 0;
	b.nseeding = 0;
	b.lastchange = daytime->now();
	b.fnow = b.fnow.new(5, nil);
	b.fdelay = b.fdelay.new(31, nil);
	b.fdelayq = b.fdelayq.new();
	return b;
}

Peerbox.add(b: self ref Peerbox, p: ref Peer)
{
	b.peers = p::b.peers;
	b.ips.add(p.np.ip, p);
	b.addrs.add(p.np.addr, p);
	b.ids.add(p.id, p);
	pl := b.maskippeers.find(p.maskip);
	if(pl == nil) {
		n := array[len b.maskips+1] of string;
		n[:] = b.maskips;
		n[len n-1] = p.maskip;
		b.maskips = n;
	} else
		b.maskippeers.del(p.maskip);
	b.maskippeers.add(p.maskip, p::pl);
	if(p.dialed)
		b.ndialed++;
	if(p.isdone())
		b.nseeding++;
	b.lastchange = daytime->now();
}

find(a: array of string, s: string): int
{
	for(i := 0; i < len a; i++)
		if(s == a[i])
			return i;
	return -1;
}

del[T](l: list of T, e: T): list of T
{
	r: list of T;
	for(; l != nil; l = tl l)
		if(hd l != e)
			r = e::r;
	return r;
}

Peerbox.del(b: self ref Peerbox, p: ref Peer)
{
	r: list of ref Peer;
	for(l := b.peers; l != nil; l = tl l)
		if((hd l) != p)
			r = hd l::r;
	b.peers = r;
	b.fnow.del(p.id);
	ql := b.fdelay.find(p.id);
	if(ql != nil) {
		b.fdelay.del(p.id);
		b.fdelayq.unlink(ql);
	}
	if(b.lucky == p)
		b.lucky = nil;
	b.ips.del(p.np.ip);
	b.addrs.del(p.np.addr);
	b.ids.del(p.id);
	pl := b.maskippeers.find(p.maskip);
	b.maskippeers.del(p.maskip);
	pl = del(pl, p);
	if(pl == nil) {
		i := find(b.maskips, p.maskip);
		if(i < 0)
			raise "missing masked ip";
		b.maskips[i:] = b.maskips[i+1:];
		b.maskips = b.maskips[:len b.maskips-1];
	} else
		b.maskippeers.add(p.maskip, pl);
	if(p.dialed)
		b.ndialed--;
	if(p.isdone())
		b.nseeding--;
	b.lastchange = daytime->now();
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

Peerbox.oldestunchoke(b: self ref Peerbox): ref Peer
{
	oldest: ref Peer;
	for(l := b.peers; l != nil; l = tl l) {
		p := hd l;
		if(!p.localchoking() && (oldest == nil || p.lastunchokemsg < oldest.lastunchokemsg))
			oldest = p;
	}
	return oldest;
}

Peerbox.maskipused(b: self ref Peerbox, i: int, maskip: string)
{
	if(b.maskips[i] != maskip)
		raise "mismatch masked ip";
	b.maskips[i:] = b.maskips[i+1:];
	b.maskips[len b.maskips-1] = maskip;
}

trafficscore(p: ref Peer): int
{
	return max(0, 5-int (big 50*p.down.total()/state.t.length));
}

timescore(p: ref Peer, now: int): int
{
	# scoring from 0 to 5 (inclusive).  score > 0 from 10 minutes on.
	secs := max(0, now-p.createtime-10*60);
	secs >>= 4;
	for(i := 0; i < 5 && secs > 0; i++)
		secs >>= 4;
	return i;
}

Peerbox.droppable(b: self ref Peerbox): ref Peer
{
	peers := l2a(b.peers);
	scores := array[len peers] of int;
	sum := 0;
	now := daytime->now();
	for(i := 0; i < len peers; i++) {
		p := peers[i];
		# note: the higher the score, the more likely disconnection is
		v := trafficscore(p)+timescore(p, now);
		if(p.rhave.isfull() && b.nseeding > len b.peers/2)
			v++;
		else if(!p.rhave.isfull() && b.nseeding < len b.peers/2)
			v++;
		if(p.dialed && b.ndialed > len b.peers/2)
			v++;
		else if(!p.dialed && b.ndialed < len b.peers/2)
			v++;
		scores[i] = v;
		sum += v;
	}
	r := rand->rand(sum);
	sum = 0;
	for(i = 0; i < len peers; i++) {
		if(r >= sum)
			return peers[i];
		sum += scores[i];
	}
warn(1, "droppable fail");
	return peers[len peers-1];
}

Peerbox.ninteresting(b: self ref Peerbox): int
{
	n := 0;
	for(l := b.peers; l != nil; l = tl l)
		if((hd l).localinterested())
			n++;
	return n;
}

Peerbox.ngiving(b: self ref Peerbox): int
{
	n := 0;
	for(l := b.peers; l != nil; l = tl l)
		if((hd l).localinterested() && !(hd l).remotechoking())
			n++;
	return n;
}

Peerbox.listflushnow(b: self ref Peerbox): list of ref Peer
{
	return tablist(b.fnow);
}

Peerbox.peekflushdelay(b: self ref Peerbox): ref Peer
{
	if(b.haveflushdelay())
		p := b.fdelayq.first.e;
	return p;
}

Peerbox.addflushnow(b: self ref Peerbox, p: ref Peer)
{
	if(b.fnow.find(p.id) != nil)
		return;
	l := b.fdelay.find(p.id);
	if(l != nil) {
		b.fdelay.del(p.id);
		b.fdelayq.unlink(l);
	}
	b.fnow.add(p.id, p);
}

Peerbox.addflushdelay(b: self ref Peerbox, p: ref Peer)
{
	if(b.fnow.find(p.id) != nil)
		return;
	if(b.fdelay.find(p.id) == nil) {
		l := b.fdelayq.append(p);
		b.fdelay.add(p.id, l);
	}
}

Peerbox.haveflushdelay(b: self ref Peerbox): int
{
	return b.fdelayq.length > 0;
}

Peerbox.delflush(b: self ref Peerbox, p: ref Peer)
{
	b.fnow.del(p.id);
	l := b.fdelay.find(p.id);
	if(l != nil) {
		b.fdelay.del(p.id);
		b.fdelayq.unlink(l);
	}
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

randvar(v: int): int
{
	return rand->rand(v)-v/2;
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

genrandom(n: int): array of byte
{
	d := array[n] of byte;
	for(i := 0; i < len d; i++)
		d[i] = byte rand->rand(256);
	return d;
}

ipport(s: string): (string, int)
{
	(ip, portstr) := str->splitstrl(s, "!");
	if(portstr != nil)
		portstr = portstr[1:];
	return (ip, int portstr);
}

aafmt(a: array of array of string): string
{
	s := "";
	for(i := 0; i < len a; i++) {
		s += "; ";
		r := "";
		for(j := 0; j < len a[i]; j++)
			r += ","+a[i][j];
		if(r != nil)
			s += r[1:];
	}
	if(s != nil)
		s = s[len "; ":];
	return s;
}

hangup(fd: ref Sys->FD)
{
	writefile(str->splitstrr(sys->fd2path(fd), "/").t0+"ctl", 0, array of byte "hangup");
}

warn(put: int, s: string)
{
	sys->fprint(sys->fildes(2), "%-11d %s\n", sys->millisec(), s);
	if(put)
		putprogress(ref Progress.Error (s));
}

pwarn(p: ref Peer, put: int, s: string)
{
	warn(put, sprint("peer %d: %s", p.id, s));
}

warnstop(s: string)
{
	warn(1, s);
	stop(s);
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
