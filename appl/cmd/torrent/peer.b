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
include "rand.m";
	rand: Rand;
include "lists.m";
	lists: Lists;
include "keyring.m";
	kr: Keyring;
include "security.m";
	random: Random;
include "math.m";
include "styx.m";
	styx: Styx;
	Tmsg, Rmsg: import styx;
include "styxservers.m";
	styxservers: Styxservers;
	Styxserver, Fid, Navigator, Navop: import styxservers;
include "tables.m";
	tables: Tables;
	Table: import tables;
include "util0.m";
	util: Util0;
	pid, kill, killgrp, min, warn, l2a, g32i, readfile, readfd, inssort, sizefmt, sizeparse: import util;

include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";
	bittorrent: Bittorrent;
	Bee, Msg, File, Torrent: import bittorrent;
include "../../lib/bittorrent/peer.m";
	misc: Misc;
	pools: Pools;
	Pool: import pools;
	rate: Rate;
	Traffic: import rate;
	pieces: Pieces;
	Piece, Block: import pieces;
	peers: Peers;
	Peer, Newpeer, Buf: import peers;
	requests: Requests;
	Req, Reqs, Batch: import requests;
	verify: Verify;
	sched: Schedule;
	state: State;


Dflag: int;
Pflag: int;
Lflag: int;
nofix: int;

torrentpath:	string;
torrent:	ref Torrent;
dstfds:		list of ref (ref Sys->FD, big);  # fd, size
statefd:	ref Sys->FD;
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
maxdownload	:= big -1;
maxupload	:= big -1;

# tracker
trackkickchan:	chan of int;
trackreqchan:	chan of (big, big, big, int, string);  # up, down, left, listenport, event
trackchan:	chan of (int, array of (string, int, array of byte), string);  # interval, peers, error

# dialer/listener
canlistenchan:	chan of int;
newpeerchan:	chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);

# upload/download rate limiter
upchan, downchan:	chan of (int, chan of int);

# progress/state
stopped := 0;
ndialers:	int;  # number of active dialers
rotateips:	ref Pool[string];  # masked ip address
faulty:		list of (string, int);  # ip, time
islistening:	int;  # whether listener() is listening

peerinmsgchan:	chan of (ref Peer, ref Msg, chan of list of ref (int, int, array of byte));
wantmsgchan:	chan of ref Peer;
diskwritechan:	chan of ref (int, int, array of byte);
diskwrittenchan:	chan of (int, int, int, string);
diskreadchan:	chan of (ref Peer, int, int, array of byte, string);
mainwrites:	list of ref (int, int, array of byte);

# ticker
tickchan: chan of int;


Progress: adt {
	next:	cyclic ref Progress;
	pick {
	Nil or
	Done or
	Started or
	Stopped =>
	Piece =>
		p,
		have,
		total:	int;
	Block =>
		p,
		b,
		have,
		total:	int;
	Pieces =>
		l:	list of int;
	Blocks =>
		p:	int;
		l:	list of int;
	Filedone =>
		index:	int;
		path,
		origpath:	string;
	Tracker =>
		interval:	int;
		npeers:	int;
		err:	string;
	}

	text:	fn(p: self ref Progress): string;
};

Progressfid: adt {
	fid:	int;
	r:	list of ref Tmsg.Read;
	last:	ref Progress;

	new:		fn(fid: int): ref Progressfid;
	putread:	fn(pf: self ref Progressfid, r: ref Tmsg.Read);
	read:		fn(pf: self ref Progressfid): ref Rmsg.Read;
	flushtag:	fn(pf: self ref Progressfid, tag: int): int;
};

Peerfid: adt {
	fid:	int;
	r:	list of ref Tmsg.Read;
	last:	ref Peerevent;

	new:		fn(fid: int): ref Peerfid;
	putread:	fn(pf: self ref Peerfid, r: ref Tmsg.Read);
	read:		fn(pf: self ref Peerfid): ref Rmsg.Read;
	flushtag:	fn(pf: self ref Peerfid, tag: int): int;
};

Schoking, Sunchoking, Sinterested, Suninterested: con iota;
Slocal, Sremote: con iota<<2;
Peerevent: adt {
	next:	cyclic ref Peerevent;
	pick {
	Nil =>	
	Endofstate =>
	Dialing =>
		addr:	string;
	Tracker =>
		addr:	string;
	New or
	Gone =>
		addr:	string;
		id:	int;
		peeridhex:	string;
		dialed:	int;
	Bad =>
		addr:	string;
		mtime:	int;
	State =>
		id:	int;
		s:	int;
	Piece =>
		id:	int;
		piece:	int;
	Pieces =>
		id:	int;
		pieces:	list of int;
	Done =>
		id:	int;
	}

	text:	fn(pp: self ref Peerevent): string;
};
progresshead:	ref Progress;
progressfids:	ref Table[ref Progressfid];
peereventhead:	ref Peerevent;
peerfids:	ref Table[ref Peerfid];


Qroot, Qctl, Qinfo, Qstate, Qfiles, Qprogress, Qpeerevents, Qpeers, Qpeerstracker, Qpeersbad: con iota;
Qfirst:	con Qctl;
Qlast:	con Qpeersbad;
tab := array[] of {
	(Qroot,		".",		Sys->DMDIR|8r555),
	(Qctl,		"ctl",		8r222),
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
	random = load Random Random->PATH;
	rand = load Rand Rand->PATH;
	rand->init(random->randomint(Random->ReallyRandom));
	lists = load Lists Lists->PATH;
	styx = load Styx Styx->PATH;
	styx->init();
	styxservers = load Styxservers Styxservers->PATH;
	styxservers->init(styx);
	tables = load Tables Tables->PATH;
	util = load Util0 Util0->PATH;
	util->init();
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init();

	misc = load Misc Misc->PATH;
	misc->init();
	pools = load Pools Pools->PATH;
	pools->init();
	rate = load Rate Rate->PATH;
	rate->init();
	pieces = load Pieces Pieces->PATH;
	pieces->init();
	requests = load Requests Requests->PATH;
	requests->init();
	peers = load Peers Peers->PATH;
	peers->init();
	verify = load Verify Verify->PATH;
	verify->init();
	state = load State State->PATH;
	state->init();
	sched = load Schedule Schedule->PATH;
	sched->init(state, peers, pieces);

	progressfids = progressfids.new(4, nil);
	progresshead = ref Progress.Nil (nil);
	peerfids = peerfids.new(4, nil);
	peereventhead = ref Peerevent.Nil (nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-DPLn] [-m ratio] [-d maxdownload] [-u maxupload] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'D' =>	Dflag++;
		'P' =>	Pflag++;
		'L' =>	bittorrent->dflag++;
		'n' =>	nofix = 1;
		'm' =>	maxratio = real arg->earg();
			if(maxratio <= 1.1)
				fail("invalid maximum ratio");
		'd' =>	maxdownload = sizeparse(arg->earg());
			if(maxdownload < big 0)
				fail("invalid maximum download rate");
		'u' =>	maxupload = sizeparse(arg->earg());
			if(maxupload < big (10*1024))
				fail("invalid maximum upload rate");
		* =>
			arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();

	sys->pctl(Sys->NEWPGRP, nil);

	err: string;
	torrentpath = hd args;
	(torrent, err) = Torrent.open(torrentpath);
	if(err != nil)
		fail(sprint("%s: %s", torrentpath, err));

	created: int;
	(dstfds, created, err) = torrent.openfiles(nofix, 0);
	if(err != nil)
		fail(err);

	if(created) {
		# all new files, we don't have pieces yet
		trackerevent = "started";
		say("no state file needed, all new files");
		state->piecehave = Bits.new(torrent.piececount);
	} else {
		# attempt to read state of pieces from .torrent.state file
		statefd = sys->open(torrent.statepath, Sys->ORDWR);
		if(statefd != nil) {
			say("using .state file");
			d := util->readfd(statefd, 64*1024);
			if(d == nil)
				fail(sprint("%r"));
			(state->piecehave, err) = Bits.mk(torrent.piececount, d);
			if(err != nil)
				fail(sprint("%s: invalid state", torrent.statepath));
		} else {
			# otherwise, read through all data
			say("starting to check all pieces in files...");
			state->piecehave = Bits.new(torrent.piececount);
			verify->torrenthash(dstfds, torrent, state->piecehave);
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

	totalleft = torrent.length;
	localpeerid = bittorrent->genpeerid();
	localpeeridhex = util->hex(localpeerid);

	trackkickchan = chan of int;
	trackreqchan = chan of (big, big, big, int, string);
	trackchan = chan of (int, array of (string, int, array of byte), string);

	canlistenchan = chan of int;
	newpeerchan = chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);
	tickchan = chan of int;

	upchan = chan of (int, chan of int);
	downchan = chan of (int, chan of int);

	peerinmsgchan = chan of (ref Peer, ref Msg, chan of list of ref (int, int, array of byte));
	wantmsgchan = chan of ref Peer;
	diskwritechan = chan[4] of ref (int, int, array of byte);
	diskwrittenchan = chan of (int, int, int, string);
	diskreadchan = chan of (ref Peer, int, int, array of byte, string);

	trafficup = Traffic.new();
	trafficdown = Traffic.new();
	trafficmetaup = Traffic.new();
	trafficmetadown = Traffic.new();

	pieces->prepare(torrent);
	state->prepare(torrent.piececount);

	rotateips = Pool[string].new(Pools->PoolRotateRandom);

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
	spawn ticker();
	spawn track();
	spawn limiter(upchan, int maxupload);
	spawn limiter(downchan, int maxdownload);
	spawn diskwriter(diskwritechan);

	time0 = daytime->now();
	spawn trackkick(0);

	navch := chan of ref Navop;
	spawn navigator(navch);

	nav := Navigator.new(navch);
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
			pf := Progressfid.new(m.fid);
			progressfids.add(pf.fid, pf);
			putprogressstate(pf.last);

		Qpeerevents =>
			pf := Peerfid.new(m.fid);
			peerfids.add(m.fid, pf);
			putpeerstate(pf.last);
		}

	Read =>
		fid := srv.getfid(m.fid);
		if(fid.qtype & Sys->QTDIR)
			return srv.default(m);
		q := int fid.path&16rff;

		case q {
		Qctl =>
			raise "bad mode";
		Qinfo =>
			t := torrent;
			s := "";
			s += sprint("fs 0\n");
			s += sprint("torrentpath %q\n", torrentpath);
			s += sprint("infohash %s\n", util->hex(t.hash));
			s += sprint("announce %q\n", t.announce);
			s += sprint("piecelen %d\n", t.piecelen);
			s += sprint("piececount %d\n", t.piececount);
			s += sprint("length %bd\n", t.length);
			srv.reply(styxservers->readstr(m, s));
		Qstate =>
			s := "";
			s += sprint("stopped %d\n", stopped);
			s += sprint("listenport %d\n", listenport);
			s += sprint("localpeerid %s\n", localpeeridhex);
			s += sprint("maxratio %.2f\n", maxratio);
			s += sprint("maxupload %bd\n", maxupload);
			s += sprint("maxdownload %bd\n", maxdownload);
			s += sprint("totalleft %bd\n", totalleft);
			s += sprint("totalup %bd\n", trafficup.total());
			s += sprint("totaldown %bd\n", trafficdown.total());
			s += sprint("rateup %d\n", trafficup.rate());
			s += sprint("ratedown %d\n", trafficdown.rate());
			s += sprint("eta %d\n", eta());
			srv.reply(styxservers->readstr(m, s));

		Qfiles =>
			s := "";
			for(l := torrent.files; l != nil; l = tl l) {
				f := hd l;
				s += sprint("%q %q %bd %d %d\n", f.path, f.origpath, f.length, f.pfirst, f.plast);
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
				for(l := peers->peers; l != nil; l = tl l)
					s += peerline(hd l);
				fid.data = array of byte s;
			}
			srv.reply(styxservers->readbytes(m, fid.data));

		Qpeerstracker =>
			if(m.offset == big 0) {
				s := "";
				for(l := peers->trackerpeers; l != nil; l = tl l)
					s += sprint("%q %q\n", (hd l).addr, util->hex((hd l).peerid));
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
			s := string m.data;
			if(s != nil && s[len s-1] == '\n')
				s = s[:len s-1];
			l := str->unquoted(s);
			if(l == nil)
				return replyerror(m, "missing command");
			cmd := hd l;
			l = tl l;
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
				if(len l != 1 || hd l == nil || str->toint(hd l, 10).t1 != nil)
					return replyerror(m, styxservers->Ebadarg);
				p := peers->peerfind(str->toint(hd l, 10).t0);
				if(p == nil)
					return replyerror(m, "no such peer");
				peerdrop(p, 0, nil);
			* =>
				return replyerror(m, sprint("bad command %#q", cmd));
			}
		}

	Flush =>
		for(l := listprogressfids(); l != nil; l = tl l)
			if((hd l).flushtag(m.tag))
				return;
		for(ll := listpeerfids(); ll != nil; ll = tl ll)
			if((hd ll).flushtag(m.tag))
				return;

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


next0(l, n: ref Progress): ref Progress
{
	l.next = n;
	return n;
}

putprogressstate(l: ref Progress)
{
	if(stopped)
		l = next0(l, ref Progress.Stopped);
	else
		l = next0(l, ref Progress.Started);
	if(isdone())
		l = next0(l, ref Progress.Done);
	else {
		lp := (state->piecehave).all();
		if(lp != nil)
			l = next0(l, ref Progress.Pieces (nil, lp));
		for(pl := pieces->pieces; pl != nil; pl = tl pl) {
			p := hd pl;
			lb := p.have.all();
			if(lb != nil)
				l = next0(l, ref Progress.Blocks (nil, p.index, lb));
		}
		for(fl := filesdone(-1); fl != nil; fl = tl fl) {
			f := hd fl;
			l = next0(l, ref Progress.Filedone (nil, f.index, f.path, f.origpath));
		}
	}
	l.next = progresshead;
}


next(l, n: ref Peerevent): ref Peerevent
{
	l.next = n;
	return n;
}

putpeerstate(l: ref Peerevent)
{
	for(f := faulty; f != nil; f = tl f)
		l = next(l, ref Peerevent.Bad (nil, (hd f).t0, (hd f).t1));
	for(t := peers->trackerpeers; t != nil; t = tl t)
		l = next(l, ref Peerevent.Tracker (nil, (hd t).addr));
	for(pl := peers->peers; pl != nil; pl = tl pl) {
		p := hd pl;
		l = next(l, ref Peerevent.New (nil, p.np.addr, p.id, p.peeridhex, p.dialed));
		if(p.isdone())
			l = next(l, ref Peerevent.Done (nil, p.id));
		else if(p.piecehave.have != 0)
			l = next(l, ref Peerevent.Pieces (nil, p.id, p.piecehave.all()));
	}
	l.next = ref Peerevent.Endofstate (peereventhead);
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

listprogressfids(): list of ref Progressfid
{
	r: list of ref Progressfid;
	a := progressfids.items;
	for(i := 0; i < len a; i++)
		for(l := a[i]; l != nil; l = tl l)
			r = (hd l).t1::r;
	return r;
}

putprogress(p: ref Progress)
{
	progresshead.next = p;
	progresshead = p;
	for(l := listprogressfids(); l != nil; l = tl l)
		while((rm := (hd l).read()) != nil)
			srv.reply(rm);
}

progresstags := array[] of {
"", "done", "started", "stopped", "piece", "block", "pieces", "blocks", "filedone", "tracker",
};
Progress.text(pp: self ref Progress): string
{
	s := progresstags[tagof pp];
	pick p := pp {
	Nil or
	Done or
	Started or
	Stopped =>	;
	Piece =>	s += sprint(" %d %d %d", p.p, p.have, p.total);
	Block =>	s += sprint(" %d %d %d %d", p.p, p.b, p.have, p.total);
	Pieces =>	for(l := p.l; l != nil; l = tl l)
				s += " "+string hd l;
	Blocks =>	s += sprint(" %d", p.p);
			for(l := p.l; l != nil; l = tl l)
				s += " "+string hd l;
	Filedone =>	s += sprint(" %d %q %q", p.index, p.path, p.origpath);
	Tracker =>	s += sprint(" %d %d %q", p.interval, p.npeers, p.err);
	* =>	raise "missing case";
	}
	return s;
}

Progressfid.new(fid: int): ref Progressfid
{
	return ref Progressfid (fid, nil, ref Progress.Nil (nil));
}

Progressfid.putread(pf: self ref Progressfid, r: ref Tmsg.Read)
{
	pf.r = r::pf.r;
}

Progressfid.read(pf: self ref Progressfid): ref Rmsg.Read
{
	if(pf.r == nil || pf.last.next == nil)
		return nil;

	pf.r = lists->reverse(pf.r);
	m := hd pf.r;
	pf.r = lists->reverse(tl pf.r);

	# note: the following knows that progress.text() always returns ascii
	s := "";
	while(pf.last.next != nil) {
		p := pf.last.next;
		ns := p.text();
		if(s != nil && len s+len ns+1 > m.count)
			break;
		if(ns != nil)
			ns[len ns] = '\n';
		s += ns;
		pf.last = p;
	}
	if(m.count < len s)
		s = s[:m.count];
	data := array of byte s;
	return ref Rmsg.Read (m.tag, data);
}

Progressfid.flushtag(pf: self ref Progressfid, tag: int): int
{
	r: list of ref Tmsg.Read;
	for(l := pf.r; l != nil; l = tl l)
		if((hd l).tag != tag)
			r = (hd l)::r;
	if(len r == len pf.r)
		return 0;
	pf.r = lists->reverse(r);
	return 1;
}

Peerfid.new(fid: int): ref Peerfid
{
	return ref Peerfid (fid, nil, ref Peerevent.Nil (nil));
}

Peerfid.putread(pf: self ref Peerfid, r: ref Tmsg.Read)
{
	pf.r = r::pf.r;
}

Peerfid.read(pf: self ref Peerfid): ref Rmsg.Read
{
	if(pf.r == nil || pf.last.next == nil)
		return nil;

	pf.r = lists->reverse(pf.r);
	m := hd pf.r;
	pf.r = lists->reverse(tl pf.r);

	s := "";
	while(pf.last.next != nil) {
		p := pf.last.next;
		ns := p.text();
		# note: the following depends on all data being ascii!
		if(s != nil && len s+len ns+1 > m.count)
			break;
		if(ns != nil)
			ns[len ns] = '\n';
		s += ns;
		pf.last = p;
	}
	if(len s > m.count)
		s = s[:m.count];
	return ref Rmsg.Read (m.tag, array of byte s);
}

Peerfid.flushtag(pf: self ref Peerfid, tag: int): int
{
	r: list of ref Tmsg.Read;
	for(l := pf.r; l != nil; l = tl l)
		if((hd l).tag != tag)
			r = hd l::r;
	if(len r == len pf.r)
		return 0;
	pf.r = lists->reverse(r);
	return 1;
}

peereventtags := array[] of {
"", "endofstate", "dialing", "tracker", "new", "gone", "bad", "state", "piece", "pieces", "done",
};
eventstatestr0 := array[] of {"local", "remote"};
eventstatestr1 := array[] of {"choking", "unchoking", "interested", "uninterested"};
Peerevent.text(pp: self ref Peerevent): string
{
	s := peereventtags[tagof pp];
	pick p := pp {
	Nil or
	Endofstate =>	;
	Dialing =>	s += sprint(" %q", p.addr);
	Tracker =>	s += sprint(" %q", p.addr);
	New or 
	Gone =>		s += sprint(" %q %d %s %d", p.addr, p.id, p.peeridhex, p.dialed);
	Bad =>		s += sprint(" %q %d", p.addr, p.mtime);
	State =>	s += sprint(" %d %s %s", p.id, eventstatestr0[p.s>>2], eventstatestr1[p.s&3]);
	Piece => 	s += sprint(" %d %d", p.id, p.piece);
	Pieces =>	s += sprint(" %d", p.id);
			for(l := p.pieces; l != nil; l = tl l)
				s += " "+string hd l;
	Done =>		s += sprint(" %d", p.id);
	}
	return s;
}

listpeerfids(): list of ref Peerfid
{
	r: list of ref Peerfid;
	a := peerfids.items;
	for(i := 0; i < len a; i++)
		for(l := a[i]; l != nil; l = tl l)
			r = (hd l).t1::r;
	return r;
}

putevent(p: ref Peerevent)
{
	peereventhead.next = p;
	peereventhead = p;
	for(l := listpeerfids(); l != nil; l = tl l)
		while((rm := (hd l).read()) != nil)
			srv.reply(rm);
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
	return (state->piecehave).n == (state->piecehave).have;
}

stop()
{
	# xxx probably more to do here.  we also want a pause() and a start()
	stopped = 1;
	putprogress(ref Progress.Stopped (nil));

	# disconnect from all peers and don't do further tracker requests
	peers->luckypeer = nil;
	rotateips = nil;
	for(l := peers->peers; l != nil; l = tl l)
		peerdrop(hd l, 0, nil);
}

writestate()
{
	d := (state->piecehave).d;
	n := sys->pwrite(statefd, d, len d, big 0);
	if(n != len d)
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
	for(i := 0; i < (peer.piecehave).n && n < (peer.piecehave).have; i++)
		if((peer.piecehave).get(i)) {
			state->piececounts[i]--;
			n++;
		}

	for(l := pieces->pieces; l != nil; l = tl l) {
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

	peers->peerdel(peer);

	# peernetreader+peernetwriter
	for(pids := peer.pids; pids != nil; pids = tl pids)
		kill(hd pids);

	spawn stopreadch(peer.readch);
	spawn stopwritech(peer.writech);
	peer.readch = nil;
	peer.writech = nil;
}

stopreadch(ch: chan of ref (int, int, int))
{
	ch <-= nil;
}

stopwritech(ch: chan of ref (int, int, array of byte))
{
	ch <-= nil;
}

dialpeers()
{
	say(sprint("dialpeers, %d trackerpeers %d peers", len peers->trackerpeers, len peers->peers));

	while(peers->trackerpeers != nil && ndialers < Dialersmax && len peers->peers < Peersmax && peers->peersdialed() < Peersdialedmax) {
		np := peers->trackerpeertake();
		if(peers->peerknownip(np.ip))
			continue;
		if(isfaulty(np.ip))
			continue;

		say("spawning dialproc for "+np.text());
		putevent(ref Peerevent.Dialing (nil, np.addr));
		spawn dialer(np);
		ndialers++;
	}
}

awaitpeer()
{
	if(peers->peersdialed() < Peersmax-Peersdialedmax && !islistening) {
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
		p.datamsgs = lists->reverse(m::lists->reverse(p.datamsgs));
	* =>
		p.metamsgs = lists->reverse(m::lists->reverse(p.metamsgs));
	}
}

peersend(p: ref Peer, msg: ref Msg)
{
	peersend0(p, msg);
	peergive(p);
}

account(p: ref Peer, msgs: list of ref Msg)
{
	for(l := msgs; l != nil; l = tl l) {
		msg := hd l;
		say(sprint("sending message: %s", msg.text()));
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
	}
}

peergive(p: ref Peer)
{
	if(!p.getmsg)
		return;

	if(p.metamsgs != nil) {
		p.getmsgch <-= p.metamsgs;
		account(p, p.metamsgs);
		p.metamsgs = nil;
		p.getmsg = 0;
	} else if(p.datamsgs != nil && !p.localchoking() && p.remoteinterested()) {
		m := hd p.datamsgs;
		p.datamsgs = tl p.datamsgs;
		account(p, m::nil);
		p.getmsgch <-= m::nil;
		p.getmsg = 0;
	}
}

blocksize(req: Req): int
{
	# first a quick check
	if(req.pieceindex < torrent.piececount-1)
		return Blocksize;

	# otherwise, the full check
	if(big req.pieceindex*big torrent.piecelen + big (req.blockindex+1)*big Blocksize > torrent.length)
		return int (torrent.length % big Blocksize);
	return Blocksize;
}

readblock(p: ref Peer)
{
	if(p.wants == nil)
		return;

	alt {
	p.readch <-= hd p.wants =>
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
		msgs = ref Msg.Request(req.pieceindex, req.blockindex*Blocksize, blocksize(req))::msgs;
	}
	if(msgs != nil)
		peersendmany(peer, lists->reverse(msgs));
}

schedule(p: ref Peer)
{
	if(!sched->needblocks(p))
		return;

	reqch := chan of ref (ref Piece, list of Req, chan of int);
	spawn sched->schedule(reqch, p);
	while((r := <-reqch) != nil) {
		(piece, reqs, donech) := *r;
		request(p, piece, reqs);
		donech <-= 0;
	}
}


# peer state

choke(p: ref Peer)
{
	peersend(p, ref Msg.Choke());
	p.state |= Peers->LocalChoking;
	putevent(ref Peerevent.State (nil, p.id, Slocal|Schoking));
}

unchoke(p: ref Peer)
{
	peersend(p, ref Msg.Unchoke());
	p.state &= ~Peers->LocalChoking;
	putevent(ref Peerevent.State (nil, p.id, Slocal|Sunchoking));
	p.lastunchoke = daytime->now();
}

wantpeerpieces(p: ref Peer): ref Bits
{
	b := (state->piecehave).clone();
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
			p.state &= ~Peers->LocalInterested;
			putevent(ref Peerevent.State (nil, p.id, Slocal|Suninterested));
			peersend(p, ref Msg.Notinterested());
		}
	} else {
		if(!wantpeerpieces(p).isempty()) {
			say("we are now interested in "+p.text());
			p.state |= Peers->LocalInterested;
			putevent(ref Peerevent.State (nil, p.id, Slocal|Sinterested));
			peersend(p, ref Msg.Interested());
		}
	}
}


# faulty

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
	putevent(ref Peerevent.Bad (nil, ip, now));
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
	mainwrites = lists->reverse(ref (b.piece, b.pieceoff, b.data)::lists->reverse(mainwrites));
	say("letting mains diskwriter handle write");
	b.clear();
}

batchflushcomplete(piece: ref Piece, block: int): int
{
	b := requests->batches(piece)[block/Batchsize];
	for(i := 0; i < len b.blocks; i++)
		if(!piece.have.get(b.blocks[i]))
			return 0;

	start := (block/Batchsize)*Batchsize*Blocksize;
	end := start+Batchsize*Blocksize;
	for(l := peers->peers; l != nil; l = tl l) {
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
		for(l := peers->peers; l != nil; l = tl l) {
			p := hd l;
			if(misc->maskip(p.np.ip) == ipmasked && (peer == nil || p.lastunchoke < peer.lastunchoke))
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
	for(l := peers->peers; l != nil; l = tl l) {
		p := hd l;
		if(!p.localchoking() && p.remoteinterested()) {
			nunchoked++;
			if(oldest == nil || p.lastunchoke < oldest.lastunchoke)
				oldest = p;
		}
	}

	# find all peers that we may want to unchoke randomly
	others: list of ref Peer;
	for(l = peers->peers; l != nil; l = tl l) {
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
	misc->randomize(othersa);
	for(i := 0; i < len othersa && nunchoked+i < Seedunchokedmax; i++)
		unchoke(othersa[i]);
}

chokingdownload(gen: int)
{
	# new optimistic unchoke?
	if(gen % 3 == 0)
		peers->luckypeer = nextoptimisticunchoke();

	# make sorted array of all peers, sorted by upload rate, then by interestedness
	allpeers := array[len peers->peers] of ref (ref Peer, int);  # peer, rate
	i := 0;
	luckyindex := -1;
	for(l := peers->peers; l != nil; l = tl l) {
		if(peers->luckypeer != nil && hd l == peers->luckypeer)
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
		allpeers[unchokeend-1] = ref (peers->luckypeer, 0);
	}

	# now unchoke the N peers, and all non-interested peers that are faster.  choke all other peers if they weren't already.
	for(i = 0; i < len allpeers; i++) {
		(p, nil) := *allpeers[i];
		if(p == nil)
			say(sprint("bad allpeers, len allpeers %d, len peers %d, i %d, unchokeend %d, luckyindex %d, nintr %d", len allpeers, len peers->peers, i, unchokeend, luckyindex, nintr));
		if(i < unchokeend && p.localchoking())
			unchoke(p);
		else if(i >= unchokeend && !p.localchoking())
			choke(p);
	}
}

handleinmsg(peer: ref Peer, msg: ref Msg, needwritechan: chan of list of ref (int, int, array of byte))
{
	handleinmsg0(peer, msg, needwritechan);
	if(tagof msg == tagof Msg.Piece)
		needwritechan <-= nil;
}

# xxx fix this code to never either block entirely or delay on disk/net i/o
handleinmsg0(peer: ref Peer, msg: ref Msg, needwritechan: chan of list of ref (int, int, array of byte))
{
	peer.msgseq++;

	msize := msg.packedsize();
	dsize := 0;
	pick m := msg {
	Piece =>
		# xxx count pieces we did not request as overhead (meta)?
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
		if(peer.remotechoking())
			return say(sprint("%s choked us twice...", peer.text())); # xxx disconnect?

		say(sprint("%s choked us...", peer.text()));
		peer.state |= Peers->RemoteChoking;
		putevent(ref Peerevent.State (nil, peer.id, Sremote|Schoking));

	Unchoke =>
		if(!peer.remotechoking())
			return say(sprint("%s unchoked us twice...", peer.text())); # xxx disconnect?

		say(sprint("%s unchoked us", peer.text()));
		peer.state &= ~Peers->RemoteChoking;
		putevent(ref Peerevent.State (nil, peer.id, Sremote|Sunchoking));

		schedule(peer);

	Interested =>
		if(peer.remoteinterested())
			return say(sprint("%s is interested again...", peer.text())); # xxx disconnect?

		say(sprint("%s is interested", peer.text()));
		peer.state |= Peers->RemoteInterested;
		putevent(ref Peerevent.State (nil, peer.id, Sremote|Sinterested));

		if(!peer.localchoking() && len peers->peersactive() >= Unchokedmax && !isdone()) {
			# xxx choke slowest peer that is not the optimistic unchoke
		}

	Notinterested =>
		if(!peer.remoteinterested())
			return say(sprint("%s is uninterested again...", peer.text())); # xxx disconnect?

		say(sprint("%s is no longer interested", peer.text()));
		peer.state &= ~Peers->RemoteInterested;
		putevent(ref Peerevent.State (nil, peer.id, Sremote|Suninterested));
		peer.wants = nil;

		# if peer was unchoked, we'll unchoke another during next round

	Have =>
		if(m.index >= torrent.piececount)
			return peerdrop(peer, 1, sprint("%s sent 'have' for invalid piece %d, disconnecting", peer.text(), m.index));

		if(peer.piecehave.get(m.index)) {
			say(sprint("%s already had piece %d", peer.text(), m.index)); # xxx disconnect?
		} else {
			state->piececounts[m.index]++;
			putevent(ref Peerevent.Piece (nil, peer.id, m.index));
		}

		say(sprint("remote now has piece %d", m.index));
		peer.piecehave.set(m.index);

		interesting(peer);
		if(!peer.remotechoking())
			schedule(peer);

	Bitfield =>
		if(peer.msgseq != 1)
			return peerdrop(peer, 1, sprint("%s sent bitfield after first message, disconnecting", peer.text()));

		err: string;
		(peer.piecehave, err) = Bits.mk(torrent.piececount, m.d);
		if(err != nil)
			return peerdrop(peer, 1, sprint("%s sent bogus bitfield message: %s, disconnecting", peer.text(), err));

		say("remote sent bitfield, haves "+peer.piecehave.text());

		n := 0;
		for(i := 0; i < peer.piecehave.n && n < peer.piecehave.have; i++)
			if(peer.piecehave.get(i)) {
				state->piececounts[i]++;
				n++;
			}

		interesting(peer);

	Piece =>
		say(sprint("%s sent data for piece=%d begin=%d length=%d", peer.text(), m.index, m.begin, len m.d));

		req := Req.new(m.index, m.begin/Blocksize);
		if(blocksize(req) != len m.d)
			return peerdrop(peer, 1, sprint("%s sent bad size for block %s, disconnecting", peer.text(), req.text()));

		piece := pieces->piecefind(m.index);
		if(piece == nil)
			return peerdrop(peer, 1, sprint("got data for inactive piece %d", m.index));

		if(!peer.reqs.take(req)) {
			exp := "nothing";
			if(!peer.reqs.isempty())
				exp = peer.reqs.peek().text();
			return peerdrop(peer, 1, sprint("%s sent block %s, expected %s, disconnecting", peer.text(), req.text(), exp));
		}

		blockindex := m.begin/Blocksize;

		if(piece.have.get(blockindex))
			return say("already have this block, skipping...");

		# possibly send cancel to other peer
		busy := piece.busy[blockindex];
		busypeer: ref Peer;
		if(busy.t0 >= 0 && busy.t0 != peer.id) {
			busypeer = peers->peerfind(busy.t0);
			piece.busy[blockindex].t0 = -1;
		} else if(busy.t1 >= 0 && busy.t1 != peer.id) {
			busypeer = peers->peerfind(busy.t1);
			piece.busy[blockindex].t1 = -1;
		}
		if(busypeer != nil) {
			peer.reqs.cancel(Req.new(m.index, m.begin/Blocksize));
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
			needwritechan <-= lists->reverse(needwrites);
		}

		# progress with piece
		if(piece.hashstateoff == m.begin)
			piece.hashadd(m.d);

		piece.have.set(blockindex);
		piece.done[blockindex] = peer.id;
		totalleft -= big len m.d;
		putprogress(ref Progress.Block (nil, piece.index, blockindex, piece.have.have, piece.have.n));

		if(piece.isdone()) {
			# flush all bufs about this piece, also from other peers, to disk.  to make hash-checking read right data.
			for(l := peers->peers; l != nil; l = tl l) {
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

		if(pieces->blockhave(peer.wants, b))
			return say("peer already wanted block, skipping");

		if(len peer.wants >= Blockqueuemax)
			return peerdrop(peer, 0, sprint("peer scheduled one too many blocks, already has %d scheduled, disconnecting", len peer.wants));

		peer.wants = lists->reverse(b::lists->reverse(peer.wants));
		if(!peer.localchoking() && peer.remoteinterested() && len peer.wants > 0)
			readblock(peer);

	Cancel =>
		b := Block.new(m.index, m.begin, m.length);
		say(sprint("%s sent cancel for %s", peer.text(), b.text()));
		nwants := pieces->blockdel(peer.wants, b);
		if(len nwants == len peer.wants)
			return say("peer did not want block before, skipping");
		peer.wants = nwants;
	}
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
	boguschan := chan of ref (int, int, array of byte);

	for(;;) {
	# warning: for block runs to end of main()

	# the alt statement doesn't do conditional sending/receiving on a channel.
	# so, before we start the alt, we evaluate the condition.
	# if true, we use the real channel we want to send on.
	# if false, we use a bogus channel without receiver, so the case is never taken.
	curwritechan := boguschan;
	curmainwrite: ref (int, int, array of byte);
	if(mainwrites != nil) {
		curmainwrite = hd mainwrites;
		curwritechan = diskwritechan;
	}
 
	alt {
	mm := <-msgc =>
		if(mm == nil) {
			killgrp(pid());
			fail("styx eof");
		}
		pick m := mm {
		Readerror =>
			killgrp(pid());
			fail("styx read error: "+m.error);
		}
		dostyx(mm);

	<-tickchan =>
		say(sprint("ticking, %d peers", len peers->peers));
		for(l := peers->peers; l != nil; l = tl l) {
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

	<-trackkickchan =>
		if(!stopped) {
			trackreqchan <-= (trafficup.total(), trafficdown.total(), totalleft, listenport, trackerevent);
			trackerevent = nil;
		}

	(interval, newpeers, trackerr) := <-trackchan =>
		if(trackerr != nil) {
			warn(sprint("tracker error: %s", trackerr));
			putprogress(ref Progress.Tracker (nil, interval, 0, trackerr));
		} else {
			say("main, new peers");
			putprogress(ref Progress.Tracker (nil, interval, len newpeers, nil));
			for(i := 0; i < len newpeers; i++) {
				(ip, port, peerid) := newpeers[i];
				if(util->hex(peerid) == localpeeridhex)
					continue;  # skip self
				np := Newpeer(sprint("%s!%d", ip, port), ip, peerid);
				say("new: "+np.text());
				peers->trackerpeerdel(np);
				if(!peers->peerconnected(np.addr)) {
					peers->trackerpeeradd(np);
					putevent(ref Peerevent.Tracker (nil, np.addr));
				} else
					say("already connected to "+np.text());
			}
		}
		dialpeers();

		# schedule next call to tracker
		if(interval < Intervalmin)
			interval = Intervalmin;
		if(interval > Intervalmax)
			interval = Intervalmax;
		if(daytime->now() < time0+Intervalstartupperiod && !isdone() && len peers->peers+len peers->trackerpeers < Peersmax && interval > Intervalneed)
			interval = Intervalneed;

		say(sprint("next call to tracker will be in %d seconds", interval));
		spawn trackkick(interval);

	(dialed, np, peerfd, extensions, peerid, err) := <-newpeerchan =>
		if(!dialed)
			islistening = 0;

		if(err != nil) {
			warn(sprint("%s: %s", np.text(), err));
		} else if(util->hex(peerid) == localpeeridhex) {
			say("connected to self, dropping connection...");
		} else if(peers->peerknownip(np.ip)) {
			say("new connection from known ip address, dropping new connection...");
		} else if(isfaulty(np.ip)) {
			say(sprint("connected to faulty ip %s, dropping connection...", np.ip));
		} else {
			peer := Peer.new(np, peerfd, extensions, peerid, dialed, torrent.piececount);
			pidch := chan of int;
			spawn peernetreader(pidch, peer);
			spawn peernetwriter(pidch, peer);
			spawn diskwriter(peer.writech);
			spawn diskreader(peer);
			peer.pids = <-pidch::<-pidch::peer.pids;
			peers->peeradd(peer);
			say("new peer "+peer.fulltext());
			putevent(ref Peerevent.New (nil, peer.np.addr, peer.id, peer.peeridhex, peer.dialed));

			rotateips.pooladdunique(misc->maskip(np.ip));

			if((state->piecehave).have == 0) {
				peersend(peer, ref Msg.Keepalive());
			} else {
				say("sending bitfield to peer: "+(state->piecehave).text());
				peersend(peer, ref Msg.Bitfield((state->piecehave).bytes()));
			}

			if(len peers->peersactive() < Unchokedmax) {
				say("unchoking rare new peer: "+peer.text());
				unchoke(peer);
			}
		}

		if(dialed) {
			ndialers--;
			dialpeers();
		} else
			awaitpeer();

	(peer, msg, needwritech) := <-peerinmsgchan =>
		if(msg == nil)
			return peerdrop(peer, 0, "eof from peer "+peer.text());

		handleinmsg(peer, msg, needwritech);

	(pieceindex, begin, length, err) := <-diskwrittenchan =>
		if(err != nil)
			raise sprint("error writing piece %d, begin %d, length %d: %s", pieceindex, begin, length, err);

		piece := pieces->piecefind(pieceindex);
		if(piece == nil)
			raise sprint("data written for inactive piece %d", pieceindex);

		# mark written blocks as such
		first := begin/Blocksize; 
		n := (length+Blocksize-1)/Blocksize;
		for(i := 0; i < n; i++)
			piece.written.set(first+i);

		if(!piece.written.isfull())
			continue;

		say("last parts of piece have been written, verifying...");

		wanthash := util->hex(torrent.piecehashes[piece.index]);
		(piecehash, herr) := verify->piecehash(dstfds, torrent.piecelen, piece);
		if(herr != nil)
			fail("verifying hash: "+herr);
		havehash := util->hex(piecehash);
		if(wanthash != havehash) {
			# xxx blame peers
			say(sprint("%s did not check out, want %s, have %s, disconnecting", piece.text(), wanthash, havehash));
			piece.hashstate = nil;
			piece.hashstateoff = 0;
			piece.have.clearall();
			if(Dflag) {
				for(i = 0; i < len piece.busy; i++)
					if(piece.busy[i].t0 >= 0 || piece.busy[i].t1 >= 0)
						raise sprint("piece %d should be complete, but block %d is busy", piece.index, i);
			}
			
			# xxx what do to with other peers?
			return;
		}

		(state->piecehave).set(piece.index);
		pieces->piecedel(pieces->piecefind(piece.index));
		putprogress(ref Progress.Piece (nil, piece.index, (state->piecehave).have, (state->piecehave).n));
		for(fl := filesdone(piece.index); fl != nil; fl = tl fl) {
			f := hd fl;
			putprogress(ref Progress.Filedone (nil, f.index, f.path, f.origpath));
		}

		# this could have been the last piece this peer had, making us no longer interested
		for(l := peers->peers; l != nil; l = tl l)
			interesting(hd l);

		writestate();
		say("piece now done: "+piece.text());
		say(sprint("pieces: have %s, busy %s", (state->piecehave).text(), (state->piecebusy).text()));

		for(l = peers->peers; l != nil; l = tl l)
			peersend(hd l, ref Msg.Have(piece.index));

		if(isdone()) {
			trackerevent = "completed";
			spawn trackkick(0);
			npeers: list of ref Peer;
			for(l = peers->peers; l != nil; l = tl l) {
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
			peers->peers = lists->reverse(npeers);
			sys->print("DONE!\n");
			putprogress(ref Progress.Done (nil));
		}

	curwritechan <-= curmainwrite =>
		say("queued mainwrite to diskwriter");
		mainwrites = tl mainwrites;

	peer := <-wantmsgchan =>
		say(sprint("%s: wants message", peer.text()));
		peer.getmsg = 1;

		if(!stopped && isdone()) {
			ratio := ratio();
			if(ratio >= 1.1 && ratio >= maxratio) {
				say(sprint("stopping due to max ratio achieved (%.2f)", ratio));
				stop();
				continue;
			}
		}

		peergive(peer);
		if(!peer.localchoking() && peer.remoteinterested() && peer.getmsg && len peer.wants > 0)
			readblock(peer);

	(peer, pieceindex, begin, buf, err) := <-diskreadchan =>
		if(err != nil)
			raise sprint("error writing piece %d, begin %d, length %d: %s", pieceindex, begin, len buf, err);

		say("have block from disk");
		peersend(peer, ref Msg.Piece(pieceindex, begin, buf));
		# xxx calculate if we need to request another block already
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
		
		np := Newpeer(remaddr, str->splitstrl(remaddr, "!").t0, nil);
		if(err != nil)
			say("error handshaking incoming connection: "+err);
		newpeerchan <-= (0, np, fd, extensions, peerid, err);
		<-canlistenchan;
	}
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


dialkiller(pidch: chan of int, ppid: int, np: Newpeer)
{
	pidch <-= pid();
	sys->sleep(Dialtimeout*1000);
	kill(ppid);
	newpeerchan <-= (1, np, nil, nil, nil, sprint("dial/handshake %s: timeout", np.addr));
}

dialer(np: Newpeer)
{
	ppid := pid();
	spawn dialkiller(pidch := chan of int, ppid, np);
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

	if(util->hex(hash) != util->hex(torrent.hash))
		return (nil, nil, sprint("peer wants torrent hash %s, not %s", util->hex(hash), util->hex(torrent.hash)));

	return (extensions, peerid, nil);
}


# read, going through limiter
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

# write, going through limiter
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
	(size, nil) := g32i(buf, 0);
	buf = array[size] of byte;

	n = netread(fd, buf, len buf);
	if(n < 0)
		return (nil, sprint("reading: %r"));
	if(n < len buf)
		return (nil, sprint("short read"));

	return Msg.unpack(buf);
}


peernetreader(pidch: chan of int, peer: ref Peer)
{
	pidch <-= pid();

	needwritechan := chan of list of ref (int, int, array of byte);
	for(;;) {
		(m, err) := msgread(peer.fd);
		if(err != nil)
			fail("reading msg: "+err);  # xxx return error to main
		if(Pflag)
			sys->fprint(sys->fildes(2), "<< %s\n", m.text());
		peerinmsgchan <-= (peer, m, needwritechan);
		if(m == nil)
			break;

		if(tagof m == tagof Msg.Piece) {
			needwrites := <-needwritechan;
			say(sprint("peernetreader: have %d needwrites", len needwrites));
			if(needwrites != nil) {
				for(l := needwrites; l != nil; l = tl l)
					peer.writech <-= hd l;  # will block if disk is slow, slowing down peer as well
				<-needwritechan;
			}
		}
	}
}

peernetwriter(pidch: chan of int, peer: ref Peer)
{
	pidch <-= pid();

	for(;;) {
		wantmsgchan <-= peer;
		ml := <- peer.getmsgch;
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
			
			if(Pflag)
				sys->fprint(sys->fildes(2), ">> %s\n", m.text());
			size := m.packedsize();
			say(sprint("peernetwriter: len d %d, o %d, size %d", len d, o, size));
			d[o:] = m.pack();
			o += size;
		}
		n := netwrite(peer.fd, d, len d);
		if(n != len d)
			fail(sprint("writing msgs: %r"));
	}
}

diskwriter(reqch: chan of ref (int, int, array of byte))
{
	for(;;) {
		req := <-reqch;
		if(req == nil) {
			say("diskwriter: stopping...");
			break;
		}
		(piece, begin, buf) := *req;

		off := big piece*big torrent.piecelen + big begin;
		err := bittorrent->torrentpwritex(dstfds, buf, len buf, off);
		diskwrittenchan <-= (piece, begin, len buf, err);
	}
}

diskreader(peer: ref Peer)
{
	for(;;) {
		req := <-peer.readch;
		if(req == nil) {
			say("diskreader: stopping...");
			break;
		}
		(piece, begin, length) := *req;
		off := big piece*big torrent.piecelen + big begin;
		err := bittorrent->torrentpreadx(dstfds, buf := array[length] of byte, len buf, off);
		diskreadchan <-= (peer, piece, begin, buf, err);
	}
}


# misc

filedone(f: ref File): int
{
	for(i := f.pfirst; i <= f.plast; i++)
		if(!(state->piecehave).get(i))
			return 0;
	return 1;
}

filesdone(pindex: int): list of ref File
{
	l: list of ref File;
	for(fl := torrent.files; fl != nil; fl = tl fl) {
		f := hd fl;
		if(pindex >= 0 && pindex > f.plast)
			break;
		if((pindex < 0 || f.pfirst >= pindex && pindex <= f.plast) && filedone(f))
			l = f::l;
	}
	return lists->reverse(l);
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
	r := trafficdown.rate();
	if(r <= 0)
		return -1;
	return int (totalleft / big r);
}

say(s: string)
{
	if(Dflag)
		warn(s);
}

fail(s: string)
{
	warn(s);
	killgrp(pid());
	raise "fail:"+s;
}
