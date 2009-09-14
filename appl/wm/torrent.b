implement WmTorrent;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
	draw: Draw;
	Display, Image, Point, Rect: import draw;
include "arg.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "daytime.m";
	daytime: Daytime;
include "string.m";
	str: String;
include "rand.m";
	rand: Rand;
include "tk.m";
	tk: Tk;
include "tkclient.m";
	tkclient: Tkclient;
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "tables.m";
	tables: Tables;
	Table: import tables;
include "util0.m";
	util: Util0;
	warn, pid, kill, killgrp, max, min, unhex, writefile, rev, l2a, sizefmt: import util;

WmTorrent: module {
	init:	fn(ctxt: ref Draw->Context, argv: list of string);
};


File: adt {
	path,
	origpath:	string;
	length:	big;
	ps, pe:	int;	# piece start,end
	pieces:		ref Bits;
};

Info: adt {
	torrentpath,
	infohash,
	tracker:	string;
	piecelen,
	npieces:	int;
	length:		big;
	files:		array of ref File;
};

State: adt {
	stopped:	int;
	up,
	down,
	left:	big;
	upr,
	downr:	int;
	eta:	int;
	hashfails,
	npeers,
	nseeds,
	ndialed,
	knownpeers,
	knownseeds,
	unusedpeers,
	listeners,
	nfaultypeers:	int;
};

Config: adt {
	listenport:	int;
	localpeerid:	string;
	maxratio:	real;
	maxuprate,
	maxdownrate,
	maxuptotal,
	maxdowntotal:	big;
	debugpeer,
	debuglib,
	debugpeerlib:	int;
};

Prog: adt {
	done:	int;
	pieces:	ref Bits;
	started:	int;
};

Peercount: adt {
	up, down:	big;
	upr, downr:	int;
	lreqs,
	olreqs,
	rreqs,
	orreqs:		int;
};

Choking, Interested: con 1<<iota;
Remote:	con 0;
Local:	con 2;
Peer: adt {
	addr:	string;
	id:	int;
	peerid:	string;
	dialed:	int;
	done:	int;
	pieces:	ref Bits;
	state:	int;
	counts:	Peercount;
};

Badpeer: adt {
	ip:	string;
	time:	int;
	peerid:	string;
	banreason:	string;
};

Tracker: adt {
	interval,
	next,
	npeers:	int;
	err:	string;
};

Bar: adt {
	panel:	string;
	p:	array of int;	# make it a bit string?  and find something to do for the available pieces bar...
	pixels:	array of real;	# 0..1 for x pixels
	i,
	filli:	ref Image;
	fill:	int;
	div:	int;	# number of pieces that is full fill color
	pixelscale:	real;
};

Stateinterval: con 3*1000;
Peerinterval: con 5*1000;

peers: ref Table[ref Peer];
badpeers: list of ref Badpeer;

dflag: int;
top: ref Tk->Toplevel;
wmctl: chan of string;
disp: ref Display;
mtpt: string;

info: ref Info;
state: ref State;
config: ref Config;

statefd: ref Sys->FD;
statepid := -1;
statec: chan of ref State;

ctlfd: ref Sys->FD;
newconfigc: chan of int;
configc: chan of ref Config;

prog: ref Prog;
peersfd: ref Sys->FD;
peerspid := -1;
peersc: chan of list of (int, Peercount);
lasttracker: ref Tracker;
lasterror: string;

havebar,
availbar: ref Bar;

Vmain, Vfiles, Vpeers, Vbadpeers, Vtorrentlog, Vlog: con iota;
view := Vmain;
views := array[] of {"main", "files", "peers", "badpeers", "torrentlog", "log", "errors"};

tkcmds0 := array[] of {
"frame .ctl",
"button .ctl.info -text main -command {send cmd main}",
"button .ctl.files -text files -command {send cmd files}",
"button .ctl.peers -text peers -command {send cmd peers}",
"button .ctl.badpeers -text badpeers -command {send cmd badpeers}",
"button .ctl.torrentlog -text torrentlog -command {send cmd torrentlog}",
"button .ctl.log -text peerlog -command {send cmd log}",
"button .ctl.errors -text errors -command {send cmd errors}",
"button .ctl.x -text x -command {send cmd x}",
"pack .ctl.info .ctl.files .ctl.peers .ctl.badpeers .ctl.torrentlog .ctl.log .ctl.errors .ctl.x -fill x -side left",
"pack .ctl -fill x",

"frame .v", # view

"frame .v.m",	# main
"canvas .v.m.c -yscrollcommand {.v.m.yscroll set} -xscrollcommand {.v.m.xscroll set}",
"scrollbar .v.m.yscroll -command {.v.m.c yview}",
"scrollbar .v.m.xscroll -orient horizontal -command {.v.m.c xview}",
"frame .v.m.c.m",
".v.m.c create window 0 0 -window .v.m.c.m -anchor nw",
"pack .v.m.yscroll -side left -fill y",
"pack .v.m.xscroll -side bottom -fill x",
"frame .v.m.c.m.p -borderwidth 5",	# pieces
"label .v.m.c.m.p.lhave -text have:",
"label .v.m.c.m.p.lavail -text avail:",
"panel .v.m.c.m.p.have -height 20 -width 400",	# have
"panel .v.m.c.m.p.avail -height 20 -width 400",	# avail
"grid .v.m.c.m.p.lhave -row 0 -column 0 -sticky w -pady 2 -padx 10",
"grid .v.m.c.m.p.have -row 0 -column 1 -sticky we -pady 2 -padx 10",
"grid .v.m.c.m.p.lavail -row 1 -column 0 -sticky w -pady 2 -padx 10",
"grid .v.m.c.m.p.avail -row 1 -column 1 -sticky we -pady 2 -padx 10",
"frame .v.m.c.m.s -borderwidth 5",	# state
"frame .v.m.c.m.s.ctl",
"button .v.m.c.m.s.ctl.start -text start -command {send cmd ctl start}",
"button .v.m.c.m.s.ctl.stop -text stop -command {send cmd ctl stop}",
"button .v.m.c.m.s.ctl.track -text track -command {send cmd ctl track}",
"label .v.m.c.m.s.ctl.ldebug -text debug:",
"button .v.m.c.m.s.ctl.dpeer -text peer -command {send cmd ctl debug peer}",
"button .v.m.c.m.s.ctl.dlib -text lib -command {send cmd ctl debug lib}",
"button .v.m.c.m.s.ctl.dpeerlib -text peerlib -command {send cmd ctl debug peerlib}",
"label .v.m.c.m.s.ctl.error",
"pack .v.m.c.m.s.ctl.start .v.m.c.m.s.ctl.stop .v.m.c.m.s.ctl.track .v.m.c.m.s.ctl.error .v.m.c.m.s.ctl.ldebug .v.m.c.m.s.ctl.dpeer .v.m.c.m.s.ctl.dlib .v.m.c.m.s.ctl.dpeerlib -side left -anchor w",
"frame .v.m.c.m.s.g",
"pack .v.m.c.m.s.ctl .v.m.c.m.s.g -anchor w",
"frame .v.m.c.m.c -borderwidth 5",	# config
"frame .v.m.c.m.c.g",
"pack .v.m.c.m.c.g -anchor w",
"frame .v.m.c.m.i -borderwidth 5",	# info
"pack .v.m.c.m.p .v.m.c.m.s .v.m.c.m.c .v.m.c.m.i -anchor w",
"pack .v.m.c -anchor w -fill both -expand 1",

"frame .v.f -bg blue",	# files
"frame .v.p",	# peers
"frame .v.b",	# badpeers
"frame .v.t",	# torrentlog
"frame .v.l",	# peerlog
"frame .v.e",	# errors

# files
"canvas .v.f.c -yscrollcommand {.v.f.yscroll set} -xscrollcommand {.v.f.xscroll set}",
"frame .v.f.c.g",
".v.f.c create window 0 0 -window .v.f.c.g -anchor nw",
"scrollbar .v.f.yscroll -command {.v.f.c yview}",
"scrollbar .v.f.xscroll -orient horizontal -command {.v.f.c xview}",
"pack .v.f.yscroll -side left -fill y",
"pack .v.f.xscroll -side bottom -fill x",
"pack .v.f.c -fill both -expand 1",

# peers
"canvas .v.p.c -yscrollcommand {.v.p.yscroll set} -xscrollcommand {.v.p.xscroll set}",
"frame .v.p.c.g",
".v.p.c create window 0 0 -window .v.p.c.g -anchor nw",
"scrollbar .v.p.yscroll -command {.v.p.c yview}",
"scrollbar .v.p.xscroll -orient horizontal -command {.v.p.c xview}",
"pack .v.p.yscroll -side left -fill y",
"pack .v.p.xscroll -side bottom -fill x",
"pack .v.p.c -fill both -expand 1",

# badpeers
"canvas .v.b.c -yscrollcommand {.v.b.yscroll set} -xscrollcommand {.v.b.xscroll set}",
"frame .v.b.c.g",
".v.b.c create window 0 0 -window .v.b.c.g -anchor nw",
"scrollbar .v.b.yscroll -command {.v.b.c yview}",
"scrollbar .v.b.xscroll -orient horizontal -command {.v.b.c xview}",
"pack .v.b.yscroll -side left -fill y",
"pack .v.b.xscroll -side bottom -fill x",
"pack .v.b.c -fill both -expand 1",

# torrentlog
"scrollbar .v.t.scroll -command {.v.t.t yview}",
"pack .v.t.scroll -side left -fill y",
"text .v.t.t -yscrollcommand {.v.t.scroll set}",
"pack .v.t.t -fill both -expand 1",

# peerlog
"scrollbar .v.l.scroll -command {.v.l.t yview}",
"pack .v.l.scroll -side left -fill y",
"text .v.l.t -yscrollcommand {.v.l.scroll set}",
"pack .v.l.t -fill both -expand 1",

# errors
"scrollbar .v.e.scroll -command {.v.e.t yview}",
"pack .v.e.scroll -side left -fill y",
"text .v.e.t -yscrollcommand {.v.e.scroll set}",
"pack .v.e.t -fill both -expand 1",

"pack .v.m -anchor w -fill both -expand 1",
"pack .v -anchor w -fill both -expand 1",

"bind . <Configure> {send cmd resize}",  # xxx doesn't work?
};

init(ctxt: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	if(ctxt == nil)
		fail("no window context");
	draw = load Draw Draw->PATH;
	arg := load Arg Arg->PATH;
	bufio = load Bufio Bufio->PATH;
	daytime = load Daytime Daytime->PATH;
	str = load String String->PATH;
	rand = load Rand Rand->PATH;
	tk = load Tk Tk->PATH;
	tkclient = load Tkclient Tkclient->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	tables = load Tables Tables->PATH;
	util = load Util0 Util0->PATH;
	util->init();

	dflag++;

	arg->init(args);
	arg->setusage(arg->progname()+" [-d] /mnt/torrent");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	dflag++;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 1)
		arg->usage();
	mtpt = hd args;

	sys->pctl(Sys->NEWPGRP, nil);
	tkclient->init();
	(top, wmctl) = tkclient->toplevel(ctxt, "", "wm/torrent", Tkclient->Appl);

	tkcmdc := chan of string;
	tk->namechan(top, tkcmdc, "cmd");
	tkcmds(tkcmds0);

	peers = peers.new(32, nil);
	info = readinfo();
	statefd = sys->open(mtpt+"/state", Sys->OREAD);
	if(statefd == nil)
		fail(sprint("open state: %r"));
	ctlfd = sys->open(mtpt+"/ctl", Sys->ORDWR);
	if(ctlfd == nil)
		fail(sprint("open ctl: %r"));
	peersfd = sys->open(mtpt+"/peers", Sys->OREAD);
	if(peersfd == nil)
		fail(sprint("open peers: %r"));
	prog = ref Prog (0, Bits.new(info.npieces), 0);

	infogrid := list of {
	l2("infohash",	info.infohash),
	l2("tracker",	info.tracker),
	l2("pieces",	sprint("%d, %s each", info.npieces, sizefmt(big info.piecelen))),
	l2("length",	sprint("%s (%bd bytes)", sizefmt(info.length), info.length)),
	};
	tkgrid(".v.m.c.m.i", infogrid);

	setfiles();
	setpeers();
	setbadpeers();

	tkcmd("pack propagate . 0");
	tkcmd(". configure -width 740 -height 560");

	tkclient->onscreen(top, nil);
	tkclient->startinput(top, "kbd"::"ptr"::nil);

	disp = ctxt.display;
	havebar = ref Bar (".v.m.c.m.p.have", array[info.npieces] of {* => 0}, nil, nil, disp.color(Draw->Blue), Draw->Blue, 1, 0.0);
	barinit(havebar);
	availbar = ref Bar (".v.m.c.m.p.avail", array[info.npieces] of {* => 0}, nil, nil, disp.color(Draw->Green), Draw->Green, 5, 0.0);
	barinit(availbar);

	config = readconfig();
	setconfig();

	state = readstate();
	statec = chan of ref State;
	spawn readstates(pidc := chan of int);
	statepid = <-pidc;

	newconfigc = chan of int;
	configc = chan of ref Config;
	spawn readconfigs();

	peersc = chan of list of (int, Peercount);

	spawn reader(mtpt+"/progress", progressc := chan of list of string);
	spawn reader(mtpt+"/peerevents", peereventsc := chan of list of string);

	for(;;) alt {
	s := <-top.ctxt.kbd =>
		tk->keyboard(top, s);
	s := <-top.ctxt.ptr =>
		tk->pointer(top, *s);
	s := <-top.ctxt.ctl or
	s = <-top.wreq =>
		tkclient->wmctl(top, s);
	menu := <-wmctl =>
		case menu {
		"exit" =>
			killgrp(sys->pctl(0, nil));
			exit;
		* =>
			tkclient->wmctl(top, menu);
		}

	s := <-tkcmdc =>
		say(sprint("cmd: %q", s));
		cmd(s);

	s := <-statec =>
		state = s;
		setstate();
		tkcmd("update");

	l := <-peersc =>
		for(; l != nil; l = tl l) {
			(id, pc) := hd l;
			p := peers.find(id);
			if(p == nil)
				continue; # we'll see this peer later
			p.counts = pc;
		}
		setpeers();
		tkcmd("update");

	l := <-progressc =>
		redraw := 0;
		for(; l != nil; l = tl l) {
			s := hd l;
			t := l2a(str->unquoted(s));
			checkwords("progress", s, t, progresswords);
			tkcmd(".v.t.t insert end '"+s+"\n");
			redraw = progressword(t) || redraw;
		}
		if(redraw) {
			setstate();
			setfiles();
		}
		tkcmd("update");

	cfg := <-configc =>
		config = cfg;
		setconfig();

	l := <-peereventsc =>
		redraw := 0;
		for(; l != nil; l = tl l) {
			s := hd l;
			t := l2a(str->unquoted(s));
			checkwords("peerevents", s, t, peerwords);
			tkcmd(".v.l.t insert end '"+s+"\n");
			redraw = peerword(t) || redraw;
		}
		if(redraw) {
			setstate();
			setpeers();
		}
		tkcmd("update");
	}
}

setstate()
{
	if(view != Vmain)
		return;

	s := state;
	stategrid := list of {
	l2("stopped",		string s.stopped),
	l2("eta",		etastr(s.eta)),
	l2("progress",		sprint("%3d%%, %s left, %d/%d pieces, %d hash fails", prog.pieces.have*100/prog.pieces.n, sizefmt(s.left), prog.pieces.have, prog.pieces.n, s.hashfails)),
	l2("total",		sprint("%5s   up, %5s   down, ratio: %s ", sizefmt(s.up), sizefmt(s.down), ratio(s.up, s.down))),
	l2("rate",		sprint("%5s/s up, %5s/s down", sizefmt(big s.upr), sizefmt(big s.downr))),
	l2("connected",		sprint("%d peer%s (%d seed%s), %d dialed", s.npeers, trails(s.npeers), s.nseeds, trails(s.nseeds), s.ndialed)),
	l2("known",		sprint("%d peer%s (%d seed%s), %d in dial queue, %d listeners, %d faulty", s.knownpeers, trails(s.knownpeers), s.knownseeds, trails(s.knownseeds), s.unusedpeers, s.listeners, s.nfaultypeers)),
	l2("tracker",		trackerstr()),
	l2("last error",	lasterror),
	};
	tkgrid(".v.m.c.m.s.g", stategrid);
	setscrollregion(".v.m.c", ".v.m.c.m");
}

ratio(a, b: big): string
{
	if(a == big 0)
		return "0.00";
	if(b == big 0)
		return "∞";
	return sprint("%.2f", real a/real b);
}

setconfig()
{
	if(view != Vmain)
		return;

	c := config;
	configgrid := list of {
	l2("listen port",	string c.listenport),
	l2("local peerid",	c.localpeerid),
	l2("max",		sprint("ratio %s,  rate: up %s, down %s,  total: up %s, down %s", ratiofmt(c.maxratio), infsizefmt(c.maxuprate), infsizefmt(c.maxdownrate), infsizefmt(c.maxuptotal), infsizefmt(c.maxdowntotal))),
	l2("debug",		sprint("peer %d, lib %d, peerlib %d", c.debugpeer, c.debuglib, c.debugpeerlib)),
	};
	tkgrid(".v.m.c.m.c.g", configgrid);
	setscrollregion(".v.m.c", ".v.m.c.m");
}

ratiofmt(r: real): string
{
	if(r == 0.0)
		return "∞";
	return sprint("%.2f", r);
}

trails(n: int): string
{
	if(n == 1)
		return "";	
	return "s";
}

trailes(n: int): string
{
	if(n == 1)
		return "";
	return "es";
}

infsizefmt(v: big): string
{
	if(v < big 0)
		return "∞";
	return sizefmt(v);
}

trackerstr(): string
{
	t := lasttracker;
	if(t == nil)
		return "n/a";
	s := sprint("interval %d, ", t.interval);
	if(t.err != nil)
		s += t.err;
	else
		s += sprint("%d peers", t.npeers);
	s += sprint(", next in %4ds", max(0, t.next-daytime->now()));
	return s;
}

readstates(pidc: chan of int)
{
	pidc <-= pid();
	for(;;) {
		statec <-= readstate();
		sys->sleep(Stateinterval);
	}
}

readconfigs()
{
	for(;;) {
		<-newconfigc;
		configc <-= readconfig();
	}
}

readpeers(pidc: chan of int)
{
	pidc <-= pid();
	for(;;) {
		peersc <-= readpeers0();
		sys->sleep(Peerinterval);
	}
}

progresswords := array[] of {
("endofstate",	0),
("done",	0),
("started",	0),
("stopped",	0),
("newctl",	0),
("piece",	3),
("block",	4),
("pieces",	-1),
("blocks",	-2),
("filedone",	3),
("tracker",	4),
("error",	1),
("hashfail",	1),
};
progressword(t: array of string): int
{
	case t[0] {
	"endofstate" =>	;
	"done" =>
		prog.done = 1;
		prog.pieces.setall();
		for(i := 0; i < len info.files; i++)
			info.files[i].pieces.setall();
		barfill(havebar);
		barflush(havebar);
		return 1;
	"started" =>
		prog.started = 1;
		return 1;
	"stopped" =>
		prog.started = 0;
		return 1;
	"newctl" =>
		alt {
		newconfigc <-= 1 =>	;
		* =>	;
		}
	"piece" =>
		p := getint(t[1]);
		setfilepiece(p);
		barhave(havebar, p);
		barflush(havebar);
		return 1;
	"pieces" =>
		for(i := 1; i < len t; i++) {
			p := getint(t[i]);
			setfilepiece(p);
			barhave(havebar, p);
		}
		barflush(havebar);
		return 1;
	"block" or
	"blocks" =>	; # not keeping track of blocks
	"filedone" =>
		f := info.files[getint(t[1])];
		f.pieces.setall();
		setfiles();
		return 1;
	"tracker" =>
		lasttracker = ref Tracker (getint(t[1]), daytime->now()+getint(t[2]), getint(t[3]), t[4]);
		return 1;
	"error" =>
		lasterror = t[1];
		tkcmd(".v.e.t insert end '"+t[1]+"\n");
		return 1;
	"hashfail" =>
		lasterror = sprint("piece %d failed hashcheck", getint(t[1]));
	* =>	raise "missing case";
	}
	return 0;
}

setfilepiece(p: int)
{
	prog.pieces.set(p);
	for(i := 0; i < len info.files; i++) {
		f := info.files[i];
		if(p < f.ps)
			continue;
		if(p >= f.pe)
			break;
		f.pieces.set(p-f.ps);
	}
}

peerwords := array[] of {
("endofstate",	0),
("dialing",	1),
("tracker",	1),
("new",		4),
("gone",	1),
("bad",		4),
("state",	3),
("piece",	2),
("pieces",	-2),
("done",	1),
};
peerword(t: array of string): int
{
	case t[0] {
	"endofstate" =>
		;
	"dialing" or
	"tracker" =>
		; # not keeping track of peers from tracker
	"new" =>
		c := Peercount (big 0, big 0, 0, 0, 0, 0, 0, 0);
		p := ref Peer (t[1], getint(t[2]), peeridfmtstr(t[3]), getint(t[4]), 0, Bits.new(info.npieces), 0, c);
		peers.add(p.id, p);
		return 1;
	"gone" =>
		p := getpeer(getint(t[1]));
		peers.del(p.id);

		# xxx should do this more efficiently
		barclear(availbar);
		for(l := peerall(); l != nil; l = tl l) {
			p = hd l;
			for(pl := p.pieces.all(); pl != nil; pl = tl pl)
				barhave(availbar, hd pl);
		}
		barflush(availbar);
		return 1;
	"bad" =>
		badpeers = ref Badpeer (t[1], getint(t[2]), peeridfmtstr(t[3]), t[4])::badpeers;
		setbadpeers();
		return 1;
	"state" =>
		p := getpeer(getint(t[1]));
		v := 0;
		case t[2] {
		"local" =>	v = Local;
		"remote" =>	v = Remote;
		* =>	fail(sprint("bad peer state %#q", t[2]));
		}
		case t[3] {
		"choking" =>		p.state |= Choking<<v;
		"unchoking" =>		p.state &= ~(Choking<<v);
		"interested" =>		p.state |= Interested<<v;
		"uninterested" =>	p.state &= ~(Interested<<v);
		* =>	fail(sprint("bad peer state %#q", t[3]));
		}
		return 1;
	"piece" =>
		i := getint(t[2]);
		p := getpeer(getint(t[1]));
		p.pieces.set(i);
		barhave(availbar, i);
		barflush(availbar);
		return 1;
	"pieces" =>
		p := getpeer(getint(t[1]));
		for(i := 2; i < len t; i++) {
			pc := getint(t[i]);
			p.pieces.set(pc);
			barhave(availbar, pc);
		}
		barflush(availbar);
		return 1;
	"done" =>
		p := getpeer(getint(t[1]));
		if(!p.done) {
			# we haven't seen anything from peer yet, this is part of state
			p.done = 1;
			p.pieces.setall();
			for(i := 0; i < p.pieces.n; i++)
				barhave(availbar, i);
			barflush(availbar);
		}
		return 1;
	* =>	raise "missing case";
	}
	return 0;
}

getint(s: string): int
{
	(r, rem) := str->toint(s, 10);
	if(s == nil || rem != nil)
		fail(sprint("bad int %#q", s));
	return r;
}

getbig(s: string): big
{
	(r, rem) := str->tobig(s, 10);
	if(s == nil || rem != nil)
		fail(sprint("bad big %#q", s));
	return r;
}

getreal(s: string): real
{
	(r, rem) := str->toreal(s, 10);
	if(s == nil || rem != nil)
		fail(sprint("bad real %#q", s));
	return r;
}

getpeer(id: int): ref Peer
{
	p := peers.find(id);
	if(p == nil)
		fail(sprint("missing peer, id %d", id));
	return p;
}

checkwords(s, l: string, t: array of string, w: array of (string, int))
{
	if(len t == 0)
		fail(sprint("bad %s line: %s", s, l));
	for(i := 0; i < len w; i++)
		if(w[i].t0 == t[0]) {
			n := w[i].t1;
			if(n >= 0 && len t-1 != n)
				fail(sprint("bad %s line, needs %d args, has %d: %s", s, n, len t-1, l));
			else if(n < 0 && len t-1 < -n-1)  # note: minus values are off by one, to be able to denote 0 or more
				fail(sprint("bad %s line, needs minimum %d args, has %d: %s", s, -n-1, len t-1, l));
			return;
		}
	fail(sprint("bad %s line, unknown op %#q: %s", s, t[0], l));
}

reader(f: string, c: chan of list of string)
{
	fd := sys->open(f, Sys->OREAD);
	if(fd == nil)
		fail(sprint("open %q: %r", f));
	buf := array[Sys->ATOMICIO] of byte;
	for(;;) {
		n := sys->read(fd, buf, len buf);
		if(n < 0)
			fail(sprint("reading %q: %r", f));
		c <-= sys->tokenize(string buf[:n], "\n").t1;
	}
}

cmd(s: string)
{
	l := sys->tokenize(s, " ").t1;
	if(l == nil)
		return;
	op := hd l;
	l = tl l;
	case op {
	"x" =>
		setscrollregion(".v.m.c", ".v.m.c.m");

	"ctl" =>
		err := writefile(mtpt+"/ctl", 0, array of byte str->quoted(l));
		tkcmd(".v.m.c.m.s.ctl.error configure -text '"+err);
		tkcmd("update");

	"main" or
	"files" or
	"peers" or
	"badpeers" or
	"torrentlog" or
	"log" or
	"errors" =>
		nview := getindex(views, op);
		if(view == nview)
			return;
		view = nview;
		tkcmd("pack forget [pack slaves .v]");
		tkcmd(sprint("pack .v.%s -anchor w -fill both -expand 1", op[:1]));
		case view {
		Vmain =>
			setstate();
			spawn readstates(pidc := chan of int);
			statepid = <-pidc;
		Vfiles =>
			setfiles();
		Vpeers =>
			setpeers();
			spawn readpeers(pidc := chan of int);
			peerspid = <-pidc;
		Vbadpeers =>
			setbadpeers();
		}
		if(view != Vmain && statepid >= 0) {
			kill(statepid);
			statepid = -1;
		}
		if(view != Vpeers && peerspid >= 0) {
			kill(peerspid);
			peerspid = -1;
		}
		tkcmd("update");
	* =>
		say(sprint("unknown command %#q", op));
	}
}

getindex(a: array of string, s: string): int
{
	for(i := 0; i < len a; i++)
		if(a[i] == s)
			return i;
	raise "missing case";
}

readinfo(): ref Info
{
	i := ref Info;
	for(l := readlines("info"); l != nil; l = tl l) {
		t := l2a(str->unquoted(hd l));
		if(len t != 2)
			fail(sprint("bad info line, not two tokens: %s", hd l));
		arg := t[1];
		case t[0] {
		"fs" =>		if(getint(arg) != 0)
					fail(sprint("unexpected fs version %d", getint(arg)));
		"torrentpath" =>	i.torrentpath = arg;
		"infohash" =>	i.infohash = arg;
		"announce" =>	i.tracker = arg;
		"piecelen" =>	i.piecelen = getint(arg);
		"piececount" =>	i.npieces = getint(arg);
		"length" =>	i.length = getbig(arg);
		* =>	fail(sprint("unexpected info key %q", t[0]));
		}
	}

	files: list of ref File;
	for(l = readlines("files"); l != nil; l = tl l) {
		t := l2a(str->unquoted(hd l));
		if(len t != 5)
			fail(sprint("bad files line, not five tokens: %s", hd l));
		ps := getint(t[3]);
		pe := 1+getint(t[4]);
		f := ref File (t[0], t[1], big t[2], ps, pe, Bits.new(pe-ps));
		files = f::files;
	}
	i.files = l2a(rev(files));
	return i;
}

readconfig(): ref Config
{
	sys->seek(ctlfd, big 0, Sys->SEEKSTART);
	b := bufio->fopen(ctlfd, Bufio->OREAD);
	if(b == nil)
		fail(sprint("fopen ctl: %r"));
	c := ref Config;
	for(l := readlines0(b); l != nil; l = tl l) {
		t := l2a(str->unquoted(hd l));
		if(len t != 2)
			fail(sprint("bad ctl line, not two tokens: %s", hd l));
		v := t[1];
		case t[0] {
		"listenport" =>		c.listenport = getint(v);
		"localpeerid" =>	c.localpeerid = peeridfmtstr(v);
		"maxratio" =>		c.maxratio = getreal(v);
		"maxuprate" =>		c.maxuprate = getbig(v);
		"maxdownrate" =>	c.maxdownrate = getbig(v);
		"maxuptotal" =>		c.maxuptotal = getbig(v);
		"maxdowntotal" =>	c.maxdowntotal = getbig(v);
		"debugpeer" =>		c.debugpeer = getint(v);
		"debuglib" =>		c.debuglib = getint(v);
		"debugpeerlib" =>	c.debugpeerlib = getint(v);
		* =>	fail(sprint("unexpected ctl key %q", t[0]));
		}
	}
	return c;
}

readstate(): ref State
{
	sys->seek(statefd, big 0, Sys->SEEKSTART);
	b := bufio->fopen(statefd, Bufio->OREAD);
	if(b == nil)
		fail(sprint("fopen state: %r"));
	s := ref State;
	for(l := readlines0(b); l != nil; l = tl l) {
		t := l2a(str->unquoted(hd l));
		if(len t != 2)
			fail(sprint("bad state line, not two tokens: %s", hd l));
		v := t[1];
		case t[0] {
		"stopped" =>	s.stopped = getint(v);
		"totalleft" =>	s.left = getbig(v);
		"totalup" =>	s.up = getbig(v);
		"totaldown" =>	s.down = getbig(v);
		"rateup" =>	s.upr = getint(v);
		"ratedown" =>	s.downr = getint(v);
		"eta" =>	s.eta = getint(v);
		"hashfails" =>	s.hashfails = getint(v);
		"peers" =>	s.npeers = getint(v);
		"seeds" =>	s.nseeds = getint(v);
		"dialed" =>	s.ndialed = getint(v);
		"knownpeers" =>	s.knownpeers = getint(v);
		"knownseeds" =>	s.knownseeds = getint(v);
		"unusedpeers" =>	s.unusedpeers = getint(v);
		"listenpeers" =>	s.listeners = getint(v);
		"faultypeers" =>	s.nfaultypeers = getint(v);
		* =>	fail(sprint("unexpected state key %q", t[0]));
		}
	}
	return s;
}

readpeers0(): list of (int, Peercount)
{
	sys->seek(peersfd, big 0, Sys->SEEKSTART);
	b := bufio->fopen(peersfd, Bufio->OREAD);
	if(b == nil)
		fail(sprint("fopen peers: %r"));
	r: list of (int, Peercount);
	for(l := readlines0(b); l != nil; l = tl l) {
		t := l2a(str->unquoted(hd l));
		if(len t != 25)
			fail(sprint("bad state line, expected 26 tokens, saw %d", len t));
		id := getint(t[0]);
		up := getbig(t[9]);
		upr := getint(t[10]);
		down := getbig(t[12]);
		downr := getint(t[13]);
		lreqs := getint(t[21]);
		olreqs := getint(t[22]);
		rreqs := getint(t[23]);
		orreqs := getint(t[24]);
		r = (id, Peercount(up, down, upr, downr, lreqs, olreqs, rreqs, orreqs))::r;
	}
	return r;
}

readlines(n: string): list of string
{
	f := mtpt+"/"+n;
	b := bufio->open(f, Bufio->OREAD);
	if(b == nil)
		fail(sprint("open %q: %r", f));
	return readlines0(b);
}

readlines0(b: ref Iobuf): list of string
{
	l: list of string;
	for(;;) {
		s := b.gets('\n');
		if(s == nil)
			break;
		if(s[len s-1] == '\n')
			s = s[:len s-1];
		l = s::l;
	}
	return rev(l);
}

setscrollregion(w, subw: string)
{
        tkcmd(sprint("%s configure -scrollregion {0 0 [%s cget -width] [%s cget -height]}", w, subw, subw));
}

setfiles()
{
	if(view != Vfiles)
		return;

	filegrid := l3("path", "size", "progress")::nil;
	for(i := 0; i < len info.files; i++) {
		f := info.files[i];
		p := f.pieces;
		filegrid = l3(f.path, sizefmt(f.length), sprint("%3d%% %d/%d", 100*p.have/p.n, p.have, p.n))::filegrid;
	}
	tkgrid(".v.f.c.g", rev(filegrid));
	setscrollregion(".v.f.c", ".v.f.c.g");
}

setpeers()
{
	if(view != Vpeers)
		return;

	peergrid := l10("id", "dir", "rate", "total", "pieces", " l/r", "reqs", "oreqs", "peerid", "addr")::nil;
	for(l := peerall(); l != nil; l = tl l) {
		p := hd l;
		dir := "in";
		if(p.dialed)
			dir = "out";
		pr := sprint("%3d%%", 100*p.pieces.have/info.npieces);
		lr := sprint("%s/%s", statefmt(p.state>>Local), statefmt(p.state>>Remote));
		c := p.counts;
		reqstr := sprint("%d/%d", c.lreqs, c.rreqs);
		oreqstr := sprint("%d/%d", c.olreqs, c.orreqs);
		rate := sprint("%s/%s", sizefmt(big c.upr), sizefmt(big c.downr));
		total := sprint("%s/%s", sizefmt(c.up), sizefmt(c.down));
		peergrid = l10(string p.id, dir, rate, total, pr, lr, reqstr, oreqstr, p.peerid, p.addr)::peergrid;
	}
	tkgrid(".v.p.c.g", rev(peergrid));
	setscrollregion(".v.p.c", ".v.p.c.g");
}

peerall(): list of ref Peer
{
	a := peers.items;
	r: list of ref Peer;
	for(i := 0; i < len a; i++)
		for(l := a[i]; l != nil; l = tl l)
			r = (hd l).t1::r;
	return r;
}

statefmt(v: int): string
{
	s := "  ";
	if(v & Choking)
		s[0] = 'c';
	if(v & Interested)
		s[1] = 'i';
	return s;
}

setbadpeers()
{
	if(view != Vbadpeers)
		return;

	badpeergrid := l4("ip", "until", "peerid", "reason")::nil;
	now := daytime->now();
	for(l := badpeers; l != nil; l = tl l) {
		b := hd l;
		badpeergrid = l4(b.ip, etastr(max(0, b.time-now)), b.peerid, b.banreason)::badpeergrid;
	}
	tkgrid(".v.b.c.g", rev(badpeergrid));
	setscrollregion(".v.b.c", ".v.b.c.g");
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
		s = sprint("%dm %ds", n/Min, n%Min);
	else if(n < Day)
		s = sprint("%dh %dm", n/Hour, n%Hour/Min);
	else if(n < Year)
		s = sprint("%dd %dh", n/Day, n%Day/Hour);
	else
		s = ">= year";
	return s;
}

peeridfmtstr(s: string): string
{
	d := unhex(s);
	if(d == nil)
		return s;
	s = "";
	for(i := 0; i < len d; i++)
		case c := int d[i] {
		'-' or
		'a' to 'z' or
		'A' to 'Z' or
		'0' to '9' =>
			s[len s] = c;
		* =>
			s += sprint("%02x", c);
		}
	return s;
}

barinit(b: ref Bar)
{
	x := int tkcmd(sprint("%s cget actwidth", b.panel));
	y := int tkcmd(sprint("%s cget actheight", b.panel));
say(sprint("barinit, %d x %d", x, y));
	b.pixels = array[x-2] of {* => real 0.0};
	b.i = disp.newimage(Rect((0,0), (x, y)), disp.image.chans, 0, Draw->White);
	if(b.i == nil)
		fail(sprint("newimage: %r"));
	err := tk->putimage(top, b.panel, b.i, disp.opaque);
	if(err != nil)
		warn("putimage: "+err);
	tkcmd(sprint("%s dirty; update", b.panel));
	b.pixelscale = real (x-2)/real info.npieces;
}

barclear(b: ref Bar)
{
	b.p = array[len b.p] of {* => 0};
	b.pixels = array[len b.pixels] of {* => 0.0};
	b.i.draw(b.i.r, disp.white, nil, ZP);
}

barresize(b: ref Bar)
{
	barinit(b);
	for(i := 0; i < len b.p; i++)
		if(b.p[i])
			barhave(b, i);
	barflush(b);
}

barflush(b: ref Bar)
{
	b.i.flush(Draw->Flushnow);
	tkcmd(sprint("%s dirty; update", b.panel));
}

ZP: con Point(0, 0);
barhave(b: ref Bar, i: int)
{
	x := len b.pixels;
	y := b.i.r.dy();
	s := real (x*i)/real len b.p;
	e := real (x*(i+1))/real len b.p;
	end := real x;
	if(e >= end)
		e = end-0.0001;

	si := int s;
	if(real si > s && si != 0)
		si--;
	ei := int e;
	if(real ei > e)
		ei--;
	b.pixels[si] += (real (si+1)-s)*b.pixelscale;
	b.pixels[ei] += (e-real ei)*b.pixelscale;

	for(j := si+1; j < ei; j++)
		b.pixels[j] += b.pixelscale;

	b.i.draw(Rect((si+1,1), (si+2,y-1)), color(b.fill, b.pixels[si], b.div), nil, ZP);
	b.i.draw(Rect((ei+1,1), (ei+2,y-1)), color(b.fill, b.pixels[ei], b.div), nil, ZP);
	if(b.div == 1)
		b.i.draw(Rect((si+2,1), (ei+1,y-1)), b.filli, nil, ZP);
	else
		for(j = si+1; j < ei; j++)
			b.i.draw(Rect((j+1,1), (j+2,y-1)), color(b.fill, b.pixels[j], b.div), nil, ZP);
			
}

barfill(b: ref Bar)
{
	r := b.i.r;
	r = Rect(r.min.add(Point(1,1)), r.max.sub(Point(1,1)));
	b.i.draw(r, b.filli, nil, ZP);
}

color(c: int, r: real, div: int): ref Image
{
	r = r*255.0/real div;
	return disp.color(draw->setalpha(c, min(255, int r)));
}

tkgrid(w: string, r: list of list of string)
{
	for(sl := sys->tokenize(tkcmd("grid slaves "+w), " ").t1; sl != nil; sl = tl sl)
		tkcmd("destroy "+hd sl);
	i := 0;
	for(; r != nil; r = tl r) {
		j := 0;
		for(l := hd r; l != nil; l = tl l) {
			lw := sprint("%s.r%dc%d", w, i, j);
			tkcmd(sprint("label %s -text '%s", lw, hd l));
			tkcmd(sprint("grid %s -row %d -column %d -padx 10 -pady 1 -sticky w", lw, i, j));
			j++;
		}
		i++;
	}
	tkcmd("update");
}

l2(a, b: string): list of string
{
	return list of {a, b};
}

l3(a, b, c: string): list of string
{
	return list of {a, b, c};
}

l4(a, b, c, d: string): list of string
{
	return list of {a, b, c, d};
}

l6(a, b, c, d, e, f: string): list of string
{
	return list of {a, b, c, d, e, f};
}

l8(a, b, c, d, e, f, g, h: string): list of string
{
	return list of {a, b, c, d, e, f, g, h};
}

l9(a, b, c, d, e, f, g, h, i: string): list of string
{
	return list of {a, b, c, d, e, f, g, h, i};
}

l10(a, b, c, d, e, f, g, h, i, j: string): list of string
{
	return list of {a, b, c, d, e, f, g, h, i, j};
}


tkcmd(s: string): string
{
	r := tk->cmd(top, s);
	if(r != nil && r[0] == '!')
		warn(sprint("tkcmd: %q: %s", s, r));
	return r;
}

tkcmds(a: array of string)
{
	for(i := 0; i < len a; i++)
		tkcmd(a[i]);
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
