implement Torrenttrackget;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "bittorrent.m";
	bt: Bittorrent;
	Bee, Torrent: import bt;
include "util0.m";
	util: Util0;
	fail, warn: import util;

Torrenttrackget: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};


dflag: int;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	bt = load Bittorrent Bittorrent->PATH;
	bt->init();
	util = load Util0 Util0->PATH;
	util->init();

	arg->init(args);
	arg->setusage(arg->progname()+" [-d] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	bt->dflag = dflag++;
		* =>	arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();

	(t, err) := Torrent.open(hd args);
	if(err != nil)
		fail(sprint("%s: %s", hd args, err));

	localpeerid := bt->genpeerid();

	(interval, peers, nil, terr) := bt->trackerget(t, localpeerid, big 0, big 0, big 0, 0, nil);
	if(terr != nil)
		fail("trackerget: "+terr);
	say("trackget okay");

	sys->print("interval=%d\n", interval);
	sys->print("number of peers=%d\n", len peers);
	for(i := 0; i < len peers; i++) {
		(ip, port, nil) := peers[i];
		sys->print("ip=%s port=%d rpeerid=...\n", ip, port);
	}
	say("done");
}

say(s: string)
{
	if(dflag)
		warn(s);
}
