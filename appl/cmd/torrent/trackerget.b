implement Torrenttrackget;

include "sys.m";
	sys: Sys;
	print, sprint: import sys;
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "bitarray.m";
include "bittorrent.m";
	bt: Bittorrent;
	Bee, Torrent, Trackreq: import bt;
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

	tr := ref Trackreq (t, localpeerid, big 0, big 0, big 0, 0, nil, nil);
	(track, terr) := bt->trackerget(tr);
	if(terr != nil)
		fail("trackerget: "+terr);
	say("trackget okay");

	print("interval: %d\n", track.interval);
	if(track.mininterval >= 0)
		print("min interval: %d\n", track.mininterval);
	print("number of peers: %d\n", len track.peers);
	for(i := 0; i < len track.peers; i++) {
		tp := track.peers[i];
		print("\tip=%s port=%d rpeerid=...\n", tp.ip, tp.port);
	}
	say("done");
}

say(s: string)
{
	if(dflag)
		warn(s);
}
