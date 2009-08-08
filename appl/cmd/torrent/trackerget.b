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
	bittorrent: Bittorrent;
	Bee, Torrent: import bittorrent;


Torrenttrackget: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

Dflag: int;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init();

	arg->init(args);
	arg->setusage(arg->progname()+" torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'D' =>	Dflag++;
		* =>	arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();

	(t, err) := Torrent.open(hd args);
	if(err != nil)
		fail(sprint("%s: %s", hd args, err));

	localpeerid := bittorrent->genpeerid();

	(interval, peers, nil, terr) := bittorrent->trackerget(t, localpeerid, big 0, big 0, big 0, 0, nil);
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

fail(s: string)
{
	sys->fprint(sys->fildes(2), "%s\n", s);
	raise "fail:"+s;
}

say(s: string)
{
	if(Dflag)
		sys->fprint(sys->fildes(2), "%s\n", s);
}
