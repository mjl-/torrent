implement Torrenttrackget;

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
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init(bitarray);

	arg->init(args);
	arg->setusage(arg->progname()+" torrentfile");
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

	(t, err) := Torrent.open(hd args);
	if(err != nil)
		fail(sprint("%s: %s", hd args, err));

	localpeerid := bittorrent->genpeerid();

	(interval, peers, nil, terr) := bittorrent->trackerget(t, localpeerid, big 0, big 0, big 0, 0, nil);
	if(terr != nil)
		fail("trackerget: "+terr);
	say("trackget okay");

	print("interval=%d\n", interval);
	print("number of peers=%d\n", len peers);
	for(i := 0; i < len peers; i++) {
		(ip, port, nil) := peers[i];
		print("ip=%s port=%d rpeerid=...\n", ip, port);
	}
	say("done");
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
