implement Torrentverify;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "keyring.m";
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";
	bittorrent: Bittorrent;
	Bee, Msg, Torrent: import bittorrent;
include "rand.m";
include "../../lib/bittorrent/peer.m";
	verify: Verify;
include "util0.m";
	util: Util0;
	fail, warn, hex: import util;

Torrentverify: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};


dflag: int;
nofix: int;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init();
	verify = load Verify Verify->PATH;
	verify->init();
	util = load Util0 Util0->PATH;
	util->init();

	arg->init(args);
	arg->setusage(arg->progname()+" [-dn] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	dflag++;
		'n' =>	nofix = 1;
		* =>	arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();

	(t, terr) := Torrent.open(hd args);
	if(terr != nil)
		fail(sprint("%s: %s", hd args, terr));

	(dstfds, nil, oerr) := t.openfiles(nofix, 1);
	if(oerr != nil)
		fail(sprint("%s", oerr));

	# xxx should print progress per file

	haves := Bits.new(t.piececount);
	if(dstfds != nil)
		verify->torrenthash(dstfds, t, haves);

	sys->print("progress:  %d/%d pieces\n", haves.have, t.piececount);
	sys->print("pieces:\n");
	for(i := 0; i < t.piececount; i++)
		if(haves.get(i))
			sys->print("1");
		else
			sys->print("0");
	sys->print("\n");
}

say(s: string)
{
	if(dflag)
		warn(s);
}
