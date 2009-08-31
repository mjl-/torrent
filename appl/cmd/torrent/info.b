implement Torrentinfo;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "keyring.m";
	kr: Keyring;
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";
	bt: Bittorrent;
	Bee, Msg, Torrent: import bt;
include "util0.m";
	util: Util0;
	fail, warn, sizefmt: import util;

Torrentinfo: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

Dflag: int;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	kr = load Keyring Keyring->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	bt = load Bittorrent Bittorrent->PATH;
	bt->init();
	util = load Util0 Util0->PATH;
	util->init();

	arg->init(args);
	arg->setusage(arg->progname()+" [-D] torrentfile");
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

	sys->print("announce url:   %s\n", t.announce);
	sys->print("piece length:   %s (%d bytes)\n", sizefmt(big t.piecelen), t.piecelen);
	sys->print("pieces:         %d\n", t.piececount);
	sys->print("total length:   %s (%bd bytes)\n", sizefmt(t.length), t.length);
	sys->print("files:\n");
	for(l := t.files; l != nil; l = tl l) {
		f := hd l;
		sys->print("%10s  %s\n", sizefmt(f.length), f.path);
	}
}

say(s: string)
{
	if(Dflag)
		warn(s);
}
