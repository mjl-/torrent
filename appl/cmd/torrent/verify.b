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
	kr: Keyring;
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";
	bt: Bittorrent;
	Bee, Msg, Torrent: import bt;
include "rand.m";
include "../../lib/bittorrentpeer.m";
	btp: Bittorrentpeer;
include "util0.m";
	util: Util0;
	killgrp, pid, warn, hex: import util;

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
	kr = load Keyring Keyring->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	bt = load Bittorrent Bittorrent->PATH;
	bt->init();
	btp = load Bittorrentpeer Bittorrentpeer->PATH;
	btp->init();
	util = load Util0 Util0->PATH;
	util->init();

	sys->pctl(Sys->NEWPGRP, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-dn] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	bt->dflag = btp->dflag = dflag++;
		'n' =>	nofix = 1;
		* =>	arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();

	f := hd args;
	(t, terr) := Torrent.open(f);
	if(terr != nil)
		fail(terr);

	(fds, nil, oerr) := t.openfiles(nofix, 1);
	if(oerr != nil)
		fail(oerr);

	spawn btp->reader(t, fds, rc := chan[2] of (array of byte, string));
	digest := array[kr->SHA1dlen] of byte;
	n := 0;
	for(i := 0; i < t.piececount; i++) {
		(buf, err) := <-rc;
		if(err != nil)
			fail(err);
		kr->sha1(buf, len buf, digest, nil);
		if(hex(digest) == hex(t.hashes[i])) {
			sys->print("1");
			n++;
		} else
			sys->print("0");
	}
	sys->print("\n");
	sys->print("progress:  %d/%d pieces\n", n, t.piececount);
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
