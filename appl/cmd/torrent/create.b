implement Torrentcreate;

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
include "bittorrent.m";
	bt: Bittorrent;
	Bee, File, Torrent: import bt;
include "mhttp.m";
	http: Http;
	Url: import http;
include "util0.m";
	util: Util0;
	fail, warn, l2a, hex, rev, sizeparse, sizefmt: import util;

Torrentcreate: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

dflag: int;
fflag: int;
vflag: int;
name: string;
piecelen := 2**18;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	kr = load Keyring Keyring->PATH;
	bt = load Bittorrent Bittorrent->PATH;
	bt->init();
	http = load Http Http->PATH;
	http->init(bufio);
	util = load Util0 Util0->PATH;
	util->init();

	arg->init(args);
	arg->setusage(arg->progname()+" [-vf] [-d dir] [-p piecelen] tracker file ...");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	name = arg->arg();
		'f' =>	fflag++;
		'p' =>	piecelen = int sizeparse(arg->arg());
			if(piecelen < 0 || nbits(piecelen) != 1)
				fail(sprint("piecelen is not a positive power of 2"));
		'v' =>	vflag++;
		* =>	arg->usage();
		}

	args = arg->argv();
	if(len args < 2)
		arg->usage();

	announce := hd args;
	paths := tl args;

	if(!fflag) {
		(url, err) := Url.unpack(announce);
		if(err != nil || url.scheme != "http" && url.scheme != "https")
			fail(sprint("bad announce url %#q: %s", announce, err));
	}

	if(len paths > 1 && name == nil)
		fail(sprint("-d dir required when using multiple files"));

	total := big 0;
	lhash: list of array of byte;
	piece := array[piecelen] of byte;
	have := 0;
	files := array[len paths] of ref File;
	i := 0;
file:
	while(paths != nil) {
		length := big 0;
		path := hd paths;
		paths = tl paths;
		fd := sys->open(path, Sys->OREAD);
		if(fd == nil)
			fail(sprint("open %q: %r", path));
		for(;;) {
			nn := sys->readn(fd, piece[have:], len piece-have);
			if(nn < 0)
				fail(sprint("read: %r"));
			if(nn == 0) {
				files[i++] = f := ref File;
				f.path = path;
				f.length = length;
				continue file;
			}
			length += big nn;
			total += big nn;
			have += nn;
			if(have == len piece || paths == nil) {
				digest := array[kr->SHA1dlen] of byte;
				kr->sha1(piece, have, digest, nil);
				lhash = digest::lhash;
				have = 0;
			}
		}
	}
	hashes := l2a(rev(lhash));

	t := ref Torrent (announce, piecelen, nil, len hashes, hashes, files, name, total);
	d := t.pack();
	if(sys->write(sys->fildes(1), d, len d) != len d)
		fail(sprint("write: %r"));

	if(vflag) {
		hash := array[kr->SHA1dlen] of byte;
		kr->sha1(d, len d, hash, nil);
		warn(sprint("total %s (%bd bytes), in %d pieces", sizefmt(total), total, len hashes));
		warn(hex(hash));
	}
}

nbits(i: int): int
{
	n := 0;
	for(j := 0; j < 31; j++)
		if(i & (1<<j))
			n++;
	return n;
}

say(s: string)
{
	if(dflag)
		warn(s);
}
