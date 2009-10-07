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
include "string.m";
	str: String;
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
	str = load String String->PATH;
	bt = load Bittorrent Bittorrent->PATH;
	bt->init();
	http = load Http Http->PATH;
	http->init(bufio);
	util = load Util0 Util0->PATH;
	util->init();

	announces: list of list of string;
	arg->init(args);
	arg->setusage(arg->progname()+" [-vf] [-d dir] [-p piecelen] [-a 'tracker ...'] tracker file ...");
	while((c := arg->opt()) != 0)
		case c {
		'a' =>	announces = str->unquoted(arg->arg())::announces;
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
		checkannounce(announce);
		for(l := announces; l != nil; l = tl l)
			for(ll := hd l; ll != nil; ll = tl ll)
				checkannounce(hd ll);
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

	nannounces := array[len announces] of array of string;
	i = 0;
	for(l := rev(announces); l != nil; l = tl l)
		nannounces[i++] = l2a(hd l);

	t := ref Torrent (announce, nannounces, piecelen, nil, len hashes, hashes, files, name, total, 0, nil, 0);
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

checkannounce(s: string)
{
	(url, err) := Url.unpack(s);
	if(err != nil || url.scheme != "http" && url.scheme != "https" && url.scheme != "udp")
		fail(sprint("bad announce url %#q: %s", s, err));
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
