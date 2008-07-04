implement Torrentverify;

include "sys.m";
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

sys: Sys;
keyring: Keyring;
bittorrent: Bittorrent;

print, sprint, fprint, fildes: import sys;
Bee, Msg, Torrent: import bittorrent;


Torrentverify: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

Dflag: int;
nofix: int;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	keyring = load Keyring Keyring->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init(bitarray);

	arg->init(args);
	arg->setusage(arg->progname()+" [-Dn] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'D' =>	Dflag++;
		'n' =>	nofix = 1;
		* =>
			fprint(fildes(2), "bad option: -%c\n", c);
			arg->usage();
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

	haves := Bits.new(len t.piecehashes);
	if(dstfds != nil)
	for(i := 0; i < len t.piecehashes; i++) {
		wanthash := t.piecehashes[i];
		(buf, err) := bittorrent->pieceread(t, dstfds, i);
		if(err != nil)
			fail(sprint("%s", err));
		
		havehash := array[Keyring->SHA1dlen] of byte;
		keyring->sha1(buf, len buf, havehash, nil);
		if(hex(wanthash) == hex(havehash))
			haves.set(i);
	}

	print("progress:  %d/%d pieces\n", haves.have, len t.piecehashes);
	print("pieces:\n");
	for(i = 0; i < len t.piecehashes; i++)
		if(haves.get(i))
			print("1");
		else
			print("0");
	print("\n");
}

hex(d: array of byte): string
{
	s := "";
	for(i := 0; i < len d; i++)
		s += sprint("%02x", int d[i]);
	return s;
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
