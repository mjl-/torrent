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

torrent: ref Torrent;
dstfd: ref Sys->FD;

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
	arg->setusage(arg->progname()+" [-D] torrentfile");
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

	err: string;
	(torrent, err) = Torrent.open(hd args);
	if(err != nil)
		fail(sprint("%s: %s", hd args, err));

	f := "torrentdata";
	dstfd = sys->open(f, Sys->OREAD);
	if(dstfd == nil)
		fail(sprint("open %s: %r", f));

	buf := array[Sys->ATOMICIO] of byte;
	off := big 0;
	for(i := 0; i < len torrent.piecehashes; i++) {
		piecelen := torrent.piecelen;
		if(big piecelen > torrent.length-off)
			piecelen = int (torrent.length-off);

		state: ref Keyring->DigestState;

		have := 0;
		while(have < piecelen) {
			want := piecelen-have;
			if(want > len buf)
				want = len buf;
			n := sys->read(dstfd, buf, want);
			if(n == 0)
				fail(sprint("early eof, at offset %bd", off));
			if(n < 0)
				fail(sprint("read at offset %bd: %r", off));
			state = keyring->sha1(buf, n, nil, state);
			off += big n;
			have += n;
		}

		havehash := array[Keyring->SHA1dlen] of byte;
		keyring->sha1(nil, 0, havehash, state);
		
		wanthash := torrent.piecehashes[i];
		if(!equal(wanthash, havehash))
			fail(sprint("piece %d wrong, want %s, have %s", i, hex(havehash), hex(wanthash)));
	}
	fprint(fildes(2), "verified!\n");
}

equal(d1, d2: array of byte): int
{
	if(len d1 != len d2)
		return 0;
	for(i := 0; i < len d1; i++)
		if(d1[i] != d2[i])
			return 0;
	return 1;
}

hexchar(c: byte): byte
{
	if(c > byte 9)
		return c-byte 10+byte 'a';
	return c+byte '0';
}

hex(d: array of byte): string
{
	s := array[2*len d] of byte;
	for(i := 0; i < len d; i++) {
		s[2*i] = hexchar(d[i]>>4);
		s[2*i+1] = hexchar(d[i] & byte 16r0f);
	}
	return string s;
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
