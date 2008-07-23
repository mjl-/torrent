implement Torrentverify;

include "sys.m";
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "rand.m";
include "keyring.m";
include "bitarray.m";
include "bittorrent.m";

include "../../lib/bittorrent/get.m";

sys: Sys;
bitarray: Bitarray;
bittorrent: Bittorrent;
verify: Verify;

print, sprint, fprint, fildes: import sys;
Bits: import bitarray;
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
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init(bitarray);
	verify = load Verify Verify->PATH;
	verify->init();

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

	haves := Bits.new(t.piececount);
	if(dstfds != nil)
		verify->torrenthash(dstfds, t, haves);

	print("progress:  %d/%d pieces\n", haves.have, t.piececount);
	print("pieces:\n");
	for(i := 0; i < t.piececount; i++)
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
