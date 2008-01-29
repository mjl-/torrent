implement Torrentparse;

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
Bee: import bittorrent;


Torrentparse: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

bout: ref Iobuf;
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

	bout = bufio->fopen(fildes(1), Bufio->OWRITE);
	if(bout == nil)
		fail(sprint("bufio open stdout: %r"));

	f := hd args;
	fd := sys->open(f, Sys->OREAD);
	if(fd == nil)
		fail(sprint("open %s: %r", f));

	d := readfile(fd);
	say("have file");
	say(sprint("file length=%d byte0=%c", len d, int d[0]));

	(b, err) := Bee.unpack(d);
	if(err != nil)
		fail(sprint("parsing %s: %s", f, err));
	say("have unpacked bee");

	beeprint(b, 0, "");
	bout.close();
	say("done");

	if(0) {
		nd := b.pack();
		if(sys->write(fildes(2), nd, len nd) != len nd)
			fail(sprint("writing: %r"));
	}
}

beeprint(bb: ref Bee, indent: int, end: string)
{
	pick b := bb {
	String =>
		s := stringfmt(b.a);
		bout.puts(sprint("%*s%s%s\n", indent, "", s, end));
	Integer =>
		bout.puts(sprint("%*s%bd%s\n", indent, "", b.i, end));
	List =>
		bout.puts(sprint("%*slist (\n", indent, ""));
		for(i := 0; i < len b.a; i++)
			beeprint(b.a[i], indent+1, ",");
		bout.puts(sprint("%*s)%s\n", indent, "", end));
	Dict =>
		bout.puts(sprint("%*sdict (\n", indent, ""));
		for(i := 0; i < len b.a; i++) {
			beeprint(b.a[i].t0, indent+1, " => ");
			beeprint(b.a[i].t1, indent+2, ",");
		}
		bout.puts(sprint("%*s)%s\n", indent, "", end));
	}
}

arrayfmt(a: array of byte): string
{
	s := "";
	for(i := 0; i <= 20 && i < len a; i++)
		s += sprint("%02x", int a[i]);
	if(len a > 20)
		s += "...";
	return "hex "+s;
}

stringfmt(a: array of byte): string
{
	for(i := 0; i < len a; i++)
		if(a[i] < byte 16r20 || a[i] >= byte 16r7f)
			return arrayfmt(a);
	return "\""+string a+"\"";
}

readfile(fd: ref Sys->FD): array of byte
{
	d := array[0] of byte;
	for(;;) {
		n := sys->read(fd, buf := array[Sys->ATOMICIO] of byte, len buf);
		if(n == 0)
			break;
		if(n < 0)
			fail(sprint("reading: %r"));
		nd := array[len d+n] of byte;
		nd[:] = d;
		nd[len d:] = buf[:n];
		d = nd;
	}
	return d;
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
