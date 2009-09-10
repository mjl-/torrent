implement Torrentparse;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";
	bt: Bittorrent;
	Bee: import bt;
include "util0.m";
	util: Util0;
	fail, warn, hex, readfile: import util;

Torrentparse: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

bout: ref Iobuf;
dflag: int;
vflag: int;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	bt = load Bittorrent Bittorrent->PATH;
	bt->init();
	util = load Util0 Util0->PATH;
	util->init();

	arg->init(args);
	arg->setusage(arg->progname()+" [-d] [-v] beefile");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	bt->dflag = dflag++;
		'v' =>	vflag++;
		* =>	arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();

	bout = bufio->fopen(sys->fildes(1), Bufio->OWRITE);
	if(bout == nil)
		fail(sprint("bufio open stdout: %r"));

	f := hd args;
	d := readfile(f, -1);
	if(d == nil)
		fail(sprint("%r"));

	(b, err) := Bee.unpack(d);
	if(err != nil)
		fail(sprint("parsing %s: %s", f, err));

	beeprint(b, 0, "");
	bout.close();

	if(0) {
		nd := b.pack();
		if(sys->write(sys->fildes(2), nd, len nd) != len nd)
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
	if(vflag || len a < 20)
		return "hex "+hex(a);
	s := "";
	for(i := 0; i < 20-3 && i < len a; i++)
		s += sprint("%02x", int a[i]);
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

say(s: string)
{
	if(dflag)
		warn(s);
}
