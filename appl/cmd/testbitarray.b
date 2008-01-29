# testbitarray n8 s0 s1 s2 p s3 s4 s5 s6 s7 p
# testbitarray n8 s0 s1 s2 p s3 s4 s5 s6 s7 p
# testbitarray n8 s0 p s1 p u0 p
# testbitarray n10 s0 s8 p
# testbitarray n1023 i p
# testbitarray n1023 s123 s433 p i p

implement Testbitarray;

include "sys.m";
include "draw.m";
include "arg.m";
include "bitarray.m";

sys: Sys;
bitarray: Bitarray;

print, fildes, sprint, fprint: import sys;
Bits: import bitarray;

Testbitarray: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};


init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := load Arg Arg->PATH;
	bitarray = load Bitarray Bitarray->PATH;

	arg->init(args);
	arg->setusage(arg->progname());
	while((c := arg->opt()) != 0)
		case c {
		* =>	fprint(fildes(2), "bad option: -%c\n", c);
			arg->usage();
		}
	b: ref Bits;
	for(args = arg->argv(); args != nil; args = tl args)
		case (hd args)[0] {
		'p' =>
			print("n=%d have=%d set=(", b.n, b.have);
			for(i := 0; i < b.n; i++)
				if(b.get(i))
					print("%d ", i);
			print(")\n");
		'n' =>
			n := int (hd args)[1:];
			b = Bits.new(n);
		's' =>
			n := int (hd args)[1:];
			b.set(n);
		'g' =>
			n := int (hd args)[1:];
			print("get: %d\n", b.get(n));
		'u' =>
			n := int (hd args)[1:];
			b.clear(n);
		'i' =>
			b.invert();
		}
}

fail(s: string)
{
	fprint(fildes(2), "%s\n", s);
	raise "fail:"+s;
}
