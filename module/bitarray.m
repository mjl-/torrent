Bitarray: module
{
	PATH:	con "/dis/lib/bitarray.dis";

	Bits: adt {
		d:	array of byte;
		n:	int;
		have:	int;

		new:		fn(n: int): ref Bits;
		mk:		fn(n: int, d: array of byte): (ref Bits, string);
		clone:		fn(b: self ref Bits): ref Bits;
		get:		fn(b: self ref Bits, i: int): int;
		set:		fn(b: self ref Bits, i: int);
		setall:		fn(b: self ref Bits);
		clear:		fn(b: self ref Bits, i: int);
		clearall:	fn(b: self ref Bits);
		invert:		fn(b: self ref Bits);
		and:		fn(l: array of ref Bits): ref Bits;
		union:		fn(l: array of ref Bits): ref Bits;
		isempty:	fn(b: self ref Bits): int;
		isfull:		fn(b: self ref Bits): int;
		bytes:		fn(b: self ref Bits): array of byte;
		all:		fn(b: self ref Bits): list of int;
		text:		fn(b: self ref Bits): string;
	};
};
