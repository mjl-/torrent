Bitarray: module {
	PATH:	con "/dis/lib/bitarray.dis";

	Bits: adt {
		d:	array of byte;
		n, have:	int;

		new:	fn(n: int): ref Bits;
		clone:	fn(b: self ref Bits): ref Bits;
		get:	fn(b: self ref Bits, i: int): int;
		set:	fn(b: self ref Bits, i: int);
		clear:	fn(b: self ref Bits, i: int);
		invert:	fn(b: self ref Bits);
		and:	fn(b1, b2: ref Bits): ref Bits;
		text:	fn(b: self ref Bits): string;
	};
};
