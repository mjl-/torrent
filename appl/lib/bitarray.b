implement Bitarray;

include "sys.m";
	sys: Sys;
include "rand.m";
	rand: Rand;
include "bitarray.m";

nibblebitcounts := array[16] of {
0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
};

init()
{
	sys = load Sys Sys->PATH;
	rand = load Rand Rand->PATH;
	rand->init(sys->millisec()^sys->pctl(0, nil));
}

nbits(b: byte): int
{
	i := int b;
	return nibblebitcounts[i>>4]+nibblebitcounts[i&15];
}

clearhigh(b: ref Bits)
{
	if((b.n&7) == 0)
		return;
	n := b.n&7;
	b.d[(b.n-1)>>3] &= ~(~(byte 0)>>n);
}

bytebit(i: int): byte
{
	return byte (1<<(7-(i&7)));
}


Bits.new(n: int): ref Bits
{
	return ref Bits(array[(n+8-1)>>3] of {* => byte 0}, n, 0);
}

Bits.mk(n: int, d: array of byte): (ref Bits, string)
{
	need := (n+8-1)>>3;
	if(len d != need)
		return (nil, "wrong number of bytes, need "+string need+", got "+string len d);
	nd := array[len d] of byte;
	nd[:] = d;
	b := ref Bits(nd, n, 0);
	clearhigh(b);
	for(i := 0; i < len b.d; i++)
		b.have += nbits(b.d[i]);
	return (b, nil);
}

Bits.clone(b: self ref Bits): ref Bits
{
	d := array[len b.d] of byte;
	d[:] = b.d;
	return ref Bits(d, b.n, b.have);
}

Bits.get(b: self ref Bits, i: int): int
{
	return int (b.d[i>>3] & bytebit(i));
}

Bits.set(b: self ref Bits, i: int)
{
	if(!b.get(i)) {
		b.d[i>>3] |= bytebit(i);
		b.have++;
	}
}

Bits.setall(b: self ref Bits)
{
	b.d = array[len b.d] of {* => byte ~0};
	clearhigh(b);
	b.have = b.n;
}

Bits.clear(b: self ref Bits, i: int)
{
	if(b.get(i)) {
		b.d[i>>3] &= ~bytebit(i);
		b.have--;
	}
}

Bits.clearall(b: self ref Bits)
{
	b.d = array[len b.d] of {* => byte 0};
	b.have = 0;
}

Bits.invert(b: self ref Bits)
{
	for(i := 0; i < len b.d; i++)
		b.d[i] = ~b.d[i];
	b.have = b.n-b.have;
}

Bits.clearbits(b: self ref Bits, o: ref Bits)
{
	for(i := 0; i < len b.d; i++) {
		b.have -= nbits(b.d[i]);
		b.d[i] &= ~o.d[i];
		b.have += nbits(b.d[i]);
	}
}

Bits.nand(a, na: ref Bits): ref Bits
{
	r := ref Bits(array[len a.d] of byte, a.n, 0);
	for(i := 0; i < len a.d; i++) {
		r.d[i] = a.d[i]&~na.d[i];
		r.have += nbits(r.d[i]);
	}
	return r;
}

Bits.isempty(b: self ref Bits): int
{
	return b.have == 0;
}

Bits.isfull(b: self ref Bits): int
{
	return b.have == b.n;
}

Bits.bytes(b: self ref Bits): array of byte
{
	d := array[len b.d] of byte;
	d[:] = b.d;
	return d;
}

Bits.rand(b: self ref Bits): int
{
	if(b.have == 0)
		raise "bits.rand, but no bits are set";
	if(sys == nil)
		init();
	n := rand->rand(b.have);
	it := b.iter();
	n--;
	for(i := 0; i < n; i++)
		it.next();
	return it.next();
}

Bits.all(b: self ref Bits): list of int
{
	l: list of int;
	for(i := b.n-1; i >= 0; i--)
		if(b.get(i))
			l = i::l;
	return l;
}

Bits.iter(b: self ref Bits): ref Bititer
{
	return ref Bititer(b, -1, 0, 0);
}

Bits.inviter(b: self ref Bits): ref Bititer
{
	return ref Bititer(b, -1, 1, 0);
}

Bits.text(b: self ref Bits): string
{
	s := "";
	for(i := 0; i < b.n; i++) {
		if(b.get(i))
			s[len s] = '1';
		else
			s[len s] = '0';
		if((i&63) == 63)
			s[len s] = '\n';
	}
	if((i&63) != 63)
		s[len s] = '\n';
	return s;
}


Bititer.next(b: self ref Bititer): int
{
	if(b.inv) {
		while(b.seen < b.b.n-b.b.have && ++b.last < b.b.n)
			if(!b.b.get(b.last)) {
				b.seen++;
				return b.last;
			}
	} else {
		while(b.seen < b.b.have && ++b.last < b.b.n)
			if(b.b.get(b.last)) {
				b.seen++;
				return b.last;
			}
	}
	return -1;
}
