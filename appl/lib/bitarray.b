implement Bitarray;

include "bitarray.m";

Bits.new(n: int): ref Bits
{
	return ref Bits(array[(n+8-1)/8] of { * => byte 0}, n, 0);
}

Bits.clone(b: self ref Bits): ref Bits
{
	d := array[len b.d] of byte;
	d[:] = b.d;
	return ref Bits(d, b.n, b.have);
}

Bits.get(b: self ref Bits, i: int): int
{
	return int (b.d[i/8] & (byte 1<<(7-(i&7))));
}

Bits.set(b: self ref Bits, i: int)
{
	if(!b.get(i)) {
		b.d[i/8] |= (byte 1<<(7-(i&7)));
		b.have++;
	}
}

Bits.clear(b: self ref Bits, i: int)
{
	if(b.get(i)) {
		b.d[i/8] &= ~(byte 1<<(7-(i&7)));
		b.have--;
	}
}

Bits.invert(b: self ref Bits)
{
	for(i := 0; i < len b.d; i++)
		b.d[i] = ~b.d[i];
	b.have = b.n-b.have;
}

Bits.and(b1, b2: ref Bits): ref Bits
{
	if(b1.n != b2.n)
		return nil;
	b := Bits.new(b1.n);
	for(i := 0; i < len b1.d; i++)
		b.d[i] = b1.d[i]&b2.d[i];
	for(i = 0; i < b.n; i++)
		if(b.get(i))
			b.have++;
	return b;
}
