implement Bitarray;

include "bitarray.m";


sethave(b: ref Bits)
{
	b.have = 0;
	for(i := 0; i < b.n; i++)
		if(b.get(i))
			b.have++;
}


Bits.new(n: int): ref Bits
{
	return ref Bits(array[(n+8-1)/8] of {* => byte 0}, n, 0);
}

newones(n: int): ref Bits
{
	return ref Bits(array[(n+8-1)/8] of {* => byte 16rFF}, n, 0);
}

Bits.mk(n: int, d: array of byte): (ref Bits, string)
{
	need := (n+8-1)/8;
	if(len d != need)
		return (nil, "wrong number of bytes, need "+string need+", got "+string len d);
	nd := array[len d] of byte;
	nd[:] = d;
	b := ref Bits(nd, n, 0);
	sethave(b);
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

Bits.and(a: array of ref Bits): ref Bits
{
	n := a[0].n;
	for(i := 0; i < len a; i++)
		if(a[i].n != n)
			return nil;  # raise error?

	b := newones(n);
	for(i = 0; i < len a; i++)
		for(j := 0; j < len b.d; j++)
			b.d[j] &= a[i].d[j];
	sethave(b);
	return b;
}

Bits.union(a: array of ref Bits): ref Bits
{
	n := a[0].n;
	for(i := 0; i < len a; i++)
		if(a[i].n != n)
			return nil;  # raise error?

	b := Bits.new(n);
	for(i = 0; i < len a; i++)
		for(j := 0; j < len b.d; j++)
			b.d[j] |= a[i].d[j];
	sethave(b);
	return b;
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

Bits.all(b: self ref Bits): list of int
{
	l: list of int;
	for(i := b.n-1; i >= 0; i--)
		if(b.get(i))
			l = i::l;
	return l;
}

Bits.text(b: self ref Bits): string
{
	s := "";
	for(i := 0; i < b.n; i++)
		if(b.get(i))
			s += "1";
		else
			s += "0";
	return s;
}
