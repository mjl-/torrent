implement State;

include "torrentget.m";

bitarray: Bitarray;
Bits: import bitarray;

init()
{
	bitarray = load Bitarray Bitarray->PATH;
}

prepare(npieces: int)
{
	piecehave = Bits.new(npieces);
	piecebusy = Bits.new(npieces);
	piececounts = array[npieces] of {* => 0};
}
