implement Pieces;

include "torrentget.m";
include "lists.m";

sys: Sys;
keyring: Keyring;
lists: Lists;
bitarray: Bitarray;
bittorrent: Bittorrent;

Bits: import bitarray;
Torrent: import bittorrent;

torrent: ref Torrent;

init()
{
	sys = load Sys Sys->PATH;
	keyring = load Keyring Keyring->PATH;
	lists = load Lists Lists->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init(bitarray);
}

prepare(t: ref Torrent)
{
	torrent = t;
}

Piece.new(index, length: int): ref Piece
{
	nblocks := (length+Torrentget->Blocksize-1)/Torrentget->Blocksize;
	return ref Piece(nil, 0, index, Bits.new(nblocks), length, array[nblocks] of {* => (-1, -1)}, array[nblocks] of {* => 0});
}

Piece.orphan(p: self ref Piece): int
{
	for(i := 0; i < len p.busy; i++)
		if(p.busy[i].t0 >= 0 || p.busy[i].t1 >= 0)
			return 0;
	return 1;
}

Piece.isdone(p: self ref Piece): int
{
	return p.have.n == p.have.have;
}


Piece.hashadd(p: self ref Piece, buf: array of byte)
{
	p.hashstate = keyring->sha1(buf, len buf, nil, p.hashstate);
	p.hashstateoff += len buf;
}

Piece.text(p: self ref Piece): string
{
	return sys->sprint("<piece %d have %s>", p.index, p.have.text());
}


Block.new(piece, begin, length: int): ref Block
{
	return ref Block(piece, begin, length);
}

Block.eq(b1, b2: ref Block): int
{
	return b1.piece == b2.piece &&
		b1.begin == b2.begin &&
		b1.length == b2.length;
}

Block.text(b: self ref Block): string
{
	return sys->sprint("<block piece %d, begin %d, length %d>", b.piece, b.begin, b.length);
}


# pieces

piecenew(index: int): ref Piece
{
	p := Piece.new(index, torrent.piecelength(index));
	pieces = p::pieces;
	return p;
}

piecedel(p: ref Piece)
{
	new: list of ref Piece;
	for(l := pieces; l != nil; l = tl l)
		if(hd l != p)
			new = hd l::new;
	pieces = new;
}

piecefind(index: int): ref Piece
{
	for(l := pieces; l != nil; l = tl l)
		if((hd l).index == index)
			return hd l;
	return nil;
}


# blocks

blockhave(l: list of ref Block, b: ref Block): int
{
	for(; l != nil; l = tl l)
		if(Block.eq(hd l, b))
			return 1;
	return 0;
}

blockdel(l: list of ref Block, b: ref Block): list of ref Block
{
	r: list of ref Block;
	for(; l != nil; l = tl l)
		if(!Block.eq(hd l, b))
			r = hd l::r;
	return lists->reverse(r);
}

blocktake(l: list of ref Block): (list of ref Block, ref Block)
{
	l = lists->reverse(l);
	b := hd l;
	return (lists->reverse(tl l), b);
}
