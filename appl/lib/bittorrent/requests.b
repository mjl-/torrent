implement Requests;

include "torrentget.m";

sys: Sys;

init()
{
	sys = load Sys Sys->PATH;
}


Req.new(pieceindex, blockindex: int): Req
{
	return Req(pieceindex, blockindex, 0);
}

Req.eq(r1, r2: Req): int
{
	return r1.pieceindex == r2.pieceindex && r1.blockindex == r2.blockindex;
}

Req.text(r: self Req): string
{
	return sys->sprint("<req piece=%d block=%d begin=%d>", r.pieceindex, r.blockindex, r.blockindex*Torrentget->Blocksize);
}


Reqs.new(size: int): ref Reqs
{
	return ref Reqs(array[size] of Req, 0, 0, nil);
}

Reqs.take(r: self ref Reqs, req: Req): int
{
	if(r.isempty())
		raise "take on empty list";

	# first, skip over cancelled requests if necessary
	first := r.first; 
	for(; first != r.next && !Req.eq(r.a[first], req); first = (first+1) % len r.a)
		if(!r.a[first].cancelled)
			return 0;

	# then see if request was the one expected, if any
	if(first == r.next && !Req.eq(r.a[first], req))
		return 0;
	r.first = (first+1) % len r.a;
	return 1;
}

Reqs.peek(r: self ref Reqs): Req
{
	if(r.isempty())
		raise "peek on empty list";
	return r.a[r.first];
}

Reqs.cancel(r: self ref Reqs, req: Req)
{
	for(i := r.first; i != r.next; i = (i+1) % len r.a)
		if(Req.eq(req, r.a[i]))
			r.a[i].cancelled = 1;
}

Reqs.add(r: self ref Reqs, req: Req)
{
	if(r.isfull())
		raise "add on full list";
	r.a[r.next] = req;
	r.next = (r.next+1) % len r.a;
	r.lastreq = ref req;
}

Reqs.flush(r: self ref Reqs)
{
	r.first = r.next = 0;
}

Reqs.last(r: self ref Reqs): ref Req
{
	return r.lastreq;
}

Reqs.isempty(r: self ref Reqs): int
{
	return r.first == r.next;
}

Reqs.isfull(r: self ref Reqs): int
{
	return (r.next+1) % len r.a == r.first;
}

Reqs.count(r: self ref Reqs): int
{
	return (len r.a+r.next-r.first) % len r.a;
}

Reqs.size(r: self ref Reqs): int
{
	return len r.a;
}

Reqs.text(r: self ref Reqs): string
{
	return sys->sprint("<reqs first=%d next=%d size=%d>", r.first, r.next, len r.a);
}


Batch.new(first, n: int, piece: ref Pieces->Piece): ref Batch
{
	blocks := array[n] of int;
	for(i := 0; i < n; i++)
		blocks[i] = first+i;
	return ref Batch(blocks, piece);
}

Batch.unused(b: self ref Batch): list of Req
{
	reqs: list of Req;
	for(i := len b.blocks-1; i >= 0; i--) {
		busy := b.piece.busy[b.blocks[i]];
		if(busy.t0 < 0 && busy.t1 < 0)
			reqs = Req.new(b.piece.index, b.blocks[i])::reqs;
	}
	return reqs;
}

Batch.usedpartial(b: self ref Batch, peer: ref Peers->Peer): list of Req
{
	reqs: list of Req;
	for(i := len b.blocks-1; i >= 0; i--)
		busy := b.piece.busy[b.blocks[i]];
		if(busy.t0 != peer.id && busy.t1 != peer.id && (busy.t0 < 0 || busy.t1 < 0))
			reqs = Req.new(b.piece.index, b.blocks[i])::reqs;
	return reqs;
}

Batch.text(nil: self ref Batch): string
{
	return "<batch ...>";
}
