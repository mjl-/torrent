implement Schedule;

include "torrentget.m";

sys: Sys;
rand: Rand;
bitarray: Bitarray;
peers: Peers;
pieces: Pieces;
requests: Requests;
misc: Misc;

sprint: import sys;
Bits: import bitarray;
Peer: import peers;
Piece: import pieces;
Req, Reqs, Batch: import requests;

init(randmod: Rand, peersmod: Peers, piecesmod: Pieces)
{
	sys = load Sys Sys->PATH;
	rand = randmod;
	bitarray = load Bitarray Bitarray->PATH;
	misc = load Misc Misc->PATH;
	misc->init(rand);
	peers = peersmod;
	pieces = piecesmod;
	requests = load Requests Requests->PATH;
}

prepare(npieces: int)
{
	piecehave = Bits.new(npieces);
	piecebusy = Bits.new(npieces);
	piececounts = array[npieces] of {* => 0};
}

request(reqch: chan of ref (ref Piece, list of Req, chan of int), p: ref Piece, reqs: list of Req)
{
	donech := chan of int;
	reqch <-= ref (p, reqs, donech);
	<-donech;
}

schedbatches(reqch: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer, b: array of ref Batch)
{
	for(i := 0; needblocks(peer) && i < len b; i++)
		request(reqch, b[i].piece, b[i].unused());
}

schedpieces(reqch: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer, a: array of ref Piece)
{
	for(i := 0; needblocks(peer) && i < len a; i++)
		if((peer.piecehave).get(a[i].index)) {
			b := requests->batches(a[i]);
			schedbatches(reqch, peer, b);
		}
}


getrandompiece(): ref Piece
{
	# will succeed, this is only called when few pieces are busy
	for(;;) {
		i := rand->rand(piecebusy.n);
		if(!piecebusy.get(i)) {
			p := pieces->piecenew(i);
			piecebusy.set(i);
			return p;
		}
	}
}

getrarestpiece(b: ref Bits)
{
	# first, determine which pieces are the rarest
	rarest: list of int;  # piece index
	min := -1;
	seen := 0;
	for(i := 0; i < b.n && seen < b.have; i++) {
		if(b.get(i)) {
			seen++;
			if(min < 0 || piececounts[i] < min) {
				rarest = nil;
				min = piececounts[i];
			}
			if(piececounts[i] <= min)
				rarest = i::rarest;
		}
	}
	if(rarest == nil)
		return;

	# next, pick a random element from the list
	skip := rand->rand(b.have);
	while(skip-- > 0)
		rarest = tl rarest;
	index := hd rarest;
	say(sprint("choose rarest piece %d", index));
}

needblocks(peer: ref Peer): int
{
	return peer.reqs.count() == 0 || peer.reqs.count()+Torrentget->Batchsize < peer.reqs.size();
}

inactiverare(): array of ref (int, int)
{
	# we just use .t0 of the ref (int, int).  it's used here so we can use polymorphic functions
	a := array[piecebusy.n-piecebusy.have] of ref (int, int);
	say(sprint("inactiverare: %d pieces, %d busy, finding %d", piecebusy.n, piecebusy.have, len a));
	j := 0;
	for(i := 0; j < len a && i < piecebusy.n; i++)
		if(!piecebusy.get(i))
			a[j++] = ref (i, 0);

	misc->randomize(a);
	return a;
}

rarepiececmp(p1, p2: ref Piece): int
{
	return piececounts[p1.index]-piececounts[p2.index];
}

piecesrareorphan(orphan: int): array of ref Piece
{
	r: list of ref Piece;
	for(l := pieces->pieces; l != nil; l = tl l) {
		p := hd l;
		if(p.orphan() && orphan)
			r = p::r;
		else if(!p.orphan() && !orphan)
			r = p::r;
	}

	a := misc->l2a(r);
	misc->sort(a, rarepiececmp);
	return a;
}

progresscmp(p1, p2: ref Piece): int
{
	return p2.have.n-p1.have.n;
}

rarestfirst(): int
{
	return piecehave.have >= Torrentget->Piecesrandom;
}

schedule(reqch: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer)
{
	schedule0(reqch, peer);
	reqch <-= nil;
}

schedule0(reqch: chan of ref (ref Piece, list of Req, chan of int), peer: ref Peer)
{
	if(rarestfirst()) {
		say("schedule: doing rarest first");

		# attempt to work on last piece this peer was working on
		say("schedule: trying previous piece peer was working on");  # xxx should check if no other peer has taken over the piece?
		req := peer.reqs.last();
		if(req != nil) {
			piece := pieces->piecefind(req.pieceindex);
			if(piece != nil)
				schedpieces(reqch, peer, array[] of {piece});
		}

		# find rarest orphan pieces to work on
		say("schedule: trying rarest orphans");
		a := piecesrareorphan(1);
		schedpieces(reqch, peer, a);
		if(!needblocks(peer))
			return;

		# find rarest inactive piece to work on
		say("schedule: trying inactive pieces");
		rare := inactiverare();
		for(i := 0; i < len rare; i++) {
			v := rare[i].t0;  # it's a ref tuple to allow using polymorphic functions
			say(sprint("using new rare piece %d", v));
			p := pieces->piecenew(v);
			piecebusy.set(v);
			schedpieces(reqch, peer, array[] of {p});
			if(!needblocks(peer))
				return;
		}

		# find rarest active non-orphan piece to work on
		say("schedule: trying rarest non-orphans");
		a = piecesrareorphan(0);
		schedpieces(reqch, peer, a);
		if(!needblocks(peer))
			return;
		
	} else {
		say("schedule: doing random");

		# schedule requests for blocks of active pieces, most completed first:  we want whole pieces fast
		a := misc->l2a(pieces->pieces);
		misc->sort(a, progresscmp);
		for(i := 0; i < len a; i++) {
			piece := a[i];
			say(sprint("schedule: looking at piece %d", piece.index));

			# skip piece if this peer doesn't have it
			if(!peer.piecehave.get(piece.index))
				continue;

			# divide piece into batches
			b := requests->batches(piece);

			# request blocks from unused batches first
			say("schedule: trying unused batches from piece");
			schedbatches(reqch, peer, b);
			if(!needblocks(peer))
				return;

			# if more requests needed, start on partially used batches too, in reverse order (for fewer duplicate data)
			say("schedule: trying partially used batches from piece");
			for(k := len b-1; k >= 0; k--) {
				request(reqch, piece, b[k].usedpartial(peer));
				if(!needblocks(peer))
					return;
			}
		}

		# otherwise, get new random pieces to work on
		say("schedule: trying random pieces");
		while(needblocks(peer) && piecebusy.have < piecebusy.n) {
			say("schedule: getting another random piece...");
			piece := getrandompiece();
			b := requests->batches(piece);
			schedbatches(reqch, peer, b);
		}
	}
}

say(s: string)
{
	if(1)
		sys->fprint(sys->fildes(2), "%s\n", s);
}
