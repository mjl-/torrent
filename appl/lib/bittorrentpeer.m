Bittorrentpeer: module
{
	PATH:	con "/dis/lib/bittorrentpeer.dis";

	dflag:	int;
	init:	fn();

	Peeridlen:	con 20;

	Piecesrandom:	con 4;
	Blocksize:	con 16*1024;
	Blockqueuemax:	con 100;  # max number of Requests a peer can queue at our side without being considered bad
	Blockqueuesize:	con 30;  # number of pending blocks to request to peer
	Diskchunksize:	con 128*1024;  # do initial write to disk for any block/piece of this size, to prevent fragmenting the file system
	Batchsize:	con Diskchunksize/Blocksize;


	pieces:		list of ref Piece;  # only active pieces
	trackerpeers:   list of Newpeer;  # peers we are not connected to
	peers:		list of ref Peer;  # peers we are connected to
	luckypeer:	ref Peer;
	piecehave:	ref Bitarray->Bits;
	piecebusy:	ref Bitarray->Bits;
	piececounts:	array of int;  # for each piece, count of peers that have it


	randomize:	fn[T](a: array of T);
	maskip:		fn(ipstr: string): string;


	PoolRandom, PoolRotateRandom, PoolInorder: con iota;  # Pool.mode
	Pool: adt[T] {
		active:	array of T;
		pool:	array of T;
		poolnext:	int;
		mode:	int;

		new:	fn(mode: int): ref Pool[T];
		fill:	fn(p: self ref Pool);
		take:	fn(p: self ref Pool): T;
		pooladd:	fn(p: self ref Pool, e: T);
		pooladdunique:	fn(p: self ref Pool, e: T);
		poolhas:	fn(p: self ref Pool, e: T): int;
		pooldel:	fn(p: self ref Pool, e: T);
		text:	fn(p: self ref Pool): string;
	};


	TrafficHistorysize:	con 10;
	Traffic: adt {
		last:	int;  # last element in `d' that may have been used
		d:	array of (int, int);  # time, bytes
		winsum:	int;
		sum:	big;
		npackets:	int;
		time0:	int;

		new:	fn(): ref Traffic;
		add:	fn(t: self ref Traffic, bytes: int);
		packet:	fn(t: self ref Traffic);
		rate:	fn(t: self ref Traffic): int;
		total:	fn(t: self ref Traffic): big;
		text:	fn(t: self ref Traffic): string;
	};


	Piece: adt {
		hashstate:	ref Keyring->DigestState;
		hashstateoff:	int;
		index:	int;
		have:	ref Bitarray->Bits;
		written:	ref Bitarray->Bits;
		length:	int;
		busy:	array of (int, int);  # peerid, peerid
		done:	array of int;  # peerid

		new:	fn(index, length: int): ref Piece;
		isdone:	fn(p: self ref Piece): int;
		orphan:	fn(p: self ref Piece): int;
		hashadd:	fn(p: self ref Piece, buf: array of byte);
		text:	fn(p: self ref Piece): string;
	};

	Block: adt {
		piece,
		begin,
		length:	int;

		new:	fn(piece, begin, length: int): ref Block;
		eq:	fn(b1, b2: ref Block): int;
		text:	fn(b: self ref Block): string;
	};


	piecenew:	fn(t: ref Torrent, index: int): ref Piece;
	piecedel:	fn(p: ref Piece);
	piecefind:	fn(index: int): ref Piece;

	blockhave:	fn(l: list of ref Block, b: ref Block): int;
	blockdel:	fn(l: list of ref Block, b: ref Block): list of ref Block;


	Newpeer: adt {
		addr:   string;
		ip:     string;
		peerid: array of byte;  # may be nil, for incoming connections or compact track responses

		text:   fn(np: self Newpeer): string;
	};

	# Peer.state
	RemoteChoking, RemoteInterested, LocalChoking, LocalInterested: con (1<<iota);
	peerstatestr:	fn(state: int): string;
	Peer: adt {
		id:		int;
		np:		Newpeer;
		fd:		ref Sys->FD;
		extensions,
		peerid:		array of byte;
		peeridhex:	string;
		getmsg:		int;
		getmsgch:	chan of list of ref Bittorrent->Msg;
		metamsgs,
		datamsgs: 	list of ref Bittorrent->Msg;
		reqs:		ref Reqs;  # we want from remote
		piecehave:	ref Bitarray->Bits;
		state:		int;  # interested/choked
		msgseq:		int;
		up,
		down,
		metaup,
		metadown: 	ref Traffic;
		wants:		list of ref Block;  # remote wants from us
		lastunchoke:	int;
		dialed:		int;  # whether we initiated connection
		buf:		ref Buf;  # unwritten part of piece
		writech:	chan of ref (int, int, array of byte);
		readch:		chan of ref (int, int, int);
		pids:		list of int;  # pids of net reader/writer to kill for cleaning up

		new:		fn(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int, npieces: int): ref Peer;
		remotechoking:	fn(p: self ref Peer): int;
		remoteinterested:	fn(p: self ref Peer): int;
		localchoking:	fn(p: self ref Peer): int;
		localinterested:	fn(p: self ref Peer): int;
		isdone:		fn(p: self ref Peer): int;
		text:		fn(p: self ref Peer): string;
		fulltext:	fn(p: self ref Peer): string;
	};

	Buf: adt {
		data:	array of byte;
		piece:	int;
		pieceoff:	int;
		piecelength:	int;

		new:		fn(): ref Buf;
		tryadd:		fn(b: self ref Buf, piece: ref Piece, begin: int, buf: array of byte): int;
		isfull:		fn(b: self ref Buf): int;
		clear:		fn(b: self ref Buf);
		overlaps:	fn(b: self ref Buf, piece, begin, end: int): int;
	};


	trackerpeerdel:	fn(np: Newpeer);
	trackerpeeradd:	fn(np: Newpeer);
	trackerpeertake:	fn(): Newpeer;


	peerconnected:	fn(addr: string): int;
	peerdel:	fn(peer: ref Peer);
	peeradd:	fn(p: ref Peer);
	peerknownip:	fn(ip: string): int;
	peerhas:	fn(p: ref Peer): int;
	peersdialed:	fn(): int;
	peerfind:	fn(id: int): ref Peer;
	peersunchoked:	fn(): list of ref Peer;
	peersactive:	fn(): list of ref Peer;


	Req: adt {
		pieceindex, blockindex, cancelled: int;

		new:	fn(pieceindex, blockindex: int): Req;
		eq:	fn(r1, r2: Req): int;
		text:	fn(r: self Req): string;
	};

	Reqs: adt {
		a:	array of Req;
		first, next:	int;
		lastreq:	ref Req;

		new:	fn(size: int): ref Reqs;
		take:	fn(r: self ref Reqs, req: Req): int;
		add:	fn(r: self ref Reqs, req: Req);
		peek:	fn(r: self ref Reqs): Req;
		cancel:	fn(r: self ref Reqs, req: Req);
		flush:	fn(r: self ref Reqs);
		last:	fn(r: self ref Reqs): ref Req;
		isempty:	fn(r: self ref Reqs): int;
		isfull:		fn(r: self ref Reqs): int;
		count:	fn(r: self ref Reqs): int;
		size:	fn(r: self ref Reqs): int;
		text:	fn(r: self ref Reqs): string;
	};

	Batch: adt {
		blocks:	array of int;
		piece:	ref Piece;

		new:	fn(first, n: int, piece: ref Piece): ref Batch;
		unused:	fn(b: self ref Batch): list of Req;
		usedpartial:	fn(b: self ref Batch, peer: ref Peer): list of Req;
		text:	fn(b: self ref Batch): string;
	};

	batches:	fn(p: ref Piece): array of ref Batch;

	piecehash:	fn(fds: list of ref (ref Sys->FD, big), piecelen: int, p: ref Piece): (array of byte, string);
	torrenthash:	fn(fds: list of ref (ref Sys->FD, big), t: ref Bittorrent->Torrent, haves: ref Bitarray->Bits): string;
	reader:		fn(t: ref Bittorrent->Torrent, fds: list of ref (ref Sys->FD, big), c: chan of (array of byte, string));


	needblocks:	fn(p: ref Peer): int;
	schedule:	fn(t: ref Torrent, reqch: chan of ref (ref Piece, list of Req, chan of int), p: ref Peer);
};