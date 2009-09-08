Bittorrentpeer: module
{
	PATH:	con "/dis/lib/bittorrentpeer.dis";

	dflag:	int;
	init:	fn(st: ref State);

	Diskchunksize:	con 128*1024;  # opportunisticly gather buffer of max this size before flushing to disk, keeps data on disk contiguously

	peeridfmt:	fn(d: array of byte): string;

	State: adt {
		t:		ref Bittorrent->Torrent;
		tx:		ref Bittorrent->Torrentx;
		have:		ref Bitarray->Bits;
		active,	
		orphans:	ref Tables->Table[ref Piece];
	};

	PoolRandom, PoolRotateRandom, PoolInorder: con iota;  # Pool.mode
	Pool: adt[T] {
		active:	array of T;
		pool:	array of T;
		poolnext:	int;
		mode:	int;

		new:	fn(mode: int): ref Pool[T];
		clear:	fn(p: self ref Pool);
		fill:	fn(p: self ref Pool);
		take:	fn(p: self ref Pool): T;
		pooladd:	fn(p: self ref Pool, e: T);
		pooladdunique:	fn(p: self ref Pool, e: T);
		poolhas:	fn(p: self ref Pool, e: T): int;
		pooldel:	fn(p: self ref Pool, e: T);
		text:	fn(p: self ref Pool): string;
	};


	TrafficHistsecs:	con 10;
	TrafficHistslotmsec2:	con 8;  # msecs for slot: 2**TrafficHistslotmsec2
	Traffic: adt {
		last:	big;  # last time, in msec
		lasti:	int;  # index in `bytes' for `last'
		bytes:	array of int; 
		winsum:	int;
		sum:	big;
		npackets:	int;
		time0:	big;

		new:	fn(): ref Traffic;
		add:	fn(t: self ref Traffic, nbytes, npkts: int);
		rate:	fn(t: self ref Traffic): int;
		total:	fn(t: self ref Traffic): big;
		text:	fn(t: self ref Traffic): string;
	};

	Newpeer: adt {
		addr:   string;
		ip:     string;
		peerid: array of byte;  # may be nil, for incoming connections or compact track responses

		text:   fn(np: self Newpeer): string;
	};

	Newpeers: adt {
		l:	list of Newpeer;

		del:	fn(n: self ref Newpeers, np: Newpeer);
		add:	fn(n: self ref Newpeers, np: Newpeer);
		take:	fn(n: self ref Newpeers): Newpeer;
		all:	fn(n: self ref Newpeers): list of Newpeer;
		empty:	fn(n: self ref Newpeers): int;
	};


	Piece: adt {
		index:		int;
		have,
		written,
		requested:	ref Bitarray->Bits;
		length:		int;
		peer:		int;	# current peer working on this piece
		peers:		list of int;	# peers that contributed
		hash:		ref Keyring->DigestState;
		hashoff:	int; 
	};


	LReq: adt {
		piece,
		block:	int;

		key:	fn(r: self ref LReq): big;
		eq:	fn(r1, r2: ref LReq): int;
		text:	fn(r: self ref LReq): string;
	};

	RReq: adt {
		piece,
		begin,
		length:	int;

		eq:	fn(r1, r2: ref RReq): int;
		text:	fn(rr: self ref RReq): string;
	};


	Link: adt[T] {
		prev,
		next:	cyclic ref Link[T];
		e:	T;
	};

	Bigtab: adt[T] {
		items:	array of list of (big, T);
		nelems:	int;

		new:	fn(n: int): ref Bigtab;
		clear:	fn(t: self ref Bigtab);
		add:	fn(t: self ref Bigtab, k: big, v: T);
		del:	fn(t: self ref Bigtab, k: big): T;
		find:	fn(t: self ref Bigtab, k: big): T;
		all:	fn(t: self ref Bigtab): list of T;
	};

	RReqs: adt {
		first,
		last:	ref Link[ref RReq];
		tab:	ref Bigtab[ref Link[ref RReq]];
		length:	int;

		new:		fn(): ref RReqs;
		clear:		fn(rr: self ref RReqs);
		add:		fn(rr: self ref RReqs, r: ref RReq);
		del:		fn(rr: self ref RReqs, r: ref RReq): int;
		dropfirst:	fn(rr: self ref RReqs);
	};

	Queue: adt[T] {
		first,
		last:	ref List[T];

		new:		fn(): ref Queue[T];
		take:		fn(q: self ref Queue): T;
		takeq:		fn(q: self ref Queue): ref Queue[T];
		prepend:	fn(q: self ref Queue, e: T);
		add:		fn(q: self ref Queue, e: T);
		empty:		fn(q: self ref Queue): int;
	};


	# Peer.state
	RemoteChoking, RemoteInterested, LocalChoking, LocalInterested: con (1<<iota);
	Peer: adt {
		id:		int;
		np:		Newpeer;
		fd:		ref Sys->FD;
		extensions,
		peerid:		array of byte;
		peeridhex:	string;
		good:		int;	# whether it has sent full, verified piece to us, all by itself
		lastpiece:	int;
		getmsg:		int;
		getmsgc:	chan of ref Queue[ref Bittorrent->Msg];
		metamsgs,
		datamsgs: 	ref Queue[ref Bittorrent->Msg];
		rhave,					# pieces remote has
		lwantinact:	ref Bitarray->Bits;	# pieces remote has, we do not, and are not active or orphan
		state:		int;  # interested/choked
		msgseq:		int;
		up,
		down,
		metaup,
		metadown: 	ref Traffic;
		lreqs:		ref Bigtab[ref LReq];  # we want from remote
		rreqs:		ref RReqs;  # remote wants from us
		lastunchoke:	int;
		dialed:		int;  # whether we initiated connection
		chunk:		ref Chunk;  # unwritten part of piece
		writec:		chan of ref Chunkwrite;
		readc:		chan of ref RReq;
		netwritepid:	int;

		new:			fn(np: Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int, npieces: int): ref Peer;
		remotechoking:		fn(p: self ref Peer): int;
		remoteinterested:	fn(p: self ref Peer): int;
		localchoking:		fn(p: self ref Peer): int;
		localinterested:	fn(p: self ref Peer): int;
		isdone:			fn(p: self ref Peer): int;
		text:			fn(p: self ref Peer): string;
	};

	Chunk: adt {
		bufs:		ref Queue[array of byte];
		piece,
		begin,	
		piecelength,
		nbytes:		int;

		new:		fn(): ref Chunk;
		tryadd:		fn(c: self ref Chunk, piece: ref Piece, begin: int, buf: array of byte): int;
		isempty:	fn(c: self ref Chunk): int;
		isfull:		fn(c: self ref Chunk): int;
		overlaps:	fn(c: self ref Chunk, piece, begin, end: int): int;
		flush:		fn(c: self ref Chunk): ref Chunkwrite;
	};

	Chunkwrite: adt {
		piece,
		begin:	int;
		bufs:	ref Queue[array of byte];
	};


	Progress: adt {
		pick {
		Endofstate or
		Done or
		Started or
		Stopped or
		Newctl =>
		Piece =>
			p,
			have,
			total:	int;
		Block =>
			p,
			b,
			have,
			total:	int;
		Pieces =>
			l:	list of int;
		Blocks =>
			p:	int;
			l:	list of int;
		Filedone =>
			index:	int;
			path,
			origpath:	string;
		Tracker =>
			interval,
			next,
			npeers:	int;
			err:	string;
		Error =>
			msg:	string;
		Hashfail =>
			index:	int;
		}

		text:	fn(p: self ref Progress): string;
	};

	Schoking, Sunchoking, Sinterested, Suninterested: con iota;
	Slocal, Sremote: con iota<<2;
	Peerevent: adt {
		pick {
		Endofstate =>
		Dialing =>
			addr:	string;
		Tracker =>
			addr:	string;
		New =>
			addr:	string;
			id:	int;
			peeridhex:	string;
			dialed:	int;
		Gone =>
			id:	int;
		Bad =>
			ip:	string;
			mtime:	int;
			err:	string;
		State =>
			id:	int;
			s:	int;
		Piece =>
			id:	int;
			piece:	int;
		Pieces =>
			id:	int;
			pieces:	list of int;
		Done =>
			id:	int;
		}

		text:	fn(pp: self ref Peerevent): string;
	};

	List: adt[T] {
		next:	cyclic ref List[T];
		e:	T;
	};

	Eventfid: adt[T]
		for {
		T =>
			text:	fn(nil: self T): string;
		}
	{
	
		fid:	int;
		r:	list of ref Styx->Tmsg.Read;
		last:	ref List[T];

		new:		fn(fid: int): ref Eventfid[T];
		putread:	fn(ef: self ref Eventfid, r: ref Styx->Tmsg.Read);
		read:		fn(ef: self ref Eventfid): ref Styx->Rmsg.Read;
		flushtag:	fn(ef: self ref Eventfid, tag: int): int;
	};
};
