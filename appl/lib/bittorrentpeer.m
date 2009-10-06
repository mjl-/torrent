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


	List: adt[T] {
		next:	cyclic ref List[T];
		e:	T;
	};

	Link: adt[T] {
		prev,
		next:	cyclic ref Link[T];
		e:	T;
	};

	Bigtab: adt[T] {
		items:	array of list of (big, T);
		length:	int;

		new:	fn(n: int): ref Bigtab[T];
		clear:	fn(t: self ref Bigtab);
		add:	fn(t: self ref Bigtab, k: big, v: T);
		del:	fn(t: self ref Bigtab, k: big): T;
		find:	fn(t: self ref Bigtab, k: big): T;
		all:	fn(t: self ref Bigtab): list of T;
	};

	Queue: adt[T] {
		first,
		last:	ref Link[T];
		length:	int;

		new:		fn(): ref Queue[T];
		take:		fn(q: self ref Queue): T;
		takeq:		fn(q: self ref Queue): ref Queue[T];
		prepend:	fn(q: self ref Queue, e: T): ref Link[T];
		append:		fn(q: self ref Queue, e: T): ref Link[T];
		empty:		fn(q: self ref Queue): int;
		unlink:		fn(q: self ref Queue, l: ref Link[T]): T;
	};

	TrafficHistsecs:	con 20;
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

	Plistener, Pdialing, Pconnected, Pseeding: con 1<<iota;
	Newpeer: adt {
		addr:		string;
		ip:		string;
		port:		int;
		peerid:		array of byte;	# may be nil, for incoming connections or compact track responses
		time,			# next time peer can be used
		waittime,		# last number of seconds back off
		state:		int;
		banreason,		# reason for ban, not a ban if empty
		lasterror:	string;

		text:   fn(np: self ref Newpeer): string;
	};

	Newpeers: adt {
		localips:	list of string;
		p:		array of ref Newpeer;
		np:		int;
		done:		int;

		new:		fn(): ref Newpeers;
		add:		fn(n: self ref Newpeers, tp: Bittorrent->Trackpeer);
		addmany:	fn(n: self ref Newpeers, tps: array of Bittorrent->Trackpeer);
		addlistener:	fn(n: self ref Newpeers, addr: string, peerid: array of byte): ref Newpeer;
		markself:	fn(n: self ref Newpeers, ip: string);
		markdone:	fn(n: self ref Newpeers);
		peers:		fn(n: self ref Newpeers): ref List[ref Newpeer];
		faulty:		fn(n: self ref Newpeers): ref List[ref Newpeer];
		npeers,
		nfaulty,
		ndialers,
		nready,
		nseeding,
		nlisteners:	fn(n: self ref Newpeers): int;
		cantake:	fn(n: self ref Newpeers): int;
		take:		fn(n: self ref Newpeers): ref Newpeer;
		dialfailed:	fn(n: self ref Newpeers, np: ref Newpeer, backoff: int, err: string);
		connected:	fn(n: self ref Newpeers, np: ref Newpeer);
		disconnected:	fn(n: self ref Newpeers, np: ref Newpeer, err: string);
		disconnectfaulty:	fn(n: self ref Newpeers, np: ref Newpeer, err: string): int;
		seeding:	fn(n: self ref Newpeers, np: ref Newpeer);
		isfaulty:	fn(n: self ref Newpeers, ip: string): int;
	};

	Piecepeer: adt {
		peerid:		int;
		canrequest,				# blocks we can still request (we don't have and we haven't requested yet)	
		requested:	ref Bitarray->Bits;	# blocks requested and response pending
	};

	Piece: adt {
		index:		int;
		have,					# blocks we have received (might not be written!)
		written,				# blocks we have written to disk (subset of "have")
		needrequest:	ref Bitarray->Bits;	# blocks we don't have and could use a request (note that if cleared, we can still send another request for same blocks to other peers)
		length:		int;
		peers:		ref Tables->Table[ref Piecepeer];	# peers working on this piece
		peersgiven:	list of int;	# peers that contributed
		hash:		ref Keyring->DigestState;
		hashoff:	int; 
	};

	Req: adt {
		piece,
		begin,
		length:	int;

		key:	fn(r: self ref Req): big;
		rkey:	fn(piece, begin, length: int): big;
		eq:	fn(r1, r2: ref Req): int;
		text:	fn(rr: self ref Req): string;
	};

	Reqs: adt {
		q:	ref Queue[ref Req];
		tab:	ref Bigtab[ref Link[ref Req]];

		new:		fn(): ref Reqs;
		clear:		fn(rr: self ref Reqs);
		add:		fn(rr: self ref Reqs, r: ref Req);
		del:		fn(rr: self ref Reqs, r: ref Req): int;
		delkey:		fn(rr: self ref Reqs, key: big): ref Req;
		takefirst:	fn(rr: self ref Reqs): ref Req;
	};

	Read: adt {
		pick {
		Piece =>
			cancelled:	int;
			r:	ref Req;
			m:	ref Bittorrent->Msg.Piece;
		Token =>
		}
	};

	RemoteChoking, RemoteInterested, LocalChoking, LocalInterested: con 1<<iota;	# Peer.state
	Gfaulty, Gunknown, Ghalfgood, Ggood: con iota;	# Peer.good
	Peer: adt {
		id:		int;
		np:		ref Newpeer;
		maskip:		string;
		fd:		ref Sys->FD;
		extensions,
		peerid:		array of byte;
		peeridhex:	string;
		good:		int;
		wantmsg:	int;
		outmsgc:	chan of ref Queue[ref Bittorrent->Msg];
		metamsgs:	ref Queue[ref Bittorrent->Msg];
		rhave,					# pieces remote has
		lwant,					# pieces remote has and we do not
		canschedule:	ref Bitarray->Bits;	# pieces remote has, we do not, and we can schedule blocks from
		state:		int;  # interested/choked
		msgseq:		int;
		up,
		down,
		metaup,
		metadown: 	ref Traffic;
		lreqs,				# we want from remote
		rreqs:		ref Reqs;	# remote wants from us
		lastunchokemsg,
		lastchokemsg,
		lastpiecemsg,
		lastrequestmsg,
		lastsend,
		lastrecv:	int;
		lastpiece:	int;
		unchokeblocks:	int;
		dialed:		int;  # whether we initiated connection
		chunk:		ref Chunk;  # unwritten part of piece
		writec:		chan of ref Chunkwrite;
		readc:		chan of (big, list of ref Read, int);
		reading:	ref Bigtab[ref Read.Piece]; # index by Req key, for cancelling
		reads:		ref Queue[ref Read];
		ctime:		int;

		new:			fn(np: ref Newpeer, fd: ref Sys->FD, extensions, peerid: array of byte, dialed: int, npieces: int, maskip: string): ref Peer;
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
			peerid,
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
			time:	int;
			peerid:	array of byte;
			banreason:	string;
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
