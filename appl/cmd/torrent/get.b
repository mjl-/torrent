implement Torrentget;

include "sys.m";
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "daytime.m";
include "string.m";
include "keyring.m";
include "security.m";
include "rand.m";
include "lists.m";
include "math.m";

include "bitarray.m";
	bitarray: Bitarray;
	Bits: import bitarray;
include "bittorrent.m";
include "../../lib/bittorrent/get.m";

sys: Sys;
daytime: Daytime;
str: String;
keyring: Keyring;
random: Random;
rand: Rand;
lists: Lists;

bittorrent: Bittorrent;
misc: Misc;
pools: Pools;
rate: Rate;
pieces: Pieces;
peers: Peers;
requests: Requests;
verify: Verify;
sched: Schedule;
state: State;

print, sprint, fprint, fildes: import sys;
Bee, Msg, Torrent: import bittorrent;
DigestState: import keyring;
Pool: import pools;
Traffic: import rate;
Req, Reqs, Batch: import requests;
Peer, Newpeer, Buf: import peers;
Piece, Block: import pieces;
sort, l2a, hex, readfd, readfile: import misc;


Dflag: int;
Pflag: int;
Lflag: int;
nofix: int;

torrent: ref Torrent;
dstfds: list of ref (ref Sys->FD, big);  # fd, size
statefd: ref Sys->FD;
starttime: int;
totalleft: big;
listenport: int;
localpeerid: array of byte;
localpeeridhex: string;
trackerevent: string;
trafficup, trafficdown, trafficmetaup, trafficmetadown: ref Traffic;  # global traffic counters.  xxx uses same sliding window as traffic speed used for choking
maxratio := 0.0;
maxdownload := big -1;
maxupload := big -1;

# tracker
trackkickchan:	chan of int;
trackreqchan:	chan of (big, big, big, int, string);  # up, down, left, listenport, event
trackchan:	chan of (int, array of (string, int, array of byte), string);  # interval, peers, error

# dialer/listener
canlistenchan: chan of int;
newpeerchan: chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);

# upload/download rate limiter
upchan, downchan: chan of (int, chan of int);

# progress/state
stopped := 0;
ndialers:	int;  # number of active dialers
rotateips:	ref Pool[string];  # masked ip address
faulty:		list of (string, int);  # ip, time
islistening:	int;  # whether listener() is listening

peerinmsgchan:	chan of (ref Peer, ref Msg, chan of list of ref (int, int, array of byte));
msgwrittenchan:	chan of ref Peer;
diskwritechan:	chan of ref (int, int, array of byte);
diskwrittenchan:	chan of (int, int, int, string);
mainwrites:	list of ref (int, int, array of byte);

# ticker
tickchan: chan of int;


init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	daytime = load Daytime Daytime->PATH;
	str = load String String->PATH;
	keyring = load Keyring Keyring->PATH;
	random = load Random Random->PATH;
	rand = load Rand Rand->PATH;
	rand->init(random->randomint(Random->ReallyRandom));
	lists = load Lists Lists->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init(bitarray);

	misc = load Misc Misc->PATH;
	misc->init(rand);
	pools = load Pools Pools->PATH;
	pools->init(rand);
	rate = load Rate Rate->PATH;
	rate->init();
	pieces = load Pieces Pieces->PATH;
	pieces->init();
	requests = load Requests Requests->PATH;
	requests->init();
	peers = load Peers Peers->PATH;
	peers->init(rand);
	verify = load Verify Verify->PATH;
	verify->init();
	state = load State State->PATH;
	state->init();
	sched = load Schedule Schedule->PATH;
	sched->init(rand, state, peers, pieces);

	arg->init(args);
	arg->setusage(arg->progname()+" [-DPLn] [-m ratio] [-d maxdownload] [-u maxupload] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'D' =>	Dflag++;
		'P' =>	Pflag++;
		'L' =>	bittorrent->dflag++;
		'n' =>	nofix = 1;
		'm' =>	maxratio = real arg->earg();
			if(maxratio <= 1.1)
				fail("invalid maximum ratio");
		'd' =>	maxdownload = bittorrent->byteparse(arg->earg());
			if(maxdownload < big 0)
				fail("invalid maximum download rate");
		'u' =>	maxupload = bittorrent->byteparse(arg->earg());
			if(maxupload < big (10*1024))
				fail("invalid maximum upload rate");
		* =>
			fprint(fildes(2), "bad option: -%c\n", c);
			arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();

	sys->pctl(Sys->NEWPGRP, nil);

	err: string;
	(torrent, err) = Torrent.open(hd args);
	if(err != nil)
		fail(sprint("%s: %s", hd args, err));

	created: int;
	(dstfds, created, err) = torrent.openfiles(nofix, 0);
	if(err != nil)
		fail(err);

	if(created) {
		# all new files, we don't have pieces yet
		trackerevent = "started";
		say("no state file needed, all new files");
		state->piecehave = Bits.new(torrent.piececount);
	} else {
		# attempt to read state of pieces from .torrent.state file
		statefd = sys->open(torrent.statepath, Sys->ORDWR);
		if(statefd != nil) {
			say("using .state file");
			(d, rerr) := readfd(statefd);
			if(rerr != nil)
				fail(sprint("%s: %s", torrent.statepath, rerr));
			(state->piecehave, err) = Bits.mk(torrent.piececount, d);
			if(err != nil)
				fail(sprint("%s: invalid state", torrent.statepath));
		} else {
			# otherwise, read through all data
			say("starting to check all pieces in files...");

			verify->torrenthash(dstfds, torrent, state->piecehave);
		}
	}

	if(isdone())
		say("already done!");

	if(statefd == nil) {
		say(sprint("creating statepath %q", torrent.statepath));
		statefd = sys->create(torrent.statepath, Sys->ORDWR, 8r666);
		if(statefd == nil)
			warn(sprint("failed to create state file (ignoring): %r"));
		else
			writestate();
	}

	totalleft = torrent.length;
	localpeerid = bittorrent->genpeerid();
	localpeeridhex = hex(localpeerid);

	trackkickchan = chan of int;
	trackreqchan = chan of (big, big, big, int, string);
	trackchan = chan of (int, array of (string, int, array of byte), string);

	canlistenchan = chan of int;
	newpeerchan = chan of (int, Newpeer, ref Sys->FD, array of byte, array of byte, string);
	tickchan = chan of int;

	upchan = chan of (int, chan of int);
	downchan = chan of (int, chan of int);

	peerinmsgchan = chan of (ref Peer, ref Msg, chan of list of ref (int, int, array of byte));
	msgwrittenchan = chan of ref Peer;
	diskwritechan = chan[4] of ref (int, int, array of byte);
	diskwrittenchan = chan of (int, int, int, string);

	trafficup = Traffic.new();
	trafficdown = Traffic.new();
	trafficmetaup = Traffic.new();
	trafficmetadown = Traffic.new();

	pieces->prepare(torrent);
	state->prepare(torrent.piececount);

	rotateips = Pool[string].new(Pools->PoolRotateRandom);

	# start listener, for incoming connections
	ok := -1;
	conn: Sys->Connection;
	listenaddr: string;

	for(i := 0; i < Listenportrange; i++) {
		listenport = Listenport+i;
		listenaddr = sprint("net!%s!%d", Listenhost, listenport);
		(ok, conn) = sys->announce(listenaddr);
		if(ok == 0)
			break;
	}
	if(ok != 0) {
		say("could not listen on any port, incoming connections will not be possible...");
		listenport = 0;
	} else {
		say(sprint("listening on addr %s", listenaddr));
	}

	spawn listener(conn);
	spawn ticker();
	spawn track();
	spawn limiter(upchan, int maxupload);
	spawn limiter(downchan, int maxdownload);
	spawn diskwriter(diskwritechan);

	starttime = daytime->now();
	spawn trackkick(0);
	main();
}

isdone(): int
{
	return (state->piecehave).n == (state->piecehave).have;
}

writestate()
{
	d := (state->piecehave).d;
	n := sys->pwrite(statefd, d, len d, big 0);
	if(n != len d)
		warn(sprint("writing state: %r"));
	else
		say("state written");
}

peerdrop(peer: ref Peer, faulty: int, err: string)
{
	if(err != nil)
		warn(err);
	if(faulty)
		setfaulty(peer.np.ip);

	n := 0;
	for(i := 0; i < (peer.piecehave).n && n < (peer.piecehave).have; i++)
		if((peer.piecehave).get(i)) {
			state->piececounts[i]--;
			n++;
		}

	for(l := pieces->pieces; l != nil; l = tl l) {
		piece := hd l;
		for(i = 0; i < len piece.busy; i++) {
			if(piece.busy[i].t0 == peer.id)
				piece.busy[i].t0 = -1;
			if(piece.busy[i].t1 == peer.id)
				piece.busy[i].t1 = -1;
		}
	}

	if(peer.dialed)
		dialpeers();
	else
		awaitpeer();

	peers->peerdel(peer);
	peer.writech <-= nil;
}

dialpeers()
{
	say(sprint("dialpeers, %d trackerpeers %d peers", len peers->trackerpeers, len peers->peers));

	while(peers->trackerpeers != nil && ndialers < Dialersmax && len peers->peers < Peersmax && peers->peersdialed() < Peersdialedmax) {
		np := peers->trackerpeertake();
		if(peers->peerknownip(np.ip))
			continue;
		if(isfaulty(np.ip))
			continue;

		say("spawning dialproc for "+np.text());
		spawn dialer(np);
		ndialers++;
	}
}

awaitpeer()
{
	if(peers->peersdialed() < Peersmax-Peersdialedmax && !islistening) {
		islistening = 1;
		spawn kicklistener();
	}
}


peersend(p: ref Peer, msg: ref Msg)
{
	msize := msg.packedsize();
	dsize := 0;
	pick m := msg {
	Piece =>
		dsize = len m.d;
		p.up.add(dsize);
		trafficup.add(dsize);
		p.up.packet();
		trafficup.packet();
	* =>
		p.metaup.packet();
		trafficmetaup.packet();
	}
	p.metaup.add(msize-dsize);
	trafficmetaup.add(msize-dsize);
	p.outmsgs <-= msg;
}

blocksize(req: Req): int
{
	# first a quick check
	if(req.pieceindex < torrent.piececount-1)
		return Blocksize;

	# otherwise, the full check
	if(big req.pieceindex*big torrent.piecelen + big (req.blockindex+1)*big Blocksize > torrent.length)
		return int (torrent.length % big Blocksize);
	return Blocksize;
}

blockreadsend(p: ref Peer)
{
	b: ref Block;
	(p.wants, b) = pieces->blocktake(p.wants);
	say(sprint("%s: sending to %s", b.text(), p.text()));

	(d, rerr) := bittorrent->blockread(torrent, dstfds, b.piece, b.begin, b.length);
	if(rerr != nil) {
		warn("reading block: "+rerr);
		return;
	}

	p.netwriting = 1;
	peersend(p, ref Msg.Piece(b.piece, b.begin, d));
}

peerratecmp(a1, a2: ref (ref Peer, int)): int
{
	(p1, r1) := *a1;
	(p2, r2) := *a2;
	n := r2-r1;
	if(n != 0)
		return n;
	if(p1.remoteinterested() == p2.remoteinterested())
		return 0;
	if(p1.remoteinterested())
		return -1;
	return 1;
}

request(peer: ref Peer, piece: ref Piece, reqs: list of Req)
{
	for(; reqs != nil; reqs = tl reqs) {
		req := hd reqs;
		busy := piece.busy[req.blockindex];
		if(busy.t0 < 0)
			piece.busy[req.blockindex].t0 = peer.id;
		else if(busy.t1 < 0)
			piece.busy[req.blockindex].t1 = peer.id;
		else
			raise "both slots busy...";

		# xxx send in one packet?
		say("request: requesting "+req.text());
		peer.reqs.add(req);
		peersend(peer, ref Msg.Request(req.pieceindex, req.blockindex*Blocksize, blocksize(req)));
	}
}

schedule(p: ref Peer)
{
	if(!sched->needblocks(p))
		return;

	reqch := chan of ref (ref Piece, list of Req, chan of int);
	spawn sched->schedule(reqch, p);
	while((r := <-reqch) != nil) {
		(piece, reqs, donech) := *r;
		request(p, piece, reqs);
		donech <-= 0;
	}
}


# peer state

choke(p: ref Peer)
{
	peersend(p, ref Msg.Choke());
	p.state |= Peers->LocalChoking;
}

unchoke(p: ref Peer)
{
	peersend(p, ref Msg.Unchoke());
	p.state &= ~Peers->LocalChoking;
	p.lastunchoke = daytime->now();
}

wantpeerpieces(p: ref Peer): ref Bits
{
	b := (state->piecehave).clone();
	b.invert();
	b = Bits.and(array[] of {p.piecehave, b});

	say("pieces peer has and we are interested in: "+b.text());
	return b;
}

interesting(p: ref Peer)
{
	# xxx we should call this more often.  a peer may have a piece we don't have yet, but we may have assigned all remaining blocks to (multiple) other peers, or we may be in paranoid mode
	if(p.localinterested()) {
		if(p.reqs.isempty() && wantpeerpieces(p).isempty()) {
			say("we are no longer interested in "+p.text());
			p.state &= ~Peers->LocalInterested;
			peersend(p, ref Msg.Notinterested());
		}
	} else {
		if(!wantpeerpieces(p).isempty()) {
			say("we are now interested in "+p.text());
			p.state |= Peers->LocalInterested;
			peersend(p, ref Msg.Interested());
		}
	}
}


# faulty

isfaulty(ip: string): int
{
	now := daytime->now();
	for(l := faulty; l != nil; l = tl l)
		if((hd l).t0 == ip && now < (hd l).t1+Ignorefaultyperiod)
			return 1;
	return 0;
}

setfaulty(ip: string)
{
	clearfaulty(nil);
	faulty = (ip, daytime->now())::faulty;
}

clearfaulty(ip: string)
{
	now := daytime->now();
	new: list of (string, int);
	for(l := faulty; l != nil; l = tl l)
		if((hd l).t0 != ip && (hd l).t1+Ignorefaultyperiod < now)
			new = hd l::new;
	faulty = new;
}


peerbufflush(b: ref Buf): ref (int, int, array of byte)
{
	say(sprint("buf: writing chunk to disk, pieceoff %d, len data %d", b.pieceoff, len b.data));
	say("letting peers diskwriter handle write");
	tmp := ref (b.piece, b.pieceoff, b.data);
	b.clear();
	return tmp;
}

mainbufflush(b: ref Buf)
{
	say(sprint("buf: writing chunk to disk, pieceoff %d, len data %d", b.pieceoff, len b.data));
	mainwrites = lists->reverse(ref (b.piece, b.pieceoff, b.data)::lists->reverse(mainwrites));
	say("letting mains diskwriter handle write");
	b.clear();
}

batchflushcomplete(piece: ref Piece, block: int): int
{
	b := requests->batches(piece)[block/Batchsize];
	for(i := 0; i < len b.blocks; i++)
		if(!piece.have.get(b.blocks[i]))
			return 0;

	start := (block/Batchsize)*Batchsize*Blocksize;
	end := start+Batchsize*Blocksize;
	for(l := peers->peers; l != nil; l = tl l) {
		peer := hd l;
		if(peer.buf.overlaps(piece.index, start, end))
			mainbufflush(peer.buf);
	}
	return 1;
}

nextoptimisticunchoke(): ref Peer
{
	rotateips.fill(); # xxx replace by markstart, and then lazily rotate

	# find next masked ip address to pick peer from (if still present)
	for(;;) {
		ipmasked := rotateips.take();
		if(ipmasked == nil)
			break;

		# find peer from the address pool with oldest unchoke
		peer: ref Peer;
		for(l := peers->peers; l != nil; l = tl l) {
			p := hd l;
			if(misc->maskip(p.np.ip) == ipmasked && (peer == nil || p.lastunchoke < peer.lastunchoke))
				peer = p;
		}
		if(peer != nil)
			return peer;
		else
			rotateips.pooldel(ipmasked);
	}
	return nil;
}

chokingupload(gen: int)
{
	if(gen % 3 == 2)
		return;

	# find the peer that has been unchoked longest
	oldest: ref Peer;
	nunchoked := 0;
	for(l := peers->peers; l != nil; l = tl l) {
		p := hd l;
		if(!p.localchoking() && p.remoteinterested()) {
			nunchoked++;
			if(oldest == nil || p.lastunchoke < oldest.lastunchoke)
				oldest = p;
		}
	}

	# find all peers that we may want to unchoke randomly
	others: list of ref Peer;
	for(l = peers->peers; l != nil; l = tl l) {
		p := hd l;
		if(p.remoteinterested() && p.localchoking() && p != oldest)
			others = p::others;
	}

	# choke oldest unchoked peer if we want to reuse its slot
	if(oldest != nil && nunchoked+len others >= Seedunchokedmax) {
		choke(oldest);
		nunchoked--;
	}

	othersa := l2a(others);
	misc->randomize(othersa);
	for(i := 0; i < len othersa && nunchoked+i < Seedunchokedmax; i++)
		unchoke(othersa[i]);
}

chokingdownload(gen: int)
{
	# new optimistic unchoke?
	if(gen % 3 == 0)
		peers->luckypeer = nextoptimisticunchoke();

	# make sorted array of all peers, sorted by upload rate, then by interestedness
	allpeers := array[len peers->peers] of ref (ref Peer, int);  # peer, rate
	i := 0;
	luckyindex := -1;
	for(l := peers->peers; l != nil; l = tl l) {
		if(peers->luckypeer != nil && hd l == peers->luckypeer)
			luckyindex = i;
		allpeers[i++] = ref (hd l, (hd l).down.rate());
	}
	sort(allpeers, peerratecmp);

	# determine N interested peers with highest upload rate
	nintr := 0;
	for(i = 0; nintr < Unchokedmax && i < len allpeers; i++)
		if(allpeers[i].t0.remoteinterested())
			nintr++;
	unchokeend := i;  # index of first peer to choke.  element before (if any) is slowest peer to unchoke

	# replace slowest of N by optimistic unchoke if lucky peer was not already going to be unchoked
	if(luckyindex >= 0 && luckyindex >= unchokeend && unchokeend-1 >= 0) {
		allpeers[luckyindex] = allpeers[unchokeend-1];
		allpeers[unchokeend-1] = ref (peers->luckypeer, 0);
	}

	# now unchoke the N peers, and all non-interested peers that are faster.  choke all other peers if they weren't already.
	for(i = 0; i < len allpeers; i++) {
		(p, nil) := *allpeers[i];
		if(p == nil)
			say(sprint("bad allpeers, len allpeers %d, len peers %d, i %d, unchokeend %d, luckyindex %d, nintr %d", len allpeers, len peers->peers, i, unchokeend, luckyindex, nintr));
		if(i < unchokeend && p.localchoking())
			unchoke(p);
		else if(i >= unchokeend && !p.localchoking())
			choke(p);
	}
}

handleinmsg(peer: ref Peer, msg: ref Msg, needwritechan: chan of list of ref (int, int, array of byte))
{
	handleinmsg0(peer, msg, needwritechan);
	if(tagof msg == tagof Msg.Piece)
		needwritechan <-= nil;
}

# xxx fix this code to never either block entirely or delay on disk/net i/o
handleinmsg0(peer: ref Peer, msg: ref Msg, needwritechan: chan of list of ref (int, int, array of byte))
{
	peer.msgseq++;

	msize := msg.packedsize();
	dsize := 0;
	pick m := msg {
	Piece =>
		# xxx count pieces we did not request as overhead (meta)?
		dsize = len m.d;
		peer.down.add(dsize);
		trafficdown.add(dsize);
		peer.down.packet();
		trafficdown.packet();
	* =>
		peer.metadown.packet();
		trafficmetadown.packet();
	}
	peer.metadown.add(msize-dsize);
	trafficmetadown.add(msize-dsize);

	pick m := msg {
	Keepalive =>
		say("keepalive");

	Choke =>
		if(peer.remotechoking())
			return say(sprint("%s choked us twice...", peer.text())); # xxx disconnect?

		say(sprint("%s choked us...", peer.text()));
		peer.state |= Peers->RemoteChoking;

	Unchoke =>
		if(!peer.remotechoking())
			return say(sprint("%s unchoked us twice...", peer.text())); # xxx disconnect?

		say(sprint("%s unchoked us", peer.text()));
		peer.state &= ~Peers->RemoteChoking;

		schedule(peer);

	Interested =>
		if(peer.remoteinterested())
			return say(sprint("%s is interested again...", peer.text())); # xxx disconnect?

		say(sprint("%s is interested", peer.text()));
		peer.state |= Peers->RemoteInterested;

		if(!peer.localchoking() && len peers->peersactive() >= Unchokedmax && !isdone()) {
			# xxx choke slowest peer that is not the optimistic unchoke
		}

	Notinterested =>
		if(!peer.remoteinterested())
			return say(sprint("%s is uninterested again...", peer.text())); # xxx disconnect?

		say(sprint("%s is no longer interested", peer.text()));
		peer.state &= ~Peers->RemoteInterested;

		# xxx if this peer was choked, unchoke another peer?

	Have =>
		if(m.index >= torrent.piececount)
			return peerdrop(peer, 1, sprint("%s sent 'have' for invalid piece %d, disconnecting", peer.text(), m.index));

		if(peer.piecehave.get(m.index)) {
			say(sprint("%s already had piece %d", peer.text(), m.index)); # xxx disconnect?
		} else
			state->piececounts[m.index]++;

		say(sprint("remote now has piece %d", m.index));
		peer.piecehave.set(m.index);

		interesting(peer);
		if(!peer.remotechoking())
			schedule(peer);

	Bitfield =>
		if(peer.msgseq != 1)
			return peerdrop(peer, 1, sprint("%s sent bitfield after first message, disconnecting", peer.text()));

		err: string;
		(peer.piecehave, err) = Bits.mk(torrent.piececount, m.d);
		if(err != nil)
			return peerdrop(peer, 1, sprint("%s sent bogus bitfield message: %s, disconnecting", peer.text(), err));

		say("remote sent bitfield, haves "+peer.piecehave.text());

		n := 0;
		for(i := 0; i < peer.piecehave.n && n < peer.piecehave.have; i++)
			if(peer.piecehave.get(i)) {
				state->piececounts[i]++;
				n++;
			}

		interesting(peer);

	Piece =>
		say(sprint("%s sent data for piece=%d begin=%d length=%d", peer.text(), m.index, m.begin, len m.d));

		req := Req.new(m.index, m.begin/Blocksize);
		if(blocksize(req) != len m.d)
			return peerdrop(peer, 1, sprint("%s sent bad size for block %s, disconnecting", peer.text(), req.text()));

		piece := pieces->piecefind(m.index);
		if(piece == nil)
			return peerdrop(peer, 1, sprint("got data for inactive piece %d", m.index));

		if(!peer.reqs.take(req)) {
			exp := "nothing";
			if(!peer.reqs.isempty())
				exp = peer.reqs.peek().text();
			return peerdrop(peer, 1, sprint("%s sent block %s, expected %s, disconnecting", peer.text(), req.text(), exp));
		}

		blockindex := m.begin/Blocksize;

		if(piece.have.get(blockindex))
			return say("already have this block, skipping...");

		# possibly send cancel to other peer
		busy := piece.busy[blockindex];
		busypeer: ref Peer;
		if(busy.t0 >= 0 && busy.t0 != peer.id) {
			busypeer = peers->peerfind(busy.t0);
			piece.busy[blockindex].t0 = -1;
		} else if(busy.t1 >= 0 && busy.t1 != peer.id) {
			busypeer = peers->peerfind(busy.t1);
			piece.busy[blockindex].t1 = -1;
		}
		if(busypeer != nil) {
			peer.reqs.cancel(Req.new(m.index, m.begin/Blocksize));
			peersend(peer, ref Msg.Cancel(m.index, m.begin, len m.d));
		}

		needwrites: list of ref (int, int, array of byte);

		# add block to buffer.  if new block does not fit in, flush the old data.
		if(!peer.buf.tryadd(piece, m.begin, m.d)) {
			needwrite := peerbufflush(peer.buf);
			if(needwrite != nil)
				needwrites = needwrite::needwrites;
			if(!peer.buf.tryadd(piece, m.begin, m.d))
				raise "tryadd failed...";
		}

		# flush now, rather then delaying it
		if(peer.buf.isfull()) {
			needwrite := peerbufflush(peer.buf);
			if(needwrite != nil)
				needwrites = needwrite::needwrites;
		} else  # if this completes the batch, flush it for all peers that have blocks belonging to it
			batchflushcomplete(piece, blockindex);

		if(needwrites != nil) {
			say(sprint("sending %d needwrites to peernetreader", len needwrites));
			needwritechan <-= lists->reverse(needwrites);
		}

		# progress with piece
		if(piece.hashstateoff == m.begin)
			piece.hashadd(m.d);

		piece.have.set(blockindex);
		piece.done[blockindex] = peer.id;
		totalleft -= big len m.d;

		if(piece.isdone()) {
			# flush all bufs about this piece, also from other peers, to disk.  to make hash-checking read right data.
			for(l := peers->peers; l != nil; l = tl l) {
				p := hd l;
				if(p.buf.piece == m.index) {
					say("flushing buf of other peer before hash check");
					mainbufflush(p.buf);
				}
			}
		}
		schedule(peer);  # xxx will this work?  piece.have != piece.written may cause trouble

	Request =>
		b := Block.new(m.index, m.begin, m.length);
		say(sprint("%s sent request for %s", peer.text(), b.text()));

		if(m.length > Blocksizemax)
			return peerdrop(peer, 0, "requested block too large, disconnecting");

		if(peer.piecehave.get(m.index))
			return peerdrop(peer, 1, "peer requested piece it already claimed to have");

		if(pieces->blockhave(peer.wants, b))
			return say("peer already wanted block, skipping");

		if(len peer.wants >= Blockqueuemax)
			return peerdrop(peer, 0, sprint("peer scheduled one too many blocks, already has %d scheduled, disconnecting", len peer.wants));

		peer.wants = b::peer.wants;
		if(!peer.localchoking() && peer.remoteinterested() && !peer.netwriting && len peer.wants > 0)
			blockreadsend(peer);

	Cancel =>
		b := Block.new(m.index, m.begin, m.length);
		say(sprint("%s sent cancel for %s", peer.text(), b.text()));
		nwants := pieces->blockdel(peer.wants, b);
		if(len nwants == len peer.wants)
			return say("peer did not want block before, skipping");
		peer.wants = nwants;
	}
}


ticker()
{
	for(;;) {
		sys->sleep(10*1000);
		tickchan <-= 0;
	}
}

main()
{
	gen := 0;
	boguschan := chan of ref (int, int, array of byte);

	for(;;) {
	# warning: for block runs to end of main()

	# the alt statement doesn't do conditional sending/receiving on a channel.
	# so, before we start the alt, we evaluate the condition.
	# if true, we use the real channel we want to send on.
	# if false, we use a bogus channel without receiver, so the case is never taken.
	curwritechan := boguschan;
	curmainwrite: ref (int, int, array of byte);
	if(mainwrites != nil) {
		curmainwrite = hd mainwrites;
		curwritechan = diskwritechan;
	}
 
	alt {
	<-tickchan =>
		say(sprint("ticking, %d peers", len peers->peers));
		for(l := peers->peers; l != nil; l = tl l) {
			peer := hd l;
			say(sprint("%s: up %s, down %s, meta %s %s", peer.fulltext(), peer.up.text(), peer.down.text(), peer.metaup.text(), peer.metadown.text()));
		}

		say(sprint("total traffic:  up %s, down %s, meta %s %s", trafficup.text(), trafficdown.text(), trafficmetaup.text(), trafficmetadown.text()));
		
		ratiostr := "infinity";
		down := trafficdown.total();
		if(down != big 0)
			ratiostr = sprint("%.2f", real trafficup.total()/real down);
		say("ratio:  "+ratiostr);
		say(sprint("packets:  up %d, down %d, meta %d %d", trafficup.npackets, trafficdown.npackets, trafficmetaup.npackets, trafficmetadown.npackets));

		etasecs := eta();
		say(sprint("eta: %d: %s", etasecs, etastr(etasecs)));

		# do choking/unchoking algorithm round
		if(isdone())
			chokingupload(gen);
		else
			chokingdownload(gen);
		gen++;

	<-trackkickchan =>
		if(!stopped) {
			trackreqchan <-= (trafficup.total(), trafficdown.total(), totalleft, listenport, trackerevent);
			trackerevent = nil;
		}

	(interval, newpeers, trackerr) := <-trackchan =>
		if(trackerr != nil) {
			warn(sprint("tracker error: %s", trackerr));
		} else {
			say("main, new peers");
			for(i := 0; i < len newpeers; i++) {
				(ip, port, peerid) := newpeers[i];
				if(hex(peerid) == localpeeridhex)
					continue;  # skip self
				np := Newpeer(sprint("%s!%d", ip, port), ip, peerid);
				say("new: "+np.text());
				peers->trackerpeerdel(np);
				if(!peers->peerconnected(np.addr))
					peers->trackerpeeradd(np);
				else
					say("already connected to "+np.text());
			}
		}
		dialpeers();

		# schedule next call to tracker
		if(interval < Intervalmin)
			interval = Intervalmin;
		if(interval > Intervalmax)
			interval = Intervalmax;
		if(daytime->now() < starttime+Intervalstartupperiod && !isdone() && len peers->peers+len peers->trackerpeers < Peersmax && interval > Intervalneed)
			interval = Intervalneed;

		say(sprint("next call to tracker will be in %d seconds", interval));
		spawn trackkick(interval);

	(dialed, np, peerfd, extensions, peerid, err) := <-newpeerchan =>
		if(!dialed)
			islistening = 0;

		if(err != nil) {
			warn(sprint("%s: %s", np.text(), err));
		} else if(hex(peerid) == localpeeridhex) {
			say("connected to self, dropping connection...");
		} else if(peers->peerknownip(np.ip)) {
			say("new connection from known ip address, dropping new connection...");
		} else if(isfaulty(np.ip)) {
			say(sprint("connected to faulty ip %s, dropping connection...", np.ip));
		} else {
			peer := Peer.new(np, peerfd, extensions, peerid, dialed, torrent.piececount);
			spawn peernetreader(peer);
			spawn peernetwriter(peer);
			spawn diskwriter(peer.writech);
			peers->peeradd(peer);
			say("new peer "+peer.fulltext());

			rotateips.pooladdunique(misc->maskip(np.ip));

			if((state->piecehave).have == 0) {
				peersend(peer, ref Msg.Keepalive());
			} else {
				say("sending bitfield to peer: "+(state->piecehave).text());
				peersend(peer, ref Msg.Bitfield((state->piecehave).bytes()));
			}

			if(len peers->peersactive() < Unchokedmax) {
				say("unchoking rare new peer: "+peer.text());
				unchoke(peer);
			}
		}

		if(dialed) {
			ndialers--;
			dialpeers();
		} else
			awaitpeer();

	(peer, msg, needwritech) := <-peerinmsgchan =>
		if(msg == nil)
			return peerdrop(peer, 0, "eof from peer "+peer.text());

		handleinmsg(peer, msg, needwritech);

	(pieceindex, begin, length, err) := <-diskwrittenchan =>
		if(err != nil)
			raise sprint("error writing piece %d, begin %d, length %d: %s", pieceindex, begin, length, err);

		piece := pieces->piecefind(pieceindex);
		if(piece == nil)
			raise sprint("data written for inactive piece %d", pieceindex);

		# mark written blocks as such
		first := begin/Blocksize; 
		n := (length+Blocksize-1)/Blocksize;
		for(i := 0; i < n; i++)
			piece.written.set(first+i);

		if(!piece.written.isfull())
			continue;

		say("last parts of piece have been written, verifying...");

		wanthash := hex(torrent.piecehashes[piece.index]);
		(piecehash, herr) := verify->piecehash(dstfds, torrent.piecelen, piece);
		if(herr != nil)
			fail("verifying hash: "+herr);
		havehash := hex(piecehash);
		if(wanthash != havehash) {
			# xxx blame peers
			say(sprint("%s did not check out, want %s, have %s, disconnecting", piece.text(), wanthash, havehash));
			piece.hashstate = nil;
			piece.hashstateoff = 0;
			piece.have.clearall();
			if(Dflag) {
				for(i = 0; i < len piece.busy; i++)
					if(piece.busy[i].t0 >= 0 || piece.busy[i].t1 >= 0)
						raise sprint("piece %d should be complete, but block %d is busy", piece.index, i);
			}
			
			# xxx what do to with other peers?
			return;
		}

		(state->piecehave).set(piece.index);
		pieces->piecedel(pieces->piecefind(piece.index));

		# this could have been the last piece this peer had, making us no longer interested
		for(l := peers->peers; l != nil; l = tl l)
			interesting(hd l);

		writestate();
		say("piece now done: "+piece.text());
		say(sprint("pieces: have %s, busy %s", (state->piecehave).text(), (state->piecebusy).text()));

		for(l = peers->peers; l != nil; l = tl l)
			peersend(hd l, ref Msg.Have(piece.index));

		if(isdone()) {
			trackerevent = "completed";
			spawn trackkick(0);
			npeers: list of ref Peer;
			for(l = peers->peers; l != nil; l = tl l) {
				p := hd l;
				if(p.isdone()) {
					say("done: dropping seed "+p.fulltext());
					peerdrop(p, 0, nil);
				} else {
					npeers = p::npeers;
					# we won't act on becoming interested while unchoked anymore
					if(!p.remoteinterested() && !p.localchoking())
						choke(p);
				}
			}
			peers->peers = lists->reverse(npeers);
			print("DONE!\n");
		}

	curwritechan <-= curmainwrite =>
		say("queued mainwrite to diskwriter");
		mainwrites = tl mainwrites;

	peer := <-msgwrittenchan =>
		say(sprint("%s: message written", peer.text()));

		if(!stopped && isdone()) {
			ratio := ratio();
			if(ratio >= 1.1 && ratio >= maxratio) {
				say(sprint("stopping due to max ratio achieved (%.2f)", ratio));
				stopped = 1;

				# disconnect from all peers and don't do further tracker requests
				peers->peers = nil;
				peers->luckypeer = nil;
				rotateips = nil;
			}
		}

		peer.netwriting = 0;
		if(!peer.localchoking() && peer.remoteinterested() && len peer.wants > 0)
			blockreadsend(peer);
	}
	}
}

trackkick(n: int)
{
	sys->sleep(n*1000);
	trackkickchan <-= 1;
}

track()
{
	for(;;) {
		(up, down, left, lport, event) := <-trackreqchan;

		say("getting new tracker info");
		(interval, newpeers, nil, err) := bittorrent->trackerget(torrent, localpeerid, up, down, left, lport, event);
		if(err != nil)
			say("trackerget: "+err);
		else
			say("trackget okay");
		trackchan <-= (interval, newpeers, err);
	}
}


kicklistener()
{
	canlistenchan <-= 1;
}

listener(aconn: Sys->Connection)
{
	<-canlistenchan;
	for(;;) {
		(ok, conn) := sys->listen(aconn);
		if(ok != 0) {
			warn(sprint("listen: %r"));
			continue;
		}

		remote := conn.dir+"/remote";
		(remaddr, rerr) := readfile(remote);
		if(rerr != nil) {
			warn(sprint("reading %s: %s", remote, rerr));
			continue;
		}
		remaddr = str->splitstrl(remaddr, "\n").t0;

		f := conn.dir+"/data";
		fd := sys->open(f, Sys->ORDWR);
		if(fd == nil) {
			warn(sprint("new connection, open %s: %r", f));
			continue;
		}

		(extensions, peerid, err) := handshake(fd);
		
		np := Newpeer(remaddr, str->splitstrl(remaddr, "!").t0, nil);
		if(err != nil)
			say("error handshaking incoming connection: "+err);
		newpeerchan <-= (0, np, fd, extensions, peerid, err);
		<-canlistenchan;
	}
}


min(a, b: int): int
{
	if(a < b)
		return a;
	return b;
}

# we are allowed to pass `max' bytes per second.
limiter(ch: chan of (int, chan of int), max: int)
{
	maxallow := min(max, Netiounit);

	for(;;) {
		(want, respch) := <-ch;
		if(max <= 0) {
			# no rate limiting, let traffic pass
			respch <-= want;
			continue;
		}

		give := min(maxallow, want);
		respch <-= give;
		sys->sleep(980*give/max);  # don't give out more bandwidth until this portion has run out
	}
}


dialkiller(pidch: chan of int, pid: int, np: Newpeer)
{
	pidch <-= sys->pctl(0, nil);
	sys->sleep(Dialtimeout*1000);
	kill(pid);
	newpeerchan <-= (1, np, nil, nil, nil, sprint("dial/handshake %s: timeout", np.addr));
}

dialer(np: Newpeer)
{
	pid := sys->pctl(0, nil);
	spawn dialkiller(pidch := chan of int, pid, np);
	killerpid := <-pidch;
	
	addr := sprint("net!%s", np.addr);
	(ok, conn) := sys->dial(addr, nil);
	if(ok < 0) {
		kill(killerpid);
		newpeerchan <-= (1, np, nil, nil, nil, sprint("dial %s: %r", np.addr));
		return;
	}

	say("dialed "+addr);
	fd := conn.dfd;

	(extensions, peerid, err) := handshake(fd);
	if(err != nil)
		fd = nil;
	kill(killerpid);
	newpeerchan <-= (1, np, fd, extensions, peerid, err);
}

handshake(fd: ref Sys->FD): (array of byte, array of byte, string)
{
	d := array[20+8+20+20] of byte;
	i := 0;
	d[i++] = byte 19;
	d[i:] = array of byte "BitTorrent protocol";
	i += 19;
	d[i:] = array[8] of {* => byte '\0'};
	i += 8;
	d[i:] = torrent.hash;
	i += 20;
	d[i:] = localpeerid;
	i += 20;
	if(i != len d)
		raise "bad peer header, internal error";

	n := netwrite(fd, d, len d);
	if(n != len d)
		return (nil, nil, sprint("writing peer header: %r"));

	rd := array[len d] of byte;
	n = netread(fd, rd, len rd);
	if(n < 0)
		return (nil, nil, sprint("reading peer header: %r"));
	if(n != len rd)
		return (nil, nil, sprint("short read on peer header (%d)", n));

	if(rd[0] != byte 19 || string rd[1:1+19] != "BitTorrent protocol")
		return (nil, nil, sprint("peer does not speak bittorrent protocol"));

	extensions := rd[20:20+8];
	hash := rd[20+8:20+8+20];
	peerid := rd[20+8+20:];

	if(hex(hash) != hex(torrent.hash))
		return (nil, nil, sprint("peer wants torrent hash %s, not %s", hex(hash), hex(torrent.hash)));

	return (extensions, peerid, nil);
}


# read, going through limiter
netread(fd: ref Sys->FD, buf: array of byte, n: int): int
{
	read := 0;
	while(n > 0) {
		downchan <-= (n, respch := chan of int);
		can := <-respch;
		nn := sys->readn(fd, buf, can);
		if(nn < 0)
			return nn;
		if(nn != can)
			return read+nn;
		n -= can;
		buf = buf[can:];
		read += can;
	}
	return read;
}

# write, going through limiter
netwrite(fd: ref Sys->FD, buf: array of byte, n: int): int
{
	wrote := 0;
	while(n > 0) {
		upchan <-= (n, respch := chan of int);
		can := <-respch;
		nn := sys->write(fd, buf, can);
		if(nn < 0)
			return nn;
		if(nn != can)
			return wrote+nn;
		n -= can;
		buf = buf[can:];
		wrote += can;
	}
	return wrote;
}


# copied from bittorrent.b Msg.read
msgread(fd: ref Sys->FD): (ref Msg, string)
{
	buf := array[4] of byte;
	n := netread(fd, buf, len buf);
	if(n < 0)
		return (nil, sprint("reading: %r"));
	if(n < len buf)
		return (nil, sprint("short read"));
	(size, nil) := bittorrent->g32(buf, 0);
	buf = array[size] of byte;

	n = netread(fd, buf, len buf);
	if(n < 0)
		return (nil, sprint("reading: %r"));
	if(n < len buf)
		return (nil, sprint("short read"));

	return Msg.unpack(buf);
}


peernetreader(peer: ref Peer)
{
	needwritechan := chan of list of ref (int, int, array of byte);
	for(;;) {
		(m, err) := msgread(peer.fd);
		if(err != nil)
			fail("reading msg: "+err);  # xxx return error to main
		if(Pflag)
			fprint(fildes(2), "<< %s\n", m.text());
		peerinmsgchan <-= (peer, m, needwritechan);
		if(m == nil)
			break;

		if(tagof m == tagof Msg.Piece) {
			needwrites := <-needwritechan;
			say(sprint("peernetreader: have %d needwrites", len needwrites));
			if(needwrites != nil) {
				for(l := needwrites; l != nil; l = tl l)
					peer.writech <-= hd l;  # will block if disk is slow, slowing down peer as well
				<-needwritechan;
			}
		}
	}
}

peernetwriter(peer: ref Peer)
{
	for(;;) {
		m := <- peer.outmsgs;
		if(m == nil)
			return;
		if(Pflag)
			fprint(fildes(2), ">> %s\n", m.text());
		d := m.pack();
		n := netwrite(peer.fd, d, len d);
		if(n != len d)
			fail(sprint("writing msg: %r"));
		if(tagof m == tagof Msg.Piece)
			msgwrittenchan <-= peer;
	}
}

diskwriter(reqch: chan of ref (int, int, array of byte))
{
	for(;;) {
		req := <-reqch;
		if(req == nil)
			break;
		(piece, begin, buf) := *req;

		off := big piece*big torrent.piecelen + big begin;
		err := bittorrent->torrentpwritex(dstfds, buf, len buf, off);
		diskwrittenchan <-= (piece, begin, len buf, err);
	}
}


# misc

ratio(): real
{
	up := trafficup.total();
	down := trafficdown.total();
	if(down == big 0)
		return Math->Infinity;
	return real up/real down;
}

etastr(secs: int): string
{
	if(secs < 0)
		return "stalled";
	else if(secs < 60*60)
		return sprint("%3dm %3ds", secs / 60, secs % 60);
	else if(secs < 24*60*60)
		return sprint("%3dh %3dm", secs / (60*60), secs % (60*60) / 60);
	else if(secs < 366*24*60*60)
		return sprint("%3dd %3dh", secs / (24*60*60), secs % (24*60*60) / (60*60));
	else
		return "  > year";
}

eta(): int
{
	r := trafficdown.rate();
	if(r <= 0)
		return -1;
	return int (totalleft / big r);
}

progctl(pid: int, s: string)
{
	path := sprint("/prog/%d/ctl", pid);
	fd := sys->open(path, Sys->OWRITE);
	if(fd != nil)
		fprint(fd, "%s", s);
}

killgrp(pid: int)
{
	progctl(pid, "killgrp");
}

kill(pid: int)
{
	progctl(pid, "kill");
}

fail(s: string)
{
	warn(s);
	raise "fail:"+s;
}

warn(s: string)
{
	fprint(fildes(2), "%s\n", s);
}

say(s: string)
{
	if(Dflag)
		fprint(fildes(2), "%s\n", s);
}
