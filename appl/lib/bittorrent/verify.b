implement Verify;

include "torrentget.m";

sys: Sys;
keyring: Keyring;
bittorrent: Bittorrent;


init()
{
	sys = load Sys Sys->PATH;
	keyring = load Keyring Keyring->PATH;
	bitarray := load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init(bitarray);
}

min(a, b: int): int
{
	if(a < b)
		return a;
	return b;
}

chunkreader(fds: list of ref (ref Sys->FD, big), reqch: chan of ref (int, big, chan of (array of byte, string)))
{
	for(;;) {
		req := <-reqch;
		if(req == nil)
			break;

		(n, off, chunkch) := *req;
		while(n > 0) {
			want := min(Torrentget->Diskchunksize, n);
			buf := array[want] of byte;
			err := bittorrent->torrentpreadx(fds, buf, len buf, off);
			if(err != nil) {
				chunkch <-= (nil, err);
				return;
			}
			off += big len buf;
			n -= len buf;
			chunkch <-= (buf, nil);
		}
		chunkch <-= (nil, nil);
	}
}

piecehash(fds: list of ref (ref Sys->FD, big), piecelen: int, p: ref Pieces->Piece): (array of byte, string)
{
	reqch := chan[1] of ref (int, big, chan of (array of byte, string));
	spawn chunkreader(fds, reqch);

	chunkch := chan of (array of byte, string);
	reqch <-= ref (p.length-p.hashstateoff, big p.index*big piecelen+big p.hashstateoff, chunkch);
	reqch <-= nil;

	state := p.hashstate;
	for(;;) {
		(buf, err) := <-chunkch;
		if(err != nil)
			return (nil, sys->sprint("reading piece %d: %s", p.index, err));
		if(buf == nil)
			break;
		state = keyring->sha1(buf, len buf, nil, state);
	}

	hash := array[Keyring->SHA1dlen] of byte;
	keyring->sha1(nil, 0, hash, p.hashstate);
	return (hash, nil);
}
