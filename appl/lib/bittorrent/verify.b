implement Verify;

include "torrentpeer.m";
	sys: Sys;
	sprint: import sys;
	kr: Keyring;
	bitarray: Bitarray;
	Bits: import bitarray;
	bittorrent: Bittorrent;
	Torrent: import bittorrent;
	util: Util0;
	preadn, min, hex: import util;

init()
{
	sys = load Sys Sys->PATH;
	kr = load Keyring Keyring->PATH;
	bitarray = load Bitarray Bitarray->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init();
	util = load Util0 Util0->PATH;
	util->init();
}

chunkreader(fds: list of ref (ref Sys->FD, big), reqch: chan of ref (int, big, chan of (array of byte, string)))
{
	for(;;) {
		req := <-reqch;
		if(req == nil)
			break;

		(n, off, chunkch) := *req;
		while(n > 0) {
			want := min(Torrentpeer->Diskchunksize, n);
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
			return (nil, sprint("reading piece %d: %s", p.index, err));
		if(buf == nil)
			break;
		state = kr->sha1(buf, len buf, nil, state);
	}

	hash := array[Keyring->SHA1dlen] of byte;
	kr->sha1(nil, 0, hash, p.hashstate);
	return (hash, nil);
}

reader(t: ref Torrent, fds: list of ref (ref Sys->FD, big), c: chan of (array of byte, string))
{
	have := 0;
	buf := array[t.piecelen] of byte;

	for(; fds != nil; fds = tl fds) {
		(fd, size) := *hd fds;
		o := big 0;
		while(o < size) {
			want := t.piecelen-have;
			if(size-o < big want)
				want = int (size-o);
			nn := preadn(fd, buf[have:], want, o);
			if(nn <= 0) {
				c <-= (nil, sprint("reading: %r"));
				return;
			}
			have += nn;
			o += big nn;
			if(have == t.piecelen) {
				c <-= (buf, nil);
				buf = array[t.piecelen] of byte;
				have = 0;
			}
		}
	}
	if(have != 0)
		c <-= (buf[:have], nil);
}

torrenthash(fds: list of ref (ref Sys->FD, big), t: ref Torrent, haves: ref Bits): string
{
	spawn reader(t, fds, c := chan[2] of (array of byte, string));
	digest := array[kr->SHA1dlen] of byte;
	for(i := 0; i < len t.hashes; i++) {
		(buf, err) := <-c;
		if(err != nil)
			return err;
		kr->sha1(buf, len buf, digest, nil);
		if(hex(digest) == hex(t.hashes[i]))
			haves.set(i);
	}
	return nil;
}
