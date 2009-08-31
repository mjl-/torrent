Bittorrent: module
{
	PATH:	con "/dis/lib/bittorrent.dis";

	dflag:	int;
	init:	fn();

	Bee: adt {
		pick {
		String =>
			a:	array of byte;
		Integer =>
			i:	big;
		List =>
			a:	cyclic array of ref Bee;
		Dict =>
			a:	cyclic array of (ref Bee.String, ref Bee);
		}

		pack:		fn(b: self ref Bee): array of byte;
		packedsize:	fn(b: self ref Bee): int;
		unpack:		fn(d: array of byte): (ref Bee, string);
		find:		fn(b: self ref Bee, s: string): ref Bee;
		get:		fn(b: self ref Bee, l: list of string): ref Bee;
		gets:		fn(b: self ref Bee, l: list of string): ref Bee.String;
		geti:		fn(b: self ref Bee, l: list of string): ref Bee.Integer;
		getl:		fn(b: self ref Bee, l: list of string): ref Bee.List;
		getd:		fn(b: self ref Bee, l: list of string): ref Bee.Dict;

	};
	beestr:	fn(s: string): ref Bee.String;
	beebytes:	fn(d: array of byte): ref Bee.String;
	beelist:	fn(l: list of ref Bee): ref Bee.List;
	beeint:	fn(i: int): ref Bee.Integer;
	beebig:	fn(i: big): ref Bee.Integer;
	beekey:	fn(s: string, b: ref Bee): (ref Bee.String, ref Bee);
	beedict:	fn(l: list of (ref Bee.String, ref Bee)): ref Bee.Dict;

	Msg: adt {
		pick {
		Keepalive or
		Choke or
		Unchoke or
		Interested or
		Notinterested =>
		Have =>
			index:	int;
		Bitfield =>
			d:	array of byte;
		Piece =>
			index:	int;
			begin:	int;
			d:	array of byte;
		Request or
		Cancel =>
			index:	int;
			begin:	int;
			length:	int;
		}

		read:		fn(fd: ref Sys->FD): (ref Msg, string);
		packedsize:	fn(m: self ref Msg): int;
		pack:		fn(m: self ref Msg): array of byte;
		unpack:		fn(d: array of byte): (ref Msg, string);
		text:		fn(m: self ref Msg): string;
	};

	File: adt {
		index:	int;
		path,
		origpath:	string;
		length,
		off:	big;
		pfirst,
		plast:	int;
	};

	Torrent: adt {
		announce:	string;
		piecelen:	int;
		hash:		array of byte;
		piececount:	int;
		hashes:		array of array of byte;
		files:		list of ref File;
		name:		string;
		length:		big;
		statepath:	string;

		open:		fn(path: string): (ref Torrent, string);
		openfiles:	fn(t: self ref Torrent, nofix, nocreate: int): (list of ref (ref Sys->FD, big), int, string);
		piecelength:	fn(t: self ref Torrent, index: int): int;
		pack:		fn(t: self ref Torrent): array of byte;
	};

	trackerget:	fn(t: ref Torrent, peerid: array of byte, up, down, left: big, lport: int, event: string): (int, array of (string, int, array of byte), ref Bee, string);
	genpeerid:	fn(): array of byte;
	pieceread:	fn(t: ref Torrent, fds: list of ref (ref Sys->FD, big), index: int): (array of byte, string);
	blockread:	fn(t: ref Torrent, fds: list of ref (ref Sys->FD, big), index, begin, length: int): (array of byte, string);
	piecewrite:	fn(t: ref Torrent, fds: list of ref (ref Sys->FD, big), index: int, buf: array of byte): string;
	torrentpwritex:	fn(dstfds: list of ref (ref Sys->FD, big), buf: array of byte, n: int, off: big): string;
	torrentpreadx:	fn(dstfds: list of ref (ref Sys->FD, big), buf: array of byte, n: int, off: big): string;
};
