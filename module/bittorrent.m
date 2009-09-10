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
	beestr:		fn(s: string): ref Bee.String;
	beebytes:	fn(d: array of byte): ref Bee.String;
	beelist:	fn(l: list of ref Bee): ref Bee.List;
	beeint:		fn(i: int): ref Bee.Integer;
	beebig:		fn(i: big): ref Bee.Integer;
	beekey:		fn(s: string, b: ref Bee): (ref Bee.String, ref Bee);
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
		packbuf:	fn(m: self ref Msg, buf: array of byte);
		unpack:		fn(d: array of byte): (ref Msg, string);
		text:		fn(m: self ref Msg): string;
	};

	File: adt {
		path:	string;
		length:	big;
	};

	Torrent: adt {
		announce:	string;
		piecelen:	int;
		infohash:	array of byte;
		piececount:	int;
		hashes:		array of array of byte;
		files:		array of ref File;
		name:		string;
		length:		big;
		private:	int;
		createdby:	string;
		createtime:	int;

		open:		fn(path: string): (ref Torrent, string);
		piecelength:	fn(t: self ref Torrent, index: int): int;
		pack:		fn(t: self ref Torrent): array of byte;
	};

	Filex: adt {
		f:	ref File;
		index:	int;
		path:	string;
		fd:		ref Sys->FD;
		offset:		big;  # in entire torrent
		pfirst,
		plast:		int;
	};

	Torrentx: adt {
		t:		ref Torrent;
		files:		array of ref Filex;
		statepath:	string;
		statefd:	ref Sys->FD;

		open:		fn(t: ref Torrent, torrentpath: string, nofix, nocreate: int): (ref Torrentx, int, string);
		pieceread:	fn(tx: self ref Torrentx, index: int): (array of byte, string);
		piecewrite:	fn(tx: self ref Torrentx, index: int, buf: array of byte): string;
		blockread:	fn(tx: self ref Torrentx, index, begin, length: int): (array of byte, string);
		pwritex:	fn(tx: self ref Torrentx, buf: array of byte, n: int, off: big): string;
		preadx:		fn(tx: self ref Torrentx, buf: array of byte, n: int, off: big): string;
	};
        reader:		fn(tx: ref Torrentx, c: chan of (array of byte, string));
	torrenthash:	fn(tx: ref Torrentx, haves: ref Bitarray->Bits): string;


	Trackpeer: adt {
		ip:	string;
		port:	int;
		peerid:	array of byte;
	};
	Track: adt {
		interval:	int;
		mininterval:	int;	# < 0 if absent
		peers:	array of Trackpeer;
		b:	ref Bee;
	};

	trackerget:	fn(t: ref Torrent, peerid: array of byte, up, down, left: big, lport: int, event, key: string): (ref Track, string);
	genpeerid:	fn(): array of byte;
};
