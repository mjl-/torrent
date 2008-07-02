Bittorrent: module {
	PATH:	con "/dis/lib/bittorrent.dis";

	init:	fn(bitarray: Bitarray);

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

		pack:	fn(b: self ref Bee): array of byte;
		packedsize:	fn(b: self ref Bee): int;
		unpack:	fn(d: array of byte): (ref Bee, string);
		find:	fn(b: self ref Bee, s: string): ref Bee;
		get:	fn(b: self ref Bee, l: list of string): ref Bee;
		gets:	fn(b: self ref Bee, l: list of string): ref Bee.String;
		geti:	fn(b: self ref Bee, l: list of string): ref Bee.Integer;
		getl:	fn(b: self ref Bee, l: list of string): ref Bee.List;
		getd:	fn(b: self ref Bee, l: list of string): ref Bee.Dict;
	};

	Msg: adt {
		pick {
		Keepalive or Choke or Unchoke or Interested or Notinterested =>
		Have =>
			index: int;
		Bitfield =>
			d: array of byte;
		Piece =>
			index, begin: int;
			d: array of byte;
		Request or Cancel =>
			index, begin, length: int;
		}

		read:	fn(fd: ref Sys->FD): (ref Msg, string);
		packedsize:	fn(m: self ref Msg): int;
		pack:	fn(m: self ref Msg): array of byte;
		unpack:	fn(d: array of byte): (ref Msg, string);
		text:	fn(m: self ref Msg): string;
	};

	Torrent: adt {
		announce:	string;
		piecelen:	int;
		hash:	array of byte;
		piecehashes:	array of array of byte;
		length:	big;

		open:	fn(path: string): (ref Torrent, string);
	};

	trackerget:	fn(t: ref Torrent, peerid: array of byte, up, down, left: big, lport: int, event: string): (int, array of (string, int, array of byte), ref Bee, string);
	genpeerid:	fn(): array of byte;
};
