implement Bittorrent;

include "sys.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "string.m";
include "security.m";
include "keyring.m";
include "filter.m";
include "ohttp.m";
include "bitarray.m";
include "bittorrent.m";

sys: Sys;
str: String;
random: Random;
keyring: Keyring;
http: Http;
bitarray: Bitarray;

fprint, fildes, sprint: import sys;
Bits: import bitarray;
Url, Rbuf: import http;

init(ba: Bitarray)
{
	sys = load Sys Sys->PATH;
	str = load String String->PATH;
	random = load Random Random->PATH;
	keyring = load Keyring Keyring->PATH;
	bufio = load Bufio Bufio->PATH;
	http = load Http Http->PATH;
	http->init(bufio);
	bitarray = ba;
}

Bee.find(bb: self ref Bee, s: string): ref Bee
{
	a := array of byte s;
	pick b := bb {
	Dict =>
		for(i := 0; i < len b.a; i++)
			if(len a == len b.a[i].t0.a && string b.a[i].t0.a == s)
				return b.a[i].t1;
	};
	return nil;
}

Bee.pack(b: self ref Bee): array of byte
{
	n := b.packedsize();
	a := array[n] of byte;
	m := beepack(b, a, 0);
	if(n != m)
		raise "fail:internal error packing bee structure";
	return a;
}

Bee.packedsize(bb: self ref Bee): int
{
	pick b := bb {
	String =>
		n := len b.a;
		return (len string n)+1+n;
	Integer =>
		return 1+(len string b.i)+1;
	List =>
		n := 1;
		for(i := 0; i < len b.a; i++)
			n += b.a[i].packedsize();
		n += 1;
		return n;
	Dict =>
		n := 1;
		for(i := 0; i < len b.a; i++) {
			n += b.a[i].t0.packedsize();
			n += b.a[i].t1.packedsize();
		}
		n += 1;
		return n;
	}
}

beepack(bb: ref Bee, d: array of byte, i: int): int
{
	begin := i;
	pick b := bb {
	String =>
		na := array of byte string len b.a;
		d[i:] = na;
		i += len na;
		d[i] = byte ':';
		i++;
		d[i:] = b.a;
		i += len b.a;
	Integer =>
		a := array of byte string b.i;
		d[i] = byte 'i';
		i++;
		d[i:] = a;
		i += len a;
		d[i] = byte 'e';
		i++;
	List =>
		d[i] = byte 'l';
		i++;
		for(j := 0; j < len b.a; j++)
			i += beepack(b.a[j], d, i);
		d[i] = byte 'e';
		i++;
	Dict =>
		d[i] = byte 'd';
		i++;
		for(j := 0; j < len b.a; j++) {
			i += beepack(b.a[j].t0, d, i);
			i += beepack(b.a[j].t1, d, i);
		}
		d[i] = byte 'e';
		i++;
	}
	return i-begin;
}

Bee.unpack(d: array of byte): (ref Bee, string)
{
	(b, n, err) := beeunpack(d, 0);
	if(err != nil)
		return (nil, err+" (at offset "+string n+")");
	if(n != len d)
		return (nil, "data still left after parsing");
	return (b, nil);
}

beeunpack(d: array of byte, o: int): (ref Bee, int, string)
{
	if(o >= len d)
		return (nil, o, "premature end");

	case int d[o] {
	'i' =>
		e := o+1;
		if(d[e] == byte '-')
			e++;
		while(e+1 < len d && d[e] >= byte '0' && d[e] <= byte '9')
			e++;
		if(e == len d)
			return (nil, o, "bad integer, missing end");
		if(d[e] != byte 'e')
			return (nil, o, "bad integer, bad end");
		s := string d[o+1:e];
		if(s == "-0" || s == "" || len s > 1 && s[0] == '0')
			return (nil, o, "bad integer, bad value");
		return (ref Bee.Integer(big s), e+1, nil);
	
	'l' =>
		a := array[0] of ref Bee;
		e := o+1;
		for(;;) {
			if(e >= len d)
				return (nil, o, "bad list, missing end");
			if(d[e] == byte 'e') {
				e++;
				break;
			}
			(b, ne, err) := beeunpack(d, e);
			if(err != nil)
				return (nil, o, err);
			e = ne;
			na := array[len a+1] of ref Bee;
			na[:] = a;
			na[len a] = b;
			a = na;
		}
		return (ref Bee.List(a), e, nil);

	'd' =>
		a := array[0] of (ref Bee.String, ref Bee);
		e := o+1;
		for(;;) {
			if(e >= len d)
				return (nil, o, "bad dict, missing end");
			if(d[e] == byte 'e') {
				e++;
				break;
			}
			
			(bb, ne, err) := beeunpack(d, e);
			if(err != nil)
				return (nil, o, err);
			e = ne;
			key: ref Bee.String;
			pick b := bb {
			String =>
				key = b;
			* =>
				return (nil, o, "bad dict, non-string as key");
			}
			(bb, e, err) = beeunpack(d, e);
			if(err != nil)
				return (nil, o, err);

			na := array[len a+1] of (ref Bee.String, ref Bee);
			na[:] = a;
			na[len a] = (key, bb);
			a = na;
		}
		return (ref Bee.Dict(a), e, nil);
		
	'0' to '9' =>
		e := o+1;
		while(e+1 < len d && d[e] >= byte '0' && d[e] <= byte '9')
			e++;
		if(e >= len d)
			return (nil, o, "bad string, missing end");
		if(d[e] != byte ':')
			return (nil, o, "bad string, bad end");
		n := int string d[o:e];
		if(e+1+n > len d)
			return (nil, o, "bad string, bad length");
		return (ref Bee.String(d[e+1:e+1+n]), e+1+n, nil);

	* =>
		return (nil, o, "bad structure type");
	}
}

Bee.get(b: self ref Bee, l: list of string): ref Bee
{
	for(; b != nil && l != nil; l = tl l)
		b = b.find(hd l);
	return b;
}

Bee.gets(b: self ref Bee, l: list of string): ref Bee.String
{
	nb := b.get(l);
	if(nb == nil)
		return nil;
	pick bb := nb {
	String =>	return bb;
	* =>		return nil;
	}
}

Bee.geti(b: self ref Bee, l: list of string): ref Bee.Integer
{
	nb := b.get(l);
	if(nb == nil)
		return nil;
	pick bb := nb {
	Integer =>	return bb;
	* =>		return nil;
	}
}

Bee.getl(b: self ref Bee, l: list of string): ref Bee.List
{
	nb := b.get(l);
	if(nb == nil)
		return nil;
	pick bb := nb {
	List =>	return bb;
	* =>		return nil;
	}
}

Bee.getd(b: self ref Bee, l: list of string): ref Bee.Dict
{
	nb := b.get(l);
	if(nb == nil)
		return nil;
	pick bb := nb {
	Dict =>		return bb;
	* =>		return nil;
	}
}


MChoke, MUnchoke, MInterested, MNotinterested, MHave, MBitfield, MRequest, MPiece, MCancel:	con iota;
MLast:	con MCancel;

tag2type := array[] of {
	tagof Msg.Choke =>	MChoke,
	tagof Msg.Unchoke =>	MUnchoke,
	tagof Msg.Interested =>	MInterested,
	tagof Msg.Notinterested =>	MNotinterested,
	tagof Msg.Have =>	MHave,
	tagof Msg.Bitfield =>	MBitfield,
	tagof Msg.Request =>	MRequest,
	tagof Msg.Piece =>	MPiece,
	tagof Msg.Cancel =>	MCancel,
};

tag2string := array[] of {
	tagof Msg.Keepalive =>	"keepalive",
	tagof Msg.Choke =>	"choke",
	tagof Msg.Unchoke =>	"unchoke",
	tagof Msg.Interested =>	"interested",
	tagof Msg.Notinterested =>	"notinterested",
	tagof Msg.Have =>	"have",
	tagof Msg.Bitfield =>	"bitfield",
	tagof Msg.Request =>	"request",
	tagof Msg.Piece =>	"piece",
	tagof Msg.Cancel =>	"cancel",
};

msizes := array[] of {
	MChoke =>	1,
	MUnchoke =>	1,
	MInterested =>	1,
	MNotinterested =>	1,
	MHave =>	1+4,
	MBitfield =>	1,	# +payload
	MRequest =>	1+3*4,
	MPiece =>	1+2*4,	# +payload
	MCancel =>	1+3*4,
};

Msg.packedsize(mm: self ref Msg): int
{
	if(tagof mm == tagof Msg.Keepalive)
		return 0;
	msize := msizes[tag2type[tagof mm]];
	pick m := mm {
	Bitfield =>	msize += len m.d;
	Piece =>	msize += len m.d;
	}
	return msize;
}

Msg.pack(mm: self ref Msg): array of byte
{
	msize := mm.packedsize();
	d := array[4+msize] of byte;
	i := p32(d, 0, msize);

	if(tagof mm == tagof Msg.Keepalive)
		return d;

	t := tag2type[tagof mm];
	d[i++] = byte t;

	pick m := mm {
	Choke or Unchoke or Interested or Notinterested =>
	Have =>
		i = p32(d, i, m.index);
	Bitfield =>
		d[i:] = m.d;
		i += len m.d;
	Piece =>
		i = p32(d, i, m.index);
		i = p32(d, i, m.begin);
		d[i:] = m.d;
		i += len m.d;
	Request or Cancel =>
		i = p32(d, i, m.index);
		i = p32(d, i, m.begin);
		i = p32(d, i, m.length);
	* =>	raise "fail:bad message type";
	};
	if(i != len d)
		raise "fail:Msg.pack internal error";
	return d;
}

Msg.unpack(d: array of byte): (ref Msg, string)
{
	if(len d == 0)
		return (ref Msg.Keepalive(), nil);
	if(int d[0] > MLast)
		return (nil, "bad message, unknown type");

	msize := msizes[int d[0]];
	if(len d < msize)
		return (nil, "bad message, too short");

	i := 1;
	m: ref Msg;
	case int d[0] {
	MChoke =>	m = ref Msg.Choke();
	MUnchoke =>	m = ref Msg.Unchoke();
	MInterested =>	m = ref Msg.Interested();
	MNotinterested =>	m = ref Msg.Notinterested();
	MHave =>
		index: int;
		(index, i) = g32(d, i);
		m = ref Msg.Have(index);
	MBitfield =>
		nd := array[len d-i] of byte;
		nd[:] = d[i:];
		i += len nd;
		m = ref Msg.Bitfield(nd);
		# xxx verify that bitfield has correct length?
	MPiece =>
		index, begin: int;
		(index, i) = g32(d, i);
		(begin, i) = g32(d, i);
		nd := array[len d-i] of byte;
		nd[:] = d[i:];
		i += len nd;
		m = ref Msg.Piece(index, begin, nd);
		# xxx verify that piece has right size?
	MRequest or MCancel =>
		index, begin, length: int;
		(index, i) = g32(d, i);
		(begin, i) = g32(d, i);
		(length, i) = g32(d, i);
		if(int d[0] == MRequest)
			m = ref Msg.Request(index, begin, length);
		else
			m = ref Msg.Cancel(index, begin, length);
	}
	if(i != len d)
		return (nil, "bad message, leftover data");
	return (m, nil);
}

Msg.read(fd: ref Sys->FD): (ref Msg, string)
{
	buf := array[4] of byte;
	n := sys->readn(fd, buf, len buf);
	if(n < 0)
		return (nil, sprint("reading: %r"));
	if(n < len buf)
		return (nil, sprint("short read"));
	(size, nil) := g32(buf, 0);
	buf = array[size] of byte;
say(sprint("msg.read: have size=%d", size));


	n = sys->readn(fd, buf, len buf);
	if(n < 0)
		return (nil, sprint("reading: %r"));
	if(n < len buf)
		return (nil, sprint("short read"));

	return Msg.unpack(buf);
}

Msg.text(mm: self ref Msg): string
{
	s := tag2string[tagof mm];
	pick m := mm {
	Have =>		s += sprint(" index=%d", m.index);
	Bitfield =>	; # xxx show bitfield...?
	Piece =>	s += sprint(" index=%d begin=%d length=%d", m.index, m.begin, len m.d);
	Request or Cancel =>	s += sprint(" index=%d begin=%d length=%d", m.index, m.begin, m.length);
	}
	return s;
}

encode(a: array of byte): string
{
	s := "";
	for(i := 0; i < len a; i++)
		s += sprint("%%%02x", int a[i]);
	return s;
}


sanitizepath(s: string): string
{
	if(str->prefix("/", s) || suffix("/", s))
		s = s[1:];
	if(str->prefix("../", s) || suffix("/..", s) || s == "..")
		return nil;
	if(str->splitstrl(s, "/../").t1 != nil)
		return nil;
	return s;
}

foldpath(l: list of string): string
{
	path := "";
	if(l == nil)
		return nil;
	for(; l != nil; l = tl l)
		if(hd l == ".." || hd l == "" || str->in('/', hd l))
			return nil;
		else
			path += "/"+hd l;
	return path[1:];
}

Torrent.open(path: string): (ref Torrent, string)
{
	fd := sys->open(path, Sys->OREAD);
	if(fd == nil)
		return (nil, sprint("open %s: %r", path));

	d := readfile(fd);
	if(d == nil)
		return (nil, sprint("reading %s: %r", path));
	#say("have file");
	#say(sprint("file length=%d byte0=%c", len d, int d[0]));

	(b, err) := Bee.unpack(d);
	if(err != nil)
		return (nil, sprint("parsing %s: %s", path, err));
say("have unpacked bee");

	bannoun := b.gets("announce"::nil);
	if(bannoun == nil)
		return (nil, sprint("%s: missing announce field", path));

	binfo := b.get("info"::nil);
	if(binfo == nil)
		return (nil, sprint("%s: missing info field", path));
	bd := binfo.pack();
	hash := array[keyring->SHA1dlen] of byte;
	keyring->sha1(bd, len bd, hash, nil);

	bpiecelen := binfo.geti("piece length"::nil);
	if(bpiecelen == nil)
		return (nil, sprint("%s: missing 'piece length' field", path));


	bpieces := binfo.gets("pieces"::nil);
	if(bpieces == nil)
		return (nil, sprint("%s: missing field 'pieces' in 'info'", path));
	if(len bpieces.a % 20 != 0)
		return (nil, sprint("%s: bad length of 'pieces', not multiple of hash size", path));

	pieces := array[len bpieces.a/20] of array of byte;
	i := 0;
	for(o := 0; o < len bpieces.a; o += 20)
		pieces[i++] = bpieces.a[o:o+20];


	# file name, or dir name for files in case of multi-file torrent
	bname := binfo.gets("name"::nil);
	if(bname == nil)
		return (nil, "missing destination file name");
	name := sanitizepath(string bname.a);
	if(name == nil)
		return (nil, sprint("weird path, refusing to create: %q", name));

	# determine paths for files, and total length
	length := big 0;
	blength := binfo.geti("length"::nil);
	files: list of ref (string, big);
	if(blength != nil) {
		length = blength.i;
		files = ref (name, length)::nil;
	} else {
		bfiles := binfo.getl("files"::nil);
		if(bfiles == nil)
			return (nil, sprint("%s: missing field 'length' or 'files' in 'info'", path));
		if(len bfiles.a == 0)
			return (nil, sprint("%s: no files in torrent", path));
		for(i = 0; i < len bfiles.a; i++) {
			blen := bfiles.a[i].geti("length"::nil);
			if(blen == nil)
				return (nil, sprint("%s: missing field 'length' in 'files[%d]' in 'info'", path, i));
			size := blen.i;

			pathl := bfiles.a[i].getl("path"::nil);
			if(pathl == nil)
				return (nil, sprint("missing or invalid 'path' for file"));
			pathls: list of string;
			for(j := len pathl.a-1; j >= 0; j--)
				pick e := pathl.a[j] {
				String =>
					pathls = string e.a::pathls;
				* =>
					return (nil, sprint("bad type for element of 'path' for file"));
				}
			dstpath := foldpath(name::pathls);
			if(dstpath == nil)
				return (nil, sprint("weird path, refusing to create: %q", join(pathls, "/")));
			files = ref (dstpath, size)::files;
			length += size;
		}
	}
	files = rev(files);

	#say("have torrent config");

	# xxx sanity checks
	return (ref Torrent(string bannoun.a, int bpiecelen.i, hash, pieces, files, files, length), nil);
}

readfile(fd: ref Sys->FD): array of byte
{
	d := array[0] of byte;
	for(;;) {
		n := sys->read(fd, buf := array[Sys->ATOMICIO] of byte, len buf);
		if(n == 0)
			break;
		if(n < 0)
			return nil;
		nd := array[len d+n] of byte;
		nd[:] = d;
		nd[len d:] = buf[:n];
		d = nd;
	}
	return d;
}


trackerget(t: ref Torrent, peerid: array of byte, up, down, left: big, lport: int, event: string): (int, array of (string, int, array of byte), ref Bee, string)
{
	(url, uerr) := Url.parse(t.announce);
	if(uerr != nil)
		return (0, nil, nil, "parsing announce url: "+uerr);

	s := "";
	s += "&info_hash="+encode(t.hash);
	s += "&peer_id="+encode(peerid);
	s += "&port="+string lport;
	s += sprint("&uploaded=%bd", up);
	s += sprint("&downloaded=%bd", down);
	s += sprint("&left=%bd", left);
	if(event != nil)
		s += "&event="+http->encode(event);
	if(url.searchpart == "")
		s = s[1:];
	url.searchpart += "?"+s;

	(rb, herr) := http->get(url, nil);
	if(herr != nil)
		return (0, nil, nil, "request: "+herr);
	(d, rerr) := rb.readall();
	if(rerr != nil)
		return (0, nil, nil, "reading: "+rerr);

	(b, err) := Bee.unpack(d);
	if(err != nil)
		return (0, nil, nil, "parsing: "+err);

        interval := b.geti("interval"::nil);
        if(interval == nil)
                return (0, nil, nil, "bad response, missing key interval");

        peers := b.getl("peers"::nil);
        if(peers == nil)
                return (0, nil, nil, "bad response, missing key peers");

	p := array[len peers.a] of (string, int, array of byte);
	for(i := 0; i < len peers.a; i++) {
		ip := peers.a[i].gets("ip"::nil);
                port := peers.a[i].geti("port"::nil);
                rpeerid := peers.a[i].gets("peer id"::nil);
		if(ip == nil || port == nil || rpeerid == nil)
			return (0, nil, nil, "bad response, missing key ip, port or peer id");
		p[i] = (string ip.a, int port.i, rpeerid.a);
	}
	
	return (int interval.i, p, b, nil);
}

genpeerid(): array of byte
{
	# xxx make it ascii-printable, and identifying client
	return random->randombuf(Random->NotQuiteRandom, 20);
}

bytefmt(bytes: big): string
{
	suffix := array[] of {"b", "k", "m", "g", "t", "p"};
	i := 0;
	while(bytes >= big 10000) {
		bytes /= big 1024;
		i++;
	}
	if(i >= len suffix)
		i = len suffix-1;
	return sprint("%bd%s", bytes, suffix[i]);
}


p32(d: array of byte, i, v: int): int
{
	d[i++] = byte (v>>24);
	d[i++] = byte (v>>16);
	d[i++] = byte (v>>8);
	d[i++] = byte (v>>0);
	return i;
}


g32(d: array of byte, i: int): (int, int)
{
	v: int;
	v = (v<<8)|int d[i++];
	v = (v<<8)|int d[i++];
	v = (v<<8)|int d[i++];
	v = (v<<8)|int d[i++];
	return (v, i);
}

rev[T](l: list of T): list of T
{
	r: list of T;
	for(; l != nil; l = tl l)
		r = hd l::r;
	return r;
}

join(l: list of string, s: string): string
{
	if(l == nil)
		return nil;
	r := hd l;
	l = tl l;
	for(; l != nil; l = tl l)
		r += s+hd l;
	return r;
}

suffix(suf, s: string): int
{
	return len s >= len suf && suf == s[len s-len suf:];
}

say(s: string)
{
	if(1)
		fprint(fildes(2), "%s\n", s);
}