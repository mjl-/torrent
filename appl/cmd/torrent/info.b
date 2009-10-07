implement Torrentinfo;

include "sys.m";
	sys: Sys;
	print, sprint: import sys;
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "daytime.m";
	daytime: Daytime;
include "keyring.m";
	kr: Keyring;
include "bitarray.m";
include "bittorrent.m";
	bt: Bittorrent;
	Bee, Msg, Torrent: import bt;
include "util0.m";
	util: Util0;
	fail, warn, sizefmt: import util;

Torrentinfo: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};


dflag: int;
vflag: int;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	daytime = load Daytime Daytime->PATH;
	kr = load Keyring Keyring->PATH;
	bt = load Bittorrent Bittorrent->PATH;
	bt->init();
	util = load Util0 Util0->PATH;
	util->init();

	arg->init(args);
	arg->setusage(arg->progname()+" [-dv] torrentfile");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	dflag++;
		'v' =>	vflag++;
		* =>	arg->usage();
		}

	args = arg->argv();
	if(len args != 1)
		arg->usage();

	(t, err) := Torrent.open(hd args);
	if(err != nil)
		fail(sprint("%s: %s", hd args, err));

	print("announce url:   %s\n", t.announce);
	print("announce url's: %s\n", aafmt(t.announces));
	print("piece length:   %s (%d bytes)\n", sizefmt(big t.piecelen), t.piecelen);
	print("pieces:         %d\n", t.piececount);
	print("total length:   %s (%bd bytes)\n", sizefmt(t.length), t.length);
	if(vflag) {
		print("private:        %d\n", t.private);
		if(t.createdby != nil)
		print("created by:     %s\n", t.createdby);
		if(t.createtime != 0)
		print("creation date:  %s\n", daytime->text(daytime->gmt(t.createtime)));
	}
	print("files:\n");
	for(i := 0; i < len t.files; i++) {
		f := t.files[i];
		print("%10s  %s\n", sizefmt(f.length), f.path);
	}
}

afmt(a: array of string): string
{
	s := "";
	for(i := 0; i < len a; i++)
		s += ","+a[i];
	if(s != nil)
		s = s[1:];
	return s;
}

aafmt(a: array of array of string): string
{
	s := "";
	for(i := 0; i < len a; i++)
		s += "; "+afmt(a[i]);
	if(s != nil)
		s = s[2:];
	return s;
}

say(s: string)
{
	if(dflag)
		warn(s);
}
