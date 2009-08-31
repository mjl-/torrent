implement Rate;

include "torrentpeer.m";
	sys: Sys;
	sprint: import sys;
	bittorrent: Bittorrent;
	util: Util0;
	sizefmt: import util;
include "daytime.m";
	daytime: Daytime;

init()
{
	sys = load Sys Sys->PATH;
	daytime = load Daytime Daytime->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init();
	util = load Util0 Util0->PATH;
	util->init();
}

Traffic.new(): ref Traffic
{
	return ref Traffic(0, array[TrafficHistorysize] of {* => (0, 0)}, 0, big 0, 0, daytime->now());
}

Traffic.add(t: self ref Traffic, bytes: int)
{
	time := daytime->now();

	if(t.d[t.last].t0 != time) {
		reclaim(t, time);
		t.last = (t.last+1) % len t.d;
		t.winsum -= t.d[t.last].t1;
		t.d[t.last] = (time, 0);
	}
	t.d[t.last].t1 += bytes;
	t.winsum += bytes;
	t.sum += big bytes;
}

reclaim(t: ref Traffic, time: int)
{
	first := t.last+1;
	for(i := 0; t.d[pos := (first+i) % len t.d].t0 < time-TrafficHistorysize && i < TrafficHistorysize; i++) {
		t.winsum -= t.d[pos].t1;
		t.d[pos] = (0, 0);
	}
}

Traffic.packet(t: self ref Traffic)
{
	t.npackets++;
}

Traffic.rate(t: self ref Traffic): int
{
	time := daytime->now();
	reclaim(t, time);

	div := TrafficHistorysize;
	if(time-t.time0< TrafficHistorysize)
		div = time-t.time0;
	if(div == 0)
		div = 1;
	return t.winsum/div;
}

Traffic.total(t: self ref Traffic): big
{
	return t.sum;
}

Traffic.text(t: self ref Traffic): string
{
	return sprint("<rate %s/s total %s>", sizefmt(big t.rate()), sizefmt(t.total()));
}
