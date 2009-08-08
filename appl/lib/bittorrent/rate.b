implement Rate;

include "torrentget.m";
	sys: Sys;
	bittorrent: Bittorrent;
include "daytime.m";
	daytime: Daytime;

init()
{
	sys = load Sys Sys->PATH;
	daytime = load Daytime Daytime->PATH;
	bittorrent = load Bittorrent Bittorrent->PATH;
	bittorrent->init();
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
	if(time-t.starttime < TrafficHistorysize)
		div = time-t.starttime;
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
	return sys->sprint("<rate %s/s total %s>", bittorrent->bytefmt(big t.rate()), bittorrent->bytefmt(t.total()));
}
