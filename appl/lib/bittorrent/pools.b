implement Pools;

include "torrentget.m";
	sys: Sys;
	rand: Rand;
	misc: Misc;

init()
{
	sys = load Sys Sys->PATH;
	rand = load Rand Rand->PATH;
	rand->init(sys->pctl(0, nil)^sys->millisec());
	misc = load Misc Misc->PATH;
	misc->init();
}

Pool[T].new(mode: int): ref Pool[T]
{
	return ref Pool[T](array[0] of T, array[0] of T, 0, mode);
}

Pool[T].fill(p: self ref Pool)
{
	case p.mode {
	Pools->PoolRandom =>
		return;
	Pools->PoolRotateRandom or Pools->PoolInorder =>
		n := len p.pool-len p.active;

		newa := array[len p.pool] of T;
		newa[:] = p.active;
		start := len p.active;
		for(i := 0; i < n; i++) {
			newa[start+i] = p.pool[p.poolnext];
			p.poolnext = (p.poolnext+1) % len p.pool;
		}
		p.active = newa;

		if(p.mode == Pools->PoolRotateRandom)
			misc->randomize(p.active[len p.active-n:]);
	* =>
		raise sys->sprint("bad mode for pool: %d", p.mode);
	}
}

Pool[T].take(p: self ref Pool): T
{
	if(len p.active == 0)
		return nil;

	case p.mode {
	Pools->PoolRandom =>
		return p.pool[rand->rand(len p.pool)];
	Pools->PoolRotateRandom or Pools->PoolInorder =>
		e := p.active[0];
		p.active = p.active[1:];
		return e;
	* =>
		raise sys->sprint("bad mode for pool: %d", p.mode);
	}
}

Pool[T].pooladd(p: self ref Pool, e: T)
{
	newp := array[len p.pool+1] of T;
	newp[:] = p.pool;
	newp[len p.pool] = e;
}

Pool[T].pooladdunique(p: self ref Pool, e: T)
{
	if(!p.poolhas(e))
		p.pooladd(e);
}

Pool[T].poolhas(p: self ref Pool, e: T): int
{
	for(i := 0; i < len p.pool; i++)
		if(p.pool[i] == e)
			return 1;
	return 0;
}

Pool[T].pooldel(p: self ref Pool, e: T)
{
	i := 0;
	while(i < len p.pool) {
		if(p.pool[i] == e) {
			p.pool[i:] = p.pool[i+1:];
			p.pool = p.pool[:len p.pool-1];
		} else
			i++;
	}
}

poolmodes := array[] of {
	"random",
	"rotaterandom",
	"inorder"
};

Pool[T].text(p: self ref Pool): string
{
	return sys->sprint("<rotation len active=%d len pool=%d poolnext=%d mode=%s>", len p.active, len p.pool, p.poolnext, poolmodes[p.mode]);
}

