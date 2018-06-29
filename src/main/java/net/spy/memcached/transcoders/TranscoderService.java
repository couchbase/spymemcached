package net.spy.memcached.transcoders;

import java.util.concurrent.Future;

import net.spy.memcached.CachedData;
import net.spy.memcached.compat.SpyObject;

public abstract class TranscoderService extends SpyObject {

	public abstract <T> Future<T> decode(final Transcoder<T> tc, final CachedData cachedData);
	public abstract void shutdown();
	public abstract boolean isShutdown();

}
