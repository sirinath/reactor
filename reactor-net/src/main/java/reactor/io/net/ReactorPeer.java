/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.net;

import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.rx.Promise;
import reactor.rx.Promises;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract base class that implements common functionality shared by clients and servers.
 * <p>
 * A Peer is network component with start and shutdown capabilities. On Start it will require a {@link ReactorChannelHandler}
 * to process the incoming {@link ReactorChannel}, regardless of being a server or a client.
 *
 * @author Stephane Maldini
 */
public abstract class ReactorPeer<IN, OUT, CONN extends ChannelStream<IN, OUT>> {

	private final   ReactorProcessor       defaultDispatcher;
	private final   Environment            defaultEnv;
	private final   Codec<Buffer, IN, OUT> defaultCodec;
	private final   long                   defaultPrefetch;
	protected final AtomicBoolean          started;

	protected ReactorPeer(Environment defaultEnv,
	                      ReactorProcessor defaultDispatcher,
	                      Codec<Buffer, IN, OUT> codec) {
		this(defaultEnv, defaultDispatcher, codec, Long.MAX_VALUE);
	}

	protected ReactorPeer(Environment defaultEnv,
	                      ReactorProcessor defaultDispatcher,
	                      Codec<Buffer, IN, OUT> codec,
	                      long prefetch) {
		this.defaultEnv = defaultEnv == null && Environment.alive() ? Environment.get() : defaultEnv;
		this.defaultCodec = codec;
		this.defaultDispatcher = defaultDispatcher != null ? defaultDispatcher : SynchronousDispatcher.INSTANCE;
		this.defaultPrefetch = prefetch > 0 ? prefetch : Long.MAX_VALUE;
		this.started = new AtomicBoolean();
	}

	/**
	 * Start this {@literal Peer}.
	 *
	 * @return a {@link reactor.rx.Promise} that will be complete when the {@link ReactorPeer} is started
	 */
	public final Promise<Void> start(
	  final ReactorChannelHandler<IN, OUT, CONN> handler) {

		if (!started.compareAndSet(false, true)) {
			throw new IllegalStateException("Peer already started");
		}

		return doStart(handler);
	}

	/**
	 * Shutdown this {@literal Peer} and complete the returned {@link reactor.rx.Promise} when shut
	 * down.
	 *
	 * @return a {@link reactor.rx.Promise} that will be complete when the {@link ReactorPeer} is shutdown
	 */
	public final Promise<Void> shutdown() {
		if (started.compareAndSet(true, false)) {
			return doShutdown();
		}
		return Promises.success();
	}

	/**
	 * @return Dispatcher assigned to this peer (and used by default on each new Channel)
	 */
	public final ReactorProcessor getDefaultDispatcher() {
		return defaultDispatcher;
	}

	/**
	 * Get the {@link Codec} in use.
	 *
	 * @return The defaultCodec. May be {@literal null}.
	 */
	public final Codec<Buffer, IN, OUT> getDefaultCodec() {
		return defaultCodec;
	}

	/**
	 * Get the default environment for all Channel
	 *
	 * @return The default environment
	 */
	public final Environment getDefaultEnvironment() {
		return defaultEnv;
	}

	/**
	 * Get the default batch read/write size
	 *
	 * @return the default capacity, default Long.MAX for unbounded
	 */
	public final long getDefaultPrefetchSize() {
		return defaultPrefetch;
	}

	protected abstract Promise<Void> doStart(ReactorChannelHandler<IN, OUT, CONN> handler);

	protected abstract Promise<Void> doShutdown();
}
