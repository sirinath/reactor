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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.rx.Stream;
import reactor.rx.Streams;

/**
 * An abstract {@link ReactorChannel} implementation that handles the basic interaction and behave as a {@link
 * reactor.rx.Stream}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class ChannelStream<IN, OUT> extends Stream<IN> implements ReactorChannel<IN, OUT> {

	protected static final Logger log = LoggerFactory.getLogger(ChannelStream.class);


	private final Environment env;

	private final ReactorProcessor eventsDispatcher;

	private final Function<Buffer, IN>  decoder;
	private final Function<OUT, Buffer> encoder;
	private final long                  prefetch;

	protected ChannelStream(final Environment env,
	                        Codec<Buffer, IN, OUT> codec,
	                        long prefetch,
	                        ReactorProcessor eventsDispatcher) {

		Assert.notNull(eventsDispatcher, "Events Reactor cannot be null");
		this.env = env;
		this.prefetch = prefetch;
		this.eventsDispatcher = eventsDispatcher;

		if (null != codec) {
			this.decoder = codec.decoder(new Consumer<IN>() {
				@Override
				public void accept(IN in) {
					doDecoded(in);
				}
			});
			this.encoder = codec.encoder();
		} else {
			this.decoder = null;
			this.encoder = null;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	final public Stream<Void> writeWith(final Publisher<? extends OUT> source) {
		final Stream<? extends OUT> sourceStream;

		if (Stream.class.isAssignableFrom(source.getClass())) {
			sourceStream = ((Stream<? extends OUT>) source);
		} else {
			sourceStream = new Stream<OUT>() {
				@Override
				public void subscribe(Subscriber<? super OUT> subscriber) {
					source.subscribe(subscriber);
				}

				@Override
				public long getCapacity() {
					return prefetch;
				}
			};
		}

		return new Stream<Void>() {
			@Override
			public void subscribe(Subscriber<? super Void> s) {
				doSubscribeWriter(sourceStream, s);
			}
		};
	}

	/**
	 * Write Buffer directly to be encoded if any codec has been setup
	 *
	 * @param source the raw source to encode
	 *
	 * @return the acknowledgement publisher from {@link #writeWith(Publisher)}
	 */
	final public Stream<Void> writeBufferWith(Publisher<? extends Buffer> source) {
		Stream<OUT> encodedSource = Streams.create(source).map(new Function<Buffer, OUT>() {
			@Override
			@SuppressWarnings("unchecked")
			public OUT apply(Buffer data) {
				if (null != encoder) {
					Buffer bytes = encoder.apply((OUT) data);
					return (OUT) bytes;
				} else {
					return (OUT) data;
				}
			}
		});

		return writeWith(encodedSource);
	}

	@Override
	public final Environment getEnvironment() {
		return env;
	}

	@Override
	public final ReactorProcessor getDispatcher() {
		return eventsDispatcher;
	}

	@Override
	final public long getCapacity() {
		return prefetch;
	}

	public final Function<Buffer, IN> getDecoder() {
		return decoder;
	}

	public final Function<OUT, Buffer> getEncoder() {
		return encoder;
	}

	/**
	 * @return the underlying native connection/channel in use
	 */
	public abstract Object delegate();

	protected abstract void doSubscribeWriter(Publisher<? extends OUT> writer, Subscriber<? super Void> postWriter);

	protected abstract void doDecoded(IN in);
}
