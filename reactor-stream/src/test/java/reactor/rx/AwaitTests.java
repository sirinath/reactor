/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.rx;

import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selectors;
import reactor.ReactorProcessor;
import reactor.core.dispatch.RingBufferDispatcher;
import reactor.core.dispatch.ThreadPoolExecutorDispatcher;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;
import reactor.rx.broadcast.Broadcaster;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class AwaitTests extends AbstractReactorTest {

	@Test
	public void testAwaitDoesntBlockUnnecessarily() throws InterruptedException {
		ThreadPoolExecutorDispatcher dispatcher = new ThreadPoolExecutorDispatcher(4, 64);

		EventBus innerReactor = EventBus.config().env(env).dispatcher(dispatcher).get();

		for (int i = 0; i < 10000; i++) {
			final Promise<String> deferred = Promises.<String>prepare(env);

			innerReactor.schedule(new Consumer() {

				@Override
				public void accept(Object t) {
					deferred.onNext("foo");
				}

			}, null);

			String latchRes = deferred.await(10, TimeUnit.SECONDS);
			assertThat("latch is not counted down : " + deferred.debug(), "foo".equals(latchRes));
		}
	}



	@Test
	public void testDoesntDeadlockOnError() throws InterruptedException {

		ReactorProcessor dispatcher = new RingBufferDispatcher("rb", 8, null, ProducerType.MULTI, new BlockingWaitStrategy());
		EventBus r = new EventBus(dispatcher);

		Broadcaster<Event<Throwable>> stream = Broadcaster.<Event<Throwable>> create();
		Promise<List<Long>> promise = stream.take(16).count().toList();
		r.on(Selectors.T(Throwable.class), stream.toBroadcastNextConsumer());
		r.on(Selectors.$("test"), (Event<?> ev) -> {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e){
					//IGNORE
				}
				throw new RuntimeException();
		});

		for(int i = 0; i<16; i++){
			r.notify("test", Event.wrap("test"));
		}
		promise.await(5, TimeUnit.SECONDS);

		assert promise.get().get(0) == 16;

	}

}
