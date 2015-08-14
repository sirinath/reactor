/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.processor;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.PublisherFactory;
import reactor.core.subscriber.SubscriberFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class RingBufferWorkProcessorTests extends AbstractProcessorTests {

	@Override
	public Processor<Long, Long> createIdentityProcessor(int bufferSize) {
		return RingBufferWorkProcessor.<Long>create("tckRingBufferProcessor", bufferSize);
	}

	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo() throws Throwable {
		//IGNORE since subscribers see distinct data
	}


	public static void main(String... args) {
		final RingBufferWorkProcessor<Long> processor = RingBufferWorkProcessor.<Long>create();

		Publisher<Long> pub = PublisherFactory.forEach(
				c -> {
					if (c.context().incrementAndGet() >= 661) {
						c.onComplete();
						processor.onComplete();
					} else {
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {

						}
						c.onNext(c.context().get());
						System.out.println(c.context() + " emit");
					}
				},
				s -> new AtomicLong()
		);

		for(int i = 0; i < 2; i++) {
			processor.subscribe(new Subscriber<Long>() {
				@Override
				public void onSubscribe(Subscription s) {
					s.request(1000);
				}

				@Override
				public void onNext(Long aLong) {
					System.out.println(Thread.currentThread() + " next " + aLong);
				}

				@Override
				public void onError(Throwable t) {

				}

				@Override
				public void onComplete() {
					System.out.println("finish");
				}
			});
		}

		processor
				.writeWith(pub)
				.subscribe(SubscriberFactory.unbounded());
	}
}
