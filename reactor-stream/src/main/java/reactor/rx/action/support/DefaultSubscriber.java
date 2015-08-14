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
package reactor.rx.action.support;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.ReactorProcessor;
import reactor.core.support.Bounded;

/**
 * @author Stephane Maldini
 */
public class DefaultSubscriber<O> implements Subscriber<O>, Bounded {

	@Override
	public void onSubscribe(Subscription s) {

	}

	@Override
	public void onNext(O o) {

	}

	@Override
	public void onError(Throwable t) {

	}

	@Override
	public void onComplete() {

	}

	@Override
	public boolean isReactivePull(ReactorProcessor dispatcher, long producerCapacity) {
		return getCapacity() < producerCapacity;
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}
}
