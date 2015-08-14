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
package reactor.rx.action.control;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.dispatch.TailRecurseDispatcher;
import reactor.fn.Consumer;
import reactor.rx.action.Action;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class RepeatAction<T> extends Action<T, T> {

	private final long numRetries;
	private long currentNumRetries = 0;
	private final Publisher<? extends T> rootPublisher;
	private       ReactorProcessor       dispatcher;
	private long pendingRequests = 0l;

	public RepeatAction(ReactorProcessor dispatcher, int numRetries, Publisher<? extends T> parentStream) {
		this.numRetries = numRetries;
		if (SynchronousDispatcher.INSTANCE == dispatcher) {
			this.dispatcher = Environment.tailRecurse();
		} else {
			this.dispatcher = dispatcher;
		}
		this.rootPublisher = parentStream;
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
		if (capacity != Long.MAX_VALUE && pendingRequests != Long.MAX_VALUE) {
			synchronized (this) {
				if (pendingRequests != Long.MAX_VALUE) {
					pendingRequests--;
				}
			}
		}
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
		long pendingRequests = this.pendingRequests;
		if (pendingRequests > 0) {
			subscription.request(pendingRequests);
		}
	}

	@Override
	public final ReactorProcessor getDispatcher() {
		return dispatcher;
	}

	@Override
	public void requestMore(long n) {
		synchronized (this) {
			if ((pendingRequests += n) < 0l) {
				pendingRequests = Long.MAX_VALUE;
			}
		}
		super.requestMore(n);
	}

	@Override
	public void onComplete() {
		cancel();
		dispatcher.dispatch(null, new Consumer<Void>() {
			@Override
			public void accept(Void nothing) {
				if (numRetries != -1 && ++currentNumRetries > numRetries) {
					RepeatAction.super.onComplete();
					currentNumRetries = 0;
				} else {
					if (rootPublisher != null) {
						if (TailRecurseDispatcher.class.isAssignableFrom(dispatcher.getClass())) {
							dispatcher.shutdown();
							dispatcher = Environment.tailRecurse();
						}
						rootPublisher.subscribe(RepeatAction.this);
					}
				}

			}
		}, null);
	}
}
