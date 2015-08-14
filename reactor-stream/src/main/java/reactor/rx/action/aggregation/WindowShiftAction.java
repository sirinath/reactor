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
package reactor.rx.action.aggregation;

import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.fn.Consumer;
import reactor.fn.Pausable;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.subscription.ReactiveSubscription;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * WindowAction is forwarding events on a steam until {@param backlog} is reached,
 * after that streams collected events further, complete it and create a fresh new stream.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class WindowShiftAction<T> extends Action<T, Stream<T>> {

	private final Consumer<Long> timeshiftTask;
	private final List<ReactiveSubscription<T>> currentWindows = new LinkedList<>();
	private final int              skip;
	private final int              batchSize;
	private final long             timeshift;
	private final TimeUnit         unit;
	private final Timer            timer;
	private final Environment      environment;
	private final ReactorProcessor dispatcher;
	private       int              index;
	private       Pausable         timeshiftRegistration;

	public WindowShiftAction(Environment environment, ReactorProcessor dispatcher, int size, int skip) {
		this(environment, dispatcher, size, skip, -1l, -1l, null, null);
	}

	public WindowShiftAction(Environment environment, final ReactorProcessor dispatcher, int size, int skip,
	                         final long timespan, final long timeshift, TimeUnit unit, final Timer timer) {
		this.dispatcher = dispatcher;
		this.skip = skip;
		this.environment = environment;
		this.batchSize = size;
		if (timespan > 0 && timeshift > 0) {
			final TimeUnit targetUnit = unit != null ? unit : TimeUnit.SECONDS;
			final Consumer<ReactiveSubscription<T>> flushTimerTask = new Consumer<ReactiveSubscription<T>>() {
				@Override
				public void accept(ReactiveSubscription<T> bucket) {
					Iterator<ReactiveSubscription<T>> it = currentWindows.iterator();
					while (it.hasNext()) {
						ReactiveSubscription<T> itBucket = it.next();
						if (bucket == itBucket) {
							it.remove();
							bucket.onComplete();
							break;
						}
					}
				}
			};

			this.timeshiftTask = new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					if (!isPublishing()) {
						return;
					}
					dispatcher.dispatch(null, new Consumer<Void>() {
						@Override
						public void accept(Void aVoid) {
							final ReactiveSubscription<T> bucket = createWindowStream();

							timer.submit(new Consumer<Long>() {
								@Override
								public void accept(Long aLong) {
									dispatcher.dispatch(bucket, flushTimerTask, null);
								}
							}, timespan, targetUnit);
						}
					}, null);
				}
			};

			this.timeshift = timeshift;
			this.unit = targetUnit;
			this.timer = timer;
		} else {
			this.timeshift = -1l;
			this.unit = null;
			this.timer = null;
			this.timeshiftTask = null;
		}
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
		if(timer != null) {
			timeshiftRegistration = timer.schedule(timeshiftTask,
					timeshift,
					unit);
		}
	}

	@Override
	protected void doNext(T value) {
		if (timer == null && index++ % skip == 0) {
			createWindowStream();
		}
		flushCallback(value);
	}

	@Override
	protected void doComplete() {
		for (ReactiveSubscription<T> bucket : currentWindows) {
			bucket.onComplete();
		}
		currentWindows.clear();
		super.doComplete();
	}

	private void flushCallback(T event) {
		Iterator<ReactiveSubscription<T>> it = currentWindows.iterator();
		while (it.hasNext()) {
			ReactiveSubscription<T> bucket = it.next();
			bucket.onNext(event);
			if (bucket.currentNextSignals() == batchSize) {
				it.remove();
				bucket.onComplete();
			}
		}
	}

	protected ReactiveSubscription<T> createWindowStream() {
		Action<T, T> action = Broadcaster.<T>create(environment, dispatcher);
		ReactiveSubscription<T> _currentWindow = new ReactiveSubscription<T>(null, action);
		currentWindows.add(_currentWindow);
		action.onSubscribe(_currentWindow);
		broadcastNext(action);
		return _currentWindow;
	}

	@Override
	public final Environment getEnvironment() {
		return this.environment;
	}

	@Override
	public final ReactorProcessor getDispatcher() {
		return this.dispatcher;
	}

	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
		for (ReactiveSubscription<T> bucket : currentWindows) {
			bucket.onError(ev);
		}
		currentWindows.clear();
	}

}
