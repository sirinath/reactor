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
package reactor.rx.action.filter;

import org.reactivestreams.Subscription;
import reactor.ReactorProcessor;
import reactor.fn.Consumer;
import reactor.fn.timer.Timer;
import reactor.rx.action.Action;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class TakeUntilTimeout<T> extends Action<T, T> {

	private final long             time;
	private final TimeUnit         unit;
	private final Timer            timer;
	private final ReactorProcessor dispatcher;


	public TakeUntilTimeout(ReactorProcessor dispatcher, long time, TimeUnit unit, Timer timer) {
		this.unit = unit;
		this.timer = timer;
		this.time = time;
		this.dispatcher = dispatcher;
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
		timer.submit(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				cancel();
				dispatcher.dispatch(null, new Consumer<Void>() {
					@Override
					public void accept(Void aVoid) {
						broadcastComplete();
					}
				}, null);

			}
		}, time, unit);
	}

	@Override
	public String toString() {
		return super.toString() + "{" +
		  "time=" + time +
		  "unit=" + unit +
		  '}';
	}
}
