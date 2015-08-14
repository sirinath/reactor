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

package reactor.rx;

import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.publisher.PublisherFactory;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.fn.tuple.Tuple3;
import reactor.fn.tuple.Tuple4;
import reactor.fn.tuple.Tuple5;
import reactor.fn.tuple.Tuple6;
import reactor.fn.tuple.Tuple7;
import reactor.fn.tuple.Tuple8;
import reactor.rx.action.combination.MergeAction;

import java.util.Arrays;
import java.util.List;

/**
 * Helper methods for creating {@link reactor.rx.Promise} instances.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public final class Promises {

	/**
	 * Create a synchronous {@link Promise}.
	 *
	 * @param <T> type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> prepare() {
		return ready(null, SynchronousDispatcher.INSTANCE);
	}

	/**
	 * Create a {@link Promise}.
	 *
	 * @param env the {@link reactor.Environment} to use
	 * @param <T> type of the expected value
	 * @return a new {@link reactor.rx.Promise}
	 */
	public static <T> Promise<T> prepare(Environment env) {
		return ready(env, env.getDefaultDispatcher());
	}

	/**
	 * Create a {@link Promise}.
	 *
	 * @param env        the {@link reactor.Environment} to use
	 * @param dispatcher the {@link ReactorProcessor} to use
	 * @param <T>        type of the expected value
	 * @return a new {@link reactor.rx.Promise}
	 */
	public static <T> Promise<T> ready(Environment env, ReactorProcessor dispatcher) {
		return new Promise<T>(dispatcher, env);
	}

	/**
	 * Create a synchronous {@link Promise} producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T>      type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> syncTask(Supplier<T> supplier) {
		return task(null, SynchronousDispatcher.INSTANCE, supplier);
	}

	/**
	 * Create a {@link Promise} producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier {@link Supplier} that will produce the value
	 * @param env      The assigned environment
	 * @param <T>      type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> task(Environment env, Supplier<T> supplier) {
		return task(env, env.getDefaultDispatcher(), supplier);
	}

	/**
	 * Create a {@link Promise} producing the value for the {@link Promise} using the
	 * given supplier.
	 *
	 * @param supplier   {@link Supplier} that will produce the value
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to schedule the value
	 * @param <T>        type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> task(Environment env, ReactorProcessor dispatcher, final Supplier<T> supplier) {
		Publisher<T> p = PublisherFactory.forEach(new Consumer<SubscriberWithContext<T, Void>>() {
			@Override
			public void accept(SubscriberWithContext<T, Void> sub) {
				sub.onNext(supplier.get());
				sub.onComplete();
			}
		});
		return Streams.wrap(p)
		              .env(env)
		              .subscribeOn(dispatcher)
		              .next();
	}

	/**
	 * Create a {@link Promise} using the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value the value to complete the {@link Promise} with
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given value
	 */
	public static <T> Promise<T> success(T value) {
		return success(null, SynchronousDispatcher.INSTANCE, value);
	}

	/**
	 * Create a {@link Promise} already completed without any data.
	 *
	 * @return A {@link Promise} that is completed
	 */
	public static Promise<Void> success() {
		return success(null, SynchronousDispatcher.INSTANCE, null);
	}

	/**
	 * Create a {@link Promise} using the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value the value to complete the {@link Promise} with
	 * @param env   The assigned environment
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given value
	 */
	public static <T> Promise<T> success(Environment env, T value) {
		return success(env, env.getDefaultDispatcher(), value);
	}

	/**
	 * Create a {@link Promise} using the given value to complete the {@link Promise}
	 * immediately.
	 *
	 * @param value      the value to complete the {@link Promise} with
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to schedule the value
	 * @param <T>        the type of the value
	 * @return A {@link Promise} that is completed with the given value
	 */
	public static <T> Promise<T> success(Environment env, ReactorProcessor dispatcher, T value) {
		return new Promise<T>(value, dispatcher, env);
	}

	/**
	 * Create synchronous {@link Promise} and use the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error the error to complete the {@link Promise} with
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given error
	 */
	public static <T> Promise<T> error(Throwable error) {
		return error(null, SynchronousDispatcher.INSTANCE, error);
	}

	/**
	 * Create a {@link Promise} and use the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error the error to complete the {@link Promise} with
	 * @param env   The assigned environment
	 * @param <T>   the type of the value
	 * @return A {@link Promise} that is completed with the given error
	 */
	public static <T> Promise<T> error(Environment env, Throwable error) {
		return error(env, env.getDefaultDispatcher(), error);
	}

	/**
	 * Create a {@link Promise} and use the given error to complete the {@link Promise}
	 * immediately.
	 *
	 * @param error      the error to complete the {@link Promise} with
	 * @param env        The assigned environment
	 * @param dispatcher The dispatcher to schedule the value
	 * @param <T>        the type of the value
	 * @return A {@link Promise} that is completed with the given error
	 */
	public static <T> Promise<T> error(Environment env, ReactorProcessor dispatcher, Throwable error) {
		return new Promise<T>(error, dispatcher, env);
	}


	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,     p2
	 *                The promises to use.
	 * @param <T1,T2> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2> Promise<Tuple2<T1, T2>> when(Promise<T1> p1, Promise<T2> p2) {
		return multiWhen(new Promise[]{p1, p2}).map(new Function<List<Object>, Tuple2<T1, T2>>() {
			@Override
			public Tuple2<T1, T2> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,        p2, p3
	 *                   The promises to use.
	 * @param <T1,T2,T3> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Promise<Tuple3<T1, T2, T3>> when(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3) {
		return multiWhen(new Promise[]{p1, p2, p3}).map(new Function<List<Object>, Tuple3<T1, T2, T3>>() {
			@Override
			public Tuple3<T1, T2, T3> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,           p2, p3, p4
	 *                      The promises to use.
	 * @param <T1,T2,T3,T4> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Promise<Tuple4<T1, T2, T3, T4>> when(Promise<T1> p1, Promise<T2> p2, Promise<T3> p3,
	                                                                    Promise<T4> p4) {
		return multiWhen(new Promise[]{p1, p2, p3, p4}).map(new Function<List<Object>, Tuple4<T1, T2, T3, T4>>() {
			@Override
			public Tuple4<T1, T2, T3, T4> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,              p2, p3, p4, p5
	 *                         The promises to use.
	 * @param <T1,T2,T3,T4,T5> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Promise<Tuple5<T1, T2, T3, T4, T5>> when(Promise<T1> p1, Promise<T2> p2,
	                                                                            Promise<T3> p3, Promise<T4> p4,
	                                                                            Promise<T5> p5) {
		return multiWhen(new Promise[]{p1, p2, p3, p4, p5}).map(new Function<List<Object>, Tuple5<T1, T2, T3, T4,
				T5>>() {
			@Override
			public Tuple5<T1, T2, T3, T4, T5> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3),
						(T5) objects.get(4));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,                 p2, p3, p4, p5, p6
	 *                            The promises to use.
	 * @param <T1,T2,T3,T4,T5,T6> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Promise<Tuple6<T1, T2, T3, T4, T5, T6>> when(Promise<T1> p1, Promise<T2> p2,
	                                                                                    Promise<T3> p3, Promise<T4> p4,
	                                                                                    Promise<T5> p5, Promise<T6> p6) {
		return multiWhen(new Promise[]{p1, p2, p3, p4, p5, p6}).map(new Function<List<Object>, Tuple6<T1, T2, T3,
				T4, T5, T6>>() {
			@Override
			public Tuple6<T1, T2, T3, T4, T5, T6> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3),
						(T5) objects.get(4), (T6) objects.get(5));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,                    p2, p3, p4, p5, p6, p7
	 *                               The promises to use.
	 * @param <T1,T2,T3,T4,T5,T6,T7> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, T7> Promise<Tuple7<T1, T2, T3, T4, T5, T6, T7>> when(Promise<T1> p1,
	                                                                                            Promise<T2> p2,
	                                                                                            Promise<T3> p3,
	                                                                                            Promise<T4> p4,
	                                                                                            Promise<T5> p5,
	                                                                                            Promise<T6> p6,
	                                                                                            Promise<T7> p7) {
		return multiWhen(new Promise[]{p1, p2, p3, p4, p5, p6, p7}).map(new Function<List<Object>, Tuple7<T1, T2,
				T3, T4, T5, T6, T7>>() {
			@Override
			public Tuple7<T1, T2, T3, T4, T5, T6, T7> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3),
						(T5) objects.get(4), (T6) objects.get(5), (T7) objects.get(6));
			}
		});
	}

	/**
	 * Merge given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise
	 * Promises} have been fulfilled.
	 *
	 * @param p1,                       p2, p3, p4, p5, p6, p7, p8
	 *                                  The promises to use.
	 * @param <T1,T2,T3,T4,T5,T6,T7,T8> The type of the function Tuple result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Promise<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> when(Promise<T1> p1,
	                                                                                                    Promise<T2> p2,
	                                                                                                    Promise<T3> p3,
	                                                                                                    Promise<T4> p4,
	                                                                                                    Promise<T5> p5,
	                                                                                                    Promise<T6> p6,
	                                                                                                    Promise<T7> p7,
	                                                                                                    Promise<T8> p8) {
		return multiWhen(new Promise[]{p1, p2, p3, p4, p5, p6, p7, p8}).map(new Function<List<Object>, Tuple8<T1,
				T2, T3, T4, T5, T6, T7, T8>>() {
			@Override
			public Tuple8<T1, T2, T3, T4, T5, T6, T7, T8> apply(List<Object> objects) {
				return Tuple.of((T1) objects.get(0), (T2) objects.get(1), (T3) objects.get(2), (T4) objects.get(3),
						(T5) objects.get(4), (T6) objects.get(5), (T7) objects.get(6), (T8) objects.get(7));
			}
		});
	}

	/**
	 * Aggregate given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
	 * Promise Promises} have been fulfilled.
	 *
	 * @param promises The promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<List<T>> when(final List<? extends Promise<T>> promises) {
		Assert.isTrue(promises.size() > 0, "Must aggregate at least one promise");

		return new MergeAction<T>(SynchronousDispatcher.INSTANCE, promises)
				.buffer(promises.size())
				.next();
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises The deferred promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	public static <T> Promise<T> any(Promise<T>... promises) {
		return any(Arrays.asList(promises));
	}


	/**
	 * Pick the first result coming from any of the given promises and populate a new {@literal Promise}.
	 *
	 * @param promises The promises to use.
	 * @param <T>      The type of the function result.
	 * @return a {@link Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise<T> any(List<? extends Promise<T>> promises) {
		Assert.isTrue(promises.size() > 0, "Must aggregate at least one promise");

		MergeAction<T> mergeAction = new MergeAction<T>(SynchronousDispatcher.INSTANCE, promises);

		return mergeAction.next();
	}


    /**
     * Aggregate given promises into a new a {@literal Promise} that will be fulfilled when all of the given {@literal
     * Promise Promises} have been fulfilled.
     *
     * @param promises The promises to use.
     * @param <T>      The type of the function result.
     * @return a {@link Promise}.
     */
    private static <T> Promise<List<T>> multiWhen(Promise<T>... promises) {
        return when(Arrays.asList(promises));
    }

}
