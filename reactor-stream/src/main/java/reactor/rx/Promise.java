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

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.dispatch.TailRecurseDispatcher;
import reactor.core.support.Bounded;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.rx.action.Action;
import reactor.rx.broadcast.BehaviorBroadcaster;
import reactor.rx.subscription.PushSubscription;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@code Promise} is a stateful event container that accepts a single value or error. In addition to {@link #get()
 * getting} or {@link #await() awaiting} the value, consumers can be registered to the outbound {@link #stream()} or via
 * , consumers can be registered to be notified of {@link
 * #onError(Consumer) notified an error}, {@link #onSuccess(Consumer) a value}, or {@link #onComplete(Consumer) both}.
 * <p>
 * A promise also provides methods for composing actions with the future value much like a {@link reactor.rx.Stream}.
 * However, where
 * a {@link reactor.rx.Stream} can process many values, a {@code Promise} processes only one value or error.
 *
 * @param <O> the type of the value that will be made available
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @see <a href="https://github.com/promises-aplus/promises-spec">Promises/A+ specification</a>
 */
public class Promise<O> implements Supplier<O>, Processor<O, O>, Consumer<O>, Bounded {

	private final ReentrantLock lock = new ReentrantLock();

	private final long             defaultTimeout;
	private final Condition        pendingCondition;
	private final ReactorProcessor dispatcher;
	private final Environment      environment;
	Action<O, O> outboundStream;

	public static enum FinalState {
		ERROR,
		COMPLETE
	}

	FinalState finalState = null;
	private O         value;
	private Throwable error;
	private boolean hasBlockers = false;

	protected Subscription subscription;

	/**
	 * Creates a new unfulfilled promise.
	 * <p>
	 * The {@code dispatcher} is used when notifying the Promise's consumers, determining the thread on which they are
	 * called. The given {@code env} is used to determine the default await timeout. The
	 * default await timeout will be 30 seconds. This Promise will consumer errors from its {@code parent} such that if
	 * the parent completes in error then so too will this Promise.
	 */
	public Promise() {
		this(SynchronousDispatcher.INSTANCE, null);
	}


	/**
	 * Creates a new unfulfilled promise.
	 * <p>
	 * The {@code dispatcher} is used when notifying the Promise's consumers, determining the thread on which they are
	 * called. The given {@code env} is used to determine the default await timeout. If {@code env} is {@code null} the
	 * default await timeout will be 30 seconds. This Promise will consumer errors from its {@code parent} such that if
	 * the parent completes in error then so too will this Promise.
	 *
	 * @param dispatcher The Dispatcher to run any downstream subscribers
	 * @param env        The Environment, if any, from which the default await timeout is obtained
	 */
	public Promise(ReactorProcessor dispatcher, @Nullable Environment env) {
		this.dispatcher = dispatcher;
		this.environment = env;
		this.defaultTimeout = env != null ? env.getLongProperty("reactor.await.defaultTimeout", 30000L) : 30000L;
		this.pendingCondition = lock.newCondition();
	}

	/**
	 * Creates a new promise that has been fulfilled with the given {@code value}.
	 * <p>
	 * The {@code observable} is used when notifying the Promise's consumers. The given {@code env} is used to
	 * determine
	 * the default await timeout. If {@code env} is {@code null} the default await timeout will be 30 seconds.
	 *
	 * @param value      The value that fulfills the promise
	 * @param dispatcher The Dispatcher to run any downstream subscribers
	 * @param env        The Environment, if any, from which the default await timeout is obtained
	 */
	public Promise(O value, ReactorProcessor dispatcher,
	               @Nullable Environment env) {
		this(dispatcher, env);
		finalState = FinalState.COMPLETE;
		this.value = value;
	}

	/**
	 * Creates a new promise that has failed with the given {@code error}.
	 * <p>
	 * The {@code observable} is used when notifying the Promise's consumers, determining the thread on which they are
	 * called. The given {@code env} is used to determine the default await timeout. If {@code env} is {@code null} the
	 * default await timeout will be 30 seconds.
	 *
	 * @param error      The error the completed the promise
	 * @param dispatcher The Dispatcher to run any downstream subscribers
	 * @param env        The Environment, if any, from which the default await timeout is obtained
	 */
	public Promise(Throwable error, ReactorProcessor dispatcher,
	               @Nullable Environment env) {
		this(dispatcher, env);
		finalState = FinalState.ERROR;
		this.error = error;
	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code Promise} is completed by either
	 * setting a value or propagating an error, or, if this {@code Promise} has already been fulfilled, is immediately
	 * scheduled to be executed on the current {@link ReactorProcessor}.
	 *
	 * @param onComplete the completion {@link Consumer}
	 * @return {@literal the new Promise}
	 */
	public Promise<O> onComplete(@Nonnull final Consumer<Promise<O>> onComplete) {
		if (dispatcher == SynchronousDispatcher.INSTANCE || TailRecurseDispatcher.class == dispatcher.getClass()) {
			lock.lock();
			try {
				if (finalState == FinalState.ERROR) {
					onComplete.accept(this);
					return Promises.error(environment, dispatcher, error);
				} else if (finalState == FinalState.COMPLETE) {
					onComplete.accept(this);
					return Promises.success(environment, dispatcher, value);
				}
			} catch (Throwable t) {
				return Promises.error(environment, dispatcher, t);
			} finally {
				lock.unlock();
			}
		}

		return stream().lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new Action<O, O>() {
					@Override
					protected void doNext(O ev) {
						onComplete.accept(Promise.this);
						broadcastNext(ev);
						broadcastComplete();
					}

					@Override
					protected void doError(Throwable ev) {
						onComplete.accept(Promise.this);
						broadcastError(ev);
					}

					@Override
					protected void doComplete() {
						onComplete.accept(Promise.this);
						broadcastComplete();
					}
				};
			}
		}).next();
	}

	/**
	 * Only forward onError and onComplete signals into the returned stream.
	 *
	 * @return {@literal new Promise}
	 */
	public final Promise<Void> after() {
		if (dispatcher == SynchronousDispatcher.INSTANCE || TailRecurseDispatcher.class == dispatcher.getClass()) {
			lock.lock();
			try {
				if (finalState == FinalState.COMPLETE) {
					return Promises.<Void>success(environment, dispatcher, null);
				}
			} catch (Throwable t) {
				return Promises.<Void>error(environment, dispatcher, t);
			} finally {
				lock.unlock();
			}
		}
		return stream().after().next();
	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code Promise} is successfully completed
	 * with
	 * a value, or, if this {@code Promise} has already been fulfilled, is immediately scheduled to be executed on the
	 * current {@link ReactorProcessor}.
	 *
	 * @param onSuccess the success {@link Consumer}
	 * @return {@literal the new Promise}
	 */
	public Promise<O> onSuccess(@Nonnull final Consumer<O> onSuccess) {
		if (dispatcher == SynchronousDispatcher.INSTANCE || TailRecurseDispatcher.class == dispatcher.getClass()) {
			lock.lock();
			try {
				if (finalState == FinalState.COMPLETE) {
					if (value != null) {
						onSuccess.accept(value);
					}
					return this;
				}
			} catch (Throwable t) {
				return Promises.error(environment, dispatcher, t);
			} finally {
				lock.unlock();
			}
		}
		return stream().observe(onSuccess).next();
	}

	/**
	 * Assign a {@link Function} that will either be invoked later, when the {@code Promise} is successfully completed
	 * with
	 * a value, or, if this {@code Promise} has already been fulfilled, is immediately scheduled to be executed on the
	 * current {@link ReactorProcessor}.
	 *
	 * @param transformation the function to apply on signal to the transformed Promise
	 * @return {@literal the new Promise}
	 */
	public <V> Promise<V> map(@Nonnull final Function<? super O, V> transformation) {
		if (dispatcher == SynchronousDispatcher.INSTANCE || TailRecurseDispatcher.class == dispatcher.getClass()) {
			lock.lock();
			try {
				if (finalState == FinalState.ERROR) {
					return Promises.error(environment, dispatcher, error);
				} else if (finalState == FinalState.COMPLETE) {
					return Promises.success(environment, dispatcher, value != null ? transformation.apply(value) : null);
				}
			} catch (Throwable t) {
				return Promises.error(environment, dispatcher, t);
			} finally {
				lock.unlock();
			}
		}
		return stream().map(transformation).next();
	}

	/**
	 * Assign a {@link Function} that will either be invoked later, when the {@code Promise} is successfully completed
	 * with
	 * a value, or, if this {@code Promise} has already been fulfilled, is immediately scheduled to be executed on the
	 * current {@link ReactorProcessor}.
	 * <p>
	 * FlatMap is typically used to listen for a delayed/async publisher, e.g. promise.flatMap( data -> Promise.success
	 * (data) ).
	 * The result is merged directly on the returned stream.
	 *
	 * @param transformation the function to apply on signal to the supplied Promise that will be merged back.
	 * @return {@literal the new Promise}
	 */
	public <V> Promise<V> flatMap(@Nonnull final Function<? super O, ? extends Publisher<? extends V>> transformation) {
		if (dispatcher == SynchronousDispatcher.INSTANCE || TailRecurseDispatcher.class == dispatcher.getClass()) {
			lock.lock();
			try {
				if (finalState == FinalState.ERROR) {
					return Promises.error(environment, dispatcher, error);
				} else if (finalState == FinalState.COMPLETE) {
					if (value != null) {
						Promise<V> successPromise = Promises.<V>ready(environment, dispatcher);
						transformation.apply(value).subscribe(successPromise);
						return successPromise;
					} else {
						return Promises.success(environment, dispatcher, null);
					}
				}
			} catch (Throwable t) {
				return Promises.error(environment, dispatcher, t);
			} finally {
				lock.unlock();
			}
		}
		return stream().flatMap(transformation).next();

	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code Promise} is completed with an error,
	 * or, if this {@code Promise} has already been fulfilled, is immediately scheduled to be executed on the current
	 * {@link ReactorProcessor}. The error is recovered and materialized as the next signal to the returned stream.
	 *
	 * @param onError the error {@link Consumer}
	 * @return {@literal the new Promise}
	 */
	public Promise<O> onError(@Nonnull final Consumer<Throwable> onError) {
		if (dispatcher == SynchronousDispatcher.INSTANCE || TailRecurseDispatcher.class == dispatcher.getClass()) {
			lock.lock();
			try {
				if (finalState == FinalState.ERROR) {
					onError.accept(error);
					return this;
				}else if(finalState == FinalState.COMPLETE){
					return this;
				}
			} catch (Throwable t) {
				return Promises.error(environment, dispatcher, t);
			} finally {
				lock.unlock();
			}
		}

		return stream().when(Throwable.class, onError).next();
	}

	/**
	 * Indicates whether this {@code Promise} has been completed with either an error or a value
	 *
	 * @return {@code true} if this {@code Promise} is complete, {@code false} otherwise.
	 * @see #isPending()
	 */

	public boolean isComplete() {
		lock.lock();
		try {
			return finalState != null;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Indicates whether this {@code Promise} has yet to be completed with a value or an error.
	 *
	 * @return {@code true} if this {@code Promise} is still pending, {@code false} otherwise.
	 * @see #isComplete()
	 */
	public boolean isPending() {
		lock.lock();
		try {
			return finalState == null;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Indicates whether this {@code Promise} has been successfully completed a value.
	 *
	 * @return {@code true} if this {@code Promise} is successful, {@code false} otherwise.
	 */
	public boolean isSuccess() {
		lock.lock();
		try {
			return finalState == FinalState.COMPLETE;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Indicates whether this {@code Promise} has been completed with an error.
	 *
	 * @return {@code true} if this {@code Promise} was completed with an error, {@code false} otherwise.
	 */
	public boolean isError() {
		lock.lock();
		try {
			return finalState == FinalState.ERROR;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A default timeout as specified in
	 * Reactor's {@link Environment} properties using the key {@code reactor.await.defaultTimeout} is used. The
	 * default is
	 * 30 seconds. If the promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return true if complete without error
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 * @throws RuntimeException     if the promise is completed with an error
	 */
	public boolean awaitSuccess() throws InterruptedException {
		return awaitSuccess(defaultTimeout, TimeUnit.MILLISECONDS);
	}


	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code Promise}.
	 *
	 * @param timeout the timeout value
	 * @param unit    the {@link TimeUnit} of the timeout value
	 * @return true if complete without error
	 * completed
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 */
	public boolean awaitSuccess(long timeout, TimeUnit unit) throws InterruptedException {
		await(timeout, unit);
		return isSuccess();
	}

	/**
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A default timeout as specified in
	 * Reactor's {@link Environment} properties using the key {@code reactor.await.defaultTimeout} is used. The
	 * default is
	 * 30 seconds. If the promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not
	 * completed
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 * @throws RuntimeException     if the promise is completed with an error
	 */
	public O await() throws InterruptedException {
		return await(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code Promise}.
	 *
	 * @param timeout the timeout value
	 * @param unit    the {@link TimeUnit} of the timeout value
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not
	 * completed
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 */
	public O await(long timeout, TimeUnit unit) throws InterruptedException {
		if (!isPending()) {
			return get();
		}

		lock.lock();
		try {
			hasBlockers = true;
			if (timeout >= 0) {
				long msTimeout = TimeUnit.MILLISECONDS.convert(timeout, unit);
				long endTime = System.currentTimeMillis() + msTimeout;
				while (finalState == null && (System.currentTimeMillis()) < endTime) {
					this.pendingCondition.await(200, TimeUnit.MILLISECONDS);
				}
			} else {
				while (finalState == null) {
					this.pendingCondition.await(200, TimeUnit.MILLISECONDS);
				}
			}
		} finally {
			hasBlockers = false;
			lock.unlock();
		}

		return get();
	}

	/**
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A default timeout as specified in
	 * Reactor's {@link Environment} properties using the key {@code reactor.await.defaultTimeout} is used. The
	 * default is
	 * 30 seconds. If the promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not
	 * completed
	 * @throws RuntimeException if the promise is completed with an error
	 */
	public O poll() {
		return poll(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code Promise}. If the
	 * promise
	 * is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @param timeout the timeout value
	 * @param unit    the {@link TimeUnit} of the timeout value
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not
	 * completed
	 */
	public O poll(long timeout, TimeUnit unit) {
		try {
			return await(timeout, unit);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			return null;
		}
	}

	/**
	 * Returns the value that completed this promise. Returns {@code null} if the promise has not been completed. If the
	 * promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value that completed the promise, or {@code null} if it has not been completed
	 * @throws RuntimeException if the promise was completed with an error
	 */
	@Override
	public O get() {
		lock.lock();
		try {
			if (finalState == FinalState.COMPLETE) {
				return value;
			} else if (finalState == FinalState.ERROR) {
				if (RuntimeException.class.isInstance(error)) {
					throw (RuntimeException) error;
				} else {
					throw new RuntimeException(error);
				}
			} else {
				return null;
			}
		} finally {
			lock.unlock();
		}
	}


	/**
	 * Return the error (if any) that has completed this {@code Promise}. Returns {@code null} if the promise has not
	 * been
	 * completed, or was completed with a value.
	 *
	 * @return the error (if any)
	 */
	public Throwable reason() {
		lock.lock();
		try {
			return error;
		} finally {
			lock.unlock();
		}
	}

	public Stream<O> stream() {
		lock.lock();
		try {
			if (outboundStream == null) {
				outboundStream = BehaviorBroadcaster.first(value, environment, dispatcher).capacity(1);
				if (isSuccess()) {
					outboundStream.onComplete();
				} else if (isError()) {
					outboundStream.onError(error);
				}
			}

		} finally {
			lock.unlock();
		}
		return outboundStream;
	}

	@Override
	public void subscribe(final Subscriber<? super O> subscriber) {
		stream().subscribe(subscriber);
	}

	public Environment getEnvironment() {
		return environment;
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		this.subscription = subscription;
		subscription.request(Long.MAX_VALUE);
	}

	@Override
	public void onNext(O element) {
		valueAccepted(element);
	}

	@Override
	public void onComplete() {
		completeAccepted();
	}

	@Override
	public void onError(Throwable cause) {
		errorAccepted(cause);
	}

	@Override
	public void accept(O o) {
		valueAccepted(o);
	}


	public StreamUtils.StreamVisitor debug() {
		Action<?, ?> debugged = findOldestStream();
		if (subscription == null || debugged == null) {
			return outboundStream != null ? outboundStream.debug() : null;
		}

		return debugged.debug();
	}

	@SuppressWarnings("unchecked")
	public Action<?, ?> findOldestStream() {
		Subscription sub = subscription;
		Action<?, ?> that = null;

		while (sub != null
				&& PushSubscription.class.isAssignableFrom(sub.getClass())
				&& ((PushSubscription<?>) sub).getPublisher() != null
				&& Action.class.isAssignableFrom(((PushSubscription<?>) sub).getPublisher().getClass())
				) {

			that = (Action<?, ?>) ((PushSubscription<?>) sub).getPublisher();
			sub = that.getSubscription();
		}
		return that;
	}


	protected void errorAccepted(Throwable error) {
		lock.lock();
		try {
			if (!isPending()) {
				if (isSuccess())
					throw new IllegalStateException(finalState.toString() + " : " + value, error);
				else
					throw new IllegalStateException(finalState.toString(), error);
			}

			this.error = error;
			this.finalState = FinalState.ERROR;

			if (subscription != null) {
				subscription.cancel();
			}

			if (outboundStream != null) {
				outboundStream.onError(error);
			}

			if (hasBlockers) {
				pendingCondition.signalAll();
				hasBlockers = false;
			}
		} finally {
			lock.unlock();
		}
	}

	protected void valueAccepted(O value) {
		lock.lock();
		try {
			if (!isPending()) {
				if (isError())
					throw new IllegalStateException(value + " >> " + finalState.toString(), error);
				else if (isSuccess())
					throw new IllegalStateException(value + " >> " + finalState.toString() + " : " + value);
				else
					throw new IllegalStateException(value + " >> " + finalState.toString());
			}
			this.value = value;
			this.finalState = FinalState.COMPLETE;


			if (subscription != null) {
				subscription.cancel();
			}

			if (outboundStream != null) {
				if(value != null) {
					outboundStream.onNext(value);
				}
				outboundStream.onComplete();
			}

			if (hasBlockers) {
				pendingCondition.signalAll();
				hasBlockers = false;
			}
		} finally {
			lock.unlock();
		}
	}

	protected void completeAccepted() {
		lock.lock();
		try {
			if (isPending()) {
				valueAccepted(null);
			}

			if (subscription != null) {
				subscription.cancel();
				subscription = null;
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isReactivePull(ReactorProcessor dispatcher, long producerCapacity) {
		return true;
	}

	@Override
	public long getCapacity() {
		return 1;
	}

	@Override
	public String toString() {
		lock.lock();
		try {
			return "Promise{" +
					"value=" + value +
					(finalState != null ? ", state=" + finalState : "") +
					", error=" + error +
					'}';
		} finally {
			lock.unlock();
		}
	}

}