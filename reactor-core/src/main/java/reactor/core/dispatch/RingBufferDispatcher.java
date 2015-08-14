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

package reactor.core.dispatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.*;
import reactor.jarjar.com.lmax.disruptor.dsl.Disruptor;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of a {@link reactor.core.Dispatcher} that uses a {@link RingBuffer} to queue tasks to execute.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public final class RingBufferDispatcher extends SingleThreadDispatcher  {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final ExecutorService            executor;
	private final Disruptor<RingBufferTask>  disruptor;
	private final RingBuffer<RingBufferTask> ringBuffer;

	/**
	 * Creates a new {@code RingBufferDispatcher} with the given {@code name}. It will use a RingBuffer with 1024 slots,
	 * configured with a producer type of {@link ProducerType#MULTI MULTI} and a {@link BlockingWaitStrategy blocking
	 * wait
	 * strategy}.
	 *
	 * @param name The name of the dispatcher.
	 */
	public RingBufferDispatcher(String name) {
		this(name, DEFAULT_BUFFER_SIZE);
	}

	/**
	 * Creates a new {@code RingBufferDispatcher} with the given {@code name} and {@param bufferSize},
	 * configured with a producer type of {@link ProducerType#MULTI MULTI} and a {@link BlockingWaitStrategy blocking
	 * wait
	 * strategy}.
	 *
	 * @param name       The name of the dispatcher
	 * @param bufferSize The size to configure the ring buffer with
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
	                            int bufferSize
	) {
		this(name, bufferSize, null);
	}

	/**
	 * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link RingBuffer} with
	 * {@code bufferSize} slots, configured with a producer type of {@link ProducerType#MULTI MULTI}
	 * and a {@link BlockingWaitStrategy blocking wait. A given @param uncaughtExceptionHandler} will catch anything not
	 * handled e.g. by the owning {@code reactor.bus.EventBus} or {@code reactor.rx.Stream}.
	 *
	 * @param name                     The name of the dispatcher
	 * @param bufferSize               The size to configure the ring buffer with
	 * @param uncaughtExceptionHandler The last resort exception handler
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
	                            int bufferSize,
	                            final Consumer<Throwable> uncaughtExceptionHandler) {
		this(name, bufferSize, uncaughtExceptionHandler, ProducerType.MULTI, new BlockingWaitStrategy());

	}

	/**
	 * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link RingBuffer} with
	 * {@code bufferSize} slots, configured with the given {@code producerType}, {@param uncaughtExceptionHandler}
	 * and {@code waitStrategy}. A null {@param uncaughtExceptionHandler} will make this dispatcher logging such
	 * exceptions.
	 *
	 * @param name                     The name of the dispatcher
	 * @param bufferSize               The size to configure the ring buffer with
	 * @param producerType             The producer type to configure the ring buffer with
	 * @param waitStrategy             The wait strategy to configure the ring buffer with
	 * @param uncaughtExceptionHandler The last resort exception handler
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
	                            int bufferSize,
	                            final Consumer<Throwable> uncaughtExceptionHandler,
	                            ProducerType producerType,
	                            WaitStrategy waitStrategy) {
		super(bufferSize);
		this.executor = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory(name, getContext()));
		this.disruptor = new Disruptor<RingBufferTask>(
				new EventFactory<RingBufferTask>() {
					@Override
					public RingBufferTask newInstance() {
						return new RingBufferTask();
					}
				},
				bufferSize,
				executor,
				producerType,
				waitStrategy
		);

		this.disruptor.handleExceptionsWith(new ExceptionHandler() {
			@Override
			public void handleEventException(Throwable ex, long sequence, Object event) {
				handleOnStartException(ex);
			}

			@Override
			public void handleOnStartException(Throwable ex) {
				if (null != uncaughtExceptionHandler) {
					uncaughtExceptionHandler.accept(ex);
				} else {
					log.error(ex.getMessage(), ex);
				}
			}

			@Override
			public void handleOnShutdownException(Throwable ex) {
				handleOnStartException(ex);
			}
		});
		this.disruptor.handleEventsWith(new EventHandler<RingBufferTask>() {
			@Override
			public void onEvent(RingBufferTask task, long sequence, boolean endOfBatch) throws Exception {
				task.run();
			}
		});

		this.ringBuffer = disruptor.start();
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		if (!alive()) {
			return false;
		}
		super.shutdown();

		long start = System.nanoTime();
		long timeoutNano = timeUnit.toNanos(timeout);

		try {
			disruptor.shutdown(timeout, timeUnit);
		} catch (TimeoutException e) {
			return false;
		}

		// This is a work-around for a case when BatchEventProcessor job is scheduled onto executor by disruptor
		// after disruptor.shutdown(...) has been called. As a result the executor becomes occupied with a job and
		// is never terminated
		final CountDownLatch latch = new CountDownLatch(1);
		executor.execute(new Runnable() {
			@Override
			public void run() {
				// to make sure BatchEventProcessor job won't be executed after the current task completes
				executor.shutdown();
				latch.countDown();
			}
		});

		try {
			while (!latch.await(1, TimeUnit.MILLISECONDS)) {
				long now = System.nanoTime();
				timeoutNano -= (now - start);
				start = now;

				if (timeoutNano <= 0) {
					return false;
				}

				disruptor.shutdown(timeoutNano, TimeUnit.NANOSECONDS);
            }
		} catch (InterruptedException | TimeoutException e) {
			return false;
		}

		try {
			timeoutNano -= (System.nanoTime() - start);

			executor.awaitTermination(timeoutNano, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			return false;
		}

		return true;
	}

	@Override
	public void shutdown() {
		executor.shutdown();
		disruptor.shutdown();
		super.shutdown();
	}

	@Override
	public void forceShutdown() {
		executor.shutdownNow();
		disruptor.halt();
		super.forceShutdown();
	}

	@Override
	public long remainingSlots() {
		return ringBuffer.remainingCapacity();
	}


	@Override
	protected Task tryAllocateTask() throws reactor.core.processor.InsufficientCapacityException {
		try {
			long seqId = ringBuffer.tryNext();
			return ringBuffer.get(seqId).setSequenceId(seqId);
		} catch (reactor.jarjar.com.lmax.disruptor.InsufficientCapacityException e) {
			throw reactor.core.processor.InsufficientCapacityException.get();
		}
	}

	@Override
	protected Task allocateTask() {
		long seqId = ringBuffer.next();
		return ringBuffer.get(seqId).setSequenceId(seqId);
	}

	protected void execute(Task task) {
		ringBuffer.publish(((RingBufferTask) task).getSequenceId());
	}

	private class RingBufferTask extends SingleThreadTask {
		private long sequenceId;

		public long getSequenceId() {
			return sequenceId;
		}

		public RingBufferTask setSequenceId(long sequenceId) {
			this.sequenceId = sequenceId;
			return this;
		}
	}

}
