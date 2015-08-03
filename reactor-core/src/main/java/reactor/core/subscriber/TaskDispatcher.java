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
package reactor.core.subscriber;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.processor.AsyncProcessor;
import reactor.core.support.Bounded;
import reactor.core.support.Resource;
import reactor.core.support.SingleUseExecutor;
import reactor.core.error.Exceptions;
import reactor.fn.Consumer;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A base processor used by executor backed processors to take care of their ExecutorService
 *
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
public final class TaskDispatcher implements Executor, Subscriber<Consumer<?>>, Resource, Bounded {

	final private Processor<Task, Task> processor;

	protected TaskDispatcher(Processor<Task, Task> processor) {
		this.processor = processor;
	}


	@Override
	public boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		try {
			shutdown();
			return executor.awaitTermination(timeout, timeUnit);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	@Override
	public void forceShutdown() {
		if (executor.isShutdown()) return;
		executor.shutdownNow();
	}

	@Override
	public boolean alive() {
		return !executor.isTerminated();
	}

	@Override
	public void shutdown() {
		try {
			onComplete();
			executor.shutdown();
		} catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			onError(t);
		}
	}

	@Override
	public void onSubscribe(Subscription s) {

	}

	@Override
	public void onNext(IN in) {

	}

	@Override
	public void onError(Throwable t) {

	}

	@Override
	public void onComplete() {
		if (executor.getClass() == SingleUseExecutor.class) {
			executor.shutdown();
		}
	}

	@Override
	public void execute(Runnable command) {

	}

	@Override
	public long getCapacity() {
		return 0;
	}

	@Override
	public boolean isExposedToOverflow(Bounded parentPublisher) {
		return Bounded.class.isAssignableFrom(processor.getClass()) &&
		  ((Bounded)processor).isExposedToOverflow(parentPublisher);
	}

	/**
	 *
	 */
	static class TailRecurser {

        private final ArrayList<Task> pile;

        private final int pileSizeIncrement;

        private final Supplier<Task> taskSupplier;

        private final Consumer<Task> pileConsumer;

        private int next = 0;

        public  TailRecurser(int backlogSize, Supplier<Task> taskSupplier, Consumer<Task> taskConsumer) {
            this.pileSizeIncrement = backlogSize * 2;
            this.taskSupplier = taskSupplier;
            this.pileConsumer = taskConsumer;
            this.pile = new ArrayList<Task>(pileSizeIncrement);
            ensureEnoughTasks();
        }

        private void ensureEnoughTasks() {
            if (next >= pile.size()) {
                pile.ensureCapacity(pile.size() + pileSizeIncrement);
                for (int i = 0; i < pileSizeIncrement; i++) {
                    pile.add(taskSupplier.get());
                }
            }
        }

        public Task next() {
            ensureEnoughTasks();
            return pile.get(next++);
        }

        public void consumeTasks() {
            if (next > 0) {
                for (int i = 0; i < next; i++) {
                    pileConsumer.accept(pile.get(i));
                }

                for (int i = next - 1; i >= pileSizeIncrement; i--) {
                    pile.remove(i);
                }
                next = 0;
            }
        }
    }

	private static final class Task<T>{

	}
}
