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
package reactor.core.error;

/**
 * An error signal from downstream subscribers consuming data when their state is denying any additional event.
 *
 * @author Stephane Maldini
 */
public final class CancelException extends RuntimeException {
	public static final CancelException INSTANCE = new CancelException();

	public static final boolean TRACE_CANCEL = Boolean.parseBoolean(System.getProperty("reactor.trace.cancel", "false"));

	private CancelException() {
		super("The subscriber has denied dispatching");
	}

	public static CancelException get() {
		return TRACE_CANCEL ? new CancelException() : INSTANCE;
	}

	@Override
	public synchronized Throwable fillInStackTrace() {
		return TRACE_CANCEL ? super.fillInStackTrace() : this;
	}
}
