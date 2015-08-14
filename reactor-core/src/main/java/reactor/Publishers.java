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
package reactor;

import org.reactivestreams.Publisher;
import reactor.core.publisher.LogPublisher;
import reactor.core.publisher.PublisherFactory;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Publishers extends PublisherFactory {

    /**
     *
     * @param publisher
     * @param <IN>
     * @return
     */
    public static <IN> Publisher<IN> log(Publisher<IN> publisher){
        return log(publisher, null);
    }

    /**
     *
     * @param publisher
     * @param category
     * @param <IN>
     * @return
     */
    public static <IN> Publisher<IN> log(Publisher<IN> publisher, String category){
        return LogPublisher.log(publisher, category);
    }

    /**
     *
     * @param publisher
     * @param <IN>
     * @return
     */
    public static <IN> Publisher<IN> tailRecurse(Publisher<IN> publisher){
        return publisher;
    }

}
