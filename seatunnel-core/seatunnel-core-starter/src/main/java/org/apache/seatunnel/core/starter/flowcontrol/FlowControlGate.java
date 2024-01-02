/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.flowcontrol;

import org.apache.seatunnel.shade.com.google.common.util.concurrent.RateLimiter;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Optional;

public class FlowControlGate {

    private static final int DEFAULT_VALUE = Integer.MAX_VALUE;

    private final Optional<RateLimiter> bytesRateLimiter;
    private final Optional<RateLimiter> countRateLimiter;

    private FlowControlGate(FlowControlStrategy flowControlStrategy) {
        final int bytesPerSecond = flowControlStrategy.getBytesPerSecond();
        final int countPreSecond = flowControlStrategy.getCountPreSecond();
        this.bytesRateLimiter =
                bytesPerSecond == DEFAULT_VALUE
                        ? Optional.empty()
                        : Optional.of(RateLimiter.create(bytesPerSecond));
        this.countRateLimiter =
                countPreSecond == DEFAULT_VALUE
                        ? Optional.empty()
                        : Optional.of(RateLimiter.create(countPreSecond));
    }

    public void audit(SeaTunnelRow row) {
        bytesRateLimiter.ifPresent(rateLimiter -> rateLimiter.acquire(row.getBytesSize()));
        countRateLimiter.ifPresent(RateLimiter::acquire);
    }

    public static FlowControlGate create(FlowControlStrategy flowControlStrategy) {
        return new FlowControlGate(flowControlStrategy);
    }
}
