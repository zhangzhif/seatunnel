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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config;

import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;

import java.io.Serializable;
import java.util.Random;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/**
 * This class defines a range of server id. The boundaries of the range are inclusive.
 *
 * @see JdbcSourceOptions#SERVER_ID
 */
public class ServerIdRange implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Start of the range (inclusive). */
    private final long startServerId;

    /** End of the range (inclusive). */
    private final long endServerId;

    public ServerIdRange(long startServerId, long endServerId) {
        this.startServerId = startServerId;
        this.endServerId = endServerId;
    }

    public long getStartServerId() {
        return startServerId;
    }

    public long getEndServerId() {
        return endServerId;
    }

    public long getServerId(int subTaskId) {
        checkArgument(subTaskId >= 0, "Subtask ID %s shouldn't be a negative number.", subTaskId);
        if ((long) subTaskId > getNumberOfServerIds()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Subtask ID %s is out of server id range %s, "
                                    + "please adjust the server id range to "
                                    + "make the number of server id larger than "
                                    + "the source parallelism.",
                            subTaskId, this));
        }
        return startServerId + subTaskId;
    }

    public long getNumberOfServerIds() {
        return endServerId - startServerId + 1L;
    }

    @Override
    public String toString() {
        if (startServerId == endServerId) {
            return String.valueOf(startServerId);
        } else {
            return startServerId + "-" + endServerId;
        }
    }

    /**
     * Returns a {@link ServerIdRange} from a server id range string which likes '5400-5408' or a
     * single server id likes '5400'.
     */
    public static ServerIdRange from(String range) {
        if (range == null) {
            long start = (new Random().nextInt(Integer.MAX_VALUE)) + 6500L;
            // 1024000 is the maybe max number of parallelism
            // mysql server id should be in range [1, 2^32-1]
            long end = start + 1024000L;
            return new ServerIdRange(start, end);
        }
        if (range.contains("-")) {
            String[] idArray = range.split("-");
            if (idArray.length != 2) {
                throw new IllegalArgumentException(
                        String.format(
                                "The server id range should be syntax like '5400-5500', but got: %s",
                                range));
            }
            return new ServerIdRange(
                    parseServerId(idArray[0].trim()), parseServerId(idArray[1].trim()));
        } else {
            long serverId = parseServerId(range);
            return new ServerIdRange(serverId, serverId);
        }
    }

    private static long parseServerId(String serverIdValue) {
        try {
            return Long.parseLong(serverIdValue);
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    String.format("The server id %s is not a valid numeric.", serverIdValue), e);
        }
    }
}
