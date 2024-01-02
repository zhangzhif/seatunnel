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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SourceSplit;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The serializer of {@link SplitWrapper}.
 *
 * @param <SplitT> The generic type of source split
 */
public class SplitWrapperSerializer<SplitT extends SourceSplit>
        implements SimpleVersionedSerializer<SplitWrapper<SplitT>> {

    private final Serializer<SplitT> serializer;

    public SplitWrapperSerializer(Serializer<SplitT> serializer) {
        this.serializer = serializer;
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(SplitWrapper<SplitT> obj) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            byte[] serialize = serializer.serialize(obj.getSourceSplit());
            out.writeInt(serialize.length);
            out.write(serialize);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public SplitWrapper<SplitT> deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final int size = in.readInt();
            final byte[] stateBytes = new byte[size];
            in.read(stateBytes);
            SplitT split = serializer.deserialize(stateBytes);
            return new SplitWrapper<>(split);
        }
    }
}
