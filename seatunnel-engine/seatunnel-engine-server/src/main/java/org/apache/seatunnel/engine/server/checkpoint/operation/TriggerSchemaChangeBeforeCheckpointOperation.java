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

package org.apache.seatunnel.engine.server.checkpoint.operation;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.CheckpointDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class TriggerSchemaChangeBeforeCheckpointOperation extends Operation
        implements IdentifiedDataSerializable {

    private TaskLocation taskLocation;

    @Override
    public int getFactoryId() {
        return CheckpointDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CheckpointDataSerializerHook.TRIGGER_SCHEMA_CHANGE_BEFORE_CHECKPOINT_OPERATOR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(taskLocation);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        taskLocation = in.readObject();
    }

    @Override
    public void run() {
        log.debug("call TriggerSchemaChangeBeforeCheckpointOperation {}", taskLocation);
        ((SeaTunnelServer) getService())
                .getCoordinatorService()
                .getJobMaster(taskLocation.getJobId())
                .getCheckpointManager()
                .triggerSchemaChangeBeforeCheckpoint(this);
        log.debug("call SchemaChangeBeforeCheckpoint finished {}", taskLocation);
    }
}
