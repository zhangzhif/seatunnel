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

package org.apache.seatunnel.udp.source;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.udp.config.UdpConfigOptions;
import org.apache.seatunnel.udp.exception.UdpConnectorException;

import java.util.Map;

@AutoService(SeaTunnelSource.class)
public class UdpSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private UdpSourceParameter parameter;
    private JobContext jobContext;
    private SeaTunnelDataType<SeaTunnelRow> seaTunnelDataType;


    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return "UDP";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, UdpConfigOptions.PORT.key(), UdpConfigOptions.FIELDS.key(), UdpConfigOptions.TYPE.key());
        if (!result.isSuccess()) {
            throw new UdpConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        this.parameter = new UdpSourceParameter(pluginConfig);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        Map<String, String> fieldsMap = parameter.getFields();
        int fieldCount = fieldsMap.keySet().size();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            seaTunnelDataTypes[i] = BasicType.STRING_TYPE;
        }
        this.seaTunnelDataType =
                new SeaTunnelRowType(
                        fieldsMap.keySet().toArray(new String[]{}), seaTunnelDataTypes);
        return this.seaTunnelDataType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) {
        return new UdpSourceReader(this.parameter);
    }
}
