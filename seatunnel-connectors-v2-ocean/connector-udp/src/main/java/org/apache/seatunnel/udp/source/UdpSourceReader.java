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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.udp.exception.UdpConnectorErrorCode;
import org.apache.seatunnel.udp.exception.UdpConnectorException;
import org.apache.seatunnel.udp.util.ByteConvertUtil;

import lombok.extern.slf4j.Slf4j;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class UdpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final UdpSourceParameter parameter;
    private final SingleSplitReaderContext context;
    private DatagramSocket datagramSocket;
    private SeaTunnelDataType<SeaTunnelRow> seaTunnelDataType;
    private Map<String, String> fields;

    UdpSourceReader(
            UdpSourceParameter parameter,
            SingleSplitReaderContext context,
            SeaTunnelDataType<SeaTunnelRow> seaTunnelDataType) {
        this.parameter = parameter;
        this.context = context;
        this.seaTunnelDataType = seaTunnelDataType;
    }

    @Override
    public void open() throws Exception {
        datagramSocket = new DatagramSocket(this.parameter.getPort());
        fields = this.parameter.getFields();
        log.info(
                "connect udp server, host:[{}], port:[{}] ",
                InetAddress.getLocalHost(),
                this.parameter.getPort());
    }

    @Override
    public void close() {
        if (datagramSocket != null) {
            datagramSocket.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        while (true) {
            List<Integer> row = new ArrayList<>();
            byte[] dataBuffer = new byte[1024];
            try {
                DatagramPacket datagramPacket =
                        new DatagramPacket(dataBuffer, 0, dataBuffer.length);
                datagramSocket.receive(datagramPacket);
                byte[] byteData = datagramPacket.getData();
                int length = datagramPacket.getLength();
                //            String data = ByteConvertUtil.bytesToHexString(byteData, length);
                String data = new String(byteData, 0, length);
                for (String field : fields.keySet()) {
                    String[] substrIndex = fields.get(field).split("-");
                    String fieldData =
                            data.substring(
                                    Integer.parseInt(substrIndex[0]),
                                    Integer.parseInt(substrIndex[1]));
                    int parseData = ByteConvertUtil.parse(fieldData);
                    row.add(parseData);
                }
                output.collect(new SeaTunnelRow(row.toArray()));
            } catch (Exception e) {
                throw new UdpConnectorException(
                        UdpConnectorErrorCode.UDP_DATA_RECEVIER_FAILED, e.getMessage());
            }
        }
    }
}
