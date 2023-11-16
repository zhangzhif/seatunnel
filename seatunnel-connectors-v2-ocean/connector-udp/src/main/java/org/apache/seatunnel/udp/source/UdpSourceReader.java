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

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.udp.exception.UdpConnectorErrorCode;
import org.apache.seatunnel.udp.exception.UdpConnectorException;
import org.apache.seatunnel.udp.util.ByteConvertUtil;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class UdpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> implements Runnable {
    private final UdpSourceParameter parameter;
    private DatagramSocket datagramSocket;
    private Map<String, String> fields;
    private Collector<SeaTunnelRow> output;
    private Thread pollThread;

    UdpSourceReader(UdpSourceParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void open() throws Exception {
        datagramSocket = new DatagramSocket(this.parameter.getPort());
        fields = this.parameter.getFields();
        log.info("connect udp server, host:[{}], port:[{}] ", InetAddress.getLocalHost(), this.parameter.getPort());
    }

    @Override
    public void close() {
        if (datagramSocket != null) {
            datagramSocket.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        // use async thread to poll data,because receive method is block,
        // /
        synchronized (this) {
            if (ObjectUtil.isEmpty(pollThread)) {
                this.output = output;
                pollThread = new Thread(this);
                pollThread.start();
            }
        }
    }

    @Override
    public void run() {
        if ("radar".equals(this.parameter.getType())) {
            radarAccept();
        }
        if ("targetType".equals(this.parameter.getType())) {
            targetTypeAccept();
        }
    }

    private void radarAccept() {
        byte[] dataBuffer = new byte[1024];
        while (true) {
            List<Integer> row = new ArrayList<>();
            DatagramPacket datagramPacket = new DatagramPacket(dataBuffer, 0, dataBuffer.length);
            try {
                datagramSocket.receive(datagramPacket);
            } catch (IOException e) {
                throw new UdpConnectorException(UdpConnectorErrorCode.UDP_DATA_RECEVIER_FAILED, e.getMessage());
            }
            byte[] byteData = datagramPacket.getData();
            int length = datagramPacket.getLength();
//            String data = ByteConvertUtil.bytesToHexString(byteData, length);
            String data = new String(byteData, 0, length);
            for (String field : fields.keySet()) {
                if (StrUtil.isNotEmpty(fields.get(field))) {
                    String[] substrIndex = fields.get(field).split("-");
                    String fieldData =
                            data.substring(
                                    Integer.parseInt(substrIndex[0]), Integer.parseInt(substrIndex[1]));
                    int parseData = ByteConvertUtil.parse(fieldData);
                    row.add(parseData);
                }
                //特效字段报文中截取不到-type、longitude、latitude、height、radar_id
                row = processSpecialField(field, row);
            }
            output.collect(new SeaTunnelRow(row.toArray()));
        }
    }

    private void targetTypeAccept() {
        byte[] dataBuffer = new byte[1024];
        while (true) {
            List<Integer> row = new ArrayList<>();
            DatagramPacket datagramPacket = new DatagramPacket(dataBuffer, 0, dataBuffer.length);
            try {
                datagramSocket.receive(datagramPacket);
            } catch (IOException e) {
                throw new UdpConnectorException(UdpConnectorErrorCode.UDP_DATA_RECEVIER_FAILED, e.getMessage());
            }
            byte[] byteData = datagramPacket.getData();
            int length = datagramPacket.getLength();
            String data = new String(byteData, 0, length);
            String[] fieldValues = data.split(this.parameter.getDelimiter());
            for (String field : fields.keySet()) {
                if (StrUtil.isNotEmpty(fields.get(field))) {
                    Integer index = Integer.valueOf(fields.get(field));
                    row.add(Integer.valueOf(fieldValues[index]));
                }
                row.add(null);
            }
            output.collect(new SeaTunnelRow(row.toArray()));
        }
    }

    private List<Integer> processSpecialField(String field, List<Integer> row) {
        if ("type".equals(field)) {
            row.add(4);
        }
        if ("longitude".equals(field)) {
            row.add(null);
        }
        if ("latitude".equals(field)) {
            row.add(null);
        }
        if ("height".equals(field)) {
            row.add(null);
        }
        if ("radar_id".equals(field)) {
            row.add(Integer.valueOf(this.parameter.getRadarSource()));
        }
        return row;
    }
}

