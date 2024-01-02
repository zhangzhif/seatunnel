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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.udp.config.UdpSourceType;
import org.apache.seatunnel.udp.exception.UdpConnectorErrorCode;
import org.apache.seatunnel.udp.exception.UdpConnectorException;
import org.apache.seatunnel.udp.util.ByteConvertUtil;
import org.apache.seatunnel.udp.util.RadarFormatUtil;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class UdpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> implements Runnable {
    private final UdpSourceParameter parameter;
    private DatagramSocket datagramSocket;
    private Collector<SeaTunnelRow> output;
    private Thread pollThread;

    UdpSourceReader(UdpSourceParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void open() throws Exception {
        datagramSocket = new DatagramSocket(this.parameter.getPort());
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
        // use async thread to poll data,because receive method is block
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
        if (UdpSourceType.Radar.getType().equals(this.parameter.getType())) {
            radarAccept();
        }
        if (UdpSourceType.TargetType.getType().equals(this.parameter.getType())) {
            targetTypeAccept();
        }
    }

    /** 雷达UDP接收 */
    private void radarAccept() {
        byte[] dataBuffer = new byte[1024];
        while (true) {
            DatagramPacket datagramPacket = new DatagramPacket(dataBuffer, 0, dataBuffer.length);
            try {
                datagramSocket.receive(datagramPacket);
            } catch (IOException e) {
                throw new UdpConnectorException(
                        UdpConnectorErrorCode.UDP_DATA_RECEVIER_FAILED, e.getMessage());
            }
            try {
                byte[] byteData = datagramPacket.getData();
                int length = datagramPacket.getLength();
                String data = ByteConvertUtil.bytesToHexString(byteData, length);
                JSONObject dataObj =
                        RadarFormatUtil.formatRadarData(
                                this.parameter.getRadarSource(),
                                data,
                                this.parameter.getHeight(),
                                this.parameter.getJwd());
                // 封装统一数据：type、data
                List<String> rows = Arrays.asList(this.parameter.getType(), dataObj.toJSONString());
                output.collect(new SeaTunnelRow(rows.toArray()));
            } catch (Exception e) {
                log.error("雷达报文数据格式化错误，原因：", e);
            }
        }
    }

    /** 识别报文类型UDP接收 */
    private void targetTypeAccept() {
        byte[] dataBuffer = new byte[1024];
        while (true) {
            DatagramPacket datagramPacket = new DatagramPacket(dataBuffer, 0, dataBuffer.length);
            try {
                datagramSocket.receive(datagramPacket);
            } catch (IOException e) {
                throw new UdpConnectorException(
                        UdpConnectorErrorCode.UDP_DATA_RECEVIER_FAILED, e.getMessage());
            }
            try {
                byte[] byteData = datagramPacket.getData();
                int length = datagramPacket.getLength();
                String data = new String(byteData, 0, length);
                JSONObject dataObj =
                        RadarFormatUtil.formatTypeData(
                                data,
                                this.parameter.getDelimiter(),
                                this.parameter.getRadarSource());
                // 封装统一数据：type、data
                List<String> rows = Arrays.asList(this.parameter.getType(), dataObj.toJSONString());
                output.collect(new SeaTunnelRow(rows.toArray()));
            } catch (Exception e) {
                log.error("目标类型报文数据格式化错误，原因：", e);
            }
        }
    }
}
