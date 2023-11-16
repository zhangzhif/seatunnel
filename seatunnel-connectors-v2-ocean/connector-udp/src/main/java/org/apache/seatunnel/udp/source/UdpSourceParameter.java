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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.udp.config.UdpConfigOptions;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class UdpSourceParameter implements Serializable {
    private final Integer port;
    private Map<String, String> fields;
    private String type;
    private String radarSource;
    private String delimiter;

    public Integer getPort() {
        return Objects.isNull(port) ? UdpConfigOptions.PORT.defaultValue() : port;
    }

    public Map<String, String> getFields() {
        return Objects.isNull(fields) ? UdpConfigOptions.FIELDS.defaultValue() : fields;
    }

    public String getType() {
        return Objects.isNull(type) ? UdpConfigOptions.TYPE.defaultValue() : type;
    }

    public String getRadarSource() {
        return Objects.isNull(radarSource) ? UdpConfigOptions.RADAR_SOURCE.defaultValue() : radarSource;
    }

    public String getDelimiter() {
        return Objects.isNull(delimiter) ? UdpConfigOptions.DELIMITER.defaultValue() : delimiter;
    }

    public UdpSourceParameter(Config config) {
        if (config.hasPath(UdpConfigOptions.PORT.key())) {
            this.port = config.getInt(UdpConfigOptions.PORT.key());
        } else {
            this.port = UdpConfigOptions.PORT.defaultValue();
        }
        if (config.hasPath(UdpConfigOptions.FIELDS.key())) {
            this.fields = (Map<String, String>) config.getAnyRef(UdpConfigOptions.FIELDS.key());
        } else {
            this.fields = UdpConfigOptions.FIELDS.defaultValue();
        }
        if (config.hasPath(UdpConfigOptions.TYPE.key())) {
            this.type = config.getString(UdpConfigOptions.TYPE.key());
        } else {
            this.type = UdpConfigOptions.TYPE.defaultValue();
        }
        if (config.hasPath(UdpConfigOptions.RADAR_SOURCE.key())) {
            this.radarSource = config.getString(UdpConfigOptions.RADAR_SOURCE.key());
        } else {
            this.radarSource = UdpConfigOptions.RADAR_SOURCE.defaultValue();
        }
        if (config.hasPath(UdpConfigOptions.DELIMITER.key())) {
            this.delimiter = config.getString(UdpConfigOptions.DELIMITER.key());
        } else {
            this.delimiter = UdpConfigOptions.DELIMITER.defaultValue();
        }
    }
}
