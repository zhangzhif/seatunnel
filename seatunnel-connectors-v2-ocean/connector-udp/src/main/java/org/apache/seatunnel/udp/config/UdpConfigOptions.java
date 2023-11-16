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

package org.apache.seatunnel.udp.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

public class UdpConfigOptions {

    public static final Option<Integer> PORT =
            Options.key("port").intType().noDefaultValue().withDescription("udp port");

    public static final Option<Map<String, String>> FIELDS =
            Options.key("fields").mapType().noDefaultValue().withDescription("udp source fields");

    public static final Option<String> TYPE =
            Options.key("type").stringType().noDefaultValue().withDescription("udp type");

    public static final Option<String> RADAR_SOURCE =
            Options.key("radar_source").stringType().noDefaultValue().withDescription("udp radar source");

    public static final Option<String> DELIMITER =
            Options.key("delimiter").stringType().noDefaultValue().withDescription("udp delimiter");
}
