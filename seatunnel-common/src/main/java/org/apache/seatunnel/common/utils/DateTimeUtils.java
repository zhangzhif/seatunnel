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

package org.apache.seatunnel.common.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class DateTimeUtils {

    private static final Map<Formatter, DateTimeFormatter> FORMATTER_MAP =
            new HashMap<Formatter, DateTimeFormatter>();

    static {
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SPOT,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SPOT.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SLASH,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SLASH.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_ISO8601.value));
    }

    public static LocalDateTime parse(String dateTime, Formatter formatter) {
        return LocalDateTime.parse(dateTime, FORMATTER_MAP.get(formatter));
    }

    public static LocalDateTime parse(long timestamp) {
        return parse(timestamp, ZoneId.systemDefault());
    }

    public static LocalDateTime parse(long timestamp, ZoneId zoneId) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return LocalDateTime.ofInstant(instant, zoneId);
    }

    public static String toString(LocalDateTime dateTime, Formatter formatter) {
        return dateTime.format(FORMATTER_MAP.get(formatter));
    }

    public static String toString(long timestamp, Formatter formatter) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return toString(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()), formatter);
    }

    public enum Formatter {
        YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SPOT("yyyy.MM.dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SLASH("yyyy/MM/dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_NO_SPLIT("yyyyMMddHHmmss"),
        YYYY_MM_DD_HH_MM_SS_ISO8601("yyyy-MM-dd'T'HH:mm:ss");

        private final String value;

        Formatter(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Formatter parse(String format) {
            Formatter[] formatters = Formatter.values();
            for (Formatter formatter : formatters) {
                if (formatter.getValue().equals(format)) {
                    return formatter;
                }
            }
            String errorMsg = String.format("Illegal format [%s]", format);
            throw new IllegalArgumentException(errorMsg);
        }
    }
}
