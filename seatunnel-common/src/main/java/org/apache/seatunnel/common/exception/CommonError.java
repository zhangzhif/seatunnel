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

package org.apache.seatunnel.common.exception;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.common.constants.PluginType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_CONNECTOR_TYPE_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.FILE_NOT_EXISTED;
import static org.apache.seatunnel.common.exception.CommonErrorCode.FILE_OPERATION_FAILED;
import static org.apache.seatunnel.common.exception.CommonErrorCode.GET_CATALOG_TABLES_WITH_UNSUPPORTED_TYPE_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.JSON_OPERATION_FAILED;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_DATA_TYPE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.WRITE_SEATUNNEL_ROW_ERROR;

/**
 * The common error of SeaTunnel. This is an alternative to {@link CommonErrorCodeDeprecated} and is
 * used to define non-bug errors or expected errors for all connectors and engines. We need to
 * define a corresponding enumeration type in {@link CommonErrorCode} to determine the output error
 * message format and content. Then define the corresponding method in {@link CommonError} to
 * construct the corresponding error instance.
 */
public class CommonError {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static SeaTunnelRuntimeException fileOperationFailed(
            String identifier, String operation, String fileName, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("operation", operation);
        params.put("fileName", fileName);
        return new SeaTunnelRuntimeException(FILE_OPERATION_FAILED, params, cause);
    }

    public static SeaTunnelRuntimeException fileOperationFailed(
            String identifier, String operation, String fileName) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("operation", operation);
        params.put("fileName", fileName);
        return new SeaTunnelRuntimeException(FILE_OPERATION_FAILED, params);
    }

    public static SeaTunnelRuntimeException fileNotExistFailed(
            String identifier, String operation, String fileName) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("operation", operation);
        params.put("fileName", fileName);
        return new SeaTunnelRuntimeException(FILE_NOT_EXISTED, params);
    }

    public static SeaTunnelRuntimeException writeSeaTunnelRowFailed(
            String connector, String row, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("seaTunnelRow", row);
        return new SeaTunnelRuntimeException(WRITE_SEATUNNEL_ROW_ERROR, params, cause);
    }

    public static SeaTunnelRuntimeException unsupportedDataType(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(UNSUPPORTED_DATA_TYPE, params);
    }

    public static SeaTunnelRuntimeException convertToSeaTunnelTypeError(
            String connector, PluginType pluginType, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("type", pluginType.getType());
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_SEATUNNEL_TYPE_ERROR, params);
    }

    public static SeaTunnelRuntimeException convertToSeaTunnelTypeError(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE, params);
    }

    public static SeaTunnelRuntimeException convertToConnectorTypeError(
            String connector, PluginType pluginType, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("type", pluginType.getType());
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_CONNECTOR_TYPE_ERROR, params);
    }

    public static SeaTunnelRuntimeException convertToConnectorTypeError(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE, params);
    }

    public static SeaTunnelRuntimeException getCatalogTableWithUnsupportedType(
            String catalogName, String tableName, Map<String, String> fieldWithDataTypes) {
        Map<String, String> params = new HashMap<>();
        params.put("catalogName", catalogName);
        params.put("tableName", tableName);
        try {
            params.put("fieldWithDataTypes", OBJECT_MAPPER.writeValueAsString(fieldWithDataTypes));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new SeaTunnelRuntimeException(GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR, params);
    }

    public static SeaTunnelRuntimeException getCatalogTablesWithUnsupportedType(
            String catalogName, Map<String, Map<String, String>> tableUnsupportedTypes) {
        Map<String, String> params = new HashMap<>();
        params.put("catalogName", catalogName);
        try {
            params.put(
                    "tableUnsupportedTypes",
                    OBJECT_MAPPER.writeValueAsString(tableUnsupportedTypes));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new SeaTunnelRuntimeException(
                GET_CATALOG_TABLES_WITH_UNSUPPORTED_TYPE_ERROR, params);
    }

    public static SeaTunnelRuntimeException jsonOperationError(String identifier, String payload) {
        return jsonOperationError(identifier, payload, null);
    }

    public static SeaTunnelRuntimeException jsonOperationError(
            String identifier, String payload, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("payload", payload);
        SeaTunnelErrorCode code = JSON_OPERATION_FAILED;

        if (cause != null) {
            return new SeaTunnelRuntimeException(code, params, cause);
        } else {
            return new SeaTunnelRuntimeException(code, params);
        }
    }
}
