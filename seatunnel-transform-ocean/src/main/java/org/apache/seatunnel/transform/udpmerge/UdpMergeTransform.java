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

package org.apache.seatunnel.transform.udpmerge;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor
@AutoService(SeaTunnelTransform.class)
public class UdpMergeTransform extends AbstractCatalogSupportTransform {
    public static final String PLUGIN_NAME = "UdpMerge";
    private String mainStream;
    private Snowflake snowflake = IdUtil.getSnowflake(1, 1);
    private static Map<String, String> typeMap = new HashMap<>();
    private static Map<String, String> remarkMap = new HashMap<>();

    public UdpMergeTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        mainStream = config.get(UdpMergeTransformConfig.MAIN_STREAMING);
    }

    @Override
    public void close() {}

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig))
                .validate(new UdpMergeTransformFactory().optionRule());
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        String[] fields = new String[] {"data"};
        SeaTunnelDataType<?>[] seaTunnelDataTypes =
                new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE};
        return new SeaTunnelRowType(fields, seaTunnelDataTypes);
    }

    /**
     * 核心处理逻辑：雷达数据来一条，获取目前类型，能拿到就拿，拿不到直接丢kafka。
     *
     * @param inputRow upstream input row data
     * @return
     */
    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        try {
            String sourceType = String.valueOf(inputRow.getField(0));
            JSONObject dataObj = JSONObject.parseObject(String.valueOf(inputRow.getField(1)));
            if (mainStream.equals(sourceType)) {
                return processElement(dataObj);
            } else {
                flushCache(dataObj);
            }
        } catch (Exception e) {
            log.error("雷达类型融合失败，原因：", e);
        }
        return null;
    }

    /**
     * 主流补充remark_id
     *
     * @param dataObj
     * @return
     */
    private SeaTunnelRow processElement(JSONObject dataObj) {
        // 当前雷达的批次号
        String batchNo = dataObj.getString("batch_no");
        String radarId = dataObj.getString("radar_id");
        String key = radarId + "_" + batchNo;
        // 类型补充
        String type = typeMap.get(key);
        if (StrUtil.isNotEmpty(type)) {
            dataObj.put("type", type);
        }
        // 获取当前批次的标识Id
        String remarkId = remarkMap.get(key);
        if (StrUtil.isNotEmpty(remarkId)) {
            dataObj.put("remark_id", remarkId);
            if ("3".equals(dataObj.getString("status"))) {
                // 清空状态，说明航迹丢失，不用在后续同批号插入相同的remark
                remarkMap.remove(key);
            }
        } else {
            // 如果状态为空，生成唯一标识赋值给雷达实体，并且状态更新
            String remarkValue = String.valueOf(snowflake.nextId());
            dataObj.put("remark_id", remarkValue);
            // 更新状态
            remarkMap.put(key, remarkValue);
        }
        Object[] rsValue = new Object[] {dataObj};
        return new SeaTunnelRow(rsValue);
    }

    /**
     * 识别报文类型写入redis
     *
     * @param dataObj
     */
    private void flushCache(JSONObject dataObj) {
        try {
            String key = dataObj.getString("radar_id") + "_" + dataObj.getString("batch_no");
            String value = dataObj.getString("type");
            typeMap.put(key, value);
        } catch (Exception e) {
            log.error("识别类型存储失败，原因：", e);
        }
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<Column> outputColumns = new ArrayList<>();
        outputColumns.add(inputCatalogTable.getTableSchema().getColumns().get(1).copy());
        List<ConstraintKey> copyConstraintKeys =
                inputCatalogTable.getTableSchema().getConstraintKeys().stream()
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());
        PrimaryKey copiedPrimaryKey =
                inputCatalogTable.getTableSchema().getPrimaryKey() == null
                        ? null
                        : inputCatalogTable.getTableSchema().getPrimaryKey().copy();
        return TableSchema.builder()
                .columns(outputColumns)
                .primaryKey(copiedPrimaryKey)
                .constraintKey(copyConstraintKeys)
                .build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }
}
