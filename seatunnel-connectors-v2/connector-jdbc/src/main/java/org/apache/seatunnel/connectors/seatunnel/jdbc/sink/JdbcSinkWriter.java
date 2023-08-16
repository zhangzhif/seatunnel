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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class JdbcSinkWriter implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> {

    private final JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>>
            outputFormat;
    private final SinkWriter.Context context;
    private final JdbcConnectionProvider connectionProvider;
    private transient boolean isOpen;
    private final JdbcSinkConfig jdbcSinkConfig;

    public JdbcSinkWriter(
            SinkWriter.Context context,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            SeaTunnelRowType rowType) {
        this.context = context;
        this.connectionProvider =
                new SimpleJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        this.outputFormat =
                new JdbcOutputFormatBuilder(dialect, connectionProvider, jdbcSinkConfig, rowType)
                        .build();
        this.jdbcSinkConfig = jdbcSinkConfig;
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            outputFormat.open();
        }
    }

    @Override
    public void prepared() throws IOException {
        if (CollectionUtils.isNotEmpty(jdbcSinkConfig.getPreSQL())) {
            Connection conn = null;
            try {
                tryOpen();
                conn = connectionProvider.getOrEstablishConnection();
                conn.setAutoCommit(false);
                for (String preSQL : jdbcSinkConfig.getPreSQL()) {
                    if (StringUtils.isNotBlank(preSQL)) {
                        PreparedStatement preparedStatement = conn.prepareStatement(preSQL.trim());
                        preparedStatement.execute();
                    }
                }
                conn.commit();
                conn.setAutoCommit(true);
            } catch (SQLException | ClassNotFoundException e) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.SQL_EXECUTION_FAILED, "Execute preSQL failed", e);
            }
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tryOpen();
        SeaTunnelRow copy = SerializationUtils.clone(element);
        outputFormat.writeRecord(copy);
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
        tryOpen();
        outputFormat.checkFlushException();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "commit failed," + e.getMessage(),
                    e);
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        tryOpen();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    CommonErrorCode.WRITER_OPERATION_FAILED, "unable to close JDBC sink write", e);
        }
        if (CollectionUtils.isNotEmpty(jdbcSinkConfig.getPostSQL())) {
            Connection conn = null;
            try {
                conn = connectionProvider.getOrEstablishConnection();
                conn.setAutoCommit(false);
                for (String postSQL : jdbcSinkConfig.getPostSQL()) {
                    if (StringUtils.isNotBlank(postSQL)) {
                        PreparedStatement preparedStatement = conn.prepareStatement(postSQL.trim());
                        preparedStatement.execute();
                    }
                }
                conn.commit();
                conn.setAutoCommit(true);
            } catch (SQLException | ClassNotFoundException e) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.SQL_EXECUTION_FAILED, "Execute preSQL failed", e);
            }
        }
        outputFormat.close();
    }
}
