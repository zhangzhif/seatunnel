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

package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_FIXED_AS_INT96;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;

@Slf4j
public class HadoopFileSystemProxy implements Serializable, Closeable {

    private transient UserGroupInformation userGroupInformation;

    private transient Configuration configuration;

    private transient FileSystem fileSystem;

    private final HadoopConf hadoopConf;

    public HadoopFileSystemProxy(@NonNull HadoopConf hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    public boolean fileExist(@NonNull String filePath) throws IOException {
        if (fileSystem == null) {
            initialize();
        }
        Path fileName = new Path(filePath);
        return fileSystem.exists(fileName);
    }

    public void createFile(@NonNull String filePath) throws IOException {
        if (fileSystem == null) {
            initialize();
        }
        Path path = new Path(filePath);
        if (!fileSystem.createNewFile(path)) {
            throw CommonError.fileOperationFailed("SeaTunnel", "create", filePath);
        }
    }

    public void deleteFile(@NonNull String filePath) throws IOException {
        if (fileSystem == null) {
            initialize();
        }
        Path path = new Path(filePath);
        if (fileSystem.exists(path)) {
            if (!fileSystem.delete(path, true)) {
                throw CommonError.fileOperationFailed("SeaTunnel", "delete", filePath);
            }
        }
    }

    public void renameFile(
            @NonNull String oldFilePath,
            @NonNull String newFilePath,
            boolean removeWhenNewFilePathExist)
            throws IOException {
        if (fileSystem == null) {
            initialize();
        }
        Path oldPath = new Path(oldFilePath);
        Path newPath = new Path(newFilePath);

        if (!fileExist(oldPath.toString())) {
            log.warn(
                    "rename file :["
                            + oldPath
                            + "] to ["
                            + newPath
                            + "] already finished in the last commit, skip");
            return;
        }

        if (removeWhenNewFilePathExist) {
            if (fileExist(newFilePath)) {
                fileSystem.delete(newPath, true);
                log.info("Delete already file: {}", newPath);
            }
        }
        if (!fileExist(newPath.getParent().toString())) {
            createDir(newPath.getParent().toString());
        }

        if (fileSystem.rename(oldPath, newPath)) {
            log.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");
        } else {
            throw CommonError.fileOperationFailed(
                    "SeaTunnel", "rename", oldFilePath + " -> " + newFilePath);
        }
    }

    public void createDir(@NonNull String filePath) throws IOException {
        if (fileSystem == null) {
            initialize();
        }
        Path dfs = new Path(filePath);
        if (!fileSystem.mkdirs(dfs)) {
            throw CommonError.fileOperationFailed("SeaTunnel", "create", filePath);
        }
    }

    public List<Path> getAllSubFiles(@NonNull String filePath) throws IOException {
        if (fileSystem == null) {
            initialize();
        }
        List<Path> pathList = new ArrayList<>();
        if (!fileExist(filePath)) {
            return pathList;
        }
        Path fileName = new Path(filePath);
        FileStatus[] status = fileSystem.listStatus(fileName);
        if (status != null) {
            for (FileStatus fileStatus : status) {
                if (fileStatus.isDirectory()) {
                    pathList.add(fileStatus.getPath());
                }
            }
        }
        return pathList;
    }

    public FileStatus[] listStatus(String filePath) throws IOException {
        if (fileSystem == null) {
            initialize();
        }
        return fileSystem.listStatus(new Path(filePath));
    }

    public FileStatus getFileStatus(String filePath) throws IOException {
        if (fileSystem == null) {
            initialize();
        }
        return fileSystem.getFileStatus(new Path(filePath));
    }

    public FSDataOutputStream getOutputStream(String filePath) throws IOException {
        if (fileSystem == null) {
            initialize();
        }
        return fileSystem.create(new Path(filePath), true);
    }

    public FSDataInputStream getInputStream(String filePath) throws IOException {
        if (fileSystem == null) {
            initialize();
        }
        return fileSystem.open(new Path(filePath));
    }

    @SneakyThrows
    public <T> T doWithHadoopAuth(HadoopLoginFactory.LoginFunction<T> loginFunction) {
        if (configuration == null) {
            this.configuration = createConfiguration();
        }
        if (enableKerberos()) {
            configuration.set("hadoop.security.authentication", "kerberos");
            return HadoopLoginFactory.loginWithKerberos(
                    configuration,
                    hadoopConf.getKrb5Path(),
                    hadoopConf.getKerberosPrincipal(),
                    hadoopConf.getKerberosKeytabPath(),
                    loginFunction);
        }
        if (enableRemoteUser()) {
            return HadoopLoginFactory.loginWithRemoteUser(
                    configuration, hadoopConf.getRemoteUser(), loginFunction);
        }
        return loginFunction.run(configuration, UserGroupInformation.getCurrentUser());
    }

    @Override
    public void close() throws IOException {
        try (FileSystem closedFileSystem = fileSystem) {
            if (userGroupInformation != null && enableKerberos()) {
                userGroupInformation.logoutUserFromKeytab();
            }
        }
    }

    @SneakyThrows
    private void initialize() {
        this.configuration = createConfiguration();
        if (enableKerberos()) {
            configuration.set("hadoop.security.authentication", "kerberos");
            initializeWithKerberosLogin();
            return;
        }
        if (enableRemoteUser()) {
            initializeWithRemoteUserLogin();
            return;
        }
        this.fileSystem = FileSystem.get(configuration);
        fileSystem.setWriteChecksum(false);
    }

    private Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(READ_INT96_AS_FIXED, true);
        configuration.setBoolean(WRITE_FIXED_AS_INT96, true);
        configuration.setBoolean(ADD_LIST_ELEMENT_RECORDS, false);
        configuration.setBoolean(WRITE_OLD_LIST_STRUCTURE, true);
        configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hadoopConf.getHdfsNameKey());
        configuration.setBoolean(
                String.format("fs.%s.impl.disable.cache", hadoopConf.getSchema()), true);
        configuration.set(
                String.format("fs.%s.impl", hadoopConf.getSchema()), hadoopConf.getFsHdfsImpl());
        hadoopConf.setExtraOptionsForConfiguration(configuration);
        return configuration;
    }

    private boolean enableKerberos() {
        boolean kerberosPrincipalEmpty = StringUtils.isEmpty(hadoopConf.getKerberosPrincipal());
        boolean kerberosKeytabPathEmpty = StringUtils.isEmpty(hadoopConf.getKerberosKeytabPath());
        boolean krb5FilePathEmpty = StringUtils.isEmpty(hadoopConf.getKrb5Path());
        if (kerberosKeytabPathEmpty && kerberosPrincipalEmpty && krb5FilePathEmpty) {
            return false;
        }
        if (!kerberosPrincipalEmpty && !kerberosKeytabPathEmpty && !krb5FilePathEmpty) {
            return true;
        }
        if (kerberosPrincipalEmpty) {
            throw new IllegalArgumentException("Please set kerberosPrincipal");
        }
        if (kerberosKeytabPathEmpty) {
            throw new IllegalArgumentException("Please set kerberosKeytabPath");
        }
        throw new IllegalArgumentException("Please set krb5FilePath");
    }

    private void initializeWithKerberosLogin() throws IOException, InterruptedException {
        HadoopLoginFactory.loginWithKerberos(
                configuration,
                hadoopConf.getKrb5Path(),
                hadoopConf.getKerberosPrincipal(),
                hadoopConf.getKerberosKeytabPath(),
                (configuration, userGroupInformation) -> {
                    this.userGroupInformation = userGroupInformation;
                    this.fileSystem = FileSystem.get(configuration);
                    return null;
                });
        // todo: Use a daemon thread to reloginFromTicketCache
    }

    private boolean enableRemoteUser() {
        return StringUtils.isNotEmpty(hadoopConf.getRemoteUser());
    }

    private void initializeWithRemoteUserLogin() throws Exception {
        final Pair<UserGroupInformation, FileSystem> pair =
                HadoopLoginFactory.loginWithRemoteUser(
                        configuration,
                        hadoopConf.getRemoteUser(),
                        (configuration, userGroupInformation) -> {
                            final FileSystem fileSystem = FileSystem.get(configuration);
                            return Pair.of(userGroupInformation, fileSystem);
                        });
        this.userGroupInformation = pair.getKey();
        this.fileSystem = pair.getValue();
    }
}
