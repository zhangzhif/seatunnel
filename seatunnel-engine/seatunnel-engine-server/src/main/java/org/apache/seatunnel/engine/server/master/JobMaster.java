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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.dag.DAGUtils;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.PlanUtils;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.JobMetricsUtil;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.task.operation.CleanTaskGroupContextOperation;
import org.apache.seatunnel.engine.server.task.operation.GetTaskGroupMetricsOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.google.common.collect.Lists;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static org.apache.seatunnel.common.constants.JobMode.BATCH;

public class JobMaster {
    private static final ILogger LOGGER = Logger.getLogger(JobMaster.class);

    private PhysicalPlan physicalPlan;
    private final Data jobImmutableInformationData;

    private final NodeEngine nodeEngine;

    private final ExecutorService executorService;

    private final FlakeIdGenerator flakeIdGenerator;

    private final ResourceManager resourceManager;

    private final JobHistoryService jobHistoryService;

    private CheckpointManager checkpointManager;

    private CompletableFuture<JobResult> jobMasterCompleteFuture;

    private ClassLoader classLoader;

    private JobImmutableInformation jobImmutableInformation;

    private LogicalDag logicalDag;

    private JobDAGInfo jobDAGInfo;

    private SeaTunnelServer seaTunnelServer;

    /**
     * we need store slot used by task in Hazelcast IMap and release or reuse it when a new master
     * node active.
     */
    private final IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap;

    private final IMap<Object, Object> runningJobStateIMap;

    private final IMap<Object, Object> runningJobStateTimestampsIMap;

    private CompletableFuture<Void> scheduleFuture;

    // TODO add config to change value
    private boolean isPhysicalDAGIInfo = true;

    private final EngineConfig engineConfig;

    private boolean isRunning = true;

    private Map<Integer, CheckpointPlan> checkpointPlanMap;

    private final IMap<Long, JobInfo> runningJobInfoIMap;

    private final IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap;

    /** If the job or pipeline cancel by user, needRestore will be false */
    @Getter private volatile boolean needRestore = true;

    private CheckpointConfig jobCheckpointConfig;

    public String getErrorMessage() {
        return errorMessage;
    }

    private String errorMessage;

    public JobMaster(
            @NonNull Data jobImmutableInformationData,
            @NonNull NodeEngine nodeEngine,
            @NonNull ExecutorService executorService,
            @NonNull ResourceManager resourceManager,
            @NonNull JobHistoryService jobHistoryService,
            @NonNull IMap runningJobStateIMap,
            @NonNull IMap runningJobStateTimestampsIMap,
            @NonNull IMap ownedSlotProfilesIMap,
            @NonNull IMap<Long, JobInfo> runningJobInfoIMap,
            @NonNull IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap,
            EngineConfig engineConfig,
            SeaTunnelServer seaTunnelServer) {
        this.jobImmutableInformationData = jobImmutableInformationData;
        this.nodeEngine = nodeEngine;
        this.executorService = executorService;
        flakeIdGenerator =
                this.nodeEngine
                        .getHazelcastInstance()
                        .getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME);
        this.ownedSlotProfilesIMap = ownedSlotProfilesIMap;
        this.resourceManager = resourceManager;
        this.jobHistoryService = jobHistoryService;
        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
        this.runningJobInfoIMap = runningJobInfoIMap;
        this.engineConfig = engineConfig;
        this.metricsImap = metricsImap;
        this.seaTunnelServer = seaTunnelServer;
    }

    public void init(long initializationTimestamp, boolean restart) throws Exception {
        jobImmutableInformation =
                nodeEngine.getSerializationService().toObject(jobImmutableInformationData);
        jobCheckpointConfig =
                createJobCheckpointConfig(
                        engineConfig.getCheckpointConfig(), jobImmutableInformation.getJobConfig());

        LOGGER.info(
                String.format(
                        "Init JobMaster for Job %s (%s) ",
                        jobImmutableInformation.getJobConfig().getName(),
                        jobImmutableInformation.getJobId()));
        LOGGER.info(
                String.format(
                        "Job %s (%s) needed jar urls %s",
                        jobImmutableInformation.getJobConfig().getName(),
                        jobImmutableInformation.getJobId(),
                        jobImmutableInformation.getPluginJarsUrls()));

        classLoader =
                new SeaTunnelChildFirstClassLoader(jobImmutableInformation.getPluginJarsUrls());
        logicalDag =
                CustomClassLoadedObject.deserializeWithCustomClassLoader(
                        nodeEngine.getSerializationService(),
                        classLoader,
                        jobImmutableInformation.getLogicalDag());

        final Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> planTuple =
                PlanUtils.fromLogicalDAG(
                        logicalDag,
                        nodeEngine,
                        jobImmutableInformation,
                        initializationTimestamp,
                        executorService,
                        flakeIdGenerator,
                        runningJobStateIMap,
                        runningJobStateTimestampsIMap,
                        engineConfig.getQueueType(),
                        engineConfig);
        this.physicalPlan = planTuple.f0();
        this.physicalPlan.setJobMaster(this);
        this.checkpointPlanMap = planTuple.f1();
        Exception initException = null;
        try {
            this.initCheckPointManager(restart);
        } catch (Exception e) {
            initException = e;
        }
        this.initStateFuture();
        if (initException != null) {
            if (restart) {
                cancelJob();
            }
            throw initException;
        }
    }

    public void initCheckPointManager(boolean restart) throws CheckpointStorageException {
        this.checkpointManager =
                new CheckpointManager(
                        jobImmutableInformation.getJobId(),
                        jobImmutableInformation.isStartWithSavePoint() || restart,
                        nodeEngine,
                        this,
                        checkpointPlanMap,
                        jobCheckpointConfig,
                        executorService,
                        runningJobStateIMap);
    }

    // TODO replace it after ReadableConfig Support parse yaml format, then use only one config to
    // read engine and env config.
    private CheckpointConfig createJobCheckpointConfig(
            CheckpointConfig defaultCheckpointConfig, JobConfig jobConfig) {
        Map<String, Object> jobEnv = jobConfig.getEnvOptions();
        CheckpointConfig jobCheckpointConfig = new CheckpointConfig();
        jobCheckpointConfig.setCheckpointTimeout(defaultCheckpointConfig.getCheckpointTimeout());
        jobCheckpointConfig.setCheckpointInterval(defaultCheckpointConfig.getCheckpointInterval());

        CheckpointStorageConfig jobCheckpointStorageConfig = new CheckpointStorageConfig();
        jobCheckpointStorageConfig.setStorage(defaultCheckpointConfig.getStorage().getStorage());
        jobCheckpointStorageConfig.setStoragePluginConfig(
                defaultCheckpointConfig.getStorage().getStoragePluginConfig());
        jobCheckpointStorageConfig.setMaxRetainedCheckpoints(
                defaultCheckpointConfig.getStorage().getMaxRetainedCheckpoints());
        jobCheckpointConfig.setStorage(jobCheckpointStorageConfig);

        if (jobEnv.containsKey(EnvCommonOptions.CHECKPOINT_INTERVAL.key())) {
            jobCheckpointConfig.setCheckpointInterval(
                    Long.parseLong(
                            jobEnv.get(EnvCommonOptions.CHECKPOINT_INTERVAL.key()).toString()));
        } else if (jobConfig.getJobContext().getJobMode() == BATCH) {
            LOGGER.info(
                    "in batch mode, the 'checkpoint.interval' configuration of env is missing, so checkpoint will be disabled");
            jobCheckpointConfig.setCheckpointEnable(false);
        }
        if (jobEnv.containsKey(EnvCommonOptions.CHECKPOINT_TIMEOUT.key())) {
            jobCheckpointConfig.setCheckpointTimeout(
                    Long.parseLong(
                            jobEnv.get(EnvCommonOptions.CHECKPOINT_TIMEOUT.key()).toString()));
        }
        return jobCheckpointConfig;
    }

    public void initStateFuture() {
        jobMasterCompleteFuture = new CompletableFuture<>();
        PassiveCompletableFuture<JobResult> jobStatusFuture = physicalPlan.initStateFuture();
        jobStatusFuture.whenComplete(
                withTryCatch(
                        LOGGER,
                        (v, t) -> {
                            JobMaster.this.errorMessage = v.getError();
                            JobResult jobResult =
                                    new JobResult(physicalPlan.getJobStatus(), v.getError());
                            cleanJob();
                            jobMasterCompleteFuture.complete(jobResult);
                        }));
    }

    public void run() {
        try {
            physicalPlan.startJob();
        } catch (Throwable e) {
            LOGGER.severe(
                    String.format(
                            "Job %s (%s) run error with: %s",
                            physicalPlan.getJobImmutableInformation().getJobConfig().getName(),
                            physicalPlan.getJobImmutableInformation().getJobId(),
                            ExceptionUtils.getMessage(e)));
        } finally {
            jobMasterCompleteFuture.join();
            if (engineConfig.getConnectorJarStorageConfig().getEnable()) {
                List<ConnectorJarIdentifier> pluginJarIdentifiers =
                        jobImmutableInformation.getPluginJarIdentifiers();
                seaTunnelServer
                        .getConnectorPackageService()
                        .cleanUpWhenJobFinished(
                                jobImmutableInformation.getJobId(), pluginJarIdentifiers);
            }
        }
    }

    public void handleCheckpointError(long pipelineId, boolean neverRestore) {
        if (neverRestore) {
            this.neverNeedRestore();
        }
        this.physicalPlan
                .getPipelineList()
                .forEach(
                        pipeline -> {
                            if (pipeline.getPipelineLocation().getPipelineId() == pipelineId) {
                                pipeline.handleCheckpointError();
                            }
                        });
    }

    private void removeJobIMap() {
        Long jobId = getJobImmutableInformation().getJobId();
        runningJobStateTimestampsIMap.remove(jobId);

        getPhysicalPlan()
                .getPipelineList()
                .forEach(
                        pipeline -> {
                            runningJobStateIMap.remove(pipeline.getPipelineLocation());
                            runningJobStateTimestampsIMap.remove(pipeline.getPipelineLocation());
                            pipeline.getCoordinatorVertexList()
                                    .forEach(
                                            coordinator -> {
                                                runningJobStateIMap.remove(
                                                        coordinator.getTaskGroupLocation());
                                                runningJobStateTimestampsIMap.remove(
                                                        coordinator.getTaskGroupLocation());
                                            });

                            pipeline.getPhysicalVertexList()
                                    .forEach(
                                            task -> {
                                                runningJobStateIMap.remove(
                                                        task.getTaskGroupLocation());
                                                runningJobStateTimestampsIMap.remove(
                                                        task.getTaskGroupLocation());
                                            });
                        });

        runningJobStateIMap.remove(jobId);
        runningJobInfoIMap.remove(jobId);
    }

    public JobDAGInfo getJobDAGInfo() {
        if (jobDAGInfo == null) {
            jobDAGInfo =
                    DAGUtils.getJobDAGInfo(
                            logicalDag, jobImmutableInformation, engineConfig, isPhysicalDAGIInfo);
        }
        return jobDAGInfo;
    }

    public void releasePipelineResource(SubPlan subPlan) {
        try {
            Map<TaskGroupLocation, SlotProfile> taskGroupLocationSlotProfileMap =
                    ownedSlotProfilesIMap.get(subPlan.getPipelineLocation());
            if (taskGroupLocationSlotProfileMap == null) {
                return;
            }

            RetryUtils.retryWithException(
                    () -> {
                        LOGGER.info(
                                String.format(
                                        "release the pipeline %s resource",
                                        subPlan.getPipelineFullName()));
                        resourceManager
                                .releaseResources(
                                        jobImmutableInformation.getJobId(),
                                        Lists.newArrayList(
                                                taskGroupLocationSlotProfileMap.values()))
                                .join();
                        ownedSlotProfilesIMap.remove(subPlan.getPipelineLocation());
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                            Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            LOGGER.warning(
                    String.format(
                            "release the pipeline %s resource failed, with exception: %s ",
                            subPlan.getPipelineFullName(), ExceptionUtils.getMessage(e)));
        }
    }

    public void cleanJob() {
        jobHistoryService.storeJobInfo(jobImmutableInformation.getJobId(), getJobDAGInfo());
        jobHistoryService.storeFinishedJobState(this);
        removeJobIMap();
    }

    public Address queryTaskGroupAddress(TaskGroupLocation taskGroupLocation) {

        PipelineLocation pipelineLocation =
                new PipelineLocation(
                        taskGroupLocation.getJobId(), taskGroupLocation.getPipelineId());

        Map<TaskGroupLocation, SlotProfile> taskGroupLocationSlotProfileMap =
                ownedSlotProfilesIMap.get(pipelineLocation);

        if (null != taskGroupLocationSlotProfileMap) {
            SlotProfile slotProfile = taskGroupLocationSlotProfileMap.get(taskGroupLocation);
            if (null != slotProfile) {
                return slotProfile.getWorker();
            }
        }
        throw new IllegalArgumentException(
                "can't find task group address from taskGroupLocation: " + taskGroupLocation);
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public void cancelJob() {
        physicalPlan.cancelJob();
    }

    public ResourceManager getResourceManager() {
        return resourceManager;
    }

    public CheckpointManager getCheckpointManager() {
        return checkpointManager;
    }

    public PassiveCompletableFuture<JobResult> getJobMasterCompleteFuture() {
        return new PassiveCompletableFuture<>(jobMasterCompleteFuture);
    }

    public JobImmutableInformation getJobImmutableInformation() {
        return jobImmutableInformation;
    }

    public JobStatus getJobStatus() {
        return physicalPlan.getJobStatus();
    }

    public List<RawJobMetrics> getCurrJobMetrics() {

        Map<TaskGroupLocation, Address> taskGroupLocationSlotProfileMap = new HashMap<>();

        ownedSlotProfilesIMap.forEach(
                (pipelineLocation, map) -> {
                    if (pipelineLocation.getJobId()
                            == this.getJobImmutableInformation().getJobId()) {
                        map.forEach(
                                (taskGroupLocation, slotProfile) -> {
                                    if (taskGroupLocation.getJobId()
                                            == this.getJobImmutableInformation().getJobId()) {
                                        taskGroupLocationSlotProfileMap.put(
                                                taskGroupLocation, slotProfile.getWorker());
                                    }
                                });
                    }
                });
        return getCurrJobMetrics(taskGroupLocationSlotProfileMap);
    }

    public List<RawJobMetrics> getCurrJobMetrics(List<PipelineLocation> pipelineLocations) {
        Map<TaskGroupLocation, Address> taskGroupLocationSlotProfileMap = new HashMap<>();

        ownedSlotProfilesIMap.forEach(
                (pipelineLocation, map) -> {
                    if (pipelineLocations.contains(pipelineLocation)) {
                        map.forEach(
                                (taskGroupLocation, slotProfile) -> {
                                    if (taskGroupLocation.getJobId()
                                            == this.getJobImmutableInformation().getJobId()) {
                                        taskGroupLocationSlotProfileMap.put(
                                                taskGroupLocation, slotProfile.getWorker());
                                    }
                                });
                    }
                });
        return getCurrJobMetrics(taskGroupLocationSlotProfileMap);
    }

    public List<RawJobMetrics> getCurrJobMetrics(
            Map<TaskGroupLocation, Address> taskGroupLocationSlotProfileMap) {
        Map<Address, List<TaskGroupLocation>> taskGroupLocationMap = new HashMap<>();

        for (Map.Entry<TaskGroupLocation, Address> entry :
                taskGroupLocationSlotProfileMap.entrySet()) {
            taskGroupLocationMap
                    .computeIfAbsent(entry.getValue(), k -> new ArrayList<>())
                    .add(entry.getKey());
        }
        List<RawJobMetrics> metrics = new ArrayList<>();
        taskGroupLocationMap.forEach(
                (address, taskGroupLocations) -> {
                    try {
                        if (nodeEngine.getClusterService().getMember(address) != null) {
                            RawJobMetrics rawJobMetrics =
                                    (RawJobMetrics)
                                            NodeEngineUtil.sendOperationToMemberNode(
                                                            nodeEngine,
                                                            new GetTaskGroupMetricsOperation(
                                                                    taskGroupLocations),
                                                            address)
                                                    .get();
                            metrics.add(rawJobMetrics);
                        }
                    }
                    // HazelcastInstanceNotActiveException. It means that the node is
                    // offline, so waiting for the taskGroup to restore can be successful
                    catch (HazelcastInstanceNotActiveException e) {
                        LOGGER.warning(
                                String.format(
                                        "%s get current job metrics with exception: %s.",
                                        Arrays.toString(taskGroupLocations.toArray()),
                                        ExceptionUtils.getMessage(e)));
                    } catch (Exception e) {
                        throw new SeaTunnelEngineException(ExceptionUtils.getMessage(e));
                    }
                });
        return metrics;
    }

    public void savePipelineMetricsToHistory(PipelineLocation pipelineLocation) {
        List<RawJobMetrics> currJobMetrics =
                this.getCurrJobMetrics(Collections.singletonList(pipelineLocation));
        JobMetrics jobMetrics = JobMetricsUtil.toJobMetrics(currJobMetrics);
        long jobId = this.getJobImmutableInformation().getJobId();
        synchronized (this) {
            jobHistoryService.storeFinishedPipelineMetrics(jobId, jobMetrics);
        }
        // Clean TaskGroupContext for TaskExecutionServer
        this.cleanTaskGroupContext(pipelineLocation);
    }

    public void removeMetricsContext(
            PipelineLocation pipelineLocation, PipelineStatus pipelineStatus) {
        if (pipelineStatus.equals(PipelineStatus.FINISHED) && !checkpointManager.isSavePointEnd()
                || pipelineStatus.equals(PipelineStatus.CANCELED)) {
            try {
                metricsImap.lock(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
                HashMap<TaskLocation, SeaTunnelMetricsContext> centralMap =
                        metricsImap.get(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
                if (centralMap != null) {
                    List<TaskLocation> collect =
                            centralMap.keySet().stream()
                                    .filter(
                                            taskLocation -> {
                                                return taskLocation
                                                        .getTaskGroupLocation()
                                                        .getPipelineLocation()
                                                        .equals(pipelineLocation);
                                            })
                                    .collect(Collectors.toList());
                    collect.forEach(centralMap::remove);
                    metricsImap.put(Constant.IMAP_RUNNING_JOB_METRICS_KEY, centralMap);
                }
            } finally {
                metricsImap.unlock(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
            }
        }
    }

    private void cleanTaskGroupContext(PipelineLocation pipelineLocation) {
        Map<TaskGroupLocation, SlotProfile> slotProfileMap =
                ownedSlotProfilesIMap.get(pipelineLocation);
        if (slotProfileMap == null) {
            return;
        }
        slotProfileMap.forEach(
                (taskGroupLocation, slotProfile) -> {
                    try {
                        if (nodeEngine.getClusterService().getMember(slotProfile.getWorker())
                                != null) {
                            NodeEngineUtil.sendOperationToMemberNode(
                                            nodeEngine,
                                            new CleanTaskGroupContextOperation(taskGroupLocation),
                                            slotProfile.getWorker())
                                    .get();
                        }
                    } catch (HazelcastInstanceNotActiveException e) {
                        LOGGER.warning(
                                String.format(
                                        "%s clean TaskGroupContext with exception: %s.",
                                        taskGroupLocation, ExceptionUtils.getMessage(e)));
                    } catch (Exception e) {
                        throw new SeaTunnelException(e.getMessage());
                    }
                });
    }

    public PhysicalPlan getPhysicalPlan() {
        return physicalPlan;
    }

    public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        this.physicalPlan
                .getPipelineList()
                .forEach(
                        pipeline -> {
                            if (pipeline.getPipelineLocation().getPipelineId()
                                    != taskExecutionState.getTaskGroupLocation().getPipelineId()) {
                                return;
                            }

                            pipeline.getCoordinatorVertexList()
                                    .forEach(
                                            task -> {
                                                if (!task.getTaskGroupLocation()
                                                        .equals(
                                                                taskExecutionState
                                                                        .getTaskGroupLocation())) {
                                                    return;
                                                }

                                                task.updateStateByExecutionService(
                                                        taskExecutionState);
                                            });

                            pipeline.getPhysicalVertexList()
                                    .forEach(
                                            task -> {
                                                if (!task.getTaskGroupLocation()
                                                        .equals(
                                                                taskExecutionState
                                                                        .getTaskGroupLocation())) {
                                                    return;
                                                }

                                                task.updateStateByExecutionService(
                                                        taskExecutionState);
                                            });
                        });
    }

    /** Execute savePoint, which will cause the job to end. */
    public CompletableFuture<Void> savePoint() {
        LOGGER.info(
                String.format(
                        "Begin do save point for Job %s (%s) ",
                        jobImmutableInformation.getJobConfig().getName(),
                        jobImmutableInformation.getJobId()));
        physicalPlan.savepointJob();
        PassiveCompletableFuture<CompletedCheckpoint>[] passiveCompletableFutures =
                checkpointManager.triggerSavePoints();
        return CompletableFuture.allOf(passiveCompletableFutures);
    }

    public void setOwnedSlotProfiles(
            @NonNull PipelineLocation pipelineLocation,
            @NonNull Map<TaskGroupLocation, SlotProfile> pipelineOwnedSlotProfiles) {
        ownedSlotProfilesIMap.put(pipelineLocation, pipelineOwnedSlotProfiles);
        try {
            RetryUtils.retryWithException(
                    () ->
                            pipelineOwnedSlotProfiles.equals(
                                    ownedSlotProfilesIMap.get(pipelineLocation)),
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            exception -> exception instanceof NullPointerException && isRunning,
                            Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            throw new SeaTunnelEngineException(
                    "Can not sync pipeline owned slot profiles with IMap", e);
        }
    }

    public SlotProfile getOwnedSlotProfiles(@NonNull TaskGroupLocation taskGroupLocation) {
        Map<TaskGroupLocation, SlotProfile> taskGroupLocationSlotProfileMap =
                ownedSlotProfilesIMap.get(
                        new PipelineLocation(
                                taskGroupLocation.getJobId(), taskGroupLocation.getPipelineId()));
        if (taskGroupLocationSlotProfileMap == null) {
            return null;
        }

        return taskGroupLocationSlotProfileMap.get(taskGroupLocation);
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void interrupt() {
        isRunning = false;
        jobMasterCompleteFuture.completeExceptionally(new InterruptedException());
    }

    public void neverNeedRestore() {
        this.needRestore = false;
    }
}
