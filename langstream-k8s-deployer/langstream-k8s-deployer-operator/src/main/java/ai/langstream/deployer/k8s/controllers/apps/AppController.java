/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.deployer.k8s.controllers.apps;

import ai.langstream.api.model.ApplicationLifecycleStatus;
import ai.langstream.api.util.ObjectMapperFactory;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpec;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpecOptions;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationStatus;
import ai.langstream.deployer.k8s.apps.AppResourcesFactory;
import ai.langstream.deployer.k8s.controllers.BaseController;
import ai.langstream.deployer.k8s.controllers.InfiniteRetry;
import ai.langstream.deployer.k8s.util.JSONComparator;
import ai.langstream.deployer.k8s.util.KubeUtil;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.deployer.k8s.util.SpecDiffer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import java.time.Duration;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.jbosslog.JBossLog;

@ControllerConfiguration(
        namespaces = Constants.WATCH_ALL_NAMESPACES,
        name = "app-controller",
        retry = InfiniteRetry.class)
@JBossLog
public class AppController extends BaseController<ApplicationCustomResource>
        implements ErrorStatusHandler<ApplicationCustomResource> {

    private static final ObjectMapper lastAppliedJsonMapper =
            ObjectMapperFactory.getDefaultMapper();

    protected static final Duration DEFAULT_RESCHEDULE_DURATION = Duration.ofSeconds(5);

    @Override
    public ErrorStatusUpdateControl<ApplicationCustomResource> updateErrorStatus(
            ApplicationCustomResource customResource,
            Context<ApplicationCustomResource> context,
            Exception e) {
        log.warnf("%s update error status ", customResourceLogRef(customResource), e);
        if (customResource.getStatus() != null
                && customResource.getStatus().getStatus() != null
                && customResource.getStatus().getStatus().getStatus()
                        == ApplicationLifecycleStatus.Status.DELETING) {
            customResource
                    .getStatus()
                    .setStatus(ApplicationLifecycleStatus.errorDeleting(e.getMessage()));
        } else {
            customResource
                    .getStatus()
                    .setStatus(ApplicationLifecycleStatus.errorDeploying(e.getMessage()));
        }
        return ErrorStatusUpdateControl.updateStatus(customResource);
    }

    @Data
    @NoArgsConstructor
    public static class AppLastApplied {
        String setup;
        String runtimeDeployer;
    }

    public record HandleJobResult(boolean proceed, Duration reschedule) {}

    public class ApplicationDeletedException extends Exception {}

    @Override
    protected PatchResult patchResources(
            ApplicationCustomResource resource, Context<ApplicationCustomResource> context) {
        final ApplicationSpecOptions applicationSpecOptions =
                ApplicationSpec.deserializeOptions(resource.getSpec().getOptions());
        AppLastApplied appLastApplied = getAppLastApplied(resource);
        Duration rescheduleDuration;
        if (applicationSpecOptions.isMarkedForDeletion()) {
            try {
                rescheduleDuration = cleanupApplication(resource, appLastApplied);
            } catch (ApplicationDeletedException exception) {
                return PatchResult.patch(UpdateControl.noUpdate());
            }
        } else {
            final HandleJobResult setupJobResult = handleJob(resource, appLastApplied, true, false);
            final String logRef = customResourceLogRef(resource);
            if (setupJobResult.proceed()) {
                log.infof("%s [deploy] setup job is completed, checking deployer", logRef);
                final HandleJobResult deployerJobResult =
                        handleJob(resource, appLastApplied, false, false);
                log.infof(
                        "%s [deploy] setup job is %s",
                        logRef, deployerJobResult.proceed() ? "completed" : "not completed");

                rescheduleDuration = deployerJobResult.reschedule();
            } else {

                log.infof("%s [deploy] setup job is not completed yet", logRef);
                rescheduleDuration = setupJobResult.reschedule();
            }
        }
        final UpdateControl<ApplicationCustomResource> updateControl =
                rescheduleDuration != null
                        ? UpdateControl.updateStatus(resource).rescheduleAfter(rescheduleDuration)
                        : UpdateControl.updateStatus(resource);
        return PatchResult.patch(updateControl).withLastApplied(appLastApplied);
    }

    @Override
    protected DeleteControl cleanupResources(
            ApplicationCustomResource resource, Context<ApplicationCustomResource> context) {
        AppLastApplied appLastApplied = getAppLastApplied(resource);
        Duration rescheduleDuration;
        try {
            rescheduleDuration = cleanupApplication(resource, appLastApplied);
        } catch (ApplicationDeletedException ex) {
            rescheduleDuration = null;
        }

        if (rescheduleDuration == null) {
            log.infof(
                    "%s cleanup is completed, removing from limiter",
                    customResourceLogRef(resource));
            appResourcesLimiter.onAppBeingDeleted(resource);
            return DeleteControl.defaultDelete();
        } else {
            return DeleteControl.noFinalizerRemoval().rescheduleAfter(rescheduleDuration);
        }
    }

    private Duration cleanupApplication(
            ApplicationCustomResource resource, AppLastApplied appLastApplied)
            throws ApplicationDeletedException {
        final HandleJobResult deployerJobResult = handleJob(resource, appLastApplied, false, true);
        Duration rescheduleDuration;
        String logRef = customResourceLogRef(resource);
        if (deployerJobResult.proceed()) {
            log.infof(
                    "%s [cleanup] deployer cleanup job is completed, checking setup cleanup",
                    logRef);

            final HandleJobResult setupJobResult = handleJob(resource, appLastApplied, true, true);
            log.infof(
                    "%s [cleanup] setup cleanup job is %s",
                    logRef, setupJobResult.proceed() ? "completed" : "not completed");
            if (setupJobResult.proceed()) {
                if (!resource.isMarkedForDeletion()) {
                    client.resource(resource).delete();
                    throw new ApplicationDeletedException();
                }
                return null;
            } else {
                return setupJobResult.reschedule();
            }
        } else {
            log.infof("%s [cleanup] deployer cleanup job is not completed yet", logRef);
            rescheduleDuration = deployerJobResult.reschedule();
        }
        return rescheduleDuration;
    }

    private HandleJobResult handleJob(
            ApplicationCustomResource application,
            AppLastApplied appLastApplied,
            boolean isSetupJob,
            boolean delete) {
        final String applicationId = application.getMetadata().getName();

        final String jobName =
                isSetupJob
                        ? AppResourcesFactory.getSetupJobName(applicationId, delete)
                        : AppResourcesFactory.getDeployerJobName(applicationId, delete);
        final String namespace = application.getMetadata().getNamespace();
        final Job currentJob =
                client.batch().v1().jobs().inNamespace(namespace).withName(jobName).get();
        if (currentJob == null || areSpecChanged(application, appLastApplied, isSetupJob)) {
            if (appLastApplied == null) {
                appLastApplied = new AppLastApplied();
            }
            if (isSetupJob) {
                appLastApplied.setSetup(SerializationUtil.writeAsJson(application.getSpec()));
            } else {
                appLastApplied.setRuntimeDeployer(
                        SerializationUtil.writeAsJson(application.getSpec()));
            }
            if (isSetupJob && !delete) {
                final boolean isDeployable = appResourcesLimiter.checkLimitsForTenant(application);
                if (!isDeployable) {
                    log.infof(
                            "%s application is not deployable, waiting for resources to be available or limit to be increased.",
                            customResourceLogRef(application));
                    application
                            .getStatus()
                            .setStatus(
                                    ApplicationLifecycleStatus.errorDeploying(
                                            "Not enough resources to deploy application"));
                    application
                            .getStatus()
                            .setResourceLimitStatus(ApplicationStatus.ResourceLimitStatus.REJECTED);
                    return new HandleJobResult(false, Duration.ofSeconds(30));
                } else {
                    application
                            .getStatus()
                            .setResourceLimitStatus(ApplicationStatus.ResourceLimitStatus.ACCEPTED);
                }
            }
            createJob(application, isSetupJob, delete);
            if (!delete) {
                application.getStatus().setStatus(ApplicationLifecycleStatus.DEPLOYING);
            } else {
                application.getStatus().setStatus(ApplicationLifecycleStatus.DELETING);
            }
            return new HandleJobResult(false, DEFAULT_RESCHEDULE_DURATION);
        } else {
            if (KubeUtil.isJobFailed(currentJob)) {
                if (isSetupJob && delete) {
                    // failed cleaning up the assets/topics
                    final ApplicationSpecOptions applicationSpecOptions =
                            ApplicationSpec.deserializeOptions(application.getSpec().getOptions());
                    if (applicationSpecOptions.getDeleteMode()
                            == ApplicationSpecOptions.DeleteMode.CLEANUP_BEST_EFFORT) {
                        return new HandleJobResult(true, null);
                    }
                }

                String errorMessage = "?";
                final Pod pod = KubeUtil.getJobPod(currentJob, client);
                if (pod != null) {
                    final KubeUtil.PodStatus status =
                            KubeUtil.getPodsStatuses(List.of(pod)).values().iterator().next();
                    if (status.getState() == KubeUtil.PodStatus.State.ERROR) {
                        errorMessage = status.getMessage();
                    }
                }

                if (delete) {
                    final String errMessageJobDescription = isSetupJob ? "assets/topics" : "agents";
                    application
                            .getStatus()
                            .setStatus(
                                    ApplicationLifecycleStatus.errorDeleting(
                                            "Failed to cleanup the %s, to delete the application, please cleanup the assets/topics manually and force-delete the application again. Error was:\n%s"
                                                    .formatted(
                                                            errMessageJobDescription,
                                                            errorMessage)));
                } else {
                    final String errMessageJobDescription = isSetupJob ? "setup" : "deployer";
                    String statusMessage =
                            "Failed to deploy the application, error during job: %s. Error was:\n%s"
                                    .formatted(errMessageJobDescription, errorMessage);
                    log.infof("%s %s", customResourceLogRef(application), statusMessage);
                    application
                            .getStatus()
                            .setStatus(ApplicationLifecycleStatus.errorDeploying(statusMessage));
                }
                return new HandleJobResult(false, null);
            } else if (KubeUtil.isJobCompleted(currentJob)) {
                if (!isSetupJob && !delete) {
                    application.getStatus().setStatus(ApplicationLifecycleStatus.DEPLOYED);
                }
                return new HandleJobResult(true, null);
            } else {
                return new HandleJobResult(false, DEFAULT_RESCHEDULE_DURATION);
            }
        }
    }

    @SneakyThrows
    private void createJob(
            ApplicationCustomResource applicationCustomResource, boolean setupJob, boolean delete) {
        final AppResourcesFactory.GenerateJobParams params =
                AppResourcesFactory.GenerateJobParams.builder()
                        .applicationCustomResource(applicationCustomResource)
                        .deleteJob(delete)
                        .clusterRuntimeConfiguration(configuration.getClusterRuntime())
                        .podTemplate(configuration.getAppDeployerPodTemplate())
                        .image(configuration.getRuntimeImage())
                        .imagePullPolicy(configuration.getRuntimeImagePullPolicy())
                        .build();

        Job job;
        ConfigMap configMap;
        if (setupJob) {
            job = AppResourcesFactory.generateSetupJob(params);
            configMap = AppResourcesFactory.generateJobConfigMap(params, true);
        } else {
            job = AppResourcesFactory.generateDeployerJob(params);
            configMap = AppResourcesFactory.generateJobConfigMap(params, false);
        }
        client.resource(configMap).serverSideApply();
        KubeUtil.patchJob(client, job);
    }

    private static boolean areSpecChanged(
            ApplicationCustomResource cr, AppLastApplied appLastApplied, boolean checkSetup) {
        String logRef = customResourceLogRef(cr);
        if (appLastApplied == null) {
            log.infof("%s spec changed, no existing found", logRef);
            return true;
        }
        final String lastAppliedAsString =
                checkSetup ? appLastApplied.getSetup() : appLastApplied.getRuntimeDeployer();
        if (lastAppliedAsString == null) {
            log.infof("%s spec changed, existing found but empty", logRef);
            return true;
        }
        final JSONComparator.Result diff =
                SpecDiffer.generateDiff(lastAppliedAsString, cr.getSpec());
        if (!diff.areEquals()) {
            log.infof("%s spec changed", logRef);
            SpecDiffer.logDetailedSpecDiff(diff);
            return true;
        }
        return false;
    }

    @SneakyThrows
    private static AppLastApplied getAppLastApplied(ApplicationCustomResource app) {
        if (app.getStatus() == null) {
            return null;
        }
        if (app.getStatus().getLastApplied() == null) {
            return null;
        }
        return lastAppliedJsonMapper.readValue(
                app.getStatus().getLastApplied(), AppLastApplied.class);
    }
}
