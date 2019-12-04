/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;
import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

/**
 * The {@link IndexLifecycleTransition} class handles cluster state transitions
 * related to ILM operations. These operations are all at the index level
 * (inside of {@link IndexMetaData}) for the index in question.
 *
 * Each method is static and only changes a given state, no actions are
 * performed by methods in this class.
 */
public final class IndexLifecycleTransition {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleTransition.class);
    private static final ToXContent.Params STACKTRACE_PARAMS =
        new ToXContent.MapParams(Collections.singletonMap(REST_EXCEPTION_SKIP_STACK_TRACE, "false"));

    /**
     * This method is intended for handling moving to different steps from {@link TransportAction} executions.
     * For this reason, it is reasonable to throw {@link IllegalArgumentException} when state is not as expected.
     *
     * @param indexName      The index whose step is to change
     * @param currentState   The current {@link ClusterState}
     * @param currentStepKey The current {@link Step.StepKey} found for the index in the current cluster state
     * @param nextStepKey    The next step to move the index into
     * @param nowSupplier    The current-time supplier for updating when steps changed
     * @param stepRegistry   The steps registry to check a step-key's existence in the index's current policy
     * @return The updated cluster state where the index moved to <code>nextStepKey</code>
     */
    static ClusterState moveClusterStateToStep(String indexName, ClusterState currentState, Step.StepKey currentStepKey,
                                               Step.StepKey nextStepKey, LongSupplier nowSupplier,
                                               PolicyStepsRegistry stepRegistry) {
        IndexMetaData idxMeta = currentState.getMetaData().index(indexName);
        validateTransition(idxMeta, currentStepKey, nextStepKey, stepRegistry);

        Settings indexSettings = idxMeta.getSettings();
        String policy = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings);
        logger.info("moving index [{}] from [{}] to [{}] in policy [{}]",
            indexName, currentStepKey, nextStepKey, policy);

        return moveClusterStateToNextStep(idxMeta.getIndex(), currentState, currentStepKey,
            nextStepKey, nowSupplier, true);
    }

    static void validateTransition(IndexMetaData idxMeta, Step.StepKey currentStepKey,
                                   Step.StepKey nextStepKey, PolicyStepsRegistry stepRegistry) {
        String indexName = idxMeta.getIndex().getName();
        Settings indexSettings = idxMeta.getSettings();
        String indexPolicySetting = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings);

        // policy could be updated in-between execution
        if (Strings.isNullOrEmpty(indexPolicySetting)) {
            throw new IllegalArgumentException("index [" + indexName + "] is not associated with an Index Lifecycle Policy");
        }

        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
        if (currentStepKey.equals(LifecycleExecutionState.getCurrentStepKey(lifecycleState)) == false) {
            throw new IllegalArgumentException("index [" + indexName + "] is not on current step [" + currentStepKey + "]");
        }

        if (stepRegistry.stepExists(indexPolicySetting, nextStepKey) == false) {
            throw new IllegalArgumentException("step [" + nextStepKey + "] for index [" + idxMeta.getIndex().getName() +
                "] with policy [" + indexPolicySetting + "] does not exist");
        }
    }

    static ClusterState moveClusterStateToNextStep(Index index, ClusterState clusterState, Step.StepKey currentStep, Step.StepKey nextStep,
                                                   LongSupplier nowSupplier, boolean forcePhaseDefinitionRefresh) {
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        IndexLifecycleMetadata ilmMeta = clusterState.metaData().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas()
            .get(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings()));
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
        LifecycleExecutionState newLifecycleState = moveExecutionStateToNextStep(policyMetadata,
            lifecycleState, currentStep, nextStep, nowSupplier, forcePhaseDefinitionRefresh);
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithLifecycleState(index, clusterState, newLifecycleState);

        return newClusterStateBuilder.build();
    }

    static ClusterState moveClusterStateToErrorStep(Index index, ClusterState clusterState, Step.StepKey currentStep, Exception cause,
                                                    LongSupplier nowSupplier,
                                                    BiFunction<IndexMetaData, Step.StepKey, Step> stepLookupFunction) throws IOException {
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        IndexLifecycleMetadata ilmMeta = clusterState.metaData().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas()
            .get(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings()));
        XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder();
        causeXContentBuilder.startObject();
        ElasticsearchException.generateThrowableXContent(causeXContentBuilder, STACKTRACE_PARAMS, cause);
        causeXContentBuilder.endObject();
        LifecycleExecutionState currentState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
        LifecycleExecutionState nextStepState = moveExecutionStateToNextStep(policyMetadata, currentState, currentStep,
            new Step.StepKey(currentStep.getPhase(), currentStep.getAction(), ErrorStep.NAME), nowSupplier, false);
        LifecycleExecutionState.Builder failedState = LifecycleExecutionState.builder(nextStepState);
        failedState.setFailedStep(currentStep.getName());
        failedState.setStepInfo(BytesReference.bytes(causeXContentBuilder).utf8ToString());
        Step failedStep = stepLookupFunction.apply(idxMeta, currentStep);
        if (failedStep != null) {
            // as an initial step we'll mark the failed step as auto retryable without actually looking at the cause to determine
            // if the error is transient/recoverable from
            failedState.setIsAutoRetryableError(failedStep.isRetryable());
            // maintain the retry count of the failed step as it will be cleared after a successful execution
            failedState.setFailedStepRetryCount(currentState.getFailedStepRetryCount());
        } else {
            logger.warn("failed step [{}] for index [{}] is not part of policy [{}] anymore, or it is invalid",
                currentStep.getName(), index, policyMetadata.getName());
        }

        ClusterState.Builder newClusterStateBuilder = newClusterStateWithLifecycleState(index, clusterState, failedState.build());
        return newClusterStateBuilder.build();
    }

    static LifecycleExecutionState moveExecutionStateToNextStep(LifecyclePolicyMetadata policyMetadata,
                                                                LifecycleExecutionState existingState,
                                                                Step.StepKey currentStep, Step.StepKey nextStep,
                                                                LongSupplier nowSupplier,
                                                                boolean forcePhaseDefinitionRefresh) {
        long nowAsMillis = nowSupplier.getAsLong();
        LifecycleExecutionState.Builder updatedState = LifecycleExecutionState.builder(existingState);
        updatedState.setPhase(nextStep.getPhase());
        updatedState.setAction(nextStep.getAction());
        updatedState.setStep(nextStep.getName());
        updatedState.setStepTime(nowAsMillis);

        // clear any step info or error-related settings from the current step
        updatedState.setFailedStep(null);
        updatedState.setStepInfo(null);
        updatedState.setIsAutoRetryableError(null);
        updatedState.setFailedStepRetryCount(null);

        if (currentStep.getPhase().equals(nextStep.getPhase()) == false || forcePhaseDefinitionRefresh) {
            final String newPhaseDefinition;
            final Phase nextPhase;
            if ("new".equals(nextStep.getPhase()) || TerminalPolicyStep.KEY.equals(nextStep)) {
                nextPhase = null;
            } else {
                nextPhase = policyMetadata.getPolicy().getPhases().get(nextStep.getPhase());
            }
            PhaseExecutionInfo phaseExecutionInfo = new PhaseExecutionInfo(policyMetadata.getName(), nextPhase,
                policyMetadata.getVersion(), policyMetadata.getModifiedDate());
            newPhaseDefinition = Strings.toString(phaseExecutionInfo, false, false);
            updatedState.setPhaseDefinition(newPhaseDefinition);
            updatedState.setPhaseTime(nowAsMillis);
        } else if (currentStep.getPhase().equals(InitializePolicyContextStep.INITIALIZATION_PHASE)) {
            // The "new" phase is the initialization phase, usually the phase
            // time would be set on phase transition, but since there is no
            // transition into the "new" phase, we set it any time in the "new"
            // phase
            updatedState.setPhaseTime(nowAsMillis);
        }

        if (currentStep.getAction().equals(nextStep.getAction()) == false) {
            updatedState.setActionTime(nowAsMillis);
        }
        return updatedState.build();
    }

    static ClusterState.Builder newClusterStateWithLifecycleState(Index index, ClusterState clusterState,
                                                                  LifecycleExecutionState lifecycleState) {
        ClusterState.Builder newClusterStateBuilder = ClusterState.builder(clusterState);
        newClusterStateBuilder.metaData(MetaData.builder(clusterState.getMetaData())
            .put(IndexMetaData.builder(clusterState.getMetaData().index(index))
                .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.asMap())));
        return newClusterStateBuilder;
    }

    /**
     * Conditionally updates cluster state with new step info. The new cluster state is only
     * built if the step info has changed, otherwise the same old <code>clusterState</code> is
     * returned
     *
     * @param index        the index to modify
     * @param clusterState the cluster state to modify
     * @param stepInfo     the new step info to update
     * @return Updated cluster state with <code>stepInfo</code> if changed, otherwise the same cluster state
     * if no changes to step info exist
     * @throws IOException if parsing step info fails
     */
    static ClusterState addStepInfoToClusterState(Index index, ClusterState clusterState, ToXContentObject stepInfo) throws IOException {
        IndexMetaData indexMetaData = clusterState.getMetaData().index(index);
        if (indexMetaData == null) {
            // This index doesn't exist anymore, we can't do anything
            return clusterState;
        }
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        final String stepInfoString;
        try (XContentBuilder infoXContentBuilder = JsonXContent.contentBuilder()) {
            stepInfo.toXContent(infoXContentBuilder, ToXContent.EMPTY_PARAMS);
            stepInfoString = BytesReference.bytes(infoXContentBuilder).utf8ToString();
        }
        if (stepInfoString.equals(lifecycleState.getStepInfo())) {
            return clusterState;
        }
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder(lifecycleState);
        newState.setStepInfo(stepInfoString);
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithLifecycleState(index, clusterState, newState.build());
        return newClusterStateBuilder.build();
    }

    public static ClusterState removePolicyForIndexes(final Index[] indices, ClusterState currentState, List<String> failedIndexes) {
        MetaData.Builder newMetadata = MetaData.builder(currentState.getMetaData());
        boolean clusterStateChanged = false;
        for (Index index : indices) {
            IndexMetaData indexMetadata = currentState.getMetaData().index(index);
            if (indexMetadata == null) {
                // Index doesn't exist so fail it
                failedIndexes.add(index.getName());
            } else {
                IndexMetaData.Builder newIdxMetadata = removePolicyForIndex(indexMetadata);
                if (newIdxMetadata != null) {
                    newMetadata.put(newIdxMetadata);
                    clusterStateChanged = true;
                }
            }
        }
        if (clusterStateChanged) {
            ClusterState.Builder newClusterState = ClusterState.builder(currentState);
            newClusterState.metaData(newMetadata);
            return newClusterState.build();
        } else {
            return currentState;
        }
    }

    private static IndexMetaData.Builder removePolicyForIndex(IndexMetaData indexMetadata) {
        Settings idxSettings = indexMetadata.getSettings();
        Settings.Builder newSettings = Settings.builder().put(idxSettings);
        boolean notChanged = true;

        notChanged &= Strings.isNullOrEmpty(newSettings.remove(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey()));
        notChanged &= Strings.isNullOrEmpty(newSettings.remove(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.getKey()));
        notChanged &= Strings.isNullOrEmpty(newSettings.remove(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.getKey()));
        long newSettingsVersion = notChanged ? indexMetadata.getSettingsVersion() : 1 + indexMetadata.getSettingsVersion();

        IndexMetaData.Builder builder = IndexMetaData.builder(indexMetadata);
        builder.removeCustom(ILM_CUSTOM_METADATA_KEY);
        return builder.settings(newSettings).settingsVersion(newSettingsVersion);
    }
}
