/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.AsyncActionStep;
import org.elasticsearch.xpack.core.ilm.AsyncWaitStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateActionStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;

import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_ORIGINATION_DATE;

public class IndexLifecycleRunner {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleRunner.class);
    private final ThreadPool threadPool;
    private PolicyStepsRegistry stepRegistry;
    private ClusterService clusterService;
    private LongSupplier nowSupplier;

    public IndexLifecycleRunner(PolicyStepsRegistry stepRegistry, ClusterService clusterService,
                                ThreadPool threadPool, LongSupplier nowSupplier) {
        this.stepRegistry = stepRegistry;
        this.clusterService = clusterService;
        this.nowSupplier = nowSupplier;
        this.threadPool = threadPool;
    }

    /**
     * Return true or false depending on whether the index is ready to be in {@code phase}
     */
    boolean isReadyToTransitionToThisPhase(final String policy, final IndexMetaData indexMetaData, final String phase) {
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        Long originationDate = indexMetaData.getSettings().getAsLong(LIFECYCLE_ORIGINATION_DATE, -1L);
        if (lifecycleState.getLifecycleDate() == null && originationDate == -1L) {
            logger.trace("no index creation or origination date has been set yet");
            return true;
        }
        final Long lifecycleDate = originationDate != -1L ? originationDate : lifecycleState.getLifecycleDate();
        assert lifecycleDate != null && lifecycleDate >= 0 : "expected index to have a lifecycle date but it did not";
        final TimeValue after = stepRegistry.getIndexAgeForPhase(policy, phase);
        final long now = nowSupplier.getAsLong();
        final TimeValue age = new TimeValue(now - lifecycleDate);
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] checking for index age to be at least [{}] before performing actions in " +
                    "the \"{}\" phase. Now: {}, lifecycle date: {}, age: [{}/{}s]",
                indexMetaData.getIndex().getName(), after, phase,
                new TimeValue(now).seconds(),
                new TimeValue(lifecycleDate).seconds(),
                age, age.seconds());
        }
        return now >= lifecycleDate + after.getMillis();
    }

    /**
     * Run the current step, only if it is an asynchronous wait step. These
     * wait criteria are checked periodically from the ILM scheduler
     */
    public void runPeriodicStep(String policy, IndexMetaData indexMetaData) {
        String index = indexMetaData.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        final Step currentStep;
        try {
            currentStep = getCurrentStep(stepRegistry, policy, indexMetaData, lifecycleState);
        } catch (Exception e) {
            markPolicyRetrievalError(policy, indexMetaData.getIndex(), lifecycleState, e);
            return;
        }

        if (currentStep == null) {
            if (stepRegistry.policyExists(policy) == false) {
                markPolicyDoesNotExist(policy, indexMetaData.getIndex(), lifecycleState);
                return;
            } else {
                logger.error("current step [{}] for index [{}] with policy [{}] is not recognized",
                    LifecycleExecutionState.getCurrentStepKey(lifecycleState), index, policy);
                return;
            }
        }

        if (currentStep instanceof TerminalPolicyStep) {
            logger.debug("policy [{}] for index [{}] complete, skipping execution", policy, index);
            return;
        } else if (currentStep instanceof ErrorStep) {
            onErrorMaybeRetryFailedStep(policy, indexMetaData);
            return;
        }

        logger.trace("[{}] maybe running periodic step ({}) with current step {}",
            index, currentStep.getClass().getSimpleName(), currentStep.getKey());
        // Only phase changing and async wait steps should be run through periodic polling
        if (currentStep instanceof PhaseCompleteStep) {
            // Only proceed to the next step if enough time has elapsed to go into the next phase
            if (isReadyToTransitionToThisPhase(policy, indexMetaData, currentStep.getNextStepKey().getPhase())) {
                moveToStep(indexMetaData.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
            }
        } else if (currentStep instanceof AsyncWaitStep) {
            logger.debug("[{}] running periodic policy with current-step [{}]", index, currentStep.getKey());
            ((AsyncWaitStep) currentStep).evaluateCondition(indexMetaData, new AsyncWaitStep.Listener() {

                @Override
                public void onResponse(boolean conditionMet, ToXContentObject stepInfo) {
                    logger.trace("cs-change-async-wait-callback, [{}] current-step: {}", index, currentStep.getKey());
                    if (conditionMet) {
                        moveToStep(indexMetaData.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
                    } else if (stepInfo != null) {
                        setStepInfo(indexMetaData.getIndex(), policy, currentStep.getKey(), stepInfo);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    moveToErrorStep(indexMetaData.getIndex(), policy, currentStep.getKey(), e);
                }
            });
        } else {
            logger.trace("[{}] ignoring non periodic step execution from step transition [{}]", index, currentStep.getKey());
        }
    }

    void onErrorMaybeRetryFailedStep(String policy, IndexMetaData indexMetaData) {
        String index = indexMetaData.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        Step failedStep = stepRegistry.getStep(indexMetaData, new StepKey(lifecycleState.getPhase(), lifecycleState.getAction(),
            lifecycleState.getFailedStep()));
        if (failedStep == null) {
            logger.warn("failed step [{}] for index [{}] is not part of policy [{}] anymore, or it is invalid. skipping execution",
                lifecycleState.getFailedStep(), index, policy);
            return;
        }

        if (lifecycleState.isAutoRetryableError() != null && lifecycleState.isAutoRetryableError()) {
            int currentRetryAttempt = lifecycleState.getFailedStepRetryCount() == null ? 1 : 1 + lifecycleState.getFailedStepRetryCount();
            logger.info("policy [{}] for index [{}] on an error step due to a transitive error, moving back to the failed " +
                "step [{}] for execution. retry attempt [{}]", policy, index, lifecycleState.getFailedStep(), currentRetryAttempt);
            clusterService.submitStateUpdateTask("ilm-retry-failed-step", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return moveClusterStateToPreviouslyFailedStep(currentState, index, true);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(new ParameterizedMessage("retry execution of step [{}] for index [{}] failed",
                        failedStep.getKey().getName(), index), e);
                }
            });
        } else {
            logger.debug("policy [{}] for index [{}] on an error step after a terminal error, skipping execution", policy, index);
        }
    }

    /**
     * If the current step (matching the expected step key) is an asynchronous action step, run it
     */
    public void maybeRunAsyncAction(ClusterState currentState, IndexMetaData indexMetaData, String policy, StepKey expectedStepKey) {
        String index = indexMetaData.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        final Step currentStep;
        try {
            currentStep = getCurrentStep(stepRegistry, policy, indexMetaData, lifecycleState);
        } catch (Exception e) {
            markPolicyRetrievalError(policy, indexMetaData.getIndex(), lifecycleState, e);
            return;
        }
        if (currentStep == null) {
            logger.warn("current step [{}] for index [{}] with policy [{}] is not recognized",
                LifecycleExecutionState.getCurrentStepKey(lifecycleState), index, policy);
            return;
        }

        logger.trace("[{}] maybe running async action step ({}) with current step {}",
            index, currentStep.getClass().getSimpleName(), currentStep.getKey());
        if (currentStep.getKey().equals(expectedStepKey) == false) {
            throw new IllegalStateException("expected index [" + indexMetaData.getIndex().getName() + "] with policy [" + policy +
                "] to have current step consistent with provided step key (" + expectedStepKey + ") but it was " + currentStep.getKey());
        }
        if (currentStep instanceof AsyncActionStep) {
            logger.debug("[{}] running policy with async action step [{}]", index, currentStep.getKey());
            ((AsyncActionStep) currentStep).performAction(indexMetaData, currentState,
                new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext()), new AsyncActionStep.Listener() {

                @Override
                public void onResponse(boolean complete) {
                    logger.trace("cs-change-async-action-callback, [{}], current-step: {}", index, currentStep.getKey());
                    if (complete && ((AsyncActionStep) currentStep).indexSurvives()) {
                        moveToStep(indexMetaData.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    moveToErrorStep(indexMetaData.getIndex(), policy, currentStep.getKey(), e);
                }
            });
        } else {
            logger.trace("[{}] ignoring non async action step execution from step transition [{}]", index, currentStep.getKey());
        }
    }

    /**
     * Run the current step that either waits for index age, or updates/waits-on cluster state.
     * Invoked after the cluster state has been changed
     */
    public void runPolicyAfterStateChange(String policy, IndexMetaData indexMetaData) {
        String index = indexMetaData.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        final Step currentStep;
        try {
            currentStep = getCurrentStep(stepRegistry, policy, indexMetaData, lifecycleState);
        } catch (Exception e) {
            markPolicyRetrievalError(policy, indexMetaData.getIndex(), lifecycleState, e);
            return;
        }
        if (currentStep == null) {
            if (stepRegistry.policyExists(policy) == false) {
                markPolicyDoesNotExist(policy, indexMetaData.getIndex(), lifecycleState);
                return;
            } else {
                logger.error("current step [{}] for index [{}] with policy [{}] is not recognized",
                    LifecycleExecutionState.getCurrentStepKey(lifecycleState), index, policy);
                return;
            }
        }

        if (currentStep instanceof TerminalPolicyStep) {
            logger.debug("policy [{}] for index [{}] complete, skipping execution", policy, index);
            return;
        } else if (currentStep instanceof ErrorStep) {
            logger.debug("policy [{}] for index [{}] on an error step, skipping execution", policy, index);
            return;
        }

        logger.trace("[{}] maybe running step ({}) after state change: {}",
            index, currentStep.getClass().getSimpleName(), currentStep.getKey());
        if (currentStep instanceof PhaseCompleteStep) {
            // Only proceed to the next step if enough time has elapsed to go into the next phase
            if (isReadyToTransitionToThisPhase(policy, indexMetaData, currentStep.getNextStepKey().getPhase())) {
                moveToStep(indexMetaData.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
            }
        } else if (currentStep instanceof ClusterStateActionStep || currentStep instanceof ClusterStateWaitStep) {
            logger.debug("[{}] running policy with current-step [{}]", indexMetaData.getIndex().getName(), currentStep.getKey());
            clusterService.submitStateUpdateTask("ilm-execute-cluster-state-steps",
                new ExecuteStepsUpdateTask(policy, indexMetaData.getIndex(), currentStep, stepRegistry, this, nowSupplier));
        } else {
            logger.trace("[{}] ignoring step execution from cluster state change event [{}]", index, currentStep.getKey());
        }
    }

    static Step getCurrentStep(PolicyStepsRegistry stepRegistry, String policy, IndexMetaData indexMetaData,
                               LifecycleExecutionState lifecycleState) {
        StepKey currentStepKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
        logger.trace("[{}] retrieved current step key: {}", indexMetaData.getIndex().getName(), currentStepKey);
        if (currentStepKey == null) {
            return stepRegistry.getFirstStep(policy);
        } else {
            return stepRegistry.getStep(indexMetaData, currentStepKey);
        }
    }


    private void moveToStep(Index index, String policy, Step.StepKey currentStepKey, Step.StepKey nextStepKey) {
        logger.debug("[{}] moving to step [{}] {} -> {}", index.getName(), policy, currentStepKey, nextStepKey);
        clusterService.submitStateUpdateTask("ilm-move-to-step",
            new MoveToNextStepUpdateTask(index, policy, currentStepKey, nextStepKey, nowSupplier, clusterState ->
            {
                IndexMetaData indexMetaData = clusterState.metaData().index(index);
                if (nextStepKey != null && nextStepKey != TerminalPolicyStep.KEY && indexMetaData != null) {
                    maybeRunAsyncAction(clusterState, indexMetaData, policy, nextStepKey);
                }
            }));
    }

    private void moveToErrorStep(Index index, String policy, Step.StepKey currentStepKey, Exception e) {
        logger.error(new ParameterizedMessage("policy [{}] for index [{}] failed on step [{}]. Moving to ERROR step",
            policy, index.getName(), currentStepKey), e);
        clusterService.submitStateUpdateTask("ilm-move-to-error-step",
            new MoveToErrorStepUpdateTask(index, policy, currentStepKey, e, nowSupplier, stepRegistry::getStep));
    }

    private void setStepInfo(Index index, String policy, Step.StepKey currentStepKey, ToXContentObject stepInfo) {
        clusterService.submitStateUpdateTask("ilm-set-step-info", new SetStepInfoUpdateTask(index, policy, currentStepKey, stepInfo));
    }

    ClusterState moveClusterStateToPreviouslyFailedStep(ClusterState currentState, String index, boolean isAutomaticRetry) {
        ClusterState newState;
        IndexMetaData indexMetaData = currentState.metaData().index(index);
        if (indexMetaData == null) {
            throw new IllegalArgumentException("index [" + index + "] does not exist");
        }
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        Step.StepKey currentStepKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
        String failedStep = lifecycleState.getFailedStep();
        if (currentStepKey != null && ErrorStep.NAME.equals(currentStepKey.getName()) && Strings.isNullOrEmpty(failedStep) == false) {
            Step.StepKey nextStepKey = new Step.StepKey(currentStepKey.getPhase(), currentStepKey.getAction(), failedStep);
            IndexLifecycleTransition.validateTransition(indexMetaData, currentStepKey, nextStepKey, stepRegistry);
            IndexLifecycleMetadata ilmMeta = currentState.metaData().custom(IndexLifecycleMetadata.TYPE);

            LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas()
                .get(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetaData.getSettings()));
            LifecycleExecutionState nextStepState = IndexLifecycleTransition.moveExecutionStateToNextStep(policyMetadata,
                lifecycleState, currentStepKey, nextStepKey, nowSupplier, true);
            LifecycleExecutionState.Builder retryStepState = LifecycleExecutionState.builder(nextStepState);
            retryStepState.setIsAutoRetryableError(lifecycleState.isAutoRetryableError());
            Integer currentRetryCount = lifecycleState.getFailedStepRetryCount();
            if (isAutomaticRetry) {
                retryStepState.setFailedStepRetryCount(currentRetryCount == null ? 1 : ++currentRetryCount);
            } else {
                // manual retries don't update the retry count
                retryStepState.setFailedStepRetryCount(lifecycleState.getFailedStepRetryCount());
            }
            newState = IndexLifecycleTransition.newClusterStateWithLifecycleState(indexMetaData.getIndex(),
                currentState, retryStepState.build()).build();
        } else {
            throw new IllegalArgumentException("cannot retry an action for an index ["
                + index + "] that has not encountered an error when running a Lifecycle Policy");
        }
        return newState;
    }

    ClusterState moveClusterStateToPreviouslyFailedStep(ClusterState currentState, String[] indices) {
        ClusterState newState = currentState;
        for (String index : indices) {
            newState = moveClusterStateToPreviouslyFailedStep(newState, index, false);
        }
        return newState;
    }

    void markPolicyDoesNotExist(String policyName, Index index, LifecycleExecutionState executionState) {
        markPolicyRetrievalError(policyName, index, executionState,
            new IllegalArgumentException("policy [" + policyName + "] does not exist"));
    }

    void markPolicyRetrievalError(String policyName, Index index, LifecycleExecutionState executionState, Exception e) {
        logger.debug(
            new ParameterizedMessage("unable to retrieve policy [{}] for index [{}], recording this in step_info for this index",
                policyName, index.getName()), e);
        setStepInfo(index, policyName, LifecycleExecutionState.getCurrentStepKey(executionState),
            new SetStepInfoUpdateTask.ExceptionWrapper(e));
    }
}
