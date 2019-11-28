/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.Index;

import java.util.List;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

/**
 * Updates the rollover information for the source index (the index that was rolled over) and set the creation date on the lifecycle
 * execution state, which will be used to calculate the age of the rolled over index.
 */
public class UpdateRolloverInfoStep extends ClusterStateActionStep {
    private static final Logger logger = LogManager.getLogger(UpdateRolloverInfoStep.class);

    public static final String NAME = "update-rollover-info";

    private final LongSupplier fallbackTimeSupplier;

    public UpdateRolloverInfoStep(StepKey key, StepKey nextStepKey, LongSupplier fallbackTimeSupplier) {
        super(key, nextStepKey);
        this.fallbackTimeSupplier = fallbackTimeSupplier;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public ClusterState performAction(Index index, ClusterState currentState) {
        IndexMetaData indexMetaData = currentState.metaData().getIndexSafe(index);

        long newIndexTime = fallbackTimeSupplier.getAsLong();

        LifecycleExecutionState.Builder newLifecycleState = LifecycleExecutionState
            .builder(LifecycleExecutionState.fromIndexMetadata(indexMetaData));
        newLifecycleState.setIndexCreationDate(newIndexTime);

        IndexMetaData.Builder newIndexMetadata = IndexMetaData.builder(indexMetaData);
        newIndexMetadata.putCustom(ILM_CUSTOM_METADATA_KEY, newLifecycleState.build().asMap());

        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(indexMetaData.getSettings());
        if (indexingComplete) {
            // The index won't have RolloverInfo if this is a Following index and indexing_complete was set by CCR
            logger.trace(indexMetaData.getIndex() + " has lifecycle complete set, skipping " + UpdateRolloverInfoStep.NAME);
        } else {
            // find the newly created index from the rollover and fetch its index.creation_date
            String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetaData.getSettings());
            if (Strings.isNullOrEmpty(rolloverAlias)) {
                throw new IllegalStateException("setting [" + RolloverAction.LIFECYCLE_ROLLOVER_ALIAS
                    + "] is not set on index [" + indexMetaData.getIndex().getName() + "]");
            }
            RolloverInfo rolloverInfo = new RolloverInfo(rolloverAlias, List.of(), newIndexTime);
            newIndexMetadata.putRolloverInfo(rolloverInfo);
        }

        return ClusterState.builder(currentState).metaData(MetaData.builder(currentState.metaData())
            .put(newIndexMetadata)).build();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && getClass() == obj.getClass() && super.equals(obj);
    }
}
