/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.index.Index;

import static org.elasticsearch.action.admin.indices.rollover.RolloverAliasAndIndexResolver.resolveRolloverIndexName;

final class WaitForIndexActiveShardsStep extends ClusterStateWaitStep {

    static final String NAME = "wait-for-active-shards";

    private final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();

    WaitForIndexActiveShardsStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        String rolloverIndexName = resolveRolloverIndexName(indexMetaData, null, indexNameExpressionResolver);
        return new Result(ActiveShardCount.DEFAULT.enoughShardsActive(clusterState, rolloverIndexName), null);
    }
}
