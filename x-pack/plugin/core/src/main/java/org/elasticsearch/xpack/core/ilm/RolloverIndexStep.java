/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.common.Strings;

import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.action.admin.indices.rollover.RolloverAliasAndIndexResolver.checkNoDuplicatedAliasInIndexTemplate;
import static org.elasticsearch.action.admin.indices.rollover.RolloverAliasAndIndexResolver.resolveRolloverIndexName;
import static org.elasticsearch.action.admin.indices.rollover.RolloverAliasAndIndexResolver.validateAlias;

/**
 * Unconditionally rolls over an index using the Rollover API.
 */
public class RolloverIndexStep extends AsyncActionStep {
    private static final Logger logger = LogManager.getLogger(RolloverIndexStep.class);

    public static final String NAME = "attempt-rollover";

    private final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();

    public RolloverIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentClusterState,
                              ClusterStateObserver observer, Listener listener) {
        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(indexMetaData.getSettings());
        if (indexingComplete) {
            logger.trace(indexMetaData.getIndex() + " has lifecycle complete set, skipping " + RolloverIndexStep.NAME);
            listener.onResponse(true);
            return;
        }

        String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetaData.getSettings());

        if (Strings.isNullOrEmpty(rolloverAlias)) {
            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT,
                "setting [%s] for index [%s] is empty or not defined", RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
                indexMetaData.getIndex().getName())));
            return;
        }

        if (indexMetaData.getRolloverInfos().get(rolloverAlias) != null) {
            logger.info("index [{}] was already rolled over for alias [{}], not attempting to roll over again",
                indexMetaData.getIndex().getName(), rolloverAlias);
            listener.onResponse(true);
            return;
        }

        if (indexMetaData.getAliases().containsKey(rolloverAlias) == false) {
            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT,
                "%s [%s] does not point to index [%s]", RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, rolloverAlias,
                indexMetaData.getIndex().getName())));
            return;
        }

        AliasOrIndex aliasOrIndex = currentClusterState.metaData().getAliasAndIndexLookup().get(rolloverAlias);
        validateAlias(aliasOrIndex);
        String rolloverIndexName = resolveRolloverIndexName(indexMetaData, null, indexNameExpressionResolver);
        try {
            MetaDataCreateIndexService.validateIndexName(rolloverIndexName, currentClusterState); // will fail if the index already exists
            checkNoDuplicatedAliasInIndexTemplate(currentClusterState.metaData(), rolloverIndexName, rolloverAlias);
        } catch (Exception e) {
           listener.onFailure(e);
           return;
        }

        getClient().admin().indices().create(new CreateIndexRequest(rolloverIndexName),
            ActionListener.wrap(response -> listener.onResponse(response.isAcknowledged()), listener::onFailure));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return super.equals(obj);
    }
}
