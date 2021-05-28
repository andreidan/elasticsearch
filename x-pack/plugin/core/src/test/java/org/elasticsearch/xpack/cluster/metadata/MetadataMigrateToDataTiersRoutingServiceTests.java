/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.metadata;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.MigratedEntities;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class MetadataMigrateToDataTiersRoutingServiceTests extends ESTestCase {

    private String lifecycleName;
    private String indexName;

    @Test
    public void migrateToDataTiersRouting() {
        String lifecycleName = "test-policy";
        ShrinkAction shrinkAction = new ShrinkAction(2, null);
        AllocateAction allocateAction = new AllocateAction(null, null, null, Map.of("data", "warm"));

        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, Collections.singletonMap("warm",
            new Phase("warm", TimeValue.ZERO, Map.of(shrinkAction.getWriteableName(), shrinkAction, allocateAction.getWriteableName(),
                allocateAction))));

        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(policy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(
                Collections.singletonMap(policyMetadata.getName(), policyMetadata), OperationMode.RUNNING)).build())
            .build();

        Tuple<ClusterState, MigratedEntities> stateMigratedEntitiesTuple =
            MetadataMigrateToDataTiersRoutingService.migrateToDataTiersRouting(state, "data", null);

        assertThat(stateMigratedEntitiesTuple.v2().migratedPolicies.get(0), is("test-policy"));
        IndexLifecycleMetadata updatedLifecycleMetadata =
            stateMigratedEntitiesTuple.v1().metadata().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicy lifecyclePolicy = updatedLifecycleMetadata.getPolicies().get("test-policy");
        assertThat(lifecyclePolicy.getPhases().get("warm").getActions().size(), is(1));
    }

    @Test
    public void convertAttributeValueToTierPreference() {
    }
}
