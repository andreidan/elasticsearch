/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.Phase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_PREFER;

public final class MetadataMigrateToDataTiersRoutingService {

    private static final Logger logger = LogManager.getLogger(MetadataMigrateToDataTiersRoutingService.class);

    private MetadataMigrateToDataTiersRoutingService() {
    }

    public static Tuple<ClusterState, MigratedEntities> migrateToDataTiersRouting(ClusterState currentState,
                                                                                  String nodeAttrName,
                                                                                  @Nullable String globalIndexTemplateToDelete) {
        Metadata.Builder mb = Metadata.builder(currentState.metadata());

        String removedIndexTemplateName = null;
        if (Strings.isNullOrEmpty(globalIndexTemplateToDelete) == false &&
            currentState.metadata().getTemplates().containsKey(globalIndexTemplateToDelete)) {
            mb.removeIndexTemplate(globalIndexTemplateToDelete);
            logger.debug("removing template [{}]", globalIndexTemplateToDelete);
            removedIndexTemplateName = globalIndexTemplateToDelete;
        }

        IndexLifecycleMetadata currentLifecycleMetadata = currentState.metadata().custom(IndexLifecycleMetadata.TYPE);
        List<String> migratedPolicies = migrateIlmPolicies(mb, currentLifecycleMetadata, nodeAttrName);
        List<String> migratedIndices = migrateIndices(mb, currentState, nodeAttrName);
        return Tuple.tuple(ClusterState.builder(currentState).metadata(mb).build(),
            new MigratedEntities(removedIndexTemplateName, migratedIndices, migratedPolicies));
    }

    static List<String> migrateIlmPolicies(Metadata.Builder mb, IndexLifecycleMetadata currentMetadata, String nodeAttrName) {
        List<String> migratedPolicies = new ArrayList<>();
        if (currentMetadata != null) {
            Map<String, LifecyclePolicyMetadata> currentPolicies = currentMetadata.getPolicyMetadatas();
            SortedMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(currentPolicies);
            for (Map.Entry<String, LifecyclePolicyMetadata> policyMetadataEntry : currentPolicies.entrySet()) {
                LifecyclePolicy lifecyclePolicy = policyMetadataEntry.getValue().getPolicy();
                LifecyclePolicy updatedLifecylePolicy = null;
                for (Map.Entry<String, Phase> phaseEntry : lifecyclePolicy.getPhases().entrySet()) {
                    Phase phase = phaseEntry.getValue();
                    AllocateAction allocateAction = (AllocateAction) phase.getActions().get(AllocateAction.NAME);
                    if (allocateActionDefinesRoutingRules(nodeAttrName, allocateAction)) {
                        Map<String, LifecycleAction> actionMap = new HashMap<>(phase.getActions());
                        // this phase contains an allocate action that defines a require rule for the attribute name so we'll remove all the
                        // rules to allow for the migrate action to be injected
                        if (allocateAction.getNumberOfReplicas() != null) {
                            // keep the number of replicas configuration
                            AllocateAction updatedAllocateAction =
                                new AllocateAction(allocateAction.getNumberOfReplicas(), null, null, null);
                            actionMap.put(allocateAction.getWriteableName(), updatedAllocateAction);
                            logger.debug("ILM policy [{}], phase [{}]: updated the allocate action to [{}]", lifecyclePolicy.getName(),
                                phase.getName(), allocateAction);
                        } else {
                            // remove the action altogether
                            actionMap.remove(allocateAction.getWriteableName());
                            logger.debug("ILM policy [{}], phase [{}]: removed the allocate action", lifecyclePolicy.getName(),
                                phase.getName());
                        }

                        Phase updatedPhase = new Phase(phase.getName(), phase.getMinimumAge(), actionMap);
                        Map<String, Phase> updatedPhases =
                            new HashMap<>(updatedLifecylePolicy == null ? lifecyclePolicy.getPhases() : updatedLifecylePolicy.getPhases());
                        updatedPhases.put(phaseEntry.getKey(), updatedPhase);
                        updatedLifecylePolicy = new LifecyclePolicy(lifecyclePolicy.getName(), updatedPhases);
                    }
                }

                if (updatedLifecylePolicy != null) {
                    // we updated at least one phase
                    long nextVersion = policyMetadataEntry.getValue().getVersion() + 1L;
                    LifecyclePolicyMetadata newPolicyMetadata = new LifecyclePolicyMetadata(updatedLifecylePolicy,
                        policyMetadataEntry.getValue().getHeaders(), nextVersion, Instant.now().toEpochMilli());
                    LifecyclePolicyMetadata oldPolicyMetadata = newPolicies.put(policyMetadataEntry.getKey(), newPolicyMetadata);
                    assert oldPolicyMetadata != null :
                        "we must only update policies, not create new ones, but " + policyMetadataEntry.getKey() + " didn't exist";

                    // TODO updateIndicesForPolicy

                    migratedPolicies.add(policyMetadataEntry.getKey());
                }
            }

            IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentMetadata.getOperationMode());
            mb.putCustom(IndexLifecycleMetadata.TYPE, newMetadata);
        }
        return migratedPolicies;
    }

    static boolean allocateActionDefinesRoutingRules(String nodeAttrName, @Nullable AllocateAction allocateAction) {
        return allocateAction != null && (allocateAction.getRequire().get(nodeAttrName) != null ||
            allocateAction.getInclude().get(nodeAttrName) != null ||
            allocateAction.getRequire().get(nodeAttrName) != null);
    }

    static List<String> migrateIndices(Metadata.Builder mb, ClusterState currentState, String nodeAttrName) {
        List<String> migratedIndices = new ArrayList<>();
        String nodeAttrIndexRoutingSetting = INDEX_ROUTING_REQUIRE_GROUP_PREFIX + nodeAttrName;
        for (ObjectObjectCursor<String, IndexMetadata> index : currentState.metadata().indices()) {
            IndexMetadata indexMetadata = index.value;
            Settings currentIndexSettings = indexMetadata.getSettings();
            if (currentIndexSettings.keySet().contains(nodeAttrIndexRoutingSetting)) {
                // look at the value, get the correct tiers config and update the settings and index metadata
                Settings.Builder newSettingsBuilder = Settings.builder().put(currentIndexSettings);
                String indexName = indexMetadata.getIndex().getName();
                if (currentIndexSettings.keySet().contains(INDEX_ROUTING_PREFER) == false) {
                    // parse the custom attribute routing into the corresponding tier preference and configure it
                    String attributeValue = currentIndexSettings.get(nodeAttrIndexRoutingSetting);
                    String convertedTierPreference = convertAttributeValueToTierPreference(attributeValue);
                    if (convertedTierPreference != null) {
                        newSettingsBuilder.put(INDEX_ROUTING_PREFER, convertedTierPreference);
                        newSettingsBuilder.remove(nodeAttrIndexRoutingSetting);
                        logger.debug("index [{}]: removed setting [{}]", indexName, nodeAttrIndexRoutingSetting);
                        logger.debug("index [{}]: configured setting [{}] to [{}]", indexName,
                            INDEX_ROUTING_PREFER, convertedTierPreference);
                    } else {
                        // log warning and do not remove setting - include this in a list of indices "with problems"
                        logger.warn("index [{}]: could not convert attribute based setting [{}] value of [{}] to a tier preference " +
                                "configuration. the only known values are: {}", indexName,
                            nodeAttrIndexRoutingSetting, attributeValue, "hot,warm,cold, and frozen");
                        continue;
                    }
                } else {
                    newSettingsBuilder.remove(nodeAttrIndexRoutingSetting);
                    logger.debug("index [{}]: removed setting [{}]", indexName, nodeAttrIndexRoutingSetting);
                }

                Settings newSettings = newSettingsBuilder.build();
                if (currentIndexSettings.equals(newSettings) == false) {
                    mb.put(IndexMetadata.builder(indexMetadata)
                        .settings(newSettings)
                        .settingsVersion(indexMetadata.getSettingsVersion() + 1));
                    migratedIndices.add(indexName);
                }
            }
        }
        return migratedIndices;
    }

    /**
     * Converts the provided node attribute value to the corresponding `_tier_preference` configuration.
     * Known (and convertible) attribute values are:
     * * hot
     * * warm
     * * cold
     * * frozen
     * and the corresponding tier preference setting values are, respectively:
     * * data_hot
     * * data_warm,data_hot
     * * data_cold,data_warm,data_hot
     * * data_frozen,data_cold,data_warm,data_hot
     * <p>
     * This returns `null` if an unknown attribute value is received.
     */
    @Nullable
    static String convertAttributeValueToTierPreference(String nodeAttributeValue) {
        String targetTier = "data_" + nodeAttributeValue;
        if (DataTier.validTierName(targetTier) == false) {
            return null;
        }
        return DataTier.getPreferredTiersConfiguration(targetTier);
    }

    public static final class MigratedEntities {

        @Nullable
        public final String removedIndexTemplateName;
        public final List<String> migratedIndices;
        public final List<String> migratedPolicies;

        public MigratedEntities(@Nullable String removedIndexTemplateName, List<String> migratedIndices, List<String> migratedPolicies) {
            this.removedIndexTemplateName = removedIndexTemplateName;
            this.migratedIndices = migratedIndices;
            this.migratedPolicies = migratedPolicies;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MigratedEntities that = (MigratedEntities) o;
            return Objects.equals(removedIndexTemplateName, that.removedIndexTemplateName) &&
                Objects.equals(migratedIndices, that.migratedIndices) &&
                Objects.equals(migratedPolicies, that.migratedPolicies);
        }

        @Override
        public int hashCode() {
            return Objects.hash(removedIndexTemplateName, migratedIndices, migratedPolicies);
        }
    }
}
