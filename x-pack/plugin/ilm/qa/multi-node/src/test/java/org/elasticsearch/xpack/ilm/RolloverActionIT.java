/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class RolloverActionIT extends ILMBaseRestTestCase {

    public void testRolloverAction() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias"));

        // create policy
        createNewSingletonPolicy(policy, "hot", new RolloverAction(null, null, 1L));
        // update policy on index
        updatePolicy(originalIndex, policy);
        // index document {"foo": "bar"} to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");
        assertBusy(() -> assertTrue(indexExists(secondIndex)));
        assertBusy(() -> assertTrue(indexExists(originalIndex)));
        assertBusy(() -> assertEquals("true", getOnlyIndexSettings(originalIndex).get(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE)));
    }

    public void testRolloverActionWithIndexingComplete() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias"));

        Request updateSettingsRequest = new Request("PUT", "/" + originalIndex + "/_settings");
        updateSettingsRequest.setJsonEntity("{\n" +
            "  \"settings\": {\n" +
            "    \"" + LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE + "\": true\n" +
            "  }\n" +
            "}");
        client().performRequest(updateSettingsRequest);
        Request updateAliasRequest = new Request("POST", "/_aliases");
        updateAliasRequest.setJsonEntity("{\n" +
            "  \"actions\": [\n" +
            "    {\n" +
            "      \"add\": {\n" +
            "        \"index\": \"" + originalIndex + "\",\n" +
            "        \"alias\": \"alias\",\n" +
            "        \"is_write_index\": false\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}");
        client().performRequest(updateAliasRequest);

        // create policy
        createNewSingletonPolicy(policy, "hot", new RolloverAction(null, null, 1L));
        // update policy on index
        updatePolicy(originalIndex, policy);
        // index document {"foo": "bar"} to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");
        assertBusy(() -> assertEquals(TerminalPolicyStep.KEY, getStepKeyForIndex(originalIndex)));
        assertBusy(() -> assertTrue(indexExists(originalIndex)));
        assertBusy(() -> assertFalse(indexExists(secondIndex)));
        assertBusy(() -> assertEquals("true", getOnlyIndexSettings(originalIndex).get(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE)));
    }

    public void testILMRolloverRetriesOnReadOnlyBlock() throws Exception {
        String firstIndex = index + "-000001";

        createNewSingletonPolicy(policy, "hot", new RolloverAction(null, TimeValue.timeValueSeconds(1), null));

        // create the index as readonly and associate the ILM policy to it
        createIndexWithSettings(
            firstIndex,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias")
                .put("index.blocks.read_only", true),
            true
        );

        // wait for ILM to start retrying the step
        assertBusy(() -> assertThat((Integer) explainIndex(firstIndex).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1)));

        // remove the read only block
        Request allowWritesOnIndexSettingUpdate = new Request("PUT", firstIndex + "/_settings");
        allowWritesOnIndexSettingUpdate.setJsonEntity("{" +
            "  \"index\": {\n" +
            "     \"blocks.read_only\" : \"false\" \n" +
            "  }\n" +
            "}");
        client().performRequest(allowWritesOnIndexSettingUpdate);

        // index is not readonly so the ILM should complete successfully
        assertBusy(() -> assertThat(getStepKeyForIndex(firstIndex), equalTo(TerminalPolicyStep.KEY)));
    }

    public void testILMRolloverRetriesIfRolloverIndexAlreadyExistsUntilIndexIsDeleted() throws Exception {
        String firstIndex = index + "-000001";
        String secondIndex = index + "-000002";

        createNewSingletonPolicy(policy, "hot", new RolloverAction(null, TimeValue.timeValueSeconds(1), null));

        // create the index
        createIndexWithSettings(
            firstIndex,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias"),
            true
        );

        createIndexWithSettings(
            secondIndex,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias"),
            false
        );

        // wait for ILM to start retrying the step
        assertBusy(() -> {
            Map<String, Object> explainIndex = explainIndex(firstIndex);
            assertThat((Integer) explainIndex.get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1));
        });

        deleteIndex(secondIndex);

        // the rollover step should now succeed
        assertBusy(() -> {
            assertThat(getStepKeyForIndex(firstIndex), equalTo(TerminalPolicyStep.KEY));
        });
    }

    public void testILMRolloverOnManuallyRolledIndex() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        String thirdIndex = index + "-000003";

        // Set up a policy with rollover
        createNewSingletonPolicy(policy, "hot", new RolloverAction(null, null, 2L));
        Request createIndexTemplate = new Request("PUT", "_template/rolling_indexes");
        createIndexTemplate.setJsonEntity("{" +
            "\"index_patterns\": [\""+ index + "-*\"], \n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": 1,\n" +
            "    \"number_of_replicas\": 0,\n" +
            "    \"index.lifecycle.name\": \"" + policy+ "\", \n" +
            "    \"index.lifecycle.rollover_alias\": \"alias\"\n" +
            "  }\n" +
            "}");
        client().performRequest(createIndexTemplate);

        createIndexWithSettings(
            originalIndex,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0),
            true
        );

        // Index a document
        index(client(), originalIndex, "1", "foo", "bar");
        Request refreshOriginalIndex = new Request("POST", "/" + originalIndex + "/_refresh");
        client().performRequest(refreshOriginalIndex);

        // Manual rollover
        Request rolloverRequest = new Request("POST", "/alias/_rollover");
        rolloverRequest.setJsonEntity("{\n" +
            "  \"conditions\": {\n" +
            "    \"max_docs\": \"1\"\n" +
            "  }\n" +
            "}"
        );
        client().performRequest(rolloverRequest);
        assertBusy(() -> assertTrue(indexExists(secondIndex)));

        // Index another document into the original index so the ILM rollover policy condition is met
        index(client(), originalIndex, "2", "foo", "bar");
        client().performRequest(refreshOriginalIndex);

        // Wait for the rollover policy to execute
        assertBusy(() -> assertThat(getStepKeyForIndex(originalIndex), equalTo(TerminalPolicyStep.KEY)));

        // ILM should manage the second index after attempting (and skipping) rolling the original index
        assertBusy(() -> assertTrue((boolean) explainIndex(secondIndex).getOrDefault("managed", true)));

        // index some documents to trigger an ILM rollover
        index(client(), "alias", "1", "foo", "bar");
        index(client(), "alias", "2", "foo", "bar");
        index(client(), "alias", "3", "foo", "bar");
        Request refreshSecondIndex = new Request("POST", "/" + secondIndex + "/_refresh");
        client().performRequest(refreshSecondIndex).getStatusLine();

        // ILM should rollover the second index even though it skipped the first one
        assertBusy(() -> assertThat(getStepKeyForIndex(secondIndex), equalTo(TerminalPolicyStep.KEY)));
        assertBusy(() -> assertTrue(indexExists(thirdIndex)));
    }
}
