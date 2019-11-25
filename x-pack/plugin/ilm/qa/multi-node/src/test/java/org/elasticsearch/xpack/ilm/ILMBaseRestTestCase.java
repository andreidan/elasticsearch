package org.elasticsearch.xpack.ilm;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public abstract class ILMBaseRestTestCase extends ESRestTestCase {
    static final String FAILED_STEP_RETRY_COUNT_FIELD = "failed_step_retry_count";
    static final String IS_AUTO_RETRYABLE_ERROR_FIELD = "is_auto_retryable_error";
    private static final Logger logger = LogManager.getLogger(ILMBaseRestTestCase.class);

    String index;
    String policy;

    @Before
    public void setupIndexAndPolicyNames() {
        index = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = randomAlphaOfLength(5);
    }

    void createFullPolicy(TimeValue hotTime) throws IOException {
        Map<String, LifecycleAction> hotActions = new HashMap<>();
        hotActions.put(SetPriorityAction.NAME, new SetPriorityAction(100));
        hotActions.put(RolloverAction.NAME,  new RolloverAction(null, null, 1L));
        Map<String, LifecycleAction> warmActions = new HashMap<>();
        warmActions.put(SetPriorityAction.NAME, new SetPriorityAction(50));
        warmActions.put(ForceMergeAction.NAME, new ForceMergeAction(1));
        warmActions.put(AllocateAction.NAME, new AllocateAction(1, singletonMap("_name", "integTest-1,integTest-2"), null, null));
        warmActions.put(ShrinkAction.NAME, new ShrinkAction(1));
        Map<String, LifecycleAction> coldActions = new HashMap<>();
        coldActions.put(SetPriorityAction.NAME, new SetPriorityAction(0));
        coldActions.put(AllocateAction.NAME, new AllocateAction(0, singletonMap("_name", "integTest-3"), null, null));
        Map<String, Phase> phases = new HashMap<>();
        phases.put("hot", new Phase("hot", hotTime, hotActions));
        phases.put("warm", new Phase("warm", TimeValue.ZERO, warmActions));
        phases.put("cold", new Phase("cold", TimeValue.ZERO, coldActions));
        phases.put("delete", new Phase("delete", TimeValue.ZERO, singletonMap(DeleteAction.NAME, new DeleteAction())));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
        // PUT policy
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policy);
        request.setEntity(entity);
        assertOK(client().performRequest(request));
    }

    static void updatePolicy(String indexName, String policy) throws IOException {
        Request changePolicyRequest = new Request("PUT", "/" + indexName + "/_settings");
        final StringEntity changePolicyEntity = new StringEntity("{ \"index.lifecycle.name\": \"" + policy + "\" }",
            ContentType.APPLICATION_JSON);
        changePolicyRequest.setEntity(changePolicyEntity);
        assertOK(client().performRequest(changePolicyRequest));
    }

    static void createNewSingletonPolicy(String policy, String phaseName, LifecycleAction action) throws IOException {
        createNewSingletonPolicy(policy, phaseName, action, TimeValue.ZERO);
    }

    static void createNewSingletonPolicy(String policy, String phaseName, LifecycleAction action, TimeValue after) throws IOException {
        Phase phase = new Phase(phaseName, after, singletonMap(action.getWriteableName(), action));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, singletonMap(phase.getName(), phase));
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policy);
        request.setEntity(entity);
        client().performRequest(request);
    }

    static void createIndexWithSettingsNoAlias(String index, Settings.Builder settings) throws IOException {
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings.build())
            + "}");
        client().performRequest(request);
        // wait for the shards to initialize
        ensureGreen(index);
    }

    static void createIndexWithSettings(String index, Settings.Builder settings) throws IOException {
        createIndexWithSettings(index, settings, randomBoolean());
    }

    static void createIndexWithSettings(String index, Settings.Builder settings, boolean useWriteIndex) throws IOException {
        Request request = new Request("PUT", "/" + index);

        String writeIndexSnippet = "";
        if (useWriteIndex) {
            writeIndexSnippet = "\"is_write_index\": true";
        }
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings.build())
            + ", \"aliases\" : { \"alias\": { " + writeIndexSnippet + " } } }");
        client().performRequest(request);
        // wait for the shards to initialize
        ensureGreen(index);
    }

    static void index(RestClient client, String index, String id, Object... fields) throws IOException {
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            document.field((String) fields[i], fields[i + 1]);
        }
        document.endObject();
        final Request request = new Request("POST", "/" + index + "/_doc/" + id);
        request.setJsonEntity(Strings.toString(document));
        assertOK(client.performRequest(request));
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> getOnlyIndexSettings(String index) throws IOException {
        Map<String, Object> response = (Map<String, Object>) getIndexSettings(index).get(index);
        if (response == null) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) response.get("settings");
    }

    public static StepKey getStepKeyForIndex(String indexName) throws IOException {
        Map<String, Object> indexResponse = explainIndex(indexName);
        if (indexResponse == null) {
            return new StepKey(null, null, null);
        }

        return getStepKey(indexResponse);
    }

    static StepKey getStepKey(Map<String, Object> explainIndexResponse) {
        String phase = (String) explainIndexResponse.get("phase");
        String action = (String) explainIndexResponse.get("action");
        String step = (String) explainIndexResponse.get("step");
        return new StepKey(phase, action, step);
    }

    static String getFailedStepForIndex(String indexName) throws IOException {
        Map<String, Object> indexResponse = explainIndex(indexName);
        if (indexResponse == null) return null;

        return (String) indexResponse.get("failed_step");
    }

    static Map<String, Object> explainIndex(String indexName) throws IOException {
        return explain(indexName, false, false).get(indexName);
    }

    static Map<String, Map<String, Object>> explain(String indexPattern, boolean onlyErrors,
                                                            boolean onlyManaged) throws IOException {
        Request explainRequest = new Request("GET", indexPattern + "/_ilm/explain");
        explainRequest.addParameter("only_errors", Boolean.toString(onlyErrors));
        explainRequest.addParameter("only_managed", Boolean.toString(onlyManaged));
        Response response = client().performRequest(explainRequest);
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        @SuppressWarnings("unchecked") Map<String, Map<String, Object>> indexResponse =
            ((Map<String, Map<String, Object>>) responseMap.get("indices"));
        return indexResponse;
    }

    static void indexDocument(String index) throws IOException {
        Request indexRequest = new Request("POST", index + "/_doc");
        indexRequest.setEntity(new StringEntity("{\"a\": \"test\"}", ContentType.APPLICATION_JSON));
        Response response = client().performRequest(indexRequest);
        logger.info(response.getStatusLine());
    }

    @SuppressWarnings("unchecked")
    static String getSnapshotState(String snapshot) throws IOException {
        Response response = client().performRequest(new Request("GET", "/_snapshot/repo/" + snapshot));
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        Map<String, Object> repoResponse = ((List<Map<String, Object>>) responseMap.get("responses")).get(0);
        Map<String, Object> snapResponse = ((List<Map<String, Object>>) repoResponse.get("snapshots")).get(0);
        assertThat(snapResponse.get("snapshot"), equalTo(snapshot));
        return (String) snapResponse.get("state");
    }
}
