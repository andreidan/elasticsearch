/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.NodesDeprecationCheckAction;
import org.elasticsearch.xpack.core.deprecation.NodesDeprecationCheckRequest;

public class TransportDeprecationInfoAction extends TransportMasterNodeReadAction<DeprecationInfoAction.Request,
        DeprecationInfoAction.Response> {
    private static final List<DeprecationChecker> PLUGIN_CHECKERS = Arrays.asList(new MlDeprecationChecker());
    private static final Logger logger = LogManager.getLogger(TransportDeprecationInfoAction.class);

    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings settings;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportDeprecationInfoAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver,
                                          NodeClient client, NamedXContentRegistry xContentRegistry) {
        super(DeprecationInfoAction.NAME, transportService, clusterService, threadPool, actionFilters, DeprecationInfoAction.Request::new,
                indexNameExpressionResolver, DeprecationInfoAction.Response::new, ThreadPool.Names.GENERIC);
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settings = settings;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected ClusterBlockException checkBlock(DeprecationInfoAction.Request request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected final void masterOperation(final DeprecationInfoAction.Request request, ClusterState state,
                                         final ActionListener<DeprecationInfoAction.Response> listener) {
        NodesDeprecationCheckRequest nodeDepReq = new NodesDeprecationCheckRequest("_all");
        ClientHelper.executeAsyncWithOrigin(client, ClientHelper.DEPRECATION_ORIGIN,
            NodesDeprecationCheckAction.INSTANCE, nodeDepReq,
            ActionListener.wrap(response -> {
            if (response.hasFailures()) {
                List<String> failedNodeIds = response.failures().stream()
                    .map(failure -> failure.nodeId() + ": " + failure.getMessage())
                    .collect(Collectors.toList());
                logger.warn("nodes failed to run deprecation checks: {}", failedNodeIds);
                for (FailedNodeException failure : response.failures()) {
                    logger.debug("node {} failed to run deprecation checks: {}", failure.nodeId(), failure);
                }
            }

            DeprecationChecker.Components components = new DeprecationChecker.Components(
                xContentRegistry,
                settings,
                new OriginSettingClient(client, ClientHelper.DEPRECATION_ORIGIN)
            );
            pluginSettingIssues(PLUGIN_CHECKERS, components, ActionListener.wrap(
                deprecationIssues -> {
                    final DeprecationInfoAction.Response finalResponse;
                    try (ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().newStoredContext(false)) {
                        // We store the context here and drop any new response headers to prevent getting a deprecation warning on the
                        // deprecation info API call when we resolve indices.
                        finalResponse = DeprecationInfoAction.Response.from(state, indexNameExpressionResolver,
                            request, response, INDEX_SETTINGS_CHECKS, CLUSTER_SETTINGS_CHECKS, deprecationIssues);
                    }
                    listener.onResponse(finalResponse);
                },
                listener::onFailure
            ));

        }, listener::onFailure));
    }

    static void pluginSettingIssues(List<DeprecationChecker> checkers,
                                    DeprecationChecker.Components components,
                                    ActionListener<Map<String, List<DeprecationIssue>>> listener) {
        List<DeprecationChecker> enabledCheckers = checkers
            .stream()
            .filter(c -> c.enabled(components.settings()))
            .collect(Collectors.toList());
        if (enabledCheckers.isEmpty()) {
            listener.onResponse(Collections.emptyMap());
            return;
        }
        GroupedActionListener<DeprecationChecker.CheckResult> groupedActionListener = new GroupedActionListener<>(ActionListener.wrap(
            checkResults -> listener.onResponse(checkResults
                    .stream()
                    .collect(Collectors.toMap(DeprecationChecker.CheckResult::getCheckerName, DeprecationChecker.CheckResult::getIssues))),
            listener::onFailure
        ), enabledCheckers.size());
        for(DeprecationChecker checker : checkers) {
            checker.check(components, groupedActionListener);
        }
    }

}
