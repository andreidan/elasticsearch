/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.List;
import org.elasticsearch.core.Set;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.junit.Assert;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;

public class TransportNodeDeprecationCheckActionTests extends ESTestCase {

    public void testNodeOperation() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put("some.deprecated.property", "someValue1");
        settingsBuilder.put("some.other.bad.deprecated.property", "someValue2");
        settingsBuilder.put("some.undeprecated.property", "someValue3");
        settingsBuilder.putList("some.undeprecated.list.property", List.of("someValue4", "someValue5"));
        settingsBuilder.putList(
            DeprecationChecks.SKIP_DEPRECATIONS_SETTING.getKey(),
            List.of("some.deprecated.property", "some.other.*.deprecated.property", "some.bad.dynamic.property")
        );
        Settings nodeSettings = settingsBuilder.build();
        settingsBuilder = Settings.builder();
        settingsBuilder.put("some.bad.dynamic.property", "someValue1");
        Settings dynamicSettings = settingsBuilder.build();
        ThreadPool threadPool = null;
        final XPackLicenseState licenseState = null;
        Metadata metadata = Metadata.builder().transientSettings(dynamicSettings).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, Set.of(DeprecationChecks.SKIP_DEPRECATIONS_SETTING));
        Mockito.when((clusterService.getClusterSettings())).thenReturn(clusterSettings);
        DiscoveryNode node = Mockito.mock(DiscoveryNode.class);
        TransportService transportService = Mockito.mock(TransportService.class);
        Mockito.when(transportService.getLocalNode()).thenReturn(node);
        PluginsService pluginsService = Mockito.mock(PluginsService.class);
        ActionFilters actionFilters = Mockito.mock(ActionFilters.class);
        TransportNodeDeprecationCheckAction transportNodeDeprecationCheckAction = new TransportNodeDeprecationCheckAction(
            nodeSettings,
            threadPool,
            licenseState,
            clusterService,
            transportService,
            pluginsService,
            actionFilters
        );
        NodesDeprecationCheckAction.NodeRequest nodeRequest = null;
        AtomicReference<Settings> visibleNodeSettings = new AtomicReference<>();
        AtomicReference<Settings> visibleClusterStateMetadataSettings = new AtomicReference<>();
        DeprecationChecks.NodeDeprecationCheck<
            Settings,
            PluginsAndModules,
            ClusterState,
            XPackLicenseState,
            DeprecationIssue> nodeSettingCheck = (settings, p, clusterState1, l) -> {
                visibleNodeSettings.set(settings);
                visibleClusterStateMetadataSettings.set(clusterState1.getMetadata().settings());
                return null;
            };
        java.util.List<
            DeprecationChecks.NodeDeprecationCheck<
                Settings,
                PluginsAndModules,
                ClusterState,
                XPackLicenseState,
                DeprecationIssue>> nodeSettingsChecks = List.of(nodeSettingCheck);
        transportNodeDeprecationCheckAction.nodeOperation(nodeRequest, nodeSettingsChecks);
        settingsBuilder = Settings.builder();
        settingsBuilder.put("some.undeprecated.property", "someValue3");
        settingsBuilder.putList("some.undeprecated.list.property", List.of("someValue4", "someValue5"));
        settingsBuilder.putList(
            DeprecationChecks.SKIP_DEPRECATIONS_SETTING.getKey(),
            List.of("some.deprecated.property", "some.other.*.deprecated.property", "some.bad.dynamic.property")
        );
        Settings expectedSettings = settingsBuilder.build();
        Assert.assertNotNull(visibleNodeSettings.get());
        Assert.assertEquals(expectedSettings, visibleNodeSettings.get());
        Assert.assertNotNull(visibleClusterStateMetadataSettings.get());
        Assert.assertEquals(Settings.EMPTY, visibleClusterStateMetadataSettings.get());

        // Testing that the setting is dynamically updatable:
        Settings newSettings = Settings.builder()
            .putList(DeprecationChecks.SKIP_DEPRECATIONS_SETTING.getKey(), List.of("some.undeprecated.property"))
            .build();
        clusterSettings.applySettings(newSettings);
        transportNodeDeprecationCheckAction.nodeOperation(nodeRequest, nodeSettingsChecks);
        settingsBuilder = Settings.builder();
        settingsBuilder.put("some.deprecated.property", "someValue1");
        settingsBuilder.put("some.other.bad.deprecated.property", "someValue2");
        settingsBuilder.putList("some.undeprecated.list.property", List.of("someValue4", "someValue5"));
        // This is the node setting (since this is the node deprecation check), not the cluster setting:
        settingsBuilder.putList(
            DeprecationChecks.SKIP_DEPRECATIONS_SETTING.getKey(),
            List.of("some.deprecated.property", "some.other.*.deprecated.property", "some.bad.dynamic.property")
        );
        expectedSettings = settingsBuilder.build();
        Assert.assertNotNull(visibleNodeSettings.get());
        Assert.assertEquals(expectedSettings, visibleNodeSettings.get());
        Assert.assertNotNull(visibleClusterStateMetadataSettings.get());
        Assert.assertEquals(
            Settings.builder().put("some.bad.dynamic.property", "someValue1").build(),
            visibleClusterStateMetadataSettings.get()
        );
    }

}
