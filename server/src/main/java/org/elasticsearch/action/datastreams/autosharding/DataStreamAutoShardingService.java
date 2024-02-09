/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.datastreams.autosharding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Calculates the optimal number of shards the data stream write index should have based on the indexing load.
 */
public class DataStreamAutoShardingService {

    private static final Logger logger = LogManager.getLogger(DataStreamAutoShardingService.class);
    public static final String DATA_STREAMS_AUTO_SHARDING_ENABLED = "data_streams.auto_sharding.enabled";

    public static final NodeFeature DATA_STREAM_AUTO_SHARDING_FEATURE = new NodeFeature("data_stream.auto_sharding");

    public static final Setting<List<String>> DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING = Setting.listSetting(
        "data_streams.auto_sharding.excludes",
        List.of("*"),
        Function.identity(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Represents the minimum amount of time between two scaling events that'll increase the number of shards.
     */
    public static final Setting<TimeValue> DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN = Setting.timeSetting(
        "data_streams.auto_sharding.increase_shards.cooldown",
        TimeValue.timeValueMinutes(15),
        TimeValue.timeValueSeconds(0),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Represents the minimum amount of time between two scaling events that'll reduce the number of shards.
     */
    public static final Setting<TimeValue> DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN = Setting.timeSetting(
        "data_streams.auto_sharding.decrease_shards.cooldown",
        TimeValue.timeValueDays(3),
        TimeValue.timeValueSeconds(0),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Represents the minimum number of write threads we expect to have in the environments where auto sharding will be enabled.
     */
    public static final Setting<Integer> CLUSTER_AUTO_SHARDING_MIN_NUMBER_WRITE_THREADS = Setting.intSetting(
        "cluster.auto_sharding.min_number_of_write_threads",
        2,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Represents the maximum number of write threads we expect to have in the environments where auto sharding will be enabled.
     */
    public static final Setting<Integer> CLUSTER_AUTO_SHARDING_MAX_NUMBER_WRITE_THREADS = Setting.intSetting(
        "cluster.auto_sharding.max_number_of_write_threads",
        32,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private final ClusterService clusterService;
    private final boolean isAutoShardingEnabled;
    private final FeatureService featureService;
    private final LongSupplier nowSupplier;
    private volatile TimeValue increaseShardsCooldown;
    private volatile TimeValue reduceShardsCooldown;
    private volatile int minNumberWriteThreads;
    private volatile int maxNumberWriteThreads;
    private volatile List<String> dataStreamExcludePatterns;

    public enum AutoShardingType {
        INCREASE_NUMBER_OF_SHARDS,
        DECREASES_NUMBER_OF_SHARDS,
        NO_CHANGE_REQUIRED,
        NOT_APPLICABLE
    }

    /**
     * Represents an auto sharding recommendation. It includes the current and target number of shards together with a remaining cooldown
     * period that needs to lapse before the current recommendation should be applied.
     * <p>
     * If auto sharding is not applicable for a data stream (e.g. due to {@link #DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING}) the target
     * number of shards will be 0 and cool down remaining {@link TimeValue#MAX_VALUE}.
     */
    public record AutoShardingResult(
        AutoShardingType type,
        int currentNumberOfShards,
        int targetNumberOfShards,
        TimeValue coolDownRemaining,
        @Nullable Double writeLoad
    ) implements Writeable, ToXContentObject {

        public static final ParseField AUTO_SHARDING_TYPE = new ParseField("type");
        public static final ParseField CURRENT_NUMBER_OF_SHARDS = new ParseField("current_number_of_shards");
        public static final ParseField TARGET_NUMBER_OF_SHARDS = new ParseField("target_number_of_shards");
        public static final ParseField COOLDOWN_REMAINING = new ParseField("cool_down_remaining");
        public static final ParseField WRITE_LOAD = new ParseField("write_load");

        public AutoShardingResult(
            AutoShardingType type,
            int currentNumberOfShards,
            int targetNumberOfShards,
            TimeValue coolDownRemaining,
            @Nullable Double writeLoad
        ) {
            this.type = type;
            this.currentNumberOfShards = currentNumberOfShards;
            this.targetNumberOfShards = targetNumberOfShards;
            this.coolDownRemaining = coolDownRemaining;
            this.writeLoad = writeLoad;
        }

        public AutoShardingResult(StreamInput in) throws IOException {
            this(in.readEnum(AutoShardingType.class), in.readVInt(), in.readVInt(), in.readTimeValue(), in.readOptionalDouble());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(AUTO_SHARDING_TYPE.getPreferredName(), type);
            builder.field(CURRENT_NUMBER_OF_SHARDS.getPreferredName(), currentNumberOfShards);
            builder.field(TARGET_NUMBER_OF_SHARDS.getPreferredName(), targetNumberOfShards);
            builder.field(COOLDOWN_REMAINING.getPreferredName(), coolDownRemaining.toHumanReadableString(2));
            builder.field(WRITE_LOAD.getPreferredName(), writeLoad);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "{ type: "
                + type
                + ", currentNumberOfShards: "
                + currentNumberOfShards
                + ", targetNumberOfShards: "
                + targetNumberOfShards
                + ", coolDownRemaining: "
                + coolDownRemaining
                + ", writeLoad: "
                + writeLoad
                + " }";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(type);
            out.writeVInt(currentNumberOfShards);
            out.writeVInt(targetNumberOfShards);
            out.writeTimeValue(coolDownRemaining);
            out.writeOptionalDouble(writeLoad);
        }
    }

    public DataStreamAutoShardingService(
        Settings settings,
        ClusterService clusterService,
        FeatureService featureService,
        LongSupplier nowSupplier
    ) {
        this.clusterService = clusterService;
        this.isAutoShardingEnabled = settings.getAsBoolean(DATA_STREAMS_AUTO_SHARDING_ENABLED, false);
        this.increaseShardsCooldown = DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN.get(settings);
        this.reduceShardsCooldown = DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN.get(settings);
        this.minNumberWriteThreads = CLUSTER_AUTO_SHARDING_MIN_NUMBER_WRITE_THREADS.get(settings);
        this.maxNumberWriteThreads = CLUSTER_AUTO_SHARDING_MAX_NUMBER_WRITE_THREADS.get(settings);
        this.dataStreamExcludePatterns = DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING.get(settings);
        this.featureService = featureService;
        this.nowSupplier = nowSupplier;
    }

    public void init() {
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN, this::updateIncreaseShardsCooldown);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN, this::updateReduceShardsCooldown);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CLUSTER_AUTO_SHARDING_MIN_NUMBER_WRITE_THREADS, this::updateMinNumberWriteThreads);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CLUSTER_AUTO_SHARDING_MAX_NUMBER_WRITE_THREADS, this::updateMaxNumberWriteThreads);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING, this::updateDataStreamExcludePatterns);
    }

    /**
     * Computes the optimal number of shards for the provided data stream according to the write index's indexing load (to check if we must
     * increase the number of shards, whilst the heuristics for decreasing the number of shards _might_ use the provide write indexing
     * load).
     * The result type will indicate the recommendation of the auto sharding service :
     * - not applicable if the data stream is excluded from auto sharding as configured by {@link #DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING}
     * or if the auto sharding functionality is disabled according to {@link #DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING}, or if the
     * cluster doesn't have the feature available
     * - increase number of shards if the optimal number of shards it deems necessary for the provided data stream is GT the current number
     * of shards
     * - decrease the number of shards if the optimal number of shards it deems necessary for the provided data stream is LT the current
     * number of shards
     */
    public AutoShardingResult calculate(ClusterState state, DataStream dataStream, @Nullable Double writeIndexLoad) {
        Metadata metadata = state.metadata();
        if (isAutoShardingEnabled == false) {
            logger.debug("Data stream auto sharding service is not enabled.");
            return new AutoShardingResult(AutoShardingType.NOT_APPLICABLE, 0, 0, TimeValue.MAX_VALUE, null);
        }

        if (featureService.clusterHasFeature(state, DataStreamAutoShardingService.DATA_STREAM_AUTO_SHARDING_FEATURE) == false) {
            logger.debug(
                "Data stream auto sharding service cannot compute the optimal number of shards for data stream [{}] because the cluster "
                    + "doesn't have the auto sharding feature",
                dataStream.getName()
            );
            return new AutoShardingResult(AutoShardingType.NOT_APPLICABLE, 0, 0, TimeValue.MAX_VALUE, null);
        }

        if (dataStreamExcludePatterns.stream().anyMatch(pattern -> Regex.simpleMatch(pattern, dataStream.getName()))) {
            logger.debug(
                "Data stream [{}] is excluded from auto sharding via the [{}] setting",
                dataStream.getName(),
                DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING.getKey()
            );
            return new AutoShardingResult(AutoShardingType.NOT_APPLICABLE, 0, 0, TimeValue.MAX_VALUE, null);
        }

        if (writeIndexLoad == null) {
            logger.debug(
                "Data stream auto sharding service cannot compute the optimal number of shards for data stream [{}] as the write index "
                    + "load is not available",
                dataStream.getName()
            );
            return new AutoShardingResult(AutoShardingType.NOT_APPLICABLE, 0, 0, TimeValue.MAX_VALUE, null);
        }
        return innerCalculate(metadata, dataStream, writeIndexLoad, nowSupplier);
    }

    // visible for testing
    AutoShardingResult innerCalculate(Metadata metadata, DataStream dataStream, double writeIndexLoad, LongSupplier nowSupplier) {
        // increasing the number of shards is calculated solely based on the index load of the write index
        long optimalIncreaseShardsNumber = computeOptimalNumberOfShards(writeIndexLoad);
        IndexMetadata writeIndex = metadata.index(dataStream.getWriteIndex());
        assert writeIndex != null : "the data stream write index must exist in the provided cluster metadata";
        TimeValue timeSinceLastAutoShardingEvent = dataStream.getAutoShardingEvent() != null
            ? dataStream.getAutoShardingEvent().getTimeSinceLastAutoShardingEvent(nowSupplier)
            : TimeValue.MAX_VALUE;

        if (optimalIncreaseShardsNumber > writeIndex.getNumberOfShards()) {
            // check cooldown for increasing the number of shards
            TimeValue remainingCoolDown = TimeValue.timeValueMillis(
                Math.max(0L, increaseShardsCooldown.millis() - timeSinceLastAutoShardingEvent.millis())
            );

            return new AutoShardingResult(
                AutoShardingType.INCREASE_NUMBER_OF_SHARDS,
                writeIndex.getNumberOfShards(),
                Math.toIntExact(optimalIncreaseShardsNumber),
                remainingCoolDown,
                writeIndexLoad
            );
        }

        double maxIndexLoadWithinCoolingPeriod = getMaxIndexLoadWithinCoolingPeriod(metadata, dataStream, writeIndexLoad, nowSupplier);
        long targetReducedNumberOfShards = computeOptimalNumberOfShards(maxIndexLoadWithinCoolingPeriod);

        if (targetReducedNumberOfShards < writeIndex.getNumberOfShards()) {
            // we should reduce the number of shards
            return new AutoShardingResult(
                AutoShardingType.DECREASES_NUMBER_OF_SHARDS,
                writeIndex.getNumberOfShards(),
                Math.toIntExact(targetReducedNumberOfShards),
                TimeValue.timeValueMillis(Math.max(0L, reduceShardsCooldown.millis() - timeSinceLastAutoShardingEvent.millis())),
                maxIndexLoadWithinCoolingPeriod
            );
        }

        return new AutoShardingResult(
            AutoShardingType.NO_CHANGE_REQUIRED,
            writeIndex.getNumberOfShards(),
            writeIndex.getNumberOfShards(),
            TimeValue.ZERO,
            writeIndexLoad
        );
    }

    private long computeOptimalNumberOfShards(double indexingLoad) {
        return Math.max(
            Math.min(Math.round(indexingLoad / ((double) minNumberWriteThreads / 2)), 3),
            Math.round(indexingLoad / ((double) maxNumberWriteThreads / 2))
        );
    }

    private double getMaxIndexLoadWithinCoolingPeriod(
        Metadata metadata,
        DataStream dataStream,
        double writeIndexLoad,
        LongSupplier nowSupplier
    ) {
        // for reducing the number of shards we look at more than just the write index
        List<IndexWriteLoad> writeLoadsWithinCoolingPeriod = getIndicesCreatedWithin(
            metadata,
            dataStream,
            reduceShardsCooldown,
            nowSupplier
        ).stream()
            .filter(index -> index.equals(dataStream.getWriteIndex()) == false)
            .map(metadata::index)
            .filter(Objects::nonNull)
            .map(IndexMetadata::getStats)
            .filter(Objects::nonNull)
            .map(IndexMetadataStats::writeLoad)
            .filter(Objects::nonNull)
            .toList();

        // assume the current write index load is the highest observed and look back to find the actual maximum
        double maxIndexLoadWithinCoolingPeriod = writeIndexLoad;
        for (IndexWriteLoad writeLoad : writeLoadsWithinCoolingPeriod) {
            double totalIndexLoad = 0;
            for (int shardId = 0; shardId < writeLoad.numberOfShards(); shardId++) {
                final OptionalDouble writeLoadForShard = writeLoad.getWriteLoadForShard(shardId);
                if (writeLoadForShard.isPresent()) {
                    totalIndexLoad += writeLoadForShard.getAsDouble();
                }
            }

            if (totalIndexLoad > maxIndexLoadWithinCoolingPeriod) {
                maxIndexLoadWithinCoolingPeriod = totalIndexLoad;
            }
        }
        return maxIndexLoadWithinCoolingPeriod;
    }

    // Visible for testing
    List<Index> getIndicesCreatedWithin(Metadata metadata, DataStream dataStream, TimeValue timeframe, LongSupplier nowSupplier) {
        final List<Index> dataStreamIndices = dataStream.getIndices();
        final long currentTimeMillis = nowSupplier.getAsLong();
        // Consider at least 1 index (including the write index) for cases where rollovers happen less often than timeframe
        int firstIndexWithinAgeRange = Math.max(dataStreamIndices.size() - 2, 0);
        for (int i = 0; i < dataStreamIndices.size(); i++) {
            Index index = dataStreamIndices.get(i);
            final IndexMetadata indexMetadata = metadata.index(index);
            final long indexAge = currentTimeMillis - indexMetadata.getCreationDate();
            if (indexAge < timeframe.getMillis()) {
                // We need to consider the previous index too in order to cover the entire max-index-age range.
                firstIndexWithinAgeRange = i == 0 ? 0 : i - 1;
                break;
            }
        }
        return dataStreamIndices.subList(firstIndexWithinAgeRange, dataStreamIndices.size());
    }

    void updateIncreaseShardsCooldown(TimeValue scaleUpCooldown) {
        this.increaseShardsCooldown = scaleUpCooldown;
    }

    void updateReduceShardsCooldown(TimeValue scaleDownCooldown) {
        this.reduceShardsCooldown = scaleDownCooldown;
    }

    void updateMinNumberWriteThreads(int minNumberWriteThreads) {
        this.minNumberWriteThreads = minNumberWriteThreads;
    }

    void updateMaxNumberWriteThreads(int maxNumberWriteThreads) {
        this.maxNumberWriteThreads = maxNumberWriteThreads;
    }

    private void updateDataStreamExcludePatterns(List<String> newExcludePatterns) {
        this.dataStreamExcludePatterns = newExcludePatterns;
    }
}
