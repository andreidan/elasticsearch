/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.INCREASE_SHARDS;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Condition for automatically increasing the number of shards for a data stream. The value is computed when the condition is
 * evaluated.
 */
public class AutoShardingCondition extends Condition<IncreaseShardsCondition> {
    public static final String NAME = "auto_sharding";

    public static final ParseField AUTO_SHARDING_TYPE = new ParseField("type");
    public static final ParseField CURRENT_NUMBER_OF_SHARDS = new ParseField("current_number_of_shards");
    public static final ParseField TARGET_NUMBER_OF_SHARDS = new ParseField("target_number_of_shards");
    public static final ParseField COOLDOWN_REMAINING = new ParseField("cool_down_remaining");
    public static final ParseField WRITE_LOAD = new ParseField("write_load");

    public static final ConstructingObjectParser<AutoShardingCondition, Void> PARSER = new ConstructingObjectParser<>(
        "auto_sharding_condition",
        false,
        (args, unused) -> new AutoShardingCondition(
            new IncreaseShardsCondition((Integer) args[1], (Integer) args[2], (TimeValue) args[3], (Double) args[4])
        )
    );

    static {
        PARSER.declareString(constructorArg(), AUTO_SHARDING_TYPE);
        PARSER.declareInt(constructorArg(), CURRENT_NUMBER_OF_SHARDS);
        PARSER.declareInt(constructorArg(), TARGET_NUMBER_OF_SHARDS);
        PARSER.declareString(
            constructorArg(),
            value -> TimeValue.parseTimeValue(value, COOLDOWN_REMAINING.getPreferredName()),
            COOLDOWN_REMAINING
        );
        PARSER.declareDouble(optionalConstructorArg(), WRITE_LOAD);
    }

    private final boolean isConditionMet;

    public AutoShardingCondition(IncreaseShardsCondition increaseShardsCondition) {
        super(NAME, Type.AUTOMATIC);
        this.value = increaseShardsCondition;
        this.isConditionMet = increaseShardsCondition.isConditionMet();
    }

    public AutoShardingCondition(StreamInput in) throws IOException {
        super(NAME, Type.AUTOMATIC);
        this.value = new IncreaseShardsCondition(in.readVInt(), in.readVInt(), in.readTimeValue(), in.readOptionalDouble());
        this.isConditionMet = value.isConditionMet();
    }

    @Override
    public Result evaluate(final Stats stats) {
        return new Result(this, isConditionMet);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(value.currentNumberOfShards());
        out.writeVInt(value.targetNumberOfShards());
        out.writeTimeValue(value.coolDownRemaining());
        out.writeOptionalDouble(value.writeLoad());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // we only save this representation in the cluster state as part of meet_conditions when this condition is met
        builder.startObject(NAME);
        builder.field(AUTO_SHARDING_TYPE.getPreferredName(), INCREASE_SHARDS);
        builder.field(CURRENT_NUMBER_OF_SHARDS.getPreferredName(), value.currentNumberOfShards());
        builder.field(TARGET_NUMBER_OF_SHARDS.getPreferredName(), value.targetNumberOfShards());
        builder.field(COOLDOWN_REMAINING.getPreferredName(), value.coolDownRemaining().toHumanReadableString(2));
        builder.field(WRITE_LOAD.getPreferredName(), value.writeLoad());
        builder.endObject();
        return builder;
    }

    public static AutoShardingCondition fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    boolean includedInVersion(TransportVersion version) {
        return version.onOrAfter(DataStream.ADDED_AUTO_SHARDING_EVENT_VERSION);
    }
}
