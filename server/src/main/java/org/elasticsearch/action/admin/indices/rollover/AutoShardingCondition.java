/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.AutoShardingResult;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.AutoShardingType.INCREASE_NUMBER_OF_SHARDS;

/**
 * Condition for automatically increasing the number of shards for a data stream. The value is computed when the condition is
 * evaluated.
 */
public class AutoShardingCondition extends Condition<AutoShardingResult> {
    public static final String NAME = "auto_sharding";
    private final boolean isConditionMet;

    public static final ConstructingObjectParser<AutoShardingCondition, Void> PARSER = new ConstructingObjectParser<>(
        "auto_sharding_condition",
        false,
        (args, unused) -> new AutoShardingCondition((AutoShardingResult) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> AutoShardingResult.fromXContent(p), new ParseField(NAME));
    }

    public AutoShardingCondition(AutoShardingResult autoShardingResult) {
        super(NAME, Type.AUTOMATIC);
        this.value = autoShardingResult;
        this.isConditionMet = (value.type() == INCREASE_NUMBER_OF_SHARDS && value.coolDownRemaining().equals(TimeValue.ZERO));
    }

    public AutoShardingCondition(StreamInput in) throws IOException {
        super(NAME, Type.AUTOMATIC);
        this.value = new AutoShardingResult(in);
        this.isConditionMet = (value.type() == INCREASE_NUMBER_OF_SHARDS && value.coolDownRemaining().equals(TimeValue.ZERO));
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
        value.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // we only save this representation in the cluster state as part of meet_conditions when this condition is met
        builder.field(NAME, value);
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
