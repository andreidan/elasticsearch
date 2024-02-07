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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.AutoShardingResult.CURRENT_NUMBER_OF_SHARDS;
import static org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.AutoShardingResult.TARGET_NUMBER_OF_SHARDS;
import static org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.AutoShardingType.SCALE_UP;

/**
 * Condition for automatically increasing the number of shards for a data stream. The value is computed when the condition is
 * evaluated.
 */
public class AutoShardingCondition extends Condition<AutoShardingResult> {
    public static final String NAME = "auto_sharding";

    private static final ParseField WRITE_LOAD = new ParseField("write_load");

    private Double writeIndexLoad;

    public AutoShardingCondition(AutoShardingResult autoShardingResult) {
        super(NAME, Type.INTERNAL);
        this.value = autoShardingResult;
        this.writeIndexLoad = null;
    }

    public AutoShardingCondition(AutoShardingResult autoShardingResult, Double writeIndexLoad) {
        super(NAME, Type.INTERNAL);
        this.value = autoShardingResult;
        this.writeIndexLoad = writeIndexLoad;
    }

    public AutoShardingCondition(StreamInput in) throws IOException {
        super(NAME, Type.INTERNAL);
        this.value = new AutoShardingResult(in);
        this.writeIndexLoad = in.readOptionalDouble();
    }

    @Override
    public Result evaluate(final Stats stats) {
        writeIndexLoad = stats.writeIndexLoad();
        if (value.type() == SCALE_UP && value.coolDownRemaining().equals(TimeValue.ZERO)) {
            return new Result(this, true);
        } else {
            return new Result(this, false);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        value.writeTo(out);
        out.writeOptionalDouble(writeIndexLoad);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // we only save this representation in the cluster state as part of meet_conditions when this condition is met
        if (value != null && value.type().equals(SCALE_UP)) {
            builder.startObject(NAME);
            builder.field(CURRENT_NUMBER_OF_SHARDS.getPreferredName(), value.currentNumberOfShards());
            builder.field(TARGET_NUMBER_OF_SHARDS.getPreferredName(), value.targetNumberOfShards());
            builder.field(WRITE_LOAD.getPreferredName(), writeIndexLoad);
            builder.endObject();
        }
        return builder;
    }

    public static AutoShardingCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            return new AutoShardingCondition(
                new AutoShardingResult(SCALE_UP, parser.intValue(), parser.intValue(), TimeValue.ZERO),
                parser.doubleValue()
            );
        } else {
            throw new IllegalArgumentException("invalid token when parsing " + NAME + " condition: " + parser.currentToken());
        }
    }

    @Override
    boolean includedInVersion(TransportVersion version) {
        return version.onOrAfter(DataStream.ADDED_AUTO_SHARDING_EVENT_VERSION);
    }
}
