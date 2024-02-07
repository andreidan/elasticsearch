/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.LongSupplier;

/**
 * Represents the last auto sharding event that occured for a data stream.
 */
public record DataStreamAutoShardingEvent(String triggerIndexName, long generation, int targetNumberOfShards, long timestamp)
    implements
        SimpleDiffable<DataStreamAutoShardingEvent>,
        ToXContentFragment {

    public static final ParseField TRIGGER_DATA_STREAM_GENERATION = new ParseField("trigger_generation");
    public static final ParseField TRIGGER_INDEX_NAME = new ParseField("trigger_index_name");
    public static final ParseField TARGET_NUMBER_OF_SHARDS = new ParseField("target_number_of_shards");
    public static final ParseField EVENT_TIMESTAMP = new ParseField("event_timestamp");

    public static final ConstructingObjectParser<DataStreamAutoShardingEvent, Void> PARSER = new ConstructingObjectParser<>(
        "auto_sharding",
        false,
        (args, unused) -> new DataStreamAutoShardingEvent((String) args[0], (long) args[1], (int) args[2], (long) args[3])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TRIGGER_INDEX_NAME);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TRIGGER_DATA_STREAM_GENERATION);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), TARGET_NUMBER_OF_SHARDS);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), EVENT_TIMESTAMP);
    }

    public static DataStreamAutoShardingEvent fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    DataStreamAutoShardingEvent(StreamInput in) throws IOException {
        this(in.readString(), in.readVLong(), in.readVInt(), in.readVLong());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TRIGGER_DATA_STREAM_GENERATION.getPreferredName(), generation);
        builder.field(TRIGGER_INDEX_NAME.getPreferredName(), triggerIndexName);
        builder.field(TARGET_NUMBER_OF_SHARDS.getPreferredName(), targetNumberOfShards);
        builder.field(EVENT_TIMESTAMP.getPreferredName(), timestamp);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(triggerIndexName);
        out.writeVLong(generation);
        out.writeVInt(targetNumberOfShards);
        out.writeVLong(timestamp);
    }

    public TimeValue getTimeSinceLastAutoShardingEvent(LongSupplier now) {
        return TimeValue.timeValueMillis(Math.max(0L, now.getAsLong() - timestamp));
    }
}
