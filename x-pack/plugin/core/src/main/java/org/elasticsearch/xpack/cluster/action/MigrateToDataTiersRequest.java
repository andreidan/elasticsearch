/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class MigrateToDataTiersRequest extends AcknowledgedRequest<MigrateToDataTiersRequest> {

    /**
     * Represents the name of
     */
    @Nullable
    private String nodeAttributeName;
    @Nullable
    private String legacyTemplateToDelete;

    public MigrateToDataTiersRequest(@Nullable String nodeAttributeName, @Nullable String legacyTemplateToDelete) {
        this.nodeAttributeName = nodeAttributeName;
        this.legacyTemplateToDelete = legacyTemplateToDelete;
    }

    public MigrateToDataTiersRequest(StreamInput in) throws IOException {
        nodeAttributeName = in.readOptionalString();
        legacyTemplateToDelete = in.readOptionalString();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(nodeAttributeName);
        out.writeOptionalString(legacyTemplateToDelete);
    }

    public String getNodeAttributeName() {
        return nodeAttributeName;
    }

    public String getLegacyTemplateToDelete() {
        return legacyTemplateToDelete;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MigrateToDataTiersRequest that = (MigrateToDataTiersRequest) o;
        return Objects.equals(nodeAttributeName, that.nodeAttributeName) && Objects.equals(legacyTemplateToDelete,
            that.legacyTemplateToDelete);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeAttributeName, legacyTemplateToDelete);
    }
}
