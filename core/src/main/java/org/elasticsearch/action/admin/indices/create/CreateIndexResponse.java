/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A response for a create index action.
 */
public class CreateIndexResponse extends AcknowledgedResponse {

    private boolean timedOutWaitingForShards;

    protected CreateIndexResponse() {
    }

    protected CreateIndexResponse(boolean acknowledged, boolean timedOutWaitingForShards) {
        super(acknowledged);
        this.timedOutWaitingForShards = timedOutWaitingForShards;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readAcknowledged(in);
        timedOutWaitingForShards = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeAcknowledged(out);
        out.writeBoolean(timedOutWaitingForShards);
    }

    /**
     * Returns true if the request timed out waiting for the required number
     * of active shards.  If the index was not successfully created in the
     * first place, then this value is meaningless.
     */
    public boolean isTimedOutWaitingForShards() {
        return timedOutWaitingForShards;
    }

    public void addCustomFields(XContentBuilder builder) throws IOException {
        builder.field("timed_out_waiting_for_shards", isTimedOutWaitingForShards());
    }
}
