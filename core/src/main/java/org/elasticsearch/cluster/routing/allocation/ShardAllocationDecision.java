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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * This class is a container for the various decisions took, if any, for where to
 * allocate or move a single shard.
 */
public final class ShardAllocationDecision implements ToXContent, Writeable {
    @Nullable
    private final AllocateUnassignedDecision allocateDecision;
    @Nullable
    private final MoveDecision moveDecision;
    @Nullable
    private final RebalanceDecision rebalanceDecision;

    public ShardAllocationDecision(@Nullable AllocateUnassignedDecision allocateDecision,
                                   @Nullable MoveDecision moveDecision,
                                   @Nullable RebalanceDecision rebalanceDecision) {
        this.allocateDecision = allocateDecision == AllocateUnassignedDecision.NOT_TAKEN ? null : allocateDecision;
        this.moveDecision = moveDecision == MoveDecision.NOT_TAKEN ? null : moveDecision;
        this.rebalanceDecision = rebalanceDecision;
    }

    public ShardAllocationDecision(StreamInput in) throws IOException {
        allocateDecision = in.readOptionalWriteable(AllocateUnassignedDecision::readFrom);
        moveDecision = in.readOptionalWriteable(MoveDecision::new);
        rebalanceDecision = in.readOptionalWriteable(RebalanceDecision::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(allocateDecision);
        out.writeOptionalWriteable(moveDecision);
        out.writeOptionalWriteable(rebalanceDecision);
    }

    /**
     * Gets the unassigned allocation decision for the shard.  Returns null if the shard was not in the unassigned state.
     */
    @Nullable
    public AllocateUnassignedDecision getAllocateDecision() {
        return allocateDecision;
    }

    /**
     * Gets the move decision for the shard.  Returns null if the shard was in the unassigned state.
     */
    @Nullable
    public MoveDecision getMoveDecision() {
        return moveDecision;
    }

    /**
     * Gets the rebalance decision for the shard.  Returns null if the shard was in the unassigned state.
     */
    @Nullable
    public RebalanceDecision getRebalanceDecision() {
        return rebalanceDecision;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        assert allocateDecision != null ^ moveDecision != null ^ rebalanceDecision != null :
            "only one of allocate, move, and rebalance decision must be set";
        if (allocateDecision != null) {
            builder.startObject("allocate_decision");
            allocateDecision.toXContent(builder, params);
            builder.endObject();
        }
        if (moveDecision != null) {
            builder.startObject("move_decision");
            moveDecision.toXContent(builder, params);
            builder.endObject();
        }
        if (rebalanceDecision != null) {
            builder.startObject("rebalance_decision");
            rebalanceDecision.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

}
