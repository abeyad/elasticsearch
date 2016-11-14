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

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * A {@code ClusterAllocationExplanation} is an explanation of why a shard is unassigned,
 * or if it is not unassigned, then which nodes it could possibly be relocated to.
 * It is an immutable class.
 */
public final class ClusterAllocationExplanation implements ToXContent, Writeable {

    private final ShardId shard;
    private final boolean primary;
    private final String currentNodeId;
    private final UnassignedInfo unassignedInfo;
    private final ClusterInfo clusterInfo;
    private final ShardAllocationDecision shardAllocationDecision;

    public ClusterAllocationExplanation(ShardId shard, boolean primary, @Nullable String currentNodeId,
                                        @Nullable UnassignedInfo unassignedInfo, @Nullable ClusterInfo clusterInfo,
                                        ShardAllocationDecision shardAllocationDecision) {
        this.shard = shard;
        this.primary = primary;
        this.currentNodeId = currentNodeId;
        this.unassignedInfo = unassignedInfo;
        this.clusterInfo = clusterInfo;
        this.shardAllocationDecision = shardAllocationDecision;
    }

    public ClusterAllocationExplanation(StreamInput in) throws IOException {
        this.shard = ShardId.readShardId(in);
        this.primary = in.readBoolean();
        this.currentNodeId = in.readOptionalString();
        this.unassignedInfo = in.readOptionalWriteable(UnassignedInfo::new);
        this.clusterInfo = in.readOptionalWriteable(ClusterInfo::new);
        this.shardAllocationDecision = new ShardAllocationDecision(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.getShard().writeTo(out);
        out.writeBoolean(this.isPrimary());
        out.writeOptionalString(currentNodeId);
        out.writeOptionalWriteable(this.getUnassignedInfo());
        out.writeOptionalWriteable(this.getClusterInfo());
        shardAllocationDecision.writeTo(out);
    }

    /** Return the shard that the explanation is about */
    public ShardId getShard() {
        return this.shard;
    }

    /** Return true if the explained shard is primary, false otherwise */
    public boolean isPrimary() {
        return this.primary;
    }

    /** Return the currently assigned node id, or null if the shard is unassigned */
    @Nullable
    public String getCurrentNodeId() {
        return currentNodeId;
    }

    /** Return the unassigned info for the shard or null if the shard is assigned */
    @Nullable
    public UnassignedInfo getUnassignedInfo() {
        return this.unassignedInfo;
    }

    /** Return the cluster disk info for the cluster or null if none available */
    @Nullable
    public ClusterInfo getClusterInfo() {
        return this.clusterInfo;
    }

    /** Return the shard allocation decision for attempting to assign or move the shard. */
    public ShardAllocationDecision getShardAllocationDecision() {
        return shardAllocationDecision;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(); {
            builder.startObject("shard"); {
                builder.field("index", shard.getIndexName());
                builder.field("index_uuid", shard.getIndex().getUUID());
                builder.field("id", shard.getId());
                builder.field("primary", primary);
            }
            builder.endObject(); // end shard
            builder.field("assigned", this.currentNodeId != null);
            // If assigned, show the node id of the node it's assigned to
            if (currentNodeId != null) {
                builder.field("current_node_id", this.currentNodeId);
            }
            // If we have unassigned info, show that
            if (unassignedInfo != null) {
                unassignedInfo.toXContent(builder, params);
            }
            shardAllocationDecision.toXContent(builder, params);
            if (this.clusterInfo != null) {
                builder.startObject("cluster_info"); {
                    this.clusterInfo.toXContent(builder, params);
                }
                builder.endObject(); // end "cluster_info"
            }
        }
        builder.endObject(); // end wrapping object
        return builder;
    }

    /** An Enum representing the state of the shard store's copy of the data on a node */
    public enum StoreCopy {
        // No data for this shard is on the node
        NONE((byte) 0),
        // A copy of the data is available on this node
        AVAILABLE((byte) 1),
        // The copy of the data on the node is corrupt
        CORRUPT((byte) 2),
        // There was an error reading this node's copy of the data
        IO_ERROR((byte) 3),
        // The copy of the data on the node is stale
        STALE((byte) 4),
        // It's unknown what the copy of the data is
        UNKNOWN((byte) 5);

        private final byte id;

        StoreCopy (byte id) {
            this.id = id;
        }

        private static StoreCopy fromId(byte id) {
            switch (id) {
                case 0: return NONE;
                case 1: return AVAILABLE;
                case 2: return CORRUPT;
                case 3: return IO_ERROR;
                case 4: return STALE;
                case 5: return UNKNOWN;
                default:
                    throw new IllegalArgumentException("unknown id for store copy: [" + id + "]");
            }
        }

        @Override
        public String toString() {
            switch (id) {
                case 0: return "NONE";
                case 1: return "AVAILABLE";
                case 2: return "CORRUPT";
                case 3: return "IO_ERROR";
                case 4: return "STALE";
                case 5: return "UNKNOWN";
                default:
                    throw new IllegalArgumentException("unknown id for store copy: [" + id + "]");
            }
        }

        static StoreCopy readFrom(StreamInput in) throws IOException {
            return fromId(in.readByte());
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }
    }
}
