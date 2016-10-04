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

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * This abstract class defining basic {@link Decision} used during shard
 * allocation process.
 *
 * @see AllocationDecider
 */
public abstract class Decision implements ToXContent {

    public static final Decision ALWAYS = new Single(Type.YES);
    public static final Decision YES = new Single(Type.YES);
    public static final Decision NO = new Single(Type.NO);
    public static final Decision THROTTLE = new Single(Type.THROTTLE);

    /**
     * Creates a simple decision
     * @param type {@link Type} of the decision
     * @param label label for the Decider that produced this decision
     * @param explanation explanation of the decision
     * @param explanationParams additional parameters for the decision
     * @return new {@link Decision} instance
     */
    public static Decision single(Type type, @Nullable String label, @Nullable String explanation, @Nullable Object... explanationParams) {
        return new Single(type, label, explanation, explanationParams);
    }

    public static void writeTo(Decision decision, StreamOutput out) throws IOException {
        if (decision instanceof Multi) {
            // Flag specifying whether it is a Multi or Single Decision
            out.writeBoolean(true);
            out.writeVInt(((Multi) decision).decisions.size());
            for (Decision d : ((Multi) decision).decisions) {
                writeTo(d, out);
            }
        } else {
            // Flag specifying whether it is a Multi or Single Decision
            out.writeBoolean(false);
            Single d = ((Single) decision);
            Type.writeTo(d.type, out);
            out.writeOptionalString(d.label);
            // Flatten explanation on serialization, so that explanationParams
            // do not need to be serialized
            out.writeOptionalString(d.getExplanation());
        }
    }

    public static Decision readFrom(StreamInput in) throws IOException {
        // Determine whether to read a Single or Multi Decision
        if (in.readBoolean()) {
            Multi result = new Multi();
            int decisionCount = in.readVInt();
            for (int i = 0; i < decisionCount; i++) {
                Decision s = readFrom(in);
                result.decisions.add(s);
            }
            return result;
        } else {
            Single result = new Single();
            result.type = Type.readFrom(in);
            result.label = in.readOptionalString();
            result.explanationString = in.readOptionalString();
            return result;
        }
    }

    /**
     * This enumeration defines the
     * possible types of decisions
     */
    public enum Type {
        YES,
        NO,
        THROTTLE,
        NOT_TAKEN;

        public static Type resolve(String s) {
            return Type.valueOf(s.toUpperCase(Locale.ROOT));
        }

        public static Type readFrom(StreamInput in) throws IOException {
            int i = in.readVInt();
            switch (i) {
                case 0:
                    return NO;
                case 1:
                    return YES;
                case 2:
                    return THROTTLE;
                case 3:
                    return NOT_TAKEN;
                default:
                    throw new IllegalArgumentException("No Type for integer [" + i + "]");
            }
        }

        public static void writeTo(Type type, StreamOutput out) throws IOException {
            switch (type) {
                case NO:
                    out.writeVInt(0);
                    break;
                case YES:
                    out.writeVInt(1);
                    break;
                case THROTTLE:
                    out.writeVInt(2);
                    break;
                case NOT_TAKEN:
                    out.writeVInt(3);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid Type [" + type + "]");
            }
        }
    }

    /**
     * Get the {@link Type} of this decision
     * @return {@link Type} of this decision
     */
    public abstract Type type();

    /**
     * Get the description label for this decision.
     */
    @Nullable
    public abstract String label();

    /**
     * Return the list of all decisions that make up this decision
     */
    public abstract List<Decision> getDecisions();

    /**
     * Simple class representing a single decision
     */
    public static class Single extends Decision {
        private Type type;
        private String label;
        private String explanation;
        private String explanationString;
        private Object[] explanationParams;

        public Single() {

        }

        /**
         * Creates a new {@link Single} decision of a given type
         * @param type {@link Type} of the decision
         */
        public Single(Type type) {
            this(type, null, null, (Object[]) null);
        }

        /**
         * Creates a new {@link Single} decision of a given type
         *
         * @param type {@link Type} of the decision
         * @param explanation An explanation of this {@link Decision}
         * @param explanationParams A set of additional parameters
         */
        public Single(Type type, @Nullable String label, @Nullable String explanation, @Nullable Object... explanationParams) {
            this.type = type;
            this.label = label;
            this.explanation = explanation;
            this.explanationParams = explanationParams;
        }

        @Override
        public Type type() {
            return this.type;
        }

        @Override
        @Nullable
        public String label() {
            return this.label;
        }

        @Override
        public List<Decision> getDecisions() {
            return Collections.singletonList(this);
        }

        /**
         * Returns the explanation string, fully formatted. Only formats the string once
         */
        @Nullable
        public String getExplanation() {
            if (explanationString == null && explanation != null) {
                explanationString = String.format(Locale.ROOT, explanation, explanationParams);
            }
            return this.explanationString;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            Decision.Single s = (Decision.Single) object;
            return this.type == s.type &&
                       Objects.equals(label, s.label) &&
                       Objects.equals(getExplanation(), s.getExplanation());
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + (label == null ? 0 : label.hashCode());
            String explanationStr = getExplanation();
            result = 31 * result + (explanationStr == null ? 0 : explanationStr.hashCode());
            return result;
        }

        @Override
        public String toString() {
            if (explanationString != null || explanation != null) {
                return type + "(" + getExplanation() + ")";
            }
            return type + "()";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("decider", label);
            builder.field("decision", type);
            String explanation = getExplanation();
            builder.field("explanation", explanation != null ? explanation : "none");
            builder.endObject();
            return builder;
        }
    }

    /**
     * Simple class representing a list of decisions
     */
    public static class Multi extends Decision {

        private final List<Decision> decisions = new ArrayList<>();

        /**
         * Add a decision to this {@link Multi}decision instance
         * @param decision {@link Decision} to add
         * @return {@link Multi}decision instance with the given decision added
         */
        public Multi add(Decision decision) {
            decisions.add(decision);
            return this;
        }

        @Override
        public Type type() {
            Type ret = Type.YES;
            for (int i = 0; i < decisions.size(); i++) {
                Type type = decisions.get(i).type();
                if (type == Type.NO) {
                    return type;
                } else if (type == Type.THROTTLE) {
                    ret = type;
                }
            }
            return ret;
        }

        @Override
        @Nullable
        public String label() {
            return null;
        }

        @Override
        public List<Decision> getDecisions() {
            return Collections.unmodifiableList(this.decisions);
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || object instanceof Multi == false) {
                return false;
            }

            final Multi m = (Multi) object;

            return this.decisions.equals(m.decisions);
        }

        @Override
        public int hashCode() {
            return 31 * decisions.hashCode();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Decision decision : decisions) {
                sb.append("[").append(decision.toString()).append("]");
            }
            return sb.toString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray("decisions");
            for (Decision d : decisions) {
                d.toXContent(builder, params);
            }
            builder.endArray();
            return builder;
        }
    }

    /**
     * A class that represents the decision taken for allocating a shard to a particular node.
     */
    public static class NodeDecision extends Multi {

        private final String nodeId;
        private final float weight;

        public NodeDecision(String nodeId) {
            this.nodeId = Objects.requireNonNull(nodeId);
            this.weight = Float.POSITIVE_INFINITY;
        }

        public NodeDecision(String nodeId, float weight) {
            this.nodeId = Objects.requireNonNull(nodeId);
            this.weight = weight;
        }

        /**
         * The node for which the allocation decision was taken.
         */
        public String getNodeId() {
            return nodeId;
        }

        /**
         * The weight of the node for which the decision was taken.
         */
        public float getWeight() {
            return weight;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            final NodeDecision d = (NodeDecision) object;
            return super.equals(d)
                       && nodeId.equals(d.nodeId)
                       && weight == d.weight;
        }

        @Override
        public int hashCode() {
            int result = 31 * super.hashCode();
            result = 31 * result + nodeId.hashCode();
            result = 31 * result + Float.floatToIntBits(weight);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[nodeId=").append(nodeId);
            if (weight != Float.POSITIVE_INFINITY) {
                sb.append(", weight=").append(weight);
            }
            sb.append(", decisions=").append(super.toString());
            return sb.append("]").toString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("node_id", nodeId);
            if (weight != Float.POSITIVE_INFINITY) {
                // only include the weight if it has a meaningful value,
                // for some node decisions, weight isn't calculated
                builder.field("weight", weight);
            }
            return super.toXContent(builder, params);
        }
    }

    /**
     * A class that represents the final decision for allocating a shard.
     */
    public static class FinalDecision extends Multi {

        public static final FinalDecision NOT_TAKEN = new FinalDecision(Type.NOT_TAKEN, null, null, null, null);

        private final Type type;
        @Nullable
        private final String explanation;
        @Nullable
        private final AllocationStatus allocationStatus;
        @Nullable
        private final String assignedNodeId;
        @Nullable
        private final String allocationId;

        private FinalDecision(Type type, String explanation, String assignedNodeId,
                              String allocationId, AllocationStatus allocationStatus) {
            assert type != null : "the type of decision must be set";
            assert type != Type.NO || allocationStatus != null : "NO decision must have an allocation status";
            assert type != Type.YES || assignedNodeId != null : "YES decision must have an assigned node id";
            assert allocationId == null || assignedNodeId != null : "must have an assigned node id if there is an allocation id";
            this.type = type;
            this.explanation = explanation;
            this.assignedNodeId = assignedNodeId;
            this.allocationId = allocationId;
            this.allocationStatus = allocationStatus;
        }

        public static FinalDecision no(AllocationStatus allocationStatus, String explanation) {
            return new FinalDecision(Type.NO, explanation, null, null, allocationStatus);
        }

        public static FinalDecision no(AllocationStatus allocationStatus, String explanation, Collection<Decision> decisions) {
            final FinalDecision finalDecision = new FinalDecision(Type.NO, explanation, null, null, allocationStatus);
            return addDecisions(finalDecision, decisions);
        }

        public static FinalDecision yes(String assignedNodeId, String allocationId, String explanation) {
            return new FinalDecision(Type.YES, explanation, assignedNodeId, allocationId, null);
        }

        public static FinalDecision yes(String assignedNodeId, String allocationId, String explanation, Collection<Decision> decisions) {
            final FinalDecision finalDecision = new FinalDecision(Type.YES, explanation, assignedNodeId, allocationId, null);
            return addDecisions(finalDecision, decisions);
        }

        public static FinalDecision throttle(String explanation) {
            return new FinalDecision(Type.THROTTLE, explanation, null, null, null);
        }

        public static FinalDecision throttle(String explanation, Collection<Decision> decisions) {
            final FinalDecision finalDecision = new FinalDecision(Type.THROTTLE, explanation, null, null,
                                                                     AllocationStatus.DECIDERS_THROTTLED);
            addDecisions(finalDecision, decisions);
            return finalDecision;
        }

        private static FinalDecision addDecisions(final FinalDecision finalDecision, final Collection<Decision> decisions) {
            if (decisions != null) {
                for (Decision decision : decisions) {
                    finalDecision.add(decision);
                }
            }
            return finalDecision;
        }

        @Override
        public Type type() {
            // not using the Multi decision's type method here because we override the final
            // type of decision in the constructor
            return type;
        }

        @Override
        public String label() {
            return explanation;
        }

        /**
         * Returns the allocation status.  Only applicable if {@link #type()} returns {@link Type#NO}.
         */
        @Nullable
        public AllocationStatus getAllocationStatus() {
            return allocationStatus;
        }

        /**
         * Returns the assigned node id for the shard as a result of this decision.  Only applicable
         * if {@link #type()} returns {@link Type#YES}.
         */
        @Nullable
        public String getAssignedNodeId() {
            return assignedNodeId;
        }

        /**
         * Returns the allocation id if using an already existing shard copy on a node to assign
         * the shard to.  Only applicable if {@link #type()} returns {@link Type#YES}.
         */
        @Nullable
        public String getAllocationId() {
            return allocationId;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            final FinalDecision fd = (FinalDecision) object;

            return super.equals(object)
                       && Objects.equals(type, fd.type)
                       && Objects.equals(explanation, fd.explanation)
                       && Objects.equals(allocationStatus, fd.allocationStatus)
                       && Objects.equals(assignedNodeId, fd.assignedNodeId)
                       && Objects.equals(allocationId, fd.allocationId);
        }

        @Override
        public int hashCode() {
            int result = 31 * super.hashCode();
            result = 31 * result + Objects.hash(type, explanation, allocationStatus, assignedNodeId, allocationId);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("FinalDecision[decision=").append(type);
            if (assignedNodeId != null) {
                sb.append(", assignedNodeId=").append(assignedNodeId);
                if (allocationId != null) {
                    sb.append(", allocationId=").append(allocationId);
                }
            } else {
                sb.append(", allocationStatus=").append(allocationStatus);
            }
            return sb.append("]").toString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            assert explanation != null : "writing the decision to x-content must include an explanation";
            builder.field("decision", type.toString());
            builder.field("explanation", explanation);
            if (assignedNodeId != null) {
                builder.field("assigned_node_id", assignedNodeId);
                if (allocationId != null) {
                    builder.field("allocation_id", allocationId);
                }
            } else {
                assert allocationStatus != null : "if node was not assigned, then allocation status must be set";
                builder.field("allocation_status", allocationStatus.toString());
            }
            return super.toXContent(builder, params);
        }
    }
}
