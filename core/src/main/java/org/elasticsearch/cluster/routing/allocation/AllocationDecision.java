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

import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * TODO: javadocs
 * TODO: should this class be split up into YesDecision and NoDecision classes?
 */
public class AllocationDecision {
    /** a constant representing a shard decision where no decision was taken */
    public static final AllocationDecision DECISION_NOT_TAKEN = new AllocationDecision(false, null, null, null, null, null, null);

    private final boolean decisionTaken;
    private final Decision decision;
    private final AllocationStatus allocationStatus;
    private final String explanation;
    private final String assignedNodeId;
    private final String allocationId;
    private final Map<String, Decision> nodeDecisions;

    private AllocationDecision(boolean decisionTaken,
                               Decision decision,
                               AllocationStatus allocationStatus,
                               String explanation,
                               String assignedNodeId,
                               String allocationId,
                               Map<String, Decision> nodeDecisions) {
        assert decision != null || decisionTaken == false : "if a decision was taken, we must have the accompanying decision";
        assert assignedNodeId != null || decision.type() != Type.YES : "a yes decision must have a node to assign the shard to";
        assert allocationStatus != null || decision.type() == Type.YES : "only a yes decision should not have an allocation status";
        assert allocationId != null || assignedNodeId == null : "allocation id can only be null if the assigned node is null";
        this.decisionTaken = decisionTaken;
        this.decision = decision;
        this.allocationStatus = allocationStatus;
        this.explanation = explanation;
        this.assignedNodeId = assignedNodeId;
        this.allocationId = allocationId;
        this.nodeDecisions = Collections.unmodifiableMap(nodeDecisions);
    }

    public static AllocationDecision noDecision(AllocationStatus allocationStatus, String explanation) {
        return new AllocationDecision(true, Decision.NO, allocationStatus, explanation, null, null, null);
    }

    public static AllocationDecision noDecision(AllocationStatus allocationStatus,
                                                String explanation,
                                                Map<String, Decision> nodeDecisions) {
        return new AllocationDecision(true, Decision.NO, allocationStatus, explanation, null, null, nodeDecisions);
    }

    public static AllocationDecision throttleDecision(AllocationStatus allocationStatus,
                                                      String explanation,
                                                      Map<String, Decision> nodeDecisions) {
        return new AllocationDecision(true, Decision.THROTTLE, allocationStatus, explanation, null, null, nodeDecisions);
    }

    public static AllocationDecision yesDecision(String explanation,
                                                 String assignedNodeId,
                                                 String allocationId,
                                                 Map<String, Decision> nodeDecisions) {
        return new AllocationDecision(true, Decision.YES, null, explanation, assignedNodeId, allocationId, nodeDecisions);
    }

    public boolean isDecisionTaken() {
        return decisionTaken;
    }

    @Nullable
    public Decision getDecision() {
        return decision;
    }

    @Nullable
    public AllocationStatus getAllocationStatus() {
        return allocationStatus;
    }

    @Nullable
    public String getExplanation() {
        return explanation;
    }

    @Nullable
    public String getAssignedNodeId() {
        return assignedNodeId;
    }

    @Nullable
    public String getAllocationId() {
        return allocationId;
    }

    @Nullable
    public Map<String, Decision> getNodeDecisions() {
        return nodeDecisions;
    }
}
