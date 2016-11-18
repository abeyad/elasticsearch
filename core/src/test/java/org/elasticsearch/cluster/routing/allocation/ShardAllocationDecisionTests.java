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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

/**
 * Unit tests for the {@link AllocateUnassignedDecision} class.
 */
public class ShardAllocationDecisionTests extends ESTestCase {

    private DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
    private DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

    public void testDecisionNotTaken() {
        AllocateUnassignedDecision shardAllocationDecision = AllocateUnassignedDecision.NOT_TAKEN;
        assertFalse(shardAllocationDecision.isDecisionTaken());
        assertNull(shardAllocationDecision.getFinalDecisionType());
        assertNull(shardAllocationDecision.getAllocationStatus());
        assertNull(shardAllocationDecision.getAllocationId());
        assertNull(shardAllocationDecision.getAssignedNodeId());
        assertEquals("decision not taken", shardAllocationDecision.getFinalExplanation());
        assertNull(shardAllocationDecision.getNodeDecisions());
        expectThrows(IllegalArgumentException.class, () -> shardAllocationDecision.getFinalDecisionSafe());
    }

    public void testNoDecision() {
        final AllocationStatus allocationStatus = randomFrom(
            AllocationStatus.DELAYED_ALLOCATION, AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA
        );
        AllocateUnassignedDecision noDecision = AllocateUnassignedDecision.no(allocationStatus, null);
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(Decision.Type.NO, noDecision.getFinalDecisionType());
        assertEquals(allocationStatus, noDecision.getAllocationStatus());
        if (allocationStatus == AllocationStatus.FETCHING_SHARD_DATA) {
            assertEquals("still fetching shard state from the nodes in the cluster", noDecision.getFinalExplanation());
        } else if (allocationStatus == AllocationStatus.DELAYED_ALLOCATION) {
            assertThat(noDecision.getFinalExplanation(), startsWith("delaying allocation of the replica"));
        } else {
            assertThat(noDecision.getFinalExplanation(),
                startsWith("shard was previously allocated, but no valid shard copy could be found"));
        }
        assertNull(noDecision.getNodeDecisions());
        assertNull(noDecision.getAssignedNodeId());
        assertNull(noDecision.getAllocationId());

        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", new NodeAllocationResult(node1, Decision.NO, randomFloat()));
        nodeDecisions.put("node2", new NodeAllocationResult(node2, Decision.NO, randomFloat()));
        final boolean reuseStore = randomBoolean();
        noDecision = AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, nodeDecisions, reuseStore);
        assertTrue(noDecision.isDecisionTaken());
        assertEquals(Decision.Type.NO, noDecision.getFinalDecisionType());
        assertEquals(AllocationStatus.DECIDERS_NO, noDecision.getAllocationStatus());
        if (reuseStore) {
            assertEquals("all nodes that hold a valid shard copy returned a NO decision", noDecision.getFinalExplanation());
        } else {
            assertEquals("shard cannot be assigned to any node in the cluster", noDecision.getFinalExplanation());
        }
        assertEquals(nodeDecisions, noDecision.getNodeDecisions());
        assertNull(noDecision.getAssignedNodeId());
        assertNull(noDecision.getAllocationId());

        // test bad values
        expectThrows(NullPointerException.class, () -> AllocateUnassignedDecision.no((AllocationStatus) null, null));
    }

    public void testThrottleDecision() {
        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", new NodeAllocationResult(node1, Decision.NO, randomFloat()));
        nodeDecisions.put("node2", new NodeAllocationResult(node2, Decision.THROTTLE, randomFloat()));
        AllocateUnassignedDecision throttleDecision = AllocateUnassignedDecision.throttle(nodeDecisions);
        assertTrue(throttleDecision.isDecisionTaken());
        assertEquals(Decision.Type.THROTTLE, throttleDecision.getFinalDecisionType());
        assertEquals(AllocationStatus.DECIDERS_THROTTLED, throttleDecision.getAllocationStatus());
        assertThat(throttleDecision.getFinalExplanation(), startsWith("allocation throttled"));
        assertEquals(nodeDecisions, throttleDecision.getNodeDecisions());
        assertNull(throttleDecision.getAssignedNodeId());
        assertNull(throttleDecision.getAllocationId());
    }

    public void testYesDecision() {
        Map<String, NodeAllocationResult> nodeDecisions = new HashMap<>();
        nodeDecisions.put("node1", new NodeAllocationResult(node1, Decision.YES, randomFloat()));
        nodeDecisions.put("node2", new NodeAllocationResult(node2, Decision.NO, randomFloat()));
        String allocId = randomBoolean() ? "allocId" : null;
        boolean reuseStore = randomBoolean();
        boolean forceAllocatePrimary = randomBoolean();
        AllocateUnassignedDecision yesDecision = AllocateUnassignedDecision.yes(
            "node1", allocId, nodeDecisions, forceAllocatePrimary, reuseStore);
        assertTrue(yesDecision.isDecisionTaken());
        assertEquals(Decision.Type.YES, yesDecision.getFinalDecisionType());
        assertNull(yesDecision.getAllocationStatus());
        if (forceAllocatePrimary) {
            assertThat(yesDecision.getFinalExplanation(), startsWith("allocating the primary shard to node"));
        } else if (allocId != null) {
            assertThat(yesDecision.getFinalExplanation(), containsString("with allocation id"));
        } else if (reuseStore) {
            assertThat(yesDecision.getFinalExplanation(), containsString("re-use its unallocated persistent store"));
        } else {
            assertThat(yesDecision.getFinalExplanation(), startsWith("shard assigned to node"));
        }
        assertEquals(nodeDecisions, yesDecision.getNodeDecisions());
        assertEquals("node1", yesDecision.getAssignedNodeId());
        assertEquals(allocId, yesDecision.getAllocationId());
    }

    public void testCachedDecisions() {
        List<AllocationStatus> cachableStatuses = Arrays.asList(AllocationStatus.DECIDERS_NO, AllocationStatus.DECIDERS_THROTTLED,
            AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA, AllocationStatus.DELAYED_ALLOCATION);
        for (AllocationStatus allocationStatus : cachableStatuses) {
            if (allocationStatus == AllocationStatus.DECIDERS_THROTTLED) {
                AllocateUnassignedDecision cached = AllocateUnassignedDecision.throttle(null);
                AllocateUnassignedDecision another = AllocateUnassignedDecision.throttle(null);
                assertSame(cached, another);
                AllocateUnassignedDecision notCached = AllocateUnassignedDecision.throttle(new HashMap<>());
                another = AllocateUnassignedDecision.throttle(new HashMap<>());
                assertNotSame(notCached, another);
            } else {
                AllocateUnassignedDecision cached = AllocateUnassignedDecision.no(allocationStatus, null);
                AllocateUnassignedDecision another = AllocateUnassignedDecision.no(allocationStatus, null);
                assertSame(cached, another);
                AllocateUnassignedDecision notCached = AllocateUnassignedDecision.no(allocationStatus, new HashMap<>());
                another = AllocateUnassignedDecision.no(allocationStatus, new HashMap<>());
                assertNotSame(notCached, another);
            }
        }

        // yes decisions are not precomputed and cached
        Map<String, NodeAllocationResult> dummyMap = Collections.emptyMap();
        AllocateUnassignedDecision first = AllocateUnassignedDecision.yes("node1", "abc", dummyMap, randomBoolean(), randomBoolean());
        AllocateUnassignedDecision second = AllocateUnassignedDecision.yes("node1", "abc", dummyMap, randomBoolean(), randomBoolean());
        // same fields for the ShardAllocationDecision, but should be different instances
        assertNotSame(first, second);
    }

}
