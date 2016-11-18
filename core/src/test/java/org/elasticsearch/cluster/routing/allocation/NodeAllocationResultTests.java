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
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult.ShardStore;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult.StoreStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * Unit tests for the {@link NodeAllocationResult} class.
 */
public class NodeAllocationResultTests extends ESTestCase {

    public void testSerialization() throws IOException {
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Decision decision = randomFrom(Decision.YES, Decision.THROTTLE, Decision.NO);
        float weight = randomFloat();
        NodeAllocationResult result = new NodeAllocationResult(node, decision, weight);
        BytesStreamOutput output = new BytesStreamOutput();
        result.writeTo(output);
        NodeAllocationResult readResult = new NodeAllocationResult(output.bytes().streamInput());
        assertEquals(result, readResult);
        assertEquals(node, result.getNode());
        assertEquals(decision, result.getDecision());
        assertEquals(0, Float.compare(weight, result.getWeight()));
        assertNull(result.getShardStore());
    }

    public void testShardStore() throws IOException {
        DiscoveryNode node = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Decision decision = randomFrom(Decision.YES, Decision.THROTTLE, Decision.NO);
        StoreStatus storeStatus = randomFrom(StoreStatus.values());
        long matchingBytes = (long) randomIntBetween(1, 1000);
        ShardStore shardStore = new ShardStore(storeStatus, matchingBytes);
        NodeAllocationResult result = new NodeAllocationResult(node, shardStore, decision);
        BytesStreamOutput output = new BytesStreamOutput();
        result.writeTo(output);
        NodeAllocationResult readResult = new NodeAllocationResult(output.bytes().streamInput());
        assertEquals(result, readResult);
        assertEquals(node, result.getNode());
        assertEquals(decision, result.getDecision());
        assertEquals(storeStatus, result.getShardStore().getStoreStatus());
        assertEquals(matchingBytes, result.getShardStore().getMatchingBytes());
        assertEquals(-1, result.getShardStore().getVersion());
        assertNull(result.getShardStore().getAllocationId());

        String allocId = randomAsciiOfLength(5);
        long version = (long) randomIntBetween(1, 1000);
        shardStore = new ShardStore(storeStatus, allocId, version);
        result = new NodeAllocationResult(node, shardStore, decision);
        output = new BytesStreamOutput();
        result.writeTo(output);
        readResult = new NodeAllocationResult(output.bytes().streamInput());
        assertEquals(result, readResult);
        assertEquals(storeStatus, result.getShardStore().getStoreStatus());
        assertEquals(-1, result.getShardStore().getMatchingBytes());
        assertEquals(version, result.getShardStore().getVersion());
        assertEquals(allocId, result.getShardStore().getAllocationId());
    }
}
