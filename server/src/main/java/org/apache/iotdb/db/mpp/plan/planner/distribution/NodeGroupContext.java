/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.plan.planner.distribution;

import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;

import java.util.HashMap;
import java.util.Map;

public class NodeGroupContext {
  protected MPPQueryContext queryContext;
  protected Map<PlanNodeId, NodeDistribution> nodeDistributionMap;

  public NodeGroupContext(MPPQueryContext queryContext) {
    this.queryContext = queryContext;
    this.nodeDistributionMap = new HashMap<>();
  }

  public void putNodeDistribution(PlanNodeId nodeId, NodeDistribution distribution) {
    this.nodeDistributionMap.put(nodeId, distribution);
  }

  public NodeDistribution getNodeDistribution(PlanNodeId nodeId) {
    return this.nodeDistributionMap.get(nodeId);
  }
}
