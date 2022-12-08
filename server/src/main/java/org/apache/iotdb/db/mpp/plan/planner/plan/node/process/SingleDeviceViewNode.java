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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SingleDeviceViewNode extends SingleChildProcessNode {

  private final String device;
  private List<String> outputColumnNames;
  private final List<Integer> deviceToMeasurementIndexes;

  public SingleDeviceViewNode(
      PlanNodeId id,
      List<String> outputColumnNames,
      String device,
      List<Integer> deviceToMeasurementIndexes) {
    super(id);
    this.device = device;
    this.outputColumnNames = outputColumnNames;
    this.deviceToMeasurementIndexes = deviceToMeasurementIndexes;
  }

  public SingleDeviceViewNode(
      PlanNodeId id, String device, List<Integer> deviceToMeasurementIndexes) {
    super(id);
    this.device = device;
    this.deviceToMeasurementIndexes = deviceToMeasurementIndexes;
  }

  @Override
  public PlanNode clone() {
    return new SingleDeviceViewNode(
        getPlanNodeId(), outputColumnNames, device, deviceToMeasurementIndexes);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public String getDevice() {
    return device;
  }

  public List<Integer> getDeviceToMeasurementIndexes() {
    return deviceToMeasurementIndexes;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSingleDeviceView(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SINGLE_DEVICE_VIEW.serialize(byteBuffer);
    ReadWriteIOUtils.write(device, byteBuffer);
    ReadWriteIOUtils.write(deviceToMeasurementIndexes.size(), byteBuffer);
    for (Integer index : deviceToMeasurementIndexes) {
      ReadWriteIOUtils.write(index, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SINGLE_DEVICE_VIEW.serialize(stream);
    ReadWriteIOUtils.write(device, stream);
    ReadWriteIOUtils.write(deviceToMeasurementIndexes.size(), stream);
    for (Integer index : deviceToMeasurementIndexes) {
      ReadWriteIOUtils.write(index, stream);
    }
  }

  public static SingleDeviceViewNode deserialize(ByteBuffer byteBuffer) {
    String device = ReadWriteIOUtils.readString(byteBuffer);
    int listSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<Integer> deviceToMeasurementIndexes = new ArrayList<>(listSize);
    while (listSize > 0) {
      deviceToMeasurementIndexes.add(ReadWriteIOUtils.readInt(byteBuffer));
      listSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SingleDeviceViewNode(planNodeId, device, deviceToMeasurementIndexes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SingleDeviceViewNode that = (SingleDeviceViewNode) o;
    return device.equals(that.device)
        && outputColumnNames.equals(that.outputColumnNames)
        && deviceToMeasurementIndexes.equals(that.deviceToMeasurementIndexes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), device, outputColumnNames, deviceToMeasurementIndexes);
  }

  @Override
  public String toString() {
    return "SingleDeviceView-" + this.getPlanNodeId();
  }
}
