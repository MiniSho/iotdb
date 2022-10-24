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

package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.UpdateConfigNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class UpdateConfigNodeProcedure extends AbstractNodeProcedure<UpdateConfigNodeState> {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateConfigNodeProcedure.class);
  private static final int retryThreshold = 5;

  private TConfigNodeLocation oldConfigNodeLocation;
  private TConfigNodeLocation newConfigNodeLocation;

  public UpdateConfigNodeProcedure(
      TConfigNodeLocation oldConfigNodeLocation, TConfigNodeLocation newConfigNodeLocation) {
    super();
    this.oldConfigNodeLocation = oldConfigNodeLocation;
    this.newConfigNodeLocation = newConfigNodeLocation;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, UpdateConfigNodeState state) {

    if (oldConfigNodeLocation == null || newConfigNodeLocation == null) {
      return Flow.NO_MORE_STATE;
    }

    try {
      switch (state) {
        case UPDATE_PEER:
          LOG.info("Executing updatePeer on {}...", oldConfigNodeLocation);
          env.updateConfigNodePeer(oldConfigNodeLocation, newConfigNodeLocation);
          LOG.info("Successfully updatePeer to {}...", newConfigNodeLocation);
          break;
        case UPDATE_SUCCESS:
          env.broadCastTheLatestConfigNodeGroup();
          LOG.info(
              "The ConfigNode: {} is successfully added to the cluster", newConfigNodeLocation);
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Update Config Node failed " + state));
      } else {
        LOG.error(
            "Retrievable error trying to update config node from {} to {}, state {}",
            oldConfigNodeLocation,
            newConfigNodeLocation,
            state,
            e);
        if (getCycles() > retryThreshold) {
          setFailure(new ProcedureException("State stuck at " + state));
        }
      }
    }
    return null;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, UpdateConfigNodeState state)
      throws IOException, InterruptedException, ProcedureException {
    if (state == UpdateConfigNodeState.UPDATE_PEER) {
      env.updateConfigNodePeer(newConfigNodeLocation, oldConfigNodeLocation);
      LOG.info("Rollback updatePeer from {} to {}", newConfigNodeLocation, oldConfigNodeLocation);
    }
  }

  @Override
  protected UpdateConfigNodeState getState(int stateId) {
    return UpdateConfigNodeState.values()[stateId];
  }

  @Override
  protected boolean isRollbackSupported(UpdateConfigNodeState state) {
    switch (state) {
      case UPDATE_PEER:
        return true;
      case UPDATE_SUCCESS:
        return false;
    }
    return false;
  }

  @Override
  protected int getStateId(UpdateConfigNodeState updateConfigNodeState) {
    return updateConfigNodeState.ordinal();
  }

  @Override
  protected UpdateConfigNodeState getInitialState() {
    return UpdateConfigNodeState.UPDATE_PEER;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.UPDATE_CONFIG_NODE_PROCEDURE.ordinal());
    super.serialize(stream);
    ThriftConfigNodeSerDeUtils.serializeTConfigNodeLocation(oldConfigNodeLocation, stream);
    ThriftConfigNodeSerDeUtils.serializeTConfigNodeLocation(newConfigNodeLocation, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      oldConfigNodeLocation = ThriftConfigNodeSerDeUtils.deserializeTConfigNodeLocation(byteBuffer);
      newConfigNodeLocation = ThriftConfigNodeSerDeUtils.deserializeTConfigNodeLocation(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOG.error("Error in deserialize UpdateConfigNodeProcedure", e);
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof UpdateConfigNodeProcedure) {
      UpdateConfigNodeProcedure thatProc = (UpdateConfigNodeProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.oldConfigNodeLocation.equals(this.oldConfigNodeLocation)
          && thatProc.newConfigNodeLocation.equals(this.newConfigNodeLocation);
    }
    return false;
  }
}
