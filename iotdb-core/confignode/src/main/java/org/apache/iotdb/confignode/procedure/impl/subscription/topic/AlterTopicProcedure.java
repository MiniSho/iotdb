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

package org.apache.iotdb.confignode.procedure.impl.subscription.topic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.subscription.AbstractOperateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.SubscriptionOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AlterTopicProcedure extends AbstractOperateSubscriptionProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterTopicProcedure.class);

  private TopicMeta updatedTopicMeta;

  private TopicMeta existedTopicMeta;

  public AlterTopicProcedure() {
    super();
  }

  public AlterTopicProcedure(TopicMeta updatedTopicMeta) {
    super();
    this.updatedTopicMeta = updatedTopicMeta;
  }

  @Override
  protected SubscriptionOperation getOperation() {
    return SubscriptionOperation.ALTER_TOPIC;
  }

  @Override
  public void executeFromValidate(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterTopicProcedure: executeFromValidate");

    validateOldAndNewTopicMeta();
  }

  public void validateOldAndNewTopicMeta() {
    try {
      subscriptionInfo.get().validateBeforeAlteringTopic(updatedTopicMeta);
    } catch (PipeException e) {
      LOGGER.error(
          "AlterTopicProcedure: executeFromValidate, validateBeforeAlteringTopic failed", e);
      setFailure(new ProcedureException(e.getMessage()));
      throw e;
    }

    this.existedTopicMeta = subscriptionInfo.get().getTopicMeta(updatedTopicMeta.getTopicName());
  }

  @Override
  public void executeFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("AlterTopicProcedure: executeFromOperateOnConfigNodes, try to alter topic");

    TSStatus response;
    try {
      response =
          env.getConfigManager().getConsensusManager().write(new AlterTopicPlan(updatedTopicMeta));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "AlterTopicProcedure: executeFromOperateOnDataNodes({})", updatedTopicMeta.getTopicName());

    try {
      if (RpcUtils.squashResponseStatusList(
                  env.pushSingleTopicOnDataNode(updatedTopicMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the topic meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to push the topic meta to data nodes, topic name: %s",
            updatedTopicMeta.getTopicName()));
  }

  @Override
  public void rollbackFromValidate(ConfigNodeProcedureEnv env) {
    LOGGER.info("AlterTopicProcedure: rollbackFromValidate({})", updatedTopicMeta.getTopicName());
  }

  @Override
  public void rollbackFromOperateOnConfigNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterTopicProcedure: rollbackFromOperateOnConfigNodes({})",
        updatedTopicMeta.getTopicName());

    try {
      if (RpcUtils.squashResponseStatusList(
                  env.pushSingleTopicOnDataNode(existedTopicMeta.serialize()))
              .getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to serialize the topic meta due to: ", e);
    }

    throw new PipeException(
        String.format(
            "Failed to push the topic meta to data nodes, topic name: %s",
            updatedTopicMeta.getTopicName()));
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterTopicProcedure: rollbackFromOperateOnDataNodes({})", updatedTopicMeta.getTopicName());
    // do nothing
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ALTER_TOPIC_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(updatedTopicMeta != null, stream);
    if (updatedTopicMeta != null) {
      stream.write(updatedTopicMeta.serialize().array());
    }

    ReadWriteIOUtils.write(existedTopicMeta != null, stream);
    if (existedTopicMeta != null) {
      stream.write(existedTopicMeta.serialize().array());
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      updatedTopicMeta = TopicMeta.deserialize(byteBuffer);
    }

    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      existedTopicMeta = TopicMeta.deserialize(byteBuffer);
    }
  }
}
