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
package org.apache.iotdb.confignode.it;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.AbstractEnv;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.ConfigNodeWrapper;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.EnvUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.checkNodeConfig;
import static org.apache.iotdb.confignode.it.utils.ConfigNodeTestUtils.getClusterNodeInfos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterRestartIT {

  protected static String originalConfigNodeConsensusProtocolClass;

  private static final int testConfigNodeNum = 3;
  private static final int testDataNodeNum = 3;

  @Before
  public void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    ConfigFactory.getConfig().setConfigNodeConsesusProtocolClass(ConsensusFactory.RatisConsensus);

    // Init 3C3D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setConfigNodeConsesusProtocolClass(originalConfigNodeConsensusProtocolClass);
  }

  @Test
  public void clusterRestartTest() throws InterruptedException {
    // Shutdown all cluster nodes
    for (int i = 0; i < testConfigNodeNum; i++) {
      EnvFactory.getEnv().shutdownConfigNode(i);
    }
    for (int i = 0; i < testDataNodeNum; i++) {
      EnvFactory.getEnv().shutdownDataNode(i);
    }

    // Sleep 1s before restart
    TimeUnit.SECONDS.sleep(1);

    // Restart all cluster nodes
    for (int i = 0; i < testConfigNodeNum; i++) {
      EnvFactory.getEnv().startConfigNode(i);
    }
    for (int i = 0; i < testDataNodeNum; i++) {
      EnvFactory.getEnv().startDataNode(i);
    }

    ((AbstractEnv) EnvFactory.getEnv()).testWorking();
  }

  @Test
  public void clusterRestartAfterConfigNodeTest() throws InterruptedException {
    // Shutdown one config node
    EnvFactory.getEnv().shutdownConfigNode(0);

    // Sleep 1s before restart
    TimeUnit.SECONDS.sleep(1);

    // Modify config node's config
    List<DataNodeWrapper> dataNodeWrapperList = EnvFactory.getEnv().getDataNodeWrapperList();
    List<ConfigNodeWrapper> configNodeWrapperList = EnvFactory.getEnv().getConfigNodeWrapperList();
    ConfigNodeWrapper configNodeWrapper = configNodeWrapperList.get(0);

    int[] portList = EnvUtils.searchAvailablePorts();
    configNodeWrapper.setPort(portList[0]);
    configNodeWrapper.setConsensusPort(portList[1]);

    // update config node files'names
    configNodeWrapper.renameFile();

    for (int i = 0; i < testDataNodeNum; i++) {
      dataNodeWrapperList.get(i).changeConfig(ConfigFactory.getConfig().getEngineProperties());
      EnvFactory.getEnv().startDataNode(i);
    }

    ((AbstractEnv) EnvFactory.getEnv()).testWorking();

    // check nodeInfo in cluster
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // check the number and status of nodes
      clusterNodes = getClusterNodeInfos(client, testConfigNodeNum, testDataNodeNum);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), clusterNodes.getStatus().getCode());

      // check the configuration of nodes
      List<TConfigNodeLocation> configNodeLocationList = clusterNodes.getConfigNodeList();
      List<TDataNodeLocation> dataNodeLocationList = clusterNodes.getDataNodeList();
      checkNodeConfig(
          configNodeLocationList, dataNodeLocationList, configNodeWrapperList, dataNodeWrapperList);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  // TODO: Add persistence tests in the future
}
