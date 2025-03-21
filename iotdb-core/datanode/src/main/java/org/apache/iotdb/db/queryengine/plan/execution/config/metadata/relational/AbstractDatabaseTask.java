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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;

public abstract class AbstractDatabaseTask implements IConfigTask {

  /////////////////////////////// Allowed properties ///////////////////////////////
  public static final String TTL_KEY = "ttl";
  public static final String TIME_PARTITION_INTERVAL_KEY = "time_partition_interval";
  public static final String SCHEMA_REGION_GROUP_NUM_KEY = "schema_region_group_num";
  public static final String DATA_REGION_GROUP_NUM_KEY = "data_region_group_num";

  // Deprecated
  public static final String SCHEMA_REPLICATION_FACTOR_KEY = "schema_replication_factor";
  public static final String DATA_REPLICATION_FACTOR_KEY = "data_replication_factor";

  /////////////////////////////// Fields ///////////////////////////////

  protected final TDatabaseSchema schema;
  // In CreateDB: If not exists
  // In AlterDB: If exists
  protected final boolean exists;

  protected AbstractDatabaseTask(final TDatabaseSchema schema, final boolean exists) {
    this.schema = schema;
    this.exists = exists;
  }
}
