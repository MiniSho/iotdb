/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The purpose of this class is to hold the current plan built so far for a relation (query, table,
 * values, etc.), and the mapping to indicate how the fields (by position) in the relation map to
 * the outputs of the plan.
 */
public class RelationPlan {

  private final PlanNode root;

  private final Scope scope;

  // for each field in the relation, the corresponding symbol from "root"
  private final List<Symbol> fieldMappings;

  private final Optional<TranslationMap> outerContext;

  public RelationPlan(
      PlanNode root,
      Scope scope,
      List<Symbol> fieldMappings,
      Optional<TranslationMap> outerContext) {
    requireNonNull(root, "root is null");
    requireNonNull(fieldMappings, "fieldMappings is null");
    requireNonNull(scope, "scope is null");

    int allFieldCount = scope.getLocalScopeFieldCount();
    //    checkArgument(
    //        allFieldCount == fieldMappings.size(),
    //        "Number of outputs (%s) doesn't match number of fields in local scope (%s)",
    //        fieldMappings.size(),
    //        allFieldCount);

    this.root = root;
    this.scope = scope;
    this.fieldMappings = ImmutableList.copyOf(fieldMappings);
    this.outerContext = outerContext;
  }

  public Symbol getSymbol(int fieldIndex) {
    checkArgument(
        fieldIndex >= 0 && fieldIndex < fieldMappings.size(),
        "No field->symbol mapping for field %s",
        fieldIndex);
    return fieldMappings.get(fieldIndex);
  }

  public PlanNode getRoot() {
    return root;
  }

  public List<Symbol> getFieldMappings() {
    return fieldMappings;
  }

  public RelationType getDescriptor() {
    return scope.getRelationType();
  }

  public Scope getScope() {
    return scope;
  }

  public Optional<TranslationMap> getOuterContext() {
    return outerContext;
  }
}
