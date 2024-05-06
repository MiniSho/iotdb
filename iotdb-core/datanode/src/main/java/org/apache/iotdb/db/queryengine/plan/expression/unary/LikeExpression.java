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

package org.apache.iotdb.db.queryengine.plan.expression.unary;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

import static org.apache.tsfile.utils.RegexUtils.compileRegex;
import static org.apache.tsfile.utils.RegexUtils.parseLikePatternToRegex;

public class LikeExpression extends UnaryExpression {

  private final String patternString;
  private final Pattern pattern;

  private final boolean isNot;

  public LikeExpression(Expression expression, String patternString, boolean isNot) {
    super(expression);
    this.patternString = patternString;
    this.isNot = isNot;
    pattern = compileRegex(parseLikePatternToRegex(patternString));
  }

  public LikeExpression(
      Expression expression, String patternString, Pattern pattern, boolean isNot) {
    super(expression);
    this.patternString = patternString;
    this.pattern = pattern;
    this.isNot = isNot;
  }

  public LikeExpression(ByteBuffer byteBuffer) {
    super(Expression.deserialize(byteBuffer));
    patternString = ReadWriteIOUtils.readString(byteBuffer);
    isNot = ReadWriteIOUtils.readBool(byteBuffer);
    pattern = compileRegex(parseLikePatternToRegex(patternString));
  }

  public String getPatternString() {
    return patternString;
  }

  public Pattern getPattern() {
    return pattern;
  }

  public boolean isNot() {
    return isNot;
  }

  @Override
  protected String getExpressionStringInternal() {
    return expression.getExpressionString() + (isNot ? " NOT" : "") + " LIKE '" + pattern + "'";
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.LIKE;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(patternString, byteBuffer);
    ReadWriteIOUtils.write(isNot, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(patternString, stream);
    ReadWriteIOUtils.write(isNot, stream);
  }

  @Override
  public String getOutputSymbolInternal() {
    return expression.getOutputSymbol() + (isNot ? " NOT" : "") + " LIKE '" + pattern + "'";
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitLikeExpression(this, context);
  }
}
