/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.sql.expressions

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.DataType

/**
 * A user-defined function. To create one, use the `udf` functions in `functions`.
 *
 * As an example:
 * {{{
 *   // Defined a UDF that returns true or false based on some numeric score.
 *   val predict = udf((score: Double) => if (score > 0.5) true else false)
 *
 *   // Projects a column that adds a prediction column based on the score column.
 *   df.select( predict(df("score")) )
 * }}}
 *
 * @note The user-defined functions must be deterministic. Due to optimization,
 * duplicate invocations may be eliminated or the function may even be invoked more times than
 * it is present in the query.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
class UserDefinedFunction protected[sql] (
    val f: AnyRef,
    val dataType: DataType,
    val inputTypes: Option[Seq[DataType]],
    val deterministic: Boolean) {

  /**
   * Returns an expression that invokes the UDF, using the given arguments.
   *
   * @since 1.3.0
   */
  def apply(exprs: Column*): Column = {
    Column(ScalaUDF(
      f, dataType, exprs.map(_.expr), udfDeterministic = deterministic, inputTypes.getOrElse(Nil)))
  }

  /**
   * Updates the StructField with a new comment value.
   */
  def nonDeterministic(): UserDefinedFunction = {
    new UserDefinedFunction(f, dataType, inputTypes, deterministic = false)
  }
}

object UserDefinedFunction {
  def apply(
      f: AnyRef,
      dataType: DataType,
      inputTypes: Option[Seq[DataType]]): UserDefinedFunction =
    new UserDefinedFunction(f, dataType, inputTypes, deterministic = true)

  def apply(
      f: AnyRef,
      dataType: DataType,
      inputTypes: Option[Seq[DataType]],
      deterministic: Boolean): UserDefinedFunction =
    new UserDefinedFunction(f, dataType, inputTypes, deterministic = true)

  def unapply(u: UserDefinedFunction): Option[(AnyRef, DataType, Option[Seq[DataType]])] =
    Some(u.f, u.dataType, u.inputTypes)
}
