/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql.validate;

import org.apache.calcite.runtime.Resources;

/**
 * Compiler-checked resources similar to CalciteResource in Calcite project. These are extra
 * exceptions we want to extend Calcite with. Ref:
 * https://issues.apache.org/jira/browse/CALCITE-6069
 */
public interface ExtraCalciteResource {

    @Resources.BaseMessage(
            "No match found for function signature {0}.\nSupported signatures are:\n{1}")
    Resources.ExInst<SqlValidatorException> validatorNoFunctionMatch(
            String invocation, String allowedSignatures);

    @Resources.BaseMessage(
            "Lambda parameter name ''{0}'' is not allowed: names beginning with ''{1}'' are reserved.")
    Resources.ExInst<SqlValidatorException> reservedLambdaParameterName(String name, String prefix);

    @Resources.BaseMessage(
            "Duplicate lambda parameter name ''{0}''. The parameters of a lambda expression must "
                    + "have unique names.")
    Resources.ExInst<SqlValidatorException> duplicateLambdaParameterName(String name);

    @Resources.BaseMessage(
            "{0} are not supported in the body of a lambda expression. A lambda body must be a "
                    + "scalar expression over its parameters and the columns it captures.")
    Resources.ExInst<SqlValidatorException> unsupportedInLambdaBody(String construct);

    @Resources.BaseMessage(
            "{0} over a lambda parameter are not supported in the body of a lambda expression. "
                    + "''{1}'' is a lambda parameter, which exists per element and has no group to "
                    + "be evaluated over; only the columns of the enclosing query can be used here.")
    Resources.ExInst<SqlValidatorException> unsupportedOverLambdaParameterInLambdaBody(
            String construct, String parameter);
}
