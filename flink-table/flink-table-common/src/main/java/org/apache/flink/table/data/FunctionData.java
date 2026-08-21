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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.FunctionType;

/**
 * A one-parameter lambda argument that accepts and returns {@link RowData internal data
 * structures}.
 *
 * <p>This is the internal-data counterpart of {@link java.util.function.Function}, which a function
 * receives when it works on external classes. Which of the two a lambda argument is passed as is
 * not a property of the lambda but of the function that receives it: it is the conversion class of
 * the argument's {@link FunctionType} and is therefore declared by the function's type inference,
 * in the same way that an {@code ARRAY} argument is received either as {@code ArrayData} or as an
 * array of external elements.
 *
 * <p>The values handed to {@link #apply(Object)} and the value it returns use the internal
 * representation of the lambda's parameter and result types, i.e. the classes that {@link
 * org.apache.flink.table.types.logical.utils.LogicalTypeUtils#toInternalConversionClass} returns
 * for them. Declaring this interface in an {@code eval()} method therefore states that contract,
 * and a function whose type inference does not agree is rejected during planning.
 *
 * <p>A mutable result may be backed by evaluator-owned memory that a later application reuses. A
 * function that retains or buffers a result across another application must deep-copy it first,
 * using a serializer for the result's data type.
 *
 * @see BiFunctionData
 * @see TriFunctionData
 */
@PublicEvolving
@FunctionalInterface
public interface FunctionData {

    /** Applies the lambda to the given internal-data argument. */
    Object apply(Object arg0);
}
