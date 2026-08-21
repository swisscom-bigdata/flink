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

/**
 * A three-parameter lambda argument that accepts and returns {@link RowData internal data
 * structures}.
 *
 * <p>This is the internal-data counterpart of {@link org.apache.flink.util.function.Function3}. See
 * {@link FunctionData1} for how a function declares which of the two it receives.
 */
@PublicEvolving
@FunctionalInterface
public interface FunctionData3 {

    /** Applies the lambda to the given internal-data arguments. */
    Object apply(Object arg0, Object arg1, Object arg2);
}
