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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.FunctionType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link InternalSerializers}. */
class InternalSerializersTest {

    @Test
    void testFunctionTypeCannotBeMaterialized() {
        // The FUNCTION type is a lambda helper type. Attempting to create a serializer for it (as
        // happens when it is used as a column, state, or persisted type) must fail with a clear
        // message rather than silently succeeding.
        final FunctionType functionType = new FunctionType(1);

        assertThatThrownBy(() -> InternalSerializers.create(functionType))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("FUNCTION data type is a helper type");
    }
}
