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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.BiFunctionData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.CollectionDataType;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_REDUCE}. */
@Internal
public class ArrayReduceFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter elementGetter;

    /**
     * The lambda body may write its result into a buffer it reuses across applications, e.g. when
     * the body constructs an {@code ARRAY} or {@code ROW}. Because the accumulator is carried into
     * the next application, it has to be detached from that buffer.
     */
    private final TypeSerializer<Object> accumulatorSerializer;

    @SuppressWarnings("unchecked")
    public ArrayReduceFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_REDUCE, context);
        final CollectionDataType arrayDataType = (CollectionDataType) getArgumentDataTypes().get(0);
        elementGetter =
                ArrayData.createElementGetter(arrayDataType.getElementDataType().getLogicalType());
        accumulatorSerializer =
                (TypeSerializer<Object>)
                        InternalSerializers.create(getOutputDataType().getLogicalType());
    }

    public @Nullable Object eval(
            @Nullable ArrayData array, @Nullable Object initial, BiFunctionData reducer) {
        if (array == null) {
            return null;
        }
        // An empty array never applies the reducer, so there is no body buffer to detach from and
        // the initial accumulator is returned by reference, aliasing the caller's argument. The
        // narrow reason this is safe: the returned reference has exactly the lifetime the `initial`
        // operand already had -- BridgingFunctionGenUtil#generateScalarFunctionCall reads that
        // operand's result term and returns it unchanged, extending nothing -- so this call imposes
        // no ownership obligation that evaluating the operand did not already impose. Deliberately
        // NOT claimed here is what the consumer does: a BinaryRowData projection serializes the
        // value through its row writer, a GenericRowData projection stores the bare reference in a
        // row that is reused per record, and an enclosing scalar call may consume the result
        // without reaching a projection at all. The invariant relied on is only the general one --
        // a value derived from the current record must be copied before it is retained past it --
        // which holds for the `initial` expression with or without this function.
        // ArrayReduceAccumulatorOwnershipITCase pins the buffering-join case.
        Object accumulator = initial;
        for (int i = 0; i < array.size(); i++) {
            // a NULL element is passed to the reducer like any other one
            final Object result =
                    reducer.apply(accumulator, elementGetter.getElementOrNull(array, i));
            accumulator = result == null ? null : accumulatorSerializer.copy(result);
        }
        return accumulator;
    }
}
