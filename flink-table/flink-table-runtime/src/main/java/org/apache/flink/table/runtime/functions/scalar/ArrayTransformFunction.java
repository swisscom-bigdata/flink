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
import org.apache.flink.table.data.FunctionData1;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_TRANSFORM}. */
@Internal
public class ArrayTransformFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter elementGetter;

    /**
     * The lambda body may write its result into a buffer it reuses across applications, e.g. when
     * the body constructs an {@code ARRAY} or {@code ROW}. Because the results are collected before
     * the output array is built, each one has to be detached from that buffer.
     */
    private final TypeSerializer<Object> resultElementSerializer;

    @SuppressWarnings("unchecked")
    public ArrayTransformFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_TRANSFORM, context);
        final DataType elementDataType =
                ((CollectionDataType) getArgumentDataTypes().get(0)).getElementDataType();
        elementGetter = ArrayData.createElementGetter(elementDataType.getLogicalType());
        final DataType resultElementDataType =
                ((CollectionDataType) getOutputDataType()).getElementDataType();
        resultElementSerializer =
                (TypeSerializer<Object>)
                        InternalSerializers.create(resultElementDataType.getLogicalType());
    }

    public @Nullable ArrayData eval(@Nullable ArrayData array, FunctionData1 lambda) {
        if (array == null) {
            return null;
        }
        final Object[] transformed = new Object[array.size()];
        for (int i = 0; i < array.size(); i++) {
            // a NULL element is passed to the lambda like any other element
            final Object element = elementGetter.getElementOrNull(array, i);
            final Object result = lambda.apply(element);
            transformed[i] = result == null ? null : resultElementSerializer.copy(result);
        }
        return new GenericArrayData(transformed);
    }
}
