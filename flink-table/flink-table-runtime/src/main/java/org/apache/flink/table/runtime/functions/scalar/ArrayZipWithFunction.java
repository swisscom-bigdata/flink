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
import org.apache.flink.table.data.FunctionData2;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_ZIP_WITH}. */
@Internal
public class ArrayZipWithFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter element1Getter;
    private final ArrayData.ElementGetter element2Getter;

    /**
     * The lambda body may write its result into a buffer it reuses across applications, e.g. when
     * the body constructs an {@code ARRAY} or {@code ROW}. Because the results are collected before
     * the output array is built, each one has to be detached from that buffer.
     */
    private final TypeSerializer<Object> resultElementSerializer;

    @SuppressWarnings("unchecked")
    public ArrayZipWithFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_ZIP_WITH, context);
        element1Getter = createElementGetter(0);
        element2Getter = createElementGetter(1);
        final DataType resultElementDataType =
                ((CollectionDataType) getOutputDataType()).getElementDataType();
        resultElementSerializer =
                (TypeSerializer<Object>)
                        InternalSerializers.create(resultElementDataType.getLogicalType());
    }

    private ArrayData.ElementGetter createElementGetter(int argumentPos) {
        final DataType elementDataType =
                ((CollectionDataType) getArgumentDataTypes().get(argumentPos)).getElementDataType();
        return ArrayData.createElementGetter(elementDataType.getLogicalType());
    }

    public @Nullable ArrayData eval(
            @Nullable ArrayData array1, @Nullable ArrayData array2, FunctionData2 lambda) {
        if (array1 == null || array2 == null) {
            return null;
        }
        // The shorter array is padded with NULLs so that no element of the longer one is dropped
        final int size = Math.max(array1.size(), array2.size());
        final Object[] zipped = new Object[size];
        for (int i = 0; i < size; i++) {
            final Object element1 =
                    i < array1.size() ? element1Getter.getElementOrNull(array1, i) : null;
            final Object element2 =
                    i < array2.size() ? element2Getter.getElementOrNull(array2, i) : null;
            final Object result = lambda.apply(element1, element2);
            zipped[i] = result == null ? null : resultElementSerializer.copy(result);
        }
        return new GenericArrayData(zipped);
    }
}
