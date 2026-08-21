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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.FunctionData1;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link BuiltInFunctionDefinitions#ARRAY_FILTER}. */
@Internal
public class ArrayFilterFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter elementGetter;

    public ArrayFilterFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ARRAY_FILTER, context);
        final DataType elementDataType =
                ((CollectionDataType) getArgumentDataTypes().get(0)).getElementDataType();
        elementGetter = ArrayData.createElementGetter(elementDataType.getLogicalType());
    }

    public @Nullable ArrayData eval(@Nullable ArrayData array, FunctionData1 predicate) {
        if (array == null) {
            return null;
        }
        final List<Object> kept = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            // a NULL element is passed to the predicate like any other element
            final Object element = elementGetter.getElementOrNull(array, i);
            // only a TRUE predicate keeps the element, so a NULL result excludes it as in WHERE
            if (Boolean.TRUE.equals(predicate.apply(element))) {
                kept.add(element);
            }
        }
        return new GenericArrayData(kept.toArray());
    }
}
