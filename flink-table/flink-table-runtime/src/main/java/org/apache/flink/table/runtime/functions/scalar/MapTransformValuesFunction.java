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
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.KeyValueDataType;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_TRANSFORM_VALUES}. */
@Internal
public class MapTransformValuesFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter keyGetter;
    private final ArrayData.ElementGetter valueGetter;

    /**
     * The lambda body may write its result into a buffer it reuses across applications, e.g. when
     * the body constructs an {@code ARRAY} or {@code ROW}. Because the values are collected before
     * the output map is built, each one has to be detached from that buffer.
     */
    private final TypeSerializer<Object> resultValueSerializer;

    @SuppressWarnings("unchecked")
    public MapTransformValuesFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_TRANSFORM_VALUES, context);
        final KeyValueDataType mapDataType = (KeyValueDataType) getArgumentDataTypes().get(0);
        keyGetter = ArrayData.createElementGetter(mapDataType.getKeyDataType().getLogicalType());
        valueGetter =
                ArrayData.createElementGetter(mapDataType.getValueDataType().getLogicalType());
        final KeyValueDataType outputDataType = (KeyValueDataType) getOutputDataType();
        resultValueSerializer =
                (TypeSerializer<Object>)
                        InternalSerializers.create(
                                outputDataType.getValueDataType().getLogicalType());
    }

    public @Nullable MapData eval(@Nullable MapData map, FunctionData2 lambda) {
        if (map == null) {
            return null;
        }
        final ArrayData keys = map.keyArray();
        final ArrayData values = map.valueArray();
        // insertion-ordered so that the entries of the result keep the order of the input map
        final Map<Object, Object> transformed = new LinkedHashMap<>();
        for (int i = 0; i < map.size(); i++) {
            // a NULL key or value is passed to the lambda like any other one
            final Object key = keyGetter.getElementOrNull(keys, i);
            final Object result = lambda.apply(key, valueGetter.getElementOrNull(values, i));
            transformed.put(key, result == null ? null : resultValueSerializer.copy(result));
        }
        return new GenericMapData(transformed);
    }
}
