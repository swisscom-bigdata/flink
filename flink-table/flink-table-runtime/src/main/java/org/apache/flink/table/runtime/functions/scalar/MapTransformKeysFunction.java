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
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.util.EqualityAndHashcodeProvider;
import org.apache.flink.table.runtime.util.ObjectContainer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_TRANSFORM_KEYS}. */
@Internal
public class MapTransformKeysFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter keyGetter;
    private final ArrayData.ElementGetter valueGetter;

    /**
     * The lambda body may write its result into a buffer it reuses across applications, e.g. when
     * the body constructs an {@code ARRAY} or {@code ROW}. Because the keys are collected before
     * the output map is built, each one has to be detached from that buffer.
     */
    private final TypeSerializer<Object> resultKeySerializer;

    /** Compares transformed keys by SQL logical equality rather than by {@link Object#equals}. */
    private final EqualityAndHashcodeProvider keyEqualityProvider;

    @SuppressWarnings("unchecked")
    public MapTransformKeysFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_TRANSFORM_KEYS, context);
        final KeyValueDataType mapDataType = (KeyValueDataType) getArgumentDataTypes().get(0);
        keyGetter = ArrayData.createElementGetter(mapDataType.getKeyDataType().getLogicalType());
        valueGetter =
                ArrayData.createElementGetter(mapDataType.getValueDataType().getLogicalType());
        final DataType resultKeyDataType =
                ((KeyValueDataType) getOutputDataType()).getKeyDataType().toInternal();
        resultKeySerializer =
                (TypeSerializer<Object>)
                        InternalSerializers.create(resultKeyDataType.getLogicalType());
        keyEqualityProvider = new EqualityAndHashcodeProvider(context, resultKeyDataType);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        keyEqualityProvider.open(context);
    }

    public @Nullable MapData eval(@Nullable MapData map, BiFunctionData lambda) {
        if (map == null) {
            return null;
        }
        final ArrayData keys = map.keyArray();
        final ArrayData values = map.valueArray();
        // insertion-ordered so that the entries of the result keep the order of the input map
        final Map<Object, Object> transformed = new LinkedHashMap<>();
        final Set<ObjectContainer> seenKeys = new HashSet<>();
        for (int i = 0; i < map.size(); i++) {
            // a NULL key or value is passed to the lambda like any other one
            final Object key = keyGetter.getElementOrNull(keys, i);
            final Object value = valueGetter.getElementOrNull(values, i);
            final Object newKey = lambda.apply(key, value);
            if (newKey == null) {
                throw new RuntimeException(
                        "MAP_TRANSFORM_KEYS: the transformed key must not be NULL.");
            }
            final Object copiedKey = resultKeySerializer.copy(newKey);
            if (!seenKeys.add(
                    new ObjectContainer(
                            copiedKey,
                            keyEqualityProvider::equals,
                            keyEqualityProvider::hashCode))) {
                throw new RuntimeException(
                        "MAP_TRANSFORM_KEYS produced a duplicate key: " + copiedKey);
            }
            transformed.put(copiedKey, value);
        }
        return new GenericMapData(transformed);
    }

    @Override
    public void close() throws Exception {
        keyEqualityProvider.close();
    }
}
