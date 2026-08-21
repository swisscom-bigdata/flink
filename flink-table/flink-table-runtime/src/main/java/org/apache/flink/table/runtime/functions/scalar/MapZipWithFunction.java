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
import org.apache.flink.table.data.FunctionData3;
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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_ZIP_WITH}. */
@Internal
public class MapZipWithFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter keyGetter;
    private final ArrayData.ElementGetter value1Getter;
    private final ArrayData.ElementGetter value2Getter;

    /**
     * The lambda body may write its result into a buffer it reuses across applications, e.g. when
     * the body constructs an {@code ARRAY} or {@code ROW}. Because the values are collected before
     * the output map is built, each one has to be detached from that buffer.
     */
    private final TypeSerializer<Object> resultValueSerializer;

    /**
     * Compares keys by SQL logical equality rather than by {@link Object#equals}. Validation casts
     * both maps to the common key type, so a key of either map is materialized in the same
     * representation and the two collide in the union as expected.
     */
    private final EqualityAndHashcodeProvider keyEqualityProvider;

    @SuppressWarnings("unchecked")
    public MapZipWithFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_ZIP_WITH, context);
        final KeyValueDataType map1DataType = (KeyValueDataType) getArgumentDataTypes().get(0);
        final KeyValueDataType map2DataType = (KeyValueDataType) getArgumentDataTypes().get(1);
        keyGetter = ArrayData.createElementGetter(map1DataType.getKeyDataType().getLogicalType());
        value1Getter =
                ArrayData.createElementGetter(map1DataType.getValueDataType().getLogicalType());
        value2Getter =
                ArrayData.createElementGetter(map2DataType.getValueDataType().getLogicalType());
        final KeyValueDataType outputDataType = (KeyValueDataType) getOutputDataType();
        resultValueSerializer =
                (TypeSerializer<Object>)
                        InternalSerializers.create(
                                outputDataType.getValueDataType().getLogicalType());
        final DataType keyDataType = outputDataType.getKeyDataType().toInternal();
        keyEqualityProvider = new EqualityAndHashcodeProvider(context, keyDataType);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        keyEqualityProvider.open(context);
    }

    public @Nullable MapData eval(
            @Nullable MapData map1, @Nullable MapData map2, FunctionData3 lambda) {
        if (map1 == null || map2 == null) {
            return null;
        }
        final Map<ObjectContainer, Object> values1 = index(map1, value1Getter);
        final Map<ObjectContainer, Object> values2 = index(map2, value2Getter);
        // the keys of the first map come first, then those only present in the second one
        final Set<ObjectContainer> union = new LinkedHashSet<>(values1.keySet());
        union.addAll(values2.keySet());

        final Map<Object, Object> zipped = new LinkedHashMap<>();
        for (ObjectContainer key : union) {
            // a key absent from a map and one mapped to NULL both reach the lambda as NULL
            final Object result =
                    lambda.apply(
                            key == null ? null : key.getObject(),
                            values1.get(key),
                            values2.get(key));
            zipped.put(
                    key == null ? null : key.getObject(),
                    result == null ? null : resultValueSerializer.copy(result));
        }
        return new GenericMapData(zipped);
    }

    private Map<ObjectContainer, Object> index(MapData map, ArrayData.ElementGetter valueGetter) {
        final ArrayData keys = map.keyArray();
        final ArrayData values = map.valueArray();
        final Map<ObjectContainer, Object> indexed = new LinkedHashMap<>();
        for (int i = 0; i < map.size(); i++) {
            indexed.put(
                    wrapKey(keyGetter.getElementOrNull(keys, i)),
                    valueGetter.getElementOrNull(values, i));
        }
        return indexed;
    }

    private @Nullable ObjectContainer wrapKey(@Nullable Object key) {
        if (key == null) {
            return null;
        }
        return new ObjectContainer(key, keyEqualityProvider::equals, keyEqualityProvider::hashCode);
    }

    @Override
    public void close() throws Exception {
        keyEqualityProvider.close();
    }
}
