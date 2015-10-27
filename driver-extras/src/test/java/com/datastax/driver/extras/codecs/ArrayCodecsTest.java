/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.extras.codecs;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.*;
import com.datastax.driver.extras.codecs.ArrayCodecs.*;

public class ArrayCodecsTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final ObjectArrayCodec<String> stringArrayCodec = new ObjectArrayCodec<>(
        DataType.list(DataType.varchar()),
        String[].class,
        TypeCodec.varchar());

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "CREATE TABLE lists (pk int PRIMARY KEY, "
                + "l_int list<int>, "
                + "l_bigint list<bigint>, "
                + "l_float list<float>, "
                + "l_double list<double>, "
                + "l_string list<text> "
                + ")",
            "INSERT INTO lists (pk, l_int, l_bigint, l_float, l_double, l_string) VALUES (1, "
                + "[1, 2, 3], "
                + "[4, 5, 6], "
                + "[1.0, 2.0, 3.0], "
                + "[4.0, 5.0, 6.0], "
                + "['a', 'b', 'c'] "
                + ")"
        );
    }

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        return builder.withCodecRegistry(
            ArrayCodecs.withPrimitiveArrayCodecs(new CodecRegistry()).register(stringArrayCodec)
        );
    }

    @DataProvider(name = "ArrayCodecsTest-serializing")
    public static Object[][] parametersForSerializationTests() {
        return new Object[][] {
            { "l_int",    int   [].class, new int   [] {1, 2, 3} },
            { "l_bigint", long  [].class, new long  [] {4, 5, 6} },
            { "l_float" , float [].class, new float [] {1, 2, 3} },
            { "l_double", double[].class, new double[] {4, 5, 6} },
            { "l_string", String[].class, new String[] {"a","b","c"} }
        };
    }

    @DataProvider(name = "ArrayCodecsTest-formatting")
    public static Object[][] parametersForFormattingTests() {
        return new Object[][] {
            { new IntArrayCodec()   , new int   [] {1, 2, 3}    , new int   [0], "[1,2,3]" },
            { new LongArrayCodec()  , new long  [] {4, 5, 6}    , new long  [0], "[4,5,6]" },
            { new FloatArrayCodec() , new float [] {1, 2, 3}    , new float [0], "[1.0,2.0,3.0]" },
            { new DoubleArrayCodec(), new double[] {4, 5, 6}    , new double[0], "[4.0,5.0,6.0]" },
            { stringArrayCodec      , new String[] {"a","b","c"}, new String[0], "['a','b','c']" }
        };
    }

    @Test(groups = "short", dataProvider = "ArrayCodecsTest-serializing")
    public <A> void should_read_list_column_as_array(String columnName, Class<A> arrayClass, A expected) {
        Row row = session.execute(String.format("SELECT %s FROM lists WHERE pk = 1", columnName)).one();
        A actual = row.get(columnName, arrayClass);
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "short", dataProvider = "ArrayCodecsTest-serializing")
    public <A> void should_set_list_column_with_array(String columnName, Class<A> arrayClass, A expected) {
        PreparedStatement ps = session.prepare(String.format("INSERT INTO lists (pk, %s) VALUES (?, ?)", columnName));
        BoundStatement bs = ps.bind()
            .setInt(0, 2)
            .set(columnName, expected, arrayClass);
        session.execute(bs);
        Row row = session.execute(String.format("SELECT %s FROM lists WHERE pk = 2", columnName)).one();
        List<?> list = row.getList(columnName, Primitives.wrap(arrayClass.getComponentType()));
        assertThat(list.toArray()).isEqualTo(expected);
    }

    @Test(groups = "unit", dataProvider = "ArrayCodecsTest-formatting")
    public <A> void should_format_array_as_cql_literal(TypeCodec<A> codec, A array, A emptyArray, String cql) {
        assertThat(codec.format(array)).isEqualTo(cql);
        assertThat(codec.format(emptyArray)).isEqualTo("[]");
        assertThat(codec.format(null)).isEqualToIgnoringCase("NULL");
    }

    @SuppressWarnings("unused")
    @Test(groups = "unit", dataProvider = "ArrayCodecsTest-formatting")
    public <A> void should_parse_array_from_cql_literal(TypeCodec<A> codec, A array, A emptyArray, String cql) {
        assertThat(codec.parse(cql)).isEqualTo(array);
        assertThat(codec.parse("NULL")).isNull();
        assertThat(codec.parse("")).isNull();
        assertThat(codec.parse(null)).isNull();
    }

}
