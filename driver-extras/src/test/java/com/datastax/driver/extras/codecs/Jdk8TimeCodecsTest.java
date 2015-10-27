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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.extras.codecs.Jdk8TimeCodecs.ZonedDateTimeCodec;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.TypeCodec.tuple;

@CassandraVersion(major=2.2)
public class Jdk8TimeCodecsTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS foo (c1 text PRIMARY KEY, " +
                        "cd date, ctime time, " +
                        "ctimestamp timestamp, " +
                        "ctuple tuple<timestamp,varchar>)");
    }

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        return builder.withCodecRegistry(Jdk8TimeCodecs.withJdk8TimeCodecs(new CodecRegistry()));
    }

    /**
     * <p>
     * Validates that a <code>time</code> column can be mapped to a {@link LocalTime} by using
     * {@link Jdk8TimeCodecs.LocalTimeCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-606
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_time_to_localtime() {
        // given
        LocalTime time = LocalTime.of(12, 16, 34, 999);
        // when
        session.execute("insert into foo (c1, ctime) values (?, ?)", "should_map_time_to_localtime", time);
        ResultSet result = session.execute("select ctime from foo where c1=?", "should_map_time_to_localtime");
        // then
        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);
        Row row = result.one();
        assertThat(row.get("ctime", LocalTime.class)).isEqualTo(time);
        assertThat(row.getTime("ctime")).isEqualTo(time.toNanoOfDay());
    }

    /**
     * <p>
     * Validates that a <code>date</code> column can be mapped to a {@link LocalDate} by using
     * {@link Jdk8TimeCodecs.LocalDateCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-606
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_date_to_localdate() {
        // given
        LocalDate localDate = LocalDate.of(2015, 1, 1);
        com.datastax.driver.core.LocalDate driverLocalDate = com.datastax.driver.core.LocalDate.fromYearMonthDay(2015, 1, 1);
        // when
        session.execute("insert into foo (c1, cd) values (?, ?)", "should_map_date_to_localdate", localDate);
        ResultSet result = session.execute("select cd from foo where c1=?", "should_map_date_to_localdate");
        // then
        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);
        Row row = result.one();
        assertThat(row.get("cd", LocalDate.class)).isEqualTo(localDate);
        assertThat(row.getDate("cd")).isEqualTo(driverLocalDate);
    }

    /**
     * <p>
     * Validates that a <code>timestamp</code> column can be mapped to a {@link Instant} by using
     * {@link Jdk8TimeCodecs.InstantCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-606
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_timestamp_to_instant() {
        // given
        ZonedDateTime zdt = ZonedDateTime.parse("2010-06-30T01:20+05:00");
        Instant instant = zdt.toInstant();
        // when
        session.execute("insert into foo (c1, ctimestamp) values (?, ?)", "should_map_timestamp_to_instant", instant);
        ResultSet result = session.execute("select ctimestamp from foo where c1=?", "should_map_timestamp_to_instant");
        // then
        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);
        Row row = result.one();
        assertThat(row.get("ctimestamp", Instant.class)).isEqualTo(instant);
        // Since timezone is not preserved in timestamp type, set it before comparing.
        assertThat(row.get("ctimestamp", Instant.class).atZone(zdt.getZone())).isEqualTo(zdt);
        assertThat(row.getTimestamp("ctimestamp")).isEqualTo(Date.from(instant));
    }

    /**
     * <p>
     * Validates that a <code>tuple&lt;timestamp,text&gt;</code> column can be mapped to a {@link ZonedDateTime} by using
     * {@link ZonedDateTimeCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-606
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_tuple_to_zoneddatetime() {
        // given
        // Register codec that maps ZonedDateTime <-> tuple<timestamp,varchar>
        TupleType dateWithTimeZoneType = cluster.getMetadata().newTupleType(timestamp(), varchar());
        ZonedDateTimeCodec instantCodec = new ZonedDateTimeCodec(tuple(dateWithTimeZoneType));
        cluster.getConfiguration().getCodecRegistry().register(instantCodec);
        ZonedDateTime expected = ZonedDateTime.parse("2010-06-30T01:20+05:30");
        // when
        PreparedStatement insertStmt = session.prepare("insert into foo (c1, ctuple) values (?, ?)");
        session.execute(insertStmt.bind("should_map_tuple_to_instant", expected));
        ResultSet result = session.execute("select ctuple from foo where c1=?", "should_map_tuple_to_instant");
        // then
        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);
        Row row = result.one();
        ZonedDateTime actual = row.get("ctuple", ZonedDateTime.class);
        // Since timezone is preserved in the tuple, the date times should match perfectly.
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.toInstant()).isEqualTo(expected.toInstant());
        // Ensure the timezones match as well.
        // (This is just a safety check as the previous assert would have failed otherwise).
        assertThat(actual.getZone()).isEqualTo(expected.getZone());
    }
}
