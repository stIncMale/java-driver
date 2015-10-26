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
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;

@CassandraVersion(major=2.2)
public class JodaTimeCodecsTest extends CCMBridge.PerClassSingleNodeCluster {

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
        return builder.withCodecRegistry(JodaTimeCodecs.withJodaTimeCodecs(new CodecRegistry()));
    }

    /**
     * <p>
     * Validates that a <code>time</code> column can be mapped to a {@link LocalTime} by using
     * {@link JodaTimeCodecs.LocalTimeCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-846
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_time_to_localtime() {
        // given
        LocalTime time = new LocalTime(12, 16, 34, 999);
        // when
        session.execute("insert into foo (c1, ctime) values (?, ?)", "should_map_time_to_localtime", time);
        ResultSet result = session.execute("select ctime from foo where c1=?", "should_map_time_to_localtime");
        // then
        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);
        Row row = result.one();
        assertThat(row.get("ctime", LocalTime.class)).isEqualTo(time);
        assertThat(row.getTime("ctime")).isEqualTo(NANOSECONDS.convert(time.getMillisOfDay(), MILLISECONDS));
    }

    /**
     * <p>
     * Validates that a <code>date</code> column can be mapped to a {@link org.joda.time.LocalDate} by using
     * {@link JodaTimeCodecs.LocalDateCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-846
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_date_to_localdate() {
        // given
        LocalDate localDate = new LocalDate(2015, 1, 1);
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
     * Validates that a <code>timestamp</code> column can be mapped to a {@link DateTime} by using
     * {@link JodaTimeCodecs.DateTimeCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-846
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_timestamp_to_datetime() {
        // given
        DateTime dateTime = DateTime.parse("2010-06-30T01:20+05:00");
        // when
        session.execute("insert into foo (c1, ctimestamp) values (?, ?)", "should_map_timestamp_to_datetime", dateTime);
        ResultSet result = session.execute("select ctimestamp from foo where c1=?", "should_map_timestamp_to_datetime");
        // then
        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);
        Row row = result.one();
        // Since timezone is not preserved in timestamp type, set it.
        assertThat(row.get("ctimestamp", DateTime.class).withZone(dateTime.getZone())).isEqualTo(dateTime);
        assertThat(row.getTimestamp("ctimestamp")).isEqualTo(dateTime.toDate());
    }

    /**
     * <p>
     * Validates that a <code>tuple&lt;timestamp,text&gt;</code> column can be mapped to a {@link DateTime} by using
     * {@link JodaTimeCodecs.TimeZonePreservingDateTimeCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @jira_ticket JAVA-846
     * @since 2.2.0
     */
    @Test(groups="short")
    public void should_map_tuple_to_datetime() {
        // given
        // Register codec that maps DateTime <-> tuple<timestamp,varchar>
        TupleType dateWithTimeZoneType = cluster.getMetadata().newTupleType(timestamp(), varchar());
        JodaTimeCodecs.TimeZonePreservingDateTimeCodec dateTimeCodec = new JodaTimeCodecs.TimeZonePreservingDateTimeCodec(TypeCodec.tuple(dateWithTimeZoneType));
        cluster.getConfiguration().getCodecRegistry().register(dateTimeCodec);
        DateTime expected = DateTime.parse("2010-06-30T01:20+05:30");
        // when
        PreparedStatement insertStmt = session.prepare("insert into foo (c1, ctuple) values (?, ?)");
        session.execute(insertStmt.bind("should_map_tuple_to_datetime", expected));
        ResultSet result = session.execute("select ctuple from foo where c1=?", "should_map_tuple_to_datetime");
        // then
        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);
        Row row = result.one();
        // Since timezone is preserved in the tuple, the date times should match perfectly.
        assertThat(row.get("ctuple", DateTime.class)).isEqualTo(expected);
        // Ensure the timezones match as well.
        // (This is just a safety check as the previous assert would have failed otherwise).
        assertThat(row.get("ctuple", DateTime.class).getZone()).isEqualTo(expected.getZone());
    }
}
