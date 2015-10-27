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
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.driver.core.*;

/**
 * A collection of convenience {@link TypeCodec} instances useful for
 * serializing between CQL types and JDK 8 types from the {@code java.time} API.
 * <p>
 * The codecs in this class provide the following mappings:
 *
 * <table summary="Supported Mappings">
 *     <tr>
 *         <th>Codec</th>
 *         <th>CQL type</th>
 *         <th>Joda Time</th>
 *     </tr>
 *     <tr>
 *         <td>{@link LocalTimeCodec}</td>
 *         <td>{@link DataType#time() time}</td>
 *         <td>{@link LocalTime}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link LocalDateCodec}</td>
 *         <td>{@link DataType#date() date}</td>
 *         <td>{@link java.time.LocalDate LocalDate}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link InstantCodec}</td>
 *         <td>{@link DataType#timestamp() timestamp}</td>
 *         <td>{@link java.time.Instant}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link ZonedDateTimeCodec}</td>
 *         <td>{@link TupleType tuple} of ({@link DataType#timestamp() timestamp}, {@link DataType#varchar() varchar})</td>
 *         <td>{@link ZonedDateTime}</td>
 *     </tr>
 * </table>
 *
 * These codecs may either be registered individually or they may all
 * (with exception to {@link ZonedDateTimeCodec})
 * be registered via {@link Jdk8TimeCodecs#withJdk8TimeCodecs(CodecRegistry)}.
 */
public abstract class Jdk8TimeCodecs {

    /**
     * <p>
     *     Registers all defined {@link TypeCodec} instances in this class
     *     (with exception to {@link ZonedDateTimeCodec}) with
     *     the given {@link CodecRegistry}.
     * </p>
     * @param registry registry to add Codecs to.
     * @return updated registry with codecs.
     */
    public static CodecRegistry withJdk8TimeCodecs(CodecRegistry registry) {
        return registry.register(LocalTimeCodec.instance, LocalDateCodec.instance, InstantCodec.instance);
    }

    /**
     * {@link TypeCodec} that maps {@link LocalTime} <-> CQL time (long representing nanoseconds since midnight)
     * allowing the setting and retrieval of <code>time</code> columns as {@link LocalTime}
     * instances.
     *
     * IMPORTANT: {@link LocalTime} as millisecond precision; nanoseconds below one millisecond will be lost
     * during deserialization.
     */
    public static class LocalTimeCodec extends MappingCodec<LocalTime, Long> {

        public static final LocalTimeCodec instance = new LocalTimeCodec();

        private LocalTimeCodec() {
            super(time(), LocalTime.class);
        }

        @Override
        protected LocalTime deserialize(Long value) {
            if (value == null)
                return null;
            return LocalTime.ofNanoOfDay(value);
        }

        @Override
        protected Long serialize(LocalTime value) {
            if (value == null)
                return null;
            return value.toNanoOfDay();
        }
    }

    /**
     * {@link TypeCodec} that maps
     * {@link java.time.LocalDate} <-> {@link LocalDate} allowing the
     * setting and retrieval of <code>date</code> columns as
     * {@link java.time.LocalDate} instances.
     */
    public static class LocalDateCodec extends MappingCodec<java.time.LocalDate, LocalDate> {

        public static final LocalDateCodec instance = new LocalDateCodec();

        private LocalDateCodec() {
            super(date(), java.time.LocalDate.class);
        }

        @Override
        protected java.time.LocalDate deserialize(LocalDate value) {
            return value == null ? null : java.time.LocalDate.of(value.getYear(), value.getMonth(), value.getDay());
        }

        @Override
        protected LocalDate serialize(java.time.LocalDate value) {
            return value == null ? null : LocalDate.fromYearMonthDay(value.getYear(), value.getMonthValue(), value.getDayOfMonth());
        }
    }

    /**
     * <p>
     *     {@link TypeCodec} that maps {@link Instant} <-> {@link Date}
     *     allowing the setting and retrieval of <code>timestamp</code>
     *     columns as {@link Instant} instances.
     * </p>
     *
     * <p>
     *     Since C* <code>timestamp</code> columns do not preserve timezones
     *     any attached timezone information will be lost.
     * </p>
     *
     * @see ZonedDateTime
     */
    public static class InstantCodec extends MappingCodec<Instant, Date> {

        public static final InstantCodec instance = new InstantCodec();

        private InstantCodec() {
            super(timestamp(), Instant.class);
        }

        @Override
        protected Instant deserialize(Date value) {
            return value == null ? null : value.toInstant();
        }

        @Override
        protected Date serialize(Instant value) {
            return value == null ? null : Date.from(value);
        }
    }

    /**
     * <p>
     *     {@link TypeCodec} that maps
     *     {@link LocalTime} <-> <code>tuple&lt;timestamp,varchar&gt;</code>
     *     providing a pattern for maintaining timezone information in
     *     cassandra.
     * </p>
     *
     * <p>
     *     Since cassandra's <code>timestamp</code> type preserves only
     *     milliseconds since epoch, any timezone information passed in with
     *     a {@link Date} instance would normally be lost.  By using a
     *     <code>tuple&lt;timestamp,varchar&gt;</code> a timezone ID can be
     *     persisted in the <code>varchar</code> field such that when the
     *     value is deserialized into a {@link Instant} the timezone is
     *     preserved.
     * </p>
     *
     */
    public static class ZonedDateTimeCodec extends MappingCodec<ZonedDateTime, TupleValue> {

        private final TupleType tupleType;

        public ZonedDateTimeCodec(TypeCodec<TupleValue> innerCodec) {
            super(innerCodec, ZonedDateTime.class);
            tupleType = (TupleType)innerCodec.getCqlType();
            List<DataType> types = tupleType.getComponentTypes();
            checkArgument(
                types.size() == 2 && types.get(0).equals(DataType.timestamp()) && types.get(1).equals(DataType.varchar()),
                "Expected tuple<timestamp,varchar>, got %s",
                tupleType);
        }

        @Override
        protected ZonedDateTime deserialize(TupleValue value) {
            if (value == null)
                return null;
            Date date = value.getTimestamp(0);
            String zoneID = value.getString(1);
            return date.toInstant().atZone(ZoneId.of(zoneID));
        }

        @Override
        protected TupleValue serialize(ZonedDateTime value) {
            if (value == null)
                return null;
            TupleValue tupleValue = tupleType.newValue();
            tupleValue.setTimestamp(0, Date.from(value.toInstant()));
            tupleValue.setString(1, value.getZone().getId());
            return tupleValue;
        }
    }

    private Jdk8TimeCodecs(){}

}
