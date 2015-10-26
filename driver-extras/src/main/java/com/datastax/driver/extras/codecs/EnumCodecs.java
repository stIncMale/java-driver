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

import java.nio.ByteBuffer;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * A collection of convenience {@link TypeCodec} instances useful for
 * serializing between CQL types and Java enums.
 */
public abstract class EnumCodecs {

    /**
     * A codec that serializes {@link Enum} instances as CQL {@code varchar}s
     * representing their programmatic names as returned by {@link Enum#name()}.
     * <p>
     * <strong>Note that this codec relies on the enum constant names;
     * it is therefore vital that enum names never change.</strong>
     *
     * @param <E> The Enum class this codec serializes from and deserializes to.
     */
    public static class EnumStringCodec<E extends Enum<E>> extends StringParsingCodec<E> {

        private final Class<E> enumClass;

        public EnumStringCodec(Class<E> enumClass) {
            this(TypeCodec.varchar(), enumClass);
        }

        public EnumStringCodec(TypeCodec<String> innerCodec, Class<E> enumClass) {
            super(innerCodec, enumClass);
            this.enumClass = enumClass;
        }

        @Override
        protected String toString(E value) {
            return value == null ? null : value.name();
        }

        @Override
        protected E fromString(String value) {
            return value == null ? null : Enum.valueOf(enumClass, value);
        }

    }

    /**
     * A codec that serializes {@link Enum} instances as CQL {@code int}s
     * representing their ordinal values as returned by {@link Enum#ordinal()}.
     * <p>
     * <strong>Note that this codec relies on the enum constants declaration order;
     * it is therefore vital that this order remains immutable.</strong>
     *
     * @param <E> The Enum class this codec serializes from and deserializes to.
     */
    public static class EnumIntCodec<E extends Enum<E>> extends TypeCodec<E> {

        private final E[] enumConstants;

        private final TypeCodec<Integer> innerCodec;

        public EnumIntCodec(Class<E> enumClass) {
            this(TypeCodec.cint(), enumClass);
        }

        public EnumIntCodec(TypeCodec<Integer> innerCodec, Class<E> enumClass) {
            super(innerCodec.getCqlType(), enumClass);
            this.enumConstants = enumClass.getEnumConstants();
            this.innerCodec = innerCodec;
        }

        @Override
        public ByteBuffer serialize(E value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return innerCodec.serialize(value.ordinal(), protocolVersion);
        }

        @Override
        public E deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return enumConstants[innerCodec.deserialize(bytes, protocolVersion)];
        }

        @Override
        public E parse(String value) throws InvalidTypeException {
            return value == null || value.isEmpty() || value.equals("NULL") ? null : enumConstants[Integer.parseInt(value)];
        }

        @Override
        public String format(E value) throws InvalidTypeException {
            if (value == null)
                return "NULL";
            return Integer.toString(value.ordinal());
        }
    }

    private EnumCodecs(){}

}
