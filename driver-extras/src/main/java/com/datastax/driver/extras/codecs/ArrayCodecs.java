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

import java.lang.reflect.Array;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import static com.datastax.driver.core.ParseUtils.skipSpaces;

/**
 * A collection of convenience {@link TypeCodec} instances useful for
 * serializing between CQL lists and Java arrays.
 * <p>
 * The codecs in this class provide the following mappings:
 *
 * <table summary="Supported Mappings">
 *     <tr>
 *         <th>Codec</th>
 *         <th>CQL type</th>
 *         <th>Java type</th>
 *     </tr>
 *     <tr>
 *         <td>{@link ObjectArrayCodec}</td>
 *         <td>{@code list<?>}</td>
 *         <td>{@code T[]}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link IntArrayCodec}</td>
 *         <td>{@code list<int>}</td>
 *         <td>{@code int[]}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link LongArrayCodec}</td>
 *         <td>{@code list<long>}</td>
 *         <td>{@code long[]}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link FloatArrayCodec}</td>
 *         <td>{@code list<float>}</td>
 *         <td>{@code float[]}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link DoubleArrayCodec}</td>
 *         <td>{@code list<double>}</td>
 *         <td>{@code double[]}</td>
 *     </tr>
 * </table>
 *
 * Codecs in this class can either be registered individually, or,
 * or they may all
 * be registered via {@link #withPrimitiveArrayCodecs(CodecRegistry)}.
 *
 */
public abstract class ArrayCodecs {

    /**
     * Registers {@link TypeCodec} instances in this class
     * that handle primitive arrays with the given {@link CodecRegistry}.
     *
     * @param registry registry to add codecs to.
     * @return updated registry with codecs.
     */
    public static CodecRegistry withPrimitiveArrayCodecs(CodecRegistry registry) {
        return registry.register(IntArrayCodec.instance, LongArrayCodec.instance, FloatArrayCodec.instance, DoubleArrayCodec.instance);
    }

    /**
     * Base class for all codecs dealing with Java arrays.
     * This class aims to reduce the amount of code required to create such codecs.
     *
     * @param <T> The Java array type this codec handles
     */
    public static abstract class AbstractArrayCodec<T> extends TypeCodec<T> {

        /**
         * @param cqlType The CQL type. Must be a list type.
         * @param javaClass The Java type. Must be an array class.
         */
        public AbstractArrayCodec(DataType.CollectionType cqlType, Class<T> javaClass) {
            super(cqlType, javaClass);
            checkArgument(cqlType.getName() == DataType.Name.LIST, "Expecting CQL list type, got %s", cqlType);
            checkArgument(javaClass.isArray(), "Expecting Java array class, got %s", javaClass);
        }

        @Override
        public String format(T array) throws InvalidTypeException {
            if (array == null)
                return "NULL";
            int length = Array.getLength(array);
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (int i = 0; i < length; i++) {
                if (i != 0)
                    sb.append(",");
                formatElement(sb, array, i);
            }
            sb.append(']');
            return sb.toString();
        }

        @Override
        public T parse(String value) throws InvalidTypeException {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
                return null;

            int idx = skipSpaces(value, 0);
            if (value.charAt(idx++) != '[')
                throw new InvalidTypeException(String.format("cannot parse list value from \"%s\", at character %d expecting '[' but got '%c'", value, idx, value.charAt(idx)));

            idx = skipSpaces(value, idx);

            if (value.charAt(idx) == ']')
                return newInstance(0);

            // first pass: determine array length
            int length = getArrayLength(value, idx);

            // second pass: parse elements
            T array = newInstance(length);
            int i = 0;
            for( ; idx < value.length(); i++) {
                int n = skipLiteral(value, idx);
                parseElement(value.substring(idx, n), array, i);
                idx = skipSpaces(value, n);
                if (value.charAt(idx) == ']')
                    return array;
                idx = skipComma(value, idx);
                idx = skipSpaces(value, idx);
            }

            throw new InvalidTypeException(String.format("Malformed list value \"%s\", missing closing ']'", value));
        }

        /**
         * Create a new array instance with the given size.
         *
         * @param size The size of the array to instantiate.
         * @return a new array instance with the given size.
         */
        protected abstract T newInstance(int size);

        /**
         * Format the {@code index}th element of {@code array} to {@code output}.
         *
         * @param output The StringBuilder to write to.
         * @param array The array to read from.
         * @param index The element index.
         */
        protected abstract void formatElement(StringBuilder output, T array, int index);

        /**
         * Parse the {@code index}th element of {@code array} from {@code input}.
         *
         * @param input The String to read from.
         * @param array The array to write to.
         * @param index The element index.
         */
        protected abstract void parseElement(String input, T array, int index);

        private int getArrayLength(String value, int idx) {
            int length = 1;
            for( ; idx < value.length(); length++) {
                idx = skipLiteral(value, idx);
                idx = skipSpaces(value, idx);
                if (value.charAt(idx) == ']')
                    break;
                idx = skipComma(value, idx);
                idx = skipSpaces(value, idx);
            }
            return length;
        }

        private int skipComma(String value, int idx) {
            if (value.charAt(idx) != ',')
                throw new InvalidTypeException(String.format("Cannot parse list value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));
            return idx + 1;
        }

        private int skipLiteral(String value, int idx) {
            try {
                return ParseUtils.skipCQLValue(value, idx);
            } catch (IllegalArgumentException e) {
                throw new InvalidTypeException(String.format("Cannot parse list value from \"%s\", invalid CQL value at character %d", value, idx), e);
            }
        }

    }

    /**
     * Codec dealing with Java object arrays.
     * Serialization and deserialization of elements in the array is
     * delegated to the provided element codec.
     * <p>
     * For example, to create a codec that maps {@code list<text>} to {@code String[]},
     * declare the following:
     * <pre>{@code
     * ObjectArrayCodec<String> stringArrayCodec = new ObjectArrayCodec<>(
     *      DataType.list(DataType.varchar()),
     *      String[].class,
     *      TypeCodec.varchar());
     * }</pre>
     *
     * @param <E> The Java array component type this codec handles
     */
    public static class ObjectArrayCodec<E> extends AbstractArrayCodec<E[]> {

        protected final TypeCodec<E> eltCodec;

        public ObjectArrayCodec(DataType.CollectionType cqlType, Class<E[]> javaClass, TypeCodec<E> eltCodec) {
            super(cqlType, javaClass);
            this.eltCodec = eltCodec;
        }

        @Override
        public ByteBuffer serialize(E[] value, ProtocolVersion protocolVersion) {
            if (value == null)
                return null;
            List<ByteBuffer> bbs = new ArrayList<>(value.length);
            for (E elt : value) {
                if (elt == null) {
                    throw new NullPointerException("Collection elements cannot be null");
                }
                ByteBuffer bb;
                try {
                    bb = eltCodec.serialize(elt, protocolVersion);
                } catch (ClassCastException e) {
                    throw new InvalidTypeException(
                        String.format("Invalid type for %s element, expecting %s but got %s",
                            cqlType, eltCodec.getJavaType(), elt.getClass()), e);
                }
                bbs.add(bb);
            }
            return CodecUtils.pack(bbs, value.length, protocolVersion);
        }

        @Override
        public E[] deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
            if (bytes == null || bytes.remaining() == 0)
                return newInstance(0);
            try {
                ByteBuffer input = bytes.duplicate();
                int n = CodecUtils.readSize(input, protocolVersion);
                E[] array = newInstance(n);
                for (int i = 0; i < n; i++) {
                    ByteBuffer databb = CodecUtils.readValue(input, protocolVersion);
                    array[i] = eltCodec.deserialize(databb, protocolVersion);
                }
                return array;
            } catch (BufferUnderflowException e) {
                throw new InvalidTypeException("Not enough bytes to deserialize list");
            }
        }

        @Override
        protected void formatElement(StringBuilder output, E[] array, int index) {
            output.append(eltCodec.format(array[index]));
        }

        @Override
        protected void parseElement(String input, E[] array, int index) {
            array[index] = eltCodec.parse(input);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected E[] newInstance(int size) {
            return (E[])Array.newInstance(getJavaType().getRawType().getComponentType(), size);
        }
    }

    /**
     * Base class for all codecs dealing with Java primitive arrays.
     * This class provides a more efficient implementation of {@link #serialize(Object, ProtocolVersion)}
     * and {@link #deserialize(ByteBuffer, ProtocolVersion)} for primitive arrays.
     *
     * @param <T> The Java primitive array type this codec handles
     */
    public static abstract class AbstractPrimitiveArrayCodec<T> extends AbstractArrayCodec<T> {

        /**
         * @param cqlType The CQL type. Must be a list type.
         * @param javaClass The Java type. Must be an array class.
         */
        public AbstractPrimitiveArrayCodec(DataType.CollectionType cqlType, Class<T> javaClass) {
            super(cqlType, javaClass);
            checkArgument(javaClass.getComponentType().isPrimitive(), "Expecting primitive array component type, got %s", javaClass.getComponentType());
        }

        @Override
        public ByteBuffer serialize(T array, ProtocolVersion protocolVersion) throws InvalidTypeException {
            if (array == null)
                return null;
            boolean isProtocolV3OrAbove = protocolVersion.compareTo(ProtocolVersion.V2) > 0;
            // native method
            int length = Array.getLength(array);
            checkArgument(isProtocolV3OrAbove || length < 65536,
                "Native protocol version %d supports up to 65535 elements in any collection - but collection contains %d elements",
                protocolVersion.toInt(), length);
            /*
             * Encoding of lists in the native protocol:
             * [size of list] [size of element 1][element1] [size of element 2][element2]...
             * Sizes are encoded on 2 bytes in protocol v1 and v2, 4 bytes otherwise.
             * (See native_protocol_v*.spec in https://github.com/apache/cassandra/blob/trunk/doc/)
             */
            // Number of bytes to encode sizes
            int sizeOfSize = isProtocolV3OrAbove ? 4 : 2;
            // Number of bytes to encode each element (preceded by its size)
            int sizeOfElement = sizeOfSize + sizeOfComponentType();
            // Number of bytes to encode the whole collection (size of collection + all elements)
            int totalSize = sizeOfSize + length * sizeOfElement;
            ByteBuffer output = ByteBuffer.allocate(totalSize);
            CodecUtils.writeSize(output, length, protocolVersion);
            for (int i = 0; i < length; i++) {
                CodecUtils.writeSize(output, sizeOfComponentType(), protocolVersion);
                serializeElement(output, array, i, protocolVersion);
            }
            output.flip();
            return output;
        }

        @Override
        public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            if (bytes == null || bytes.remaining() == 0)
                return newInstance(0);
            boolean isProtocolV3OrAbove = protocolVersion.compareTo(ProtocolVersion.V2) > 0;
            int sizeOfSize = isProtocolV3OrAbove ? 4 : 2;
            ByteBuffer input = bytes.duplicate();
            int size = CodecUtils.readSize(input, protocolVersion);
            T array = newInstance(size);
            for (int i = 0; i < size; i++) {
                // Skip size (we know it will always be 4 since the elements are ints)
                input.position(input.position() + sizeOfSize);
                deserializeElement(input, array, i, protocolVersion);
            }
            return array;
        }

        /**
         * Return the size in bytes of the array component type.
         *
         * @return the size in bytes of the array component type.
         */
        protected abstract int sizeOfComponentType();

        /**
         * Write the {@code index}th element of {@code array} to {@code output}.
         *  @param output The ByteBuffer to write to.
         * @param array The array to read from.
         * @param index The element index.
         * @param protocolVersion The protocol version to use.
         */
        protected abstract void serializeElement(ByteBuffer output, T array, int index, ProtocolVersion protocolVersion);

        /**
         * Read the {@code index}th element of {@code array} from {@code input}.
         *  @param input The ByteBuffer to read from.
         * @param array The array to write to.
         * @param index The element index.
         * @param protocolVersion The protocol version to use.
         */
        protected abstract void deserializeElement(ByteBuffer input, T array, int index, ProtocolVersion protocolVersion);

    }

    /**
     * A codec that maps the CQL type {@code list<int>} to the Java type {@code int[]}.
     * <p>
     * Note that this codec is designed for performance and converts CQL lists
     * <em>directly</em> to {@code int[]}, thus avoiding any unnecessary
     * boxing and unboxing of Java primitive {@code int} values;
     * it also instantiates arrays without the need for an intermediary
     * Java {@code List} object.
     */
    public static class IntArrayCodec extends AbstractPrimitiveArrayCodec<int[]> {

        public static final IntArrayCodec instance = new IntArrayCodec();

        public IntArrayCodec() {
            super(DataType.list(DataType.cint()), int[].class);
        }

        @Override
        protected int sizeOfComponentType() {
            return 4;
        }

        @Override
        protected void serializeElement(ByteBuffer output, int[] array, int index, ProtocolVersion protocolVersion) {
            output.putInt(array[index]);
        }

        @Override
        protected void deserializeElement(ByteBuffer input, int[] array, int index, ProtocolVersion protocolVersion) {
            array[index] = input.getInt();
        }

        @Override
        protected void formatElement(StringBuilder output, int[] array, int index) {
            output.append(array[index]);
        }

        @Override
        protected void parseElement(String input, int[] array, int index) {
            array[index] = Integer.parseInt(input);
        }

        @Override
        protected int[] newInstance(int size) {
            return new int[size];
        }

    }

    /**
     * A codec that maps the CQL type {@code list<long>} to the Java type {@code long[]}.
     * <p>
     * Note that this codec is designed for performance and converts CQL lists
     * <em>directly</em> to {@code long[]}, thus avoiding any unnecessary
     * boxing and unboxing of Java primitive {@code long} values;
     * it also instantiates arrays without the need for an intermediary
     * Java {@code List} object.
     */
    public static class LongArrayCodec extends AbstractPrimitiveArrayCodec<long[]> {

        public static final LongArrayCodec instance = new LongArrayCodec();

        public LongArrayCodec() {
            super(DataType.list(DataType.bigint()), long[].class);
        }

        @Override
        protected int sizeOfComponentType() {
            return 8;
        }

        @Override
        protected void serializeElement(ByteBuffer output, long[] array, int index, ProtocolVersion protocolVersion) {
            output.putLong(array[index]);
        }

        @Override
        protected void deserializeElement(ByteBuffer input, long[] array, int index, ProtocolVersion protocolVersion) {
            array[index] = input.getLong();
        }

        @Override
        protected void formatElement(StringBuilder output, long[] array, int index) {
            output.append(array[index]);
        }

        @Override
        protected void parseElement(String input, long[] array, int index) {
            array[index] = Long.parseLong(input);
        }

        @Override
        protected long[] newInstance(int size) {
            return new long[size];
        }

    }

    /**
     * A codec that maps the CQL type {@code list<float>} to the Java type {@code float[]}.
     * <p>
     * Note that this codec is designed for performance and converts CQL lists
     * <em>directly</em> to {@code float[]}, thus avoiding any unnecessary
     * boxing and unboxing of Java primitive {@code float} values;
     * it also instantiates arrays without the need for an intermediary
     * Java {@code List} object.
     */
    public static class FloatArrayCodec extends AbstractPrimitiveArrayCodec<float[]> {

        public static final FloatArrayCodec instance = new FloatArrayCodec();

        public FloatArrayCodec() {
            super(DataType.list(DataType.cfloat()), float[].class);
        }

        @Override
        protected int sizeOfComponentType() {
            return 4;
        }

        @Override
        protected void serializeElement(ByteBuffer output, float[] array, int index, ProtocolVersion protocolVersion) {
            output.putFloat(array[index]);
        }

        @Override
        protected void deserializeElement(ByteBuffer input, float[] array, int index, ProtocolVersion protocolVersion) {
            array[index] = input.getFloat();
        }

        @Override
        protected void formatElement(StringBuilder output, float[] array, int index) {
            output.append(array[index]);
        }

        @Override
        protected void parseElement(String input, float[] array, int index) {
            array[index] = Float.parseFloat(input);
        }

        @Override
        protected float[] newInstance(int size) {
            return new float[size];
        }

    }

    /**
     * A codec that maps the CQL type {@code list<double>} to the Java type {@code double[]}.
     * <p>
     * Note that this codec is designed for performance and converts CQL lists
     * <em>directly</em> to {@code double[]}, thus avoiding any unnecessary
     * boxing and unboxing of Java primitive {@code double} values;
     * it also instantiates arrays without the need for an intermediary
     * Java {@code List} object.
     */
    public static class DoubleArrayCodec extends AbstractPrimitiveArrayCodec<double[]> {

        public static final DoubleArrayCodec instance = new DoubleArrayCodec();

        public DoubleArrayCodec() {
            super(DataType.list(DataType.cdouble()), double[].class);
        }

        @Override
        protected int sizeOfComponentType() {
            return 8;
        }

        @Override
        protected void serializeElement(ByteBuffer output, double[] array, int index, ProtocolVersion protocolVersion) {
            output.putDouble(array[index]);
        }

        @Override
        protected void deserializeElement(ByteBuffer input, double[] array, int index, ProtocolVersion protocolVersion) {
            array[index] = input.getDouble();
        }

        @Override
        protected void formatElement(StringBuilder output, double[] array, int index) {
            output.append(array[index]);
        }

        @Override
        protected void parseElement(String input, double[] array, int index) {
            array[index] = Double.parseDouble(input);
        }

        @Override
        protected double[] newInstance(int size) {
            return new double[size];
        }

    }

    private ArrayCodecs() {}
}
