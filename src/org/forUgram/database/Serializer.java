package org.forUgram.database;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

public interface Serializer<T> {
    
    public static final Serializer<String> STRING_32 = new FixedStringSerializer(32);
    public static final Serializer<String> STRING_1024 = new FixedStringSerializer(1024);

    public static final Serializer<Integer> INTEGER = new Serializer<Integer>() {

        @Override
        public int length() {
            return Integer.BYTES;
        }

        @Override
        public ByteBuffer serialize(Integer value) {
            ByteBuffer b = ByteBuffer.allocate(length()); 
            b.putInt(value); 
            b.clear();
            return b;
        }

        @Override
        public Integer unserialize(ByteBuffer bytes) {
            return bytes.getInt();
        }
    };

    int length();

    ByteBuffer serialize(T value);

    T unserialize(ByteBuffer bytes);

    public static class FixedStringSerializer implements Serializer<String> {

        private final int length;

        public FixedStringSerializer(int length) {
            this.length = length;
        }

        @Override
        public int length() { // 실제 바이트가 들어갈때 최대크기
            return (length << 1) + 2; //  마지막 null 문자(2byte)
        }

        @Override
        public ByteBuffer serialize(String value) {
            if (value.length() > length) {
                value = value.substring(0, length);
            }

            char[] c = value.toCharArray();

            ByteBuffer b = ByteBuffer.allocate(length());
            for (int l = 0; l < c.length; ++l) {
                b.putChar(c[l]);
            }
            b.putChar('\0'); // 2byte

            b.clear();
            return b;
        }

        @Override
        public String unserialize(ByteBuffer bytes) {
            StringBuilder sb = new StringBuilder();
            byte[] b = new byte[length()];

            bytes.get(b);

            CharBuffer cb = ByteBuffer.wrap(b).asCharBuffer();
            while (cb.hasRemaining()) {
                char c = (char) cb.get();
                if (c == '\0') {
                    break; 
                }
                sb.append(c);
            }

            return sb.toString();
        }
    }
}
