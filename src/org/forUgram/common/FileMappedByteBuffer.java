package org.forUgram.common;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileMappedByteBuffer {

    private final FileChannel channel;

    public FileMappedByteBuffer(String path) throws IOException {
        this(new File(path));
    }

    public FileMappedByteBuffer(File data) throws IOException {
        this.channel = new RandomAccessFile(data, "rw").getChannel();
    }

    public long length() throws IOException {
        return channel.size();
    }

    public void seek(long offset) throws IOException {
        channel.position(offset);
    }

    public void writeInt(int value) throws IOException { 
        ByteBuffer b = ByteBuffer.allocate(Integer.BYTES);
        b.putInt(value); 
        b.flip(); 
        writeBytes(b); 
    }
    
    public void truncate(long length) throws IOException {
        channel.truncate(length);
    }

    public void writeLong(long value) throws IOException { 
        ByteBuffer b = ByteBuffer.allocate(Long.BYTES);
        b.putLong(value);
        b.flip(); 
        writeBytes(b);
    }

    public void writeBytes(ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            channel.write(bytes);
        }
        bytes.clear();
    }

    public int readInt() throws IOException {
        ByteBuffer b = ByteBuffer.allocate(Integer.BYTES);
        readBytes(b);
        return b.getInt(); 
    }

    public long readLong() throws IOException {
        ByteBuffer b = ByteBuffer.allocate(Long.BYTES);
        readBytes(b);
        return b.getLong();
    }

    public void readBytes(ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            channel.read(bytes);
        }
        bytes.clear();
    }
}
