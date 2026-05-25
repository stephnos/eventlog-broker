package com.streamhub.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only segment file with sparse in-memory offset index.
 * Index entries map offset -> byte position; we use RandomAccessFile (not mmap) for v1
 * because it keeps the implementation portable and correct under concurrent append/read
 * with explicit fsync boundaries. mmap would be the upgrade for read-heavy workloads.
 */
public final class LogSegment implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LogSegment.class);

    private final Path logFile;
    private final long baseOffset;
    private final RandomAccessFile raf;
    private final List<IndexEntry> sparseIndex = new ArrayList<>();
    private final int indexIntervalBytes;
    private long nextOffset;
    private long bytesSinceLastIndex;
    private boolean closed;

    public record IndexEntry(long offset, long position) {}

    public LogSegment(Path logFile, long baseOffset, int indexIntervalBytes) throws IOException {
        this.logFile = logFile;
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
        this.raf = new RandomAccessFile(logFile.toFile(), "rw");
        this.nextOffset = baseOffset;
        if (raf.length() > 0) {
            recoverIndex();
        } else {
            sparseIndex.add(new IndexEntry(baseOffset, 0));
        }
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public long size() throws IOException {
        return raf.length();
    }

    public synchronized long append(long timestamp, byte[] key, byte[] value) throws IOException {
        long offset = nextOffset;
        long pos = raf.length();
        byte[] recordBytes = encodeRecord(timestamp, key, value);
        raf.seek(pos);
        raf.write(recordBytes);
        nextOffset++;
        bytesSinceLastIndex += recordBytes.length;
        if (bytesSinceLastIndex >= indexIntervalBytes || sparseIndex.isEmpty()) {
            sparseIndex.add(new IndexEntry(offset, pos));
            bytesSinceLastIndex = 0;
        }
        return offset;
    }

    public synchronized List<StoredRecord> read(long fromOffset, int maxBytes) throws IOException {
        var results = new ArrayList<StoredRecord>();
        if (fromOffset < baseOffset || fromOffset >= nextOffset) {
            return results;
        }
        long pos = lookupPosition(fromOffset);
        raf.seek(pos);
        long currentOffset = offsetAtPosition(pos);
        int bytesRead = 0;
        while (currentOffset < nextOffset && bytesRead < maxBytes) {
            long recordStart = raf.getFilePointer();
            int recordLen = raf.readInt();
            long ts = raf.readLong();
            byte[] key = readBytes(raf);
            byte[] val = readBytes(raf);
            if (currentOffset >= fromOffset) {
                results.add(new StoredRecord(currentOffset, ts, key, val));
                bytesRead += recordLen;
            } else {
                raf.seek(recordStart + recordLen);
            }
            currentOffset++;
        }
        return results;
    }

    public synchronized void flush() throws IOException {
        raf.getFD().sync();
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            raf.close();
        }
    }

    public void delete() throws IOException {
        close();
        Files.deleteIfExists(logFile);
    }

    private void recoverIndex() throws IOException {
        raf.seek(0);
        long pos = 0;
        long offset = baseOffset;
        sparseIndex.clear();
        sparseIndex.add(new IndexEntry(baseOffset, 0));
        bytesSinceLastIndex = 0;
        while (pos < raf.length()) {
            if (bytesSinceLastIndex >= indexIntervalBytes) {
                sparseIndex.add(new IndexEntry(offset, pos));
                bytesSinceLastIndex = 0;
            }
            raf.seek(pos);
            int recordLen = raf.readInt();
            raf.readLong(); // timestamp
            readBytes(raf); // key
            readBytes(raf); // value
            pos += recordLen;
            offset++;
            bytesSinceLastIndex += recordLen;
        }
        nextOffset = offset;
    }

    private long lookupPosition(long offset) {
        IndexEntry best = sparseIndex.getFirst();
        for (IndexEntry e : sparseIndex) {
            if (e.offset() <= offset) best = e;
            else break;
        }
        return best.position();
    }

    private long offsetAtPosition(long pos) throws IOException {
        raf.seek(0);
        long offset = baseOffset;
        long current = 0;
        while (current < pos && offset < nextOffset) {
            int len = raf.readInt();
            raf.skipBytes(len - 4);
            offset++;
            current += len;
        }
        return offset;
    }

    private static byte[] encodeRecord(long timestamp, byte[] key, byte[] value) throws IOException {
        int keyLen = key == null ? -1 : key.length;
        int valLen = value == null ? 0 : value.length;
        int size = 4 + 8 + 4 + (keyLen < 0 ? 0 : keyLen) + 4 + valLen;
        var buf = new java.io.ByteArrayOutputStream(size);
        var out = new java.io.DataOutputStream(buf);
        out.writeInt(size);
        out.writeLong(timestamp);
        if (key == null) out.writeInt(-1);
        else {
            out.writeInt(key.length);
            out.write(key);
        }
        out.writeInt(valLen);
        if (valLen > 0) out.write(value);
        return buf.toByteArray();
    }

    private static byte[] readBytes(RandomAccessFile raf) throws IOException {
        int len = raf.readInt();
        if (len < 0) return null;
        byte[] b = new byte[len];
        raf.readFully(b);
        return b;
    }

    public record StoredRecord(long offset, long timestamp, byte[] key, byte[] value) {}
}
