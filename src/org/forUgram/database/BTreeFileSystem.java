package org.forUgram.database;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry; 
import org.forUgram.common.FileMappedByteBuffer;
import org.forUgram.common.LRUCache; 
 
public final class BTreeFileSystem<K extends Comparable, V extends Serializable> {  
    
    private static final int DEFAULT_DEGREE = 2;
    private static final int HEADER_LENGTH = Long.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES;
    
    private long length;
    private int degree;

    private long rootNode;

    private final FileMappedByteBuffer keyStorage; // Key 와 Value의 인덱스만 저장되어있음
    private final FileMappedByteBuffer valueStorage;
    private final Serializer<K> keySerializer;

    private long makedNodes = 0;
    private final Map<Long, BTreeNode> cachedNodes = new LRUCacheForFileSystem(100);

    public BTreeFileSystem(Serializer<K> keySerializer, String schema) throws IOException {
        this(keySerializer, DEFAULT_DEGREE, schema);
    }
    
    public BTreeFileSystem(Serializer<K> keySerializer, int degree, String schema) throws IOException { // 새로 만들때 
        this.degree = degree;
        
        this.keySerializer = keySerializer; 
        
        this.keyStorage = new FileMappedByteBuffer(new File(schema + ".idx"));  
        this.valueStorage = new FileMappedByteBuffer(new File(schema + ".dat"));  
 
        if ( ! loadFromKeyStorageFile()) {
            this.keyStorage.truncate(0);
            this.valueStorage.truncate(0);
            
            LeafNode ln = new LeafNode(this);
            ln.isDirty = true;
            this.rootNode = alloc(ln); 
            
            sync();
        }
    }
    
    private boolean loadFromKeyStorageFile() throws IOException {
        if (keyStorage.length() <= 0) {
            return false;
        }
        
        keyStorage.seek(0);

        long length = keyStorage.readLong();
        int degree = keyStorage.readInt();
        long rootNode = keyStorage.readLong();
        long makedNodes = keyStorage.readLong(); 

        if (length <= 0 || makedNodes <= 0) {
            return false;
        }
        
        if (rootNode < 0) {
            return false;
        }
        
        this.length = length;
        this.degree = degree;
        this.rootNode = rootNode;
        this.makedNodes = makedNodes;
        
        return true;
    }
    
    public void sync() throws IOException { 
        keyStorage.seek(0);

        keyStorage.writeLong(length);
        keyStorage.writeInt(degree);
        keyStorage.writeLong(rootNode);
        keyStorage.writeLong(makedNodes);
 
        for (Entry<Long, BTreeNode> e : cachedNodes.entrySet()) { // 나중에 iterator 로 하나씩 지워가면서 저장해야함...
            BTreeNode bn = e.getValue();
            if (bn.isDirty) { 
                keyStorage.seek(e.getKey()); 
                keyStorage.writeBytes(bn.serialize()); // 노드를 직렬화 시켜서 파일에 기록
                
                bn.isDirty = false;
            }
        }
    }
    
    public V get(K key) {
        try {
            return (V) lookup(rootNode).get(key);
        }
        catch (NoSuchKeyException e) {
            return null;
        }
    }

    public boolean contains(K key) {
        return lookup(rootNode).contains(key);
    }

    public V pop() { // 마지막값을 빼옴
        return remove(last());
    }

    public V poll() {
        return remove(first());
    }

    public K last() {
        return (K) lookup(rootNode).last();
    }

    public K first() {
        return (K) lookup(rootNode).first();
    }

    public boolean put(K key, V value) {
        if (key == null) {
            throw new NullPointerException();
        }

        try {
            if (lookup(rootNode).put(key, value)) {
                length++;
                return true;
            }
            return false;
        }
        catch (OverflowException e) {
            BranchNode bn = new BranchNode(this);
            bn.childNodes[0] = rootNode;
            bn.put(e.getKey(), e.getNewNode());
            
            bn.isDirty = true;
            
            rootNode = alloc(bn);
            
            return put(key, value);
        }
    }

    public V remove(K key) {
        BTreeNode bn = lookup(rootNode);

        Object o = null;
        try {
            o = bn.remove(key);
        }
        catch (NoSuchKeyException e) {
            return null;
        }

        if (bn.isBranch() && bn.length <= 0) {
            rootNode = bn.childNodes[0];
        }

        length--;
        return (V) o;
    }

    public long length() {
        return length;
    }

    @Override
    public String toString() {
        return lookup(rootNode).toString();
    }

    protected int getPhysicalNodeSize() {
        // 노드종류 + 키갯수 + (키의 최대 용량) + (가지노드일경우 연결되는 포인터)
        return Byte.BYTES + Byte.BYTES + Long.BYTES + Long.BYTES + (keySerializer.length() * (degree * 2)) + (Long.BYTES * (degree * 2 + 1));
    }
 
    protected long alloc(BTreeNode cacheNode) {
        long l = HEADER_LENGTH + (makedNodes++ * getPhysicalNodeSize());
        cacheNode.address = l;
        cachedNodes.put(l, cacheNode);
        return l;  
    }

    protected BTreeNode lookup(long address) {
        BTreeNode bn;
        
        if ((bn = cachedNodes.get(address)) != null) {
            return bn;
        }

        ByteBuffer b = ByteBuffer.allocate(getPhysicalNodeSize());
        
        try {
            keyStorage.seek(address);
            keyStorage.readBytes(b);
        }
        catch (IOException e) {
            return null;
        }
        
        if (b.get() == 1) {
            bn = BranchNode.unserialize(this, b);
        } else {
            bn = LeafNode.unserialize(this, b);
        }
        
        bn.address = address;
        
        cachedNodes.put(address, bn);// 없으면 unserialize 하고 caches 에 add
        return bn;
    }
    
    public Serializer<K> getKeySerializer() {
        return keySerializer;
    }
    
    public int getDegree() {
        return degree;
    }
    
    public FileMappedByteBuffer getValueStroage() {
        return valueStorage;
    }
    
    @Deprecated
    public boolean isValidate() {
        BTreeNode bn = lookup(rootNode);
        while(bn.isBranch()) { // 첫번째에 위치한 LeafNode 까지 내려가서
            bn = lookup(bn.childNodes[0]);
        }
        
        int length = 0;
        for (;;) {
         
            if ( ! bn.isValidate()) { // 키값의 순서가 올바른지 체크합니다.
                return false;
            }
            
            length += bn.length;
            
            if (bn.siblingRightNode <= 0) {
                break;
            }
            
            bn = lookup(bn.siblingRightNode);
        }
        
        return this.length == length; // 노드의 총 갯수가 올바른지 체
    }

    private class LRUCacheForFileSystem extends LRUCache<Long, BTreeNode> {

        public LRUCacheForFileSystem(int cacheSize) {
            super(cacheSize);
        }
        
        @Override
        public void entryRemoved(Entry<Long, BTreeNode> cacheNode) {
            try {
                BTreeNode bn = cacheNode.getValue();
                if (bn.isDirty) { 
                    keyStorage.seek(cacheNode.getKey());
                    keyStorage.writeBytes(bn.serialize()); // 노드를 직렬화 시킴  
                    
                    bn.isDirty = false;
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };
}
