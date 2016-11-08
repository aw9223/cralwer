package org.forUgram.database;

import java.io.Serializable;
import java.nio.ByteBuffer; 
import org.forUgram.common.StringUtils;

public final class LeafNode extends BTreeNode {

    protected LeafNode(BTreeFileSystem system) {
        super(system);

        this.values = new Serializable[system.getDegree() * 2];
    }
 
    @Override
    protected void shiftLeft(int start, int count) {
        for (int n = start, c = length - count; n < c; ++n) {  
            this.keys[n] = this.keys[n + count];
            this.values[n] = this.values[n + count];
        }
        
        this.length -= count;
        
        this.isDirty = true;
    }

    @Override
    protected void shitftRight(int start, int count) { 
        for (int n = length - 1; start <= n; --n) {  
            this.keys[n + count] = this.keys[n];
            this.values[n + count] = this.values[n];
        }
        
        this.length += count; 
        
        this.isDirty = true;
    }

    @Override
    protected boolean redistLeft() {
        if (siblingLeftNode < 0) {
            return false;
        }
        
        LeafNode siblingNode = (LeafNode) system.lookup(siblingLeftNode);
        if ((siblingNode.avaliable() - 1) < 0) {
            return false;
        }
        
        // TODO
        
        siblingNode.isDirty = true;
        this.isDirty = true;
        return true;
    }

    @Override
    protected boolean redistRight() {
        if (siblingRightNode < 0) {
            return false;
        }
        
        LeafNode siblingNode = (LeafNode) system.lookup(siblingRightNode);
        if ((siblingNode.avaliable() - 1) < 0) {
            return false;
        }
        
        // TODO
        
        siblingNode.isDirty = true;
        this.isDirty = true;
        return true;
    }

    @Override
    protected boolean mergeLeft() {
        return false;
    }

    @Override
    protected boolean mergeRight() {
        return false;
    }

    @Override
    protected boolean put(Comparable key, long childNode) {
        throw new UnsupportedOperationException("마지막 노드에서는 지원하지 않습니다.");
    }

    @Override
    protected boolean put(Comparable key, Serializable value) throws OverflowException {
        if (isFull()) { // 삽입할려고 했는데 노드의 크기가 꽉찼다면  
            int degree = system.getDegree();
            
            LeafNode ln = new LeafNode(system); // 노드를 생성하고 
            for (int n = degree; n < length; ++n) { // 반틈을 새로운 노드로 복사
                ln.put(keys[n], values[n]);
            }

            this.shiftLeft(degree, length - degree); // 현재 노드의 반틈을 삭제

            this.isDirty = true;
            ln.isDirty = true;
            
            // 나머지작업 (새로운 노드추가) 는 부모노드에서 처리
            throw new OverflowException(ln.keys[0], system.alloc(ln)); 
        }

        int n = 0;
        for (; n < length; ++n) {
            if (keys[n].compareTo(key) > 0) {
                break;
            }
        }
        
        this.shitftRight(n, 1); // 추가하기전에 공간 확보를 위해 unshift 연산 

        this.keys[n] = key;
        this.values[n] = value;

        this.isDirty = true;

        return true;
    }

    @Override
    protected Object remove(Comparable key) throws NoSuchKeyException {
        int n;
        if ((n = indexOf(key)) < 0) { // 해당 값의 위치를 찾을수 없으면
            throw new NoSuchKeyException(); // 예외발생으로 알림 (값이 null이 있을수도잇으니까...)
        }

        Object o = values[n]; // 발견하면

        this.shiftLeft(n, 1); // 삭제하고

        this.isDirty = true;

        return o; // 리턴
    }

    @Override
    protected Object get(Comparable key) throws NoSuchKeyException {
        int n;
        if ((n = indexOf(key)) < 0) { // 해당 값의 위치를 찾을수 없으면
            throw new NoSuchKeyException(); // 예외발생으로 알림 (값이 null이 있을수도잇으니까...)
        }

        return values[n];
    }

    @Override
    protected boolean contains(Comparable key) {
        return indexOf(key) >= 0;
    }

    @Override
    protected Comparable first() {
        if (length <= 0) {
            return null;
        } else {
            return keys[0];
        }
    }

    @Override
    protected Comparable last() {
        if (length <= 0) {
            return null;
        } else {
            return keys[length - 1];
        }
    }

    @Override
    protected int indexOf(Comparable key) {
        for (int n = 0; n < length; ++n) {
            if (keys[n].compareTo(key) == 0) {
                return n;
            }
        }
        return -1;
    }
    
    protected static LeafNode unserialize(BTreeFileSystem system, ByteBuffer bytes) { 
        Serializer s = system.getKeySerializer();
        int maxLength = system.getDegree() * 2;
        
        byte length = bytes.get(); 
        long siblingLeftNode = bytes.getLong();
        long siblingRightNode = bytes.getLong();
        Comparable keys[] = new Comparable[maxLength]; 
        Serializable values[] = new Serializable[maxLength];

        for (int n = 0; n < length; ++n) {
            byte[] b = new byte[s.length()];
            bytes.get(b);
            keys[n] = (Comparable) s.unserialize(ByteBuffer.wrap(b));
        }

        bytes.position(s.length() * (maxLength - length) + bytes.position()); 

        for (int n = 0; n < length; ++n) {
            values[n] = null;
        }

        LeafNode ln = new LeafNode(system);
        ln.siblingLeftNode = siblingLeftNode;
        ln.siblingRightNode = siblingRightNode;
        ln.length = length;
        ln.keys = keys;
        ln.values = values;
        return ln;        
    }

    @Override
    protected ByteBuffer serialize() {
        Serializer s = system.getKeySerializer();
        ByteBuffer b = ByteBuffer.allocate(system.getPhysicalNodeSize());
 
        b.put((byte) 0);  // 가지, 잎 검사
        b.put((byte) length); // 현재 길이
        b.putLong(siblingLeftNode);
        b.putLong(siblingRightNode);

        for (int n = 0; n < length; ++n) {
            b.put(s.serialize(keys[n]));
        }
 
        b.clear(); 
        
        return b;
    }

    @Override
    protected String toString(int depth) {
        StringBuilder sb = new StringBuilder();
        
        if (siblingRightNode > 0) {
            sb.append(StringUtils.tabbedOf(depth)).append("↑:").append(siblingRightNode).append('\n');
        }
        
        sb.append(StringUtils.tabbedOf(depth)).append("◎:").append(address).append('\n');
        
        for (int n = length - 1; n >= 0; --n) { 
            sb.append(StringUtils.tabbedOf(depth)).append("-[").append(keys[n]).append("]:").append(values[n]).append("\n");     
        }

        if (siblingLeftNode > 0) {
            sb.append(StringUtils.tabbedOf(depth)).append("↓:").append(siblingLeftNode).append('\n');
        }
        return sb.toString();
    }
}
