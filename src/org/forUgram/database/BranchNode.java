package org.forUgram.database;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.forUgram.common.StringUtils;

public class BranchNode extends BTreeNode {

    protected BranchNode(BTreeFileSystem system) {
        super(system);

        this.childNodes = new long[system.getDegree() * 2 + 1];
    }

    @Override
    protected void rotateLeft(int start, int count) { // 검증완료 
        for (int n = start, c = length - count; n < c; ++n) {
            this.keys[n] = this.keys[n + count];
            this.childNodes[n + 1] = this.childNodes[n + 1 + count];
        }
        
        this.length -= count;

        this.isDirty = true; 
    }
 
    @Override
    protected void rotateRight(int start, int count) {  
        for (int n = length - 1; start <= n; --n) {
            this.keys[n + count] = this.keys[n];
            this.childNodes[n + count + 1] = this.childNodes[n + 1];
        }

        this.length += count;
        
        this.isDirty = true;
    }

    @Override
    protected boolean put(Comparable key, Serializable value) throws OverflowException {
        if (isFull()) {
            int degree = system.getDegree();
            
            Comparable k = keys[degree];
            BranchNode bn = new BranchNode(system); // 노드 생성
            bn.childNodes[0] = childNodes[degree + 1];  // 브랜치노드의 첫번째는 복사를 직접 해줘야함...
            for (int n = degree + 1; n < length; ++n) { // 노드로 반틈 복사
                bn.put(keys[n], childNodes[n + 1]);
            } 
            
            this.rotateLeft(degree, length - degree); // 현재노드에서 반틈을 제거
            
            if (siblingRightNode > 0) {
                BTreeNode siblingNode = system.lookup(siblingRightNode);
                BTreeNode childNode = system.lookup(bn.childNodes[bn.length]);
                
                childNode.siblingRightNode = siblingNode.childNodes[0];
            }
            
            this.isDirty = true;// system.sync() 호출시 물리적으로도 업데이트 되게 유도
            bn.isDirty = true; 
            
            throw new OverflowException(k, system.alloc(bn)); // 나머지는 부모노드에서 처리
        }
        
        int n = indexOf(key);
        
        try {
            return system.lookup(childNodes[n]).put(key, value); // 삽입 작업
        }
        catch (OverflowException e) { // 삽입 작업도중 꽉찬 노드를 발견했을때 
            put(n, e.getKey(), e.getNewNode());
            
            if (siblingRightNode > 0) {
                BTreeNode siblingNode = system.lookup(siblingRightNode);
                BTreeNode childNode = system.lookup(childNodes[length]);
                
                childNode.siblingRightNode = siblingNode.childNodes[0];
            } 
            
            return put(key, value);
        }
    }

    @Override
    protected boolean put(Comparable key, long childNode) {
        int n = indexOf(key); 
 
        return put(n, key, childNode);
    }
    
    private boolean put(int Index, Comparable key, long childNode) {
        BTreeNode bn = system.lookup(childNodes[Index]);
        BTreeNode changeNode = system.lookup(childNode);
        
        bn.siblingRightNode = childNode; 
        changeNode.siblingRightNode = childNodes[Index + 1];
        
        // 공간확보
        this.rotateRight(Index, 1);

        // 자식노드 삽입
        this.keys[Index] = key;
        this.childNodes[Index + 1] = childNode;
        
        // system.sync() 호출시 물리적으로도 업데이트 되게 유도
        this.isDirty = true; 
        bn.isDirty = true;
        changeNode.isDirty = true;
        
        return true;
    }
    
    @Override
    protected Object remove(Comparable key) throws NoSuchKeyException {
        int n = indexOf(key);
        BTreeNode bn = system.lookup(childNodes[n]);
         
        Object o = bn.remove(key);  
        
        return o;
    }

    @Override
    protected Object get(Comparable key) throws NoSuchKeyException {
        return system.lookup(childNodes[indexOf(key)]).get(key);
    }

    @Override
    protected boolean contains(Comparable key) {
        return system.lookup(childNodes[indexOf(key)]).contains(key);
    }

    @Override
    protected Comparable first() {
        if (length <= 0) {
            return null;
        }

        BTreeNode bn = system.lookup(childNodes[0]);
        return bn.first();
    }

    @Override
    protected Comparable last() {
        if (length <= 0) {
            return null;
        }

        BTreeNode bn = system.lookup(childNodes[length]);
        return bn.last();
    }

    @Override
    protected int indexOf(Comparable key) { // 받은 키값이 가장 근접한 LeafNode 로 유도 
        int n = 0;
        for (; n < length; ++n) {
            if (keys[n].compareTo(key) > 0) {
                break;
            }
        }
        return n;
    }

    protected static BranchNode unserialize(BTreeFileSystem system, ByteBuffer bytes) {
        Serializer s = system.getKeySerializer();
        int maxLength = system.getDegree() * 2;
        
        byte length = bytes.get();
        long siblingLeftNode = bytes.getLong();
        long siblingRightNode = bytes.getLong();
        Comparable keys[] = new Comparable[maxLength];
        long childNodes[] = new long[maxLength + 1];

        for (int n = 0; n < length; ++n) {
            byte[] b = new byte[s.length()];
            bytes.get(b);
            keys[n] = (Comparable) s.unserialize(ByteBuffer.wrap(b));
        }
        
        bytes.position(s.length() * (maxLength - length) + bytes.position());

        if (length > 0) {
            for (int n = 0; n <= length; ++n) {
                childNodes[n] = bytes.getLong();
            }
        }

        BranchNode bn = new BranchNode(system);
        bn.siblingLeftNode = siblingLeftNode;
        bn.siblingRightNode = siblingRightNode;
        bn.length = length;
        bn.keys = keys;
        bn.childNodes = childNodes;
        return bn;
    }

    @Override
    protected ByteBuffer serialize() {
        final int maxLength = system.getDegree() * 2;
        final Serializer s = system.getKeySerializer();
        final ByteBuffer b = ByteBuffer.allocate(system.getPhysicalNodeSize());

        b.put((byte) 1);  // 가지, 잎 검사
        b.put((byte) length); // 현재 길이
        b.putLong(siblingLeftNode);
        b.putLong(siblingRightNode);

        for (int n = 0; n < length; ++n) {
            b.put(s.serialize(keys[n]));
        }

        b.position(s.length() * (maxLength - length) + b.position());

        if (length > 0) {
            for (int n = 0; n <= length; ++n) {
                b.putLong(childNodes[n]);
            }
        }

        b.clear();

        return b;
    }

    @Override
    protected String toString(int depth) {
        final StringBuilder sb = new StringBuilder();
        
        if (length > 0) {
            sb.append(system.lookup(childNodes[length]).toString(depth + 1));
        }
        
        for (int n = length - 1; n >= 0; --n) {
            
            if (siblingRightNode > 0) {
                sb.append(StringUtils.tabbedOf(depth)).append("↑:").append(siblingRightNode).append('\n');
            }   

            sb.append(StringUtils.tabbedOf(depth)).append("◎:").append(address).append('\n');
            sb.append(StringUtils.tabbedOf(depth)).append("-[").append(keys[n]).append("]<\n");
            
            if (siblingLeftNode > 0) {
                sb.append(StringUtils.tabbedOf(depth)).append("↓:").append(siblingLeftNode).append('\n');
            }
            
            sb.append(system.lookup(childNodes[n]).toString(depth + 1));
        }
        
        return sb.toString();
    }

    @Override
    protected boolean redistLeft() {
        return true; // TODO
    }

    @Override
    protected boolean redistRight() {
        return true; // TODO
    }

    @Override
    protected boolean mergeLeft() {
        return false;
    }

    @Override
    protected boolean mergeRight() {
        return false;
    }
}
