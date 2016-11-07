package org.forUgram.database;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.forUgram.common.StringUtils;

public class BranchNode_BACKUP { //extends BTreeNode {
/*
    protected BranchNode_BACKUP(BTreeFileSystem system) {
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
            final int d = system.getDegree();
            final Comparable k = keys[d];
            final BranchNode_BACKUP bn = new BranchNode_BACKUP(system); // 오른쪽 노드 생성
            bn.childNodes[0] = childNodes[d + 1];  // 브랜치노드의 첫번째는 복사를 직접 해줘야함...
            bn.isDirty = true;
            for (int n = d + 1; n < length; ++n) { // 오른쪽 노드로 반틈 복사
                bn.put(keys[n], childNodes[n + 1]);
            }
             
            this.rotateLeft(d, length - d); // 현재노드에서 반틈을 제거

            this.isDirty = true;// system.sync() 호출시 물리적으로도 업데이트 되게 유도

            throw new OverflowException(k, system.alloc(bn)); // 나머지는 부모노드에서 처리
        }
        
        final int n = indexOf(key);
        
        try {    
            return system.lookup(childNodes[n]).put(key, value); // 삽입 작업
        }
        catch (OverflowException e) { // 삽입 작업도중 꽉찬 노드를 발견했을때
            _put(n, e.getKey(), e.getNewNode());
            
            return put(key, value);
        }
    }

    @Override
    protected boolean put(Comparable key, long childNode) {
        final int n = indexOf(key); 
 
        return _put(n, key, childNode);
    }
    
    private boolean _put(int number, Comparable key, long childNode) {
        
        long l = childNodes[number];

        // 공간확보
        this.rotateRight(number, 1);

        // 자식노드 삽입
        this.keys[number] = key;
        this.childNodes[number + 1] = childNode;
        
        BTreeNode bn = system.lookup(childNode);
        bn.siblingLeftNode = l;
        
        this.isDirty = true; // system.sync() 호출시 물리적으로도 업데이트 되게 유도

        return true;
    }
    
    // childIndex - 1 만 이해하면 완료??
    // 함수 바디가 너무길다....
    private boolean redistribute(int childIndex, int siblingIndex) {
        if ( ! (0 <= siblingIndex && siblingIndex <= length)) {
            return false;
        }
        
        final boolean isRightSibling = childIndex < siblingIndex; //  형제노드가 오른쪽 방향인가? 
        final BTreeNode siblingNode = system.lookup(childNodes[siblingIndex]);
        
        if ((siblingNode.avaliable() - 1) < 0) { // 한개가 모잘라도 언더플로우 현상이 아니라면 재분배는 진행됨
            return false;
        }
        
        final BTreeNode childNode = system.lookup(childNodes[childIndex]);
        
        if (isRightSibling && childNode.isLeaf()) {
            System.out.println(1);
            childNode.rotateRight(0, 1);
            
            childNode.keys[childNode.length - 1] = siblingNode.keys[0];
            childNode.values[childNode.length - 1] = siblingNode.values[0];
            
            this.keys[childIndex] = siblingNode.keys[0]; 

            siblingNode.rotateLeft(0, 1);
            
            this.isDirty = true;
            childNode.isDirty = true;
            siblingNode.isDirty = true; 

            return true;
        }
        else if (isRightSibling && childNode.isBranch()) {
            System.out.println(2);
            childNode.rotateRight(0, 1);
             
            childNode.keys[childNode.length - 1] = this.keys[childIndex];
            childNode.childNodes[childNode.length] = siblingNode.childNodes[0]; 
            
            siblingNode.childNodes[0] = siblingNode.childNodes[1]; 
            
            this.keys[childIndex] = siblingNode.keys[0];
            
            siblingNode.rotateLeft(0, 1);

            this.isDirty = true;
            childNode.isDirty = true;
            siblingNode.isDirty = true; 

            return true;
        } 
        else if (childNode.isLeaf()) {  
            System.out.println(3);
            childNode.rotateRight(0, 1);
            
            childNode.keys[0] = siblingNode.keys[siblingNode.length - 1];
            childNode.values[0] = siblingNode.values[siblingNode.length - 1];
            
            this.keys[childIndex - 1] = siblingNode.keys[siblingNode.length - 1];
            
            siblingNode.rotateLeft(siblingNode.length - 1, 1);

            this.isDirty = true;
            childNode.isDirty = true;
            siblingNode.isDirty = true; 

            return true;
        }
        else if (childNode.isBranch()) {
            System.out.println(4);
            childNode.rotateRight(0, 1);
            
            childNode.keys[0] = this.keys[childIndex - 1];
            childNode.childNodes[1] = childNode.childNodes[0]; 
            childNode.childNodes[0] = siblingNode.childNodes[siblingNode.length];
            
            this.keys[childIndex - 1] = siblingNode.keys[siblingNode.length - 1];
            
            siblingNode.rotateLeft(siblingNode.length - 1, 1);

            this.isDirty = true;
            childNode.isDirty = true;
            siblingNode.isDirty = true; 
        
            return true; // 재분배 완성
        }
        
        return false;
    }
    
    private boolean merge(int childIndex, int siblingIndex) {
        if ( ! (0 <= siblingIndex && siblingIndex <= length)) {
            return false;
        }
        
        final boolean isRightSibling = childIndex < siblingIndex; //  형제노드가 오른쪽 방향인가?
        final BTreeNode siblingNode = system.lookup(childNodes[siblingIndex]);
        final BTreeNode childNode = system.lookup(childNodes[childIndex]);  
        
        if (isRightSibling && childNode.isLeaf()) {
            System.out.println(5);
            for(int n = 0; n < siblingNode.length; ++n) {
                childNode.put(siblingNode.keys[n], siblingNode.values[n]);
            }
            
            this.rotateLeft(childIndex, 1);
            
            this.isDirty = true;
            childNode.isDirty = true;
        }
        else if (isRightSibling && childNode.isBranch()) {
            System.out.println(6); 
            
            int c = childNode.length + 1; // 부모노드에 위치한 키값이 내려와야하므로 + 1을 더함
            
            childNode.rotateRight(0, siblingNode.length + 1);
            
            childNode.keys[c - 1] = this.keys[childIndex];
            childNode.childNodes[c] = siblingNode.childNodes[0];
            
            for(int n = 0; n < siblingNode.length; ++n) {
                childNode.keys[n + c] = siblingNode.keys[n];
                childNode.childNodes[n + c + 1] = siblingNode.childNodes[n + 1];
            }
            
            this.rotateLeft(childIndex, 1);
            
            this.isDirty = true;
            childNode.isDirty = true;
        }
        else if (childNode.isLeaf()) { 
            System.out.println(7);
            
            for(int n = 0; n < childNode.length; ++n) {
                siblingNode.put(childNode.keys[n], childNode.values[n]);
            }
            
            this.rotateLeft(childIndex - 1, 1);
             
            this.isDirty = true;
            childNode.isDirty = true; 
        } 
        else if (childNode.isBranch()) {
            System.out.println(8);
   
            int c = siblingNode.length + 1; // 부모노드에 위치한 키값이 내려와야하므로 + 1을 더함
            
            siblingNode.rotateRight(siblingNode.length - 1, childNode.length + 1);
            
            siblingNode.keys[c - 1] = this.keys[childIndex - 1];
            siblingNode.childNodes[c] = childNode.childNodes[0];
            
            for(int n = 0; n < childNode.length; ++n) {
                siblingNode.keys[n + c] = childNode.keys[n];
                siblingNode.childNodes[n + c + 1] = childNode.childNodes[n + 1];
            } 
            
            this.rotateLeft(childIndex - 1, 1);
            
            this.isDirty = true;
            childNode.isDirty = true; 
        }
        
        return true; // 합병 완성
    }
    
    /**
     * 삭제연산 이 일어난 노드가 언더플로우 현상이 생긴경우
     * 
     * 1. 왼쪽 혹은(왼쪽이 없는경우) 오른쪽에게 재분배작업
     * 2. 재분배를 하여도 부족하면 합병작업
     * 3. 순서는 왼쪽 검사후 오른쪽으로 진행
     * 
     * @param key
     * @return
     * @throws NoSuchKeyException 
     *
    @Override
    protected Object remove(Comparable key) throws NoSuchKeyException {
        int n = indexOf(key);
        final BTreeNode bn = system.lookup(childNodes[n]);
         
        final Object v = bn.remove(key);
        
        if (bn.avaliable() < 0) { // 삭제 연산을했는대 부족하다면
            
            // 왼쪽재분배, 오른쪽재분배, 왼쪽합병, 오른쪽합병 순으로 진행
            if (this.redistribute(n, n - 1)
                || this.redistribute(n, n + 1) 
                || this.merge(n, n - 1) 
                || this.merge(n, n + 1)) {
                
                return v;
            }
            
            throw new RuntimeException("재분배, 합병작업이 올바르게 진행되지 않았습니다.");
        }
        
        return v;
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

        final BTreeNode bn = system.lookup(childNodes[0]);
        return bn.first();
    }

    @Override
    protected Comparable last() {
        if (length <= 0) {
            return null;
        }

        final BTreeNode bn = system.lookup(childNodes[length]);
        return bn.last();
    }

    @Override
    protected int indexOf(Comparable key) { // 받은 키값이 가장 근접한 LeafNode 로 유도 
        int n = 0;
        for (; n < length; ++n) {
            if (keys[n].compareTo(key) > 0) {
                return n;
            }
        }
        
        return length;
    }

    protected static BranchNode_BACKUP unserialize(BTreeFileSystem system, ByteBuffer bytes) {
        final Serializer s = system.getKeySerializer();
        final int d = system.getDegree() * 2;
        
        final byte length = bytes.get();
        final Comparable keys[] = new Comparable[d];
        final long childNodes[] = new long[d + 1];

        for (int n = 0; n < length; ++n) {
            byte[] b = new byte[s.length()];
            bytes.get(b);
            keys[n] = (Comparable) s.unserialize(ByteBuffer.wrap(b));
        }
        
        bytes.position(s.length() * (d - length) + bytes.position());

        if (length > 0) {
            for (int n = 0; n <= length; ++n) {
                childNodes[n] = bytes.getLong();
            }
        }

        final BranchNode_BACKUP bn = new BranchNode_BACKUP(system);
        bn.length = length;
        bn.keys = keys;
        bn.childNodes = childNodes;
        return bn;
    }

    @Override
    protected ByteBuffer serialize() {
        final int d = system.getDegree() * 2;
        final Serializer s = system.getKeySerializer();
        final ByteBuffer b = ByteBuffer.allocate(system.getPhysicalNodeSize());

        b.put((byte) 1);  // 가지, 잎 검사
        b.put((byte) length); // 현재 길이

        for (int n = 0; n < length; ++n) {
            b.put(s.serialize(keys[n]));
        }

        b.position(s.length() * (d - length) + b.position());

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
            
            if (siblingLeftNode >= 0) {
                sb.append(StringUtils.tabbedOf(depth)).append(siblingLeftNode);
            }
            sb.append(StringUtils.tabbedOf(depth)).append("-[").append(keys[n]).append("]<\n");
            
            sb.append(system.lookup(childNodes[n]).toString(depth + 1));
        }
        return sb.toString();
    }*/
}
