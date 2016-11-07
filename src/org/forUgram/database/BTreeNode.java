package org.forUgram.database;

import java.io.Serializable;
import java.nio.ByteBuffer;

public abstract class BTreeNode {
 
    protected transient final BTreeFileSystem system;
    protected transient boolean isDirty = false; // 노드의 상태가 변함이 생겼는가 를 체크함.
    protected transient long address; // 이 노드에 대한 주소값
    
    protected Comparable[] keys;// 키 배열
    protected Serializable[] values;
    protected long[] childNodes; // 자식 노드
    protected long siblingLeftNode, siblingRightNode;

    protected byte length = 0;// 현재 개수

    protected BTreeNode(BTreeFileSystem system) {
        this.system = system; 

        this.keys = new Comparable[system.getDegree() * 2];
    }

    protected int avaliable() {
        return length - system.getDegree();
    }

    protected boolean isFull() { // 노드의 꽉 찬 여부 
        return length == keys.length;
    }

    protected boolean isLeaf() {
        return values != null;
    }

    protected boolean isBranch() {
        return childNodes != null;
    }
    
    protected boolean isValidate() { 
        for(int n = 1; n < length; ++n) {
            if (keys[n].compareTo(keys[n - 1]) < 0) { // 순서가 올바른지
                return false;
            }
        }
        
        if (length <= 0) {
            return false;
        }
        
        return true;
    }

    protected abstract void rotateLeft(int start, int count);
    protected abstract void rotateRight(int start, int count);
    
    protected abstract boolean redistLeft();
    protected abstract boolean redistRight();
    
    protected abstract boolean mergeLeft();
    protected abstract boolean mergeRight();

    protected abstract boolean put(Comparable key, Serializable value) throws OverflowException;
    protected abstract boolean put(Comparable key, long childNode); // 키, 값 넣기
    protected abstract Object remove(Comparable key) throws NoSuchKeyException; // 키로 제거하기
    protected abstract Object get(Comparable key) throws NoSuchKeyException;   // 키로 값 가져오기

    protected abstract Comparable first(); // 뒤에서 차례대로 
    protected abstract Comparable last(); // 뒤에서 차례대로 

    protected abstract boolean contains(Comparable key); // 해당 키가 있는지 찾는것
    protected abstract int indexOf(Comparable key); // 가지노드에서는 제일 근접한 위치를 앞에서 부터 찾아줌 

    protected abstract ByteBuffer serialize(); 

    protected abstract String toString(int depth);

    @Override
    public String toString() {
        return toString(0);
    }
}
