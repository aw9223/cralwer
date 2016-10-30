package org.forUgram.example;


import java.io.IOException;
import org.forUgram.database.BTreeFileSystem;
import org.forUgram.database.BranchNode;
import org.forUgram.database.LeafNode;
import org.forUgram.database.Serializer;

public class BTree111 {

    public static void main(String args[]) throws IOException, InterruptedException {
BTreeFileSystem btrfs = new BTreeFileSystem(Serializer.INTEGER, "forUgram2"); 
for(int i = 14; i >= 0; --i)
btrfs.put(i, i); 
//btrfs.put(1, 0);
//btrfs.put(9, 1);
//btrfs.put(1, 2);
//btrfs.put(7, 3);
//btrfs.put(8, 4);
//btrfs.put(7, 5);
//btrfs.put(3, 6);
//btrfs.put(6, 7);
//btrfs.put(2, 8);
//btrfs.put(4, 9);
//btrfs.put(7, 10);
//btrfs.put(8, 11);
//btrfs.put(1, 12);
//btrfs.put(8, 13);
//btrfs.put(7, 14);
//btrfs.put(3, 15);
//btrfs.put(4, 16);
//btrfs.put(5, 17);
//btrfs.put(6, 18);
//btrfs.put(2, 19);
//btrfs.put(7, 20);
//btrfs.put(9, 21);
//btrfs.put(0, 22);
//btrfs.put(8, 23);
//btrfs.put(5, 24);
//btrfs.put(1, 25);

btrfs.DEBUG = true;
btrfs.put(6, 26);

btrfs.DEBUG = false;


System.out.println(btrfs); 
System.out.println("isValidate() = " + btrfs.isValidate()); 
//System.out.println(btrfs.isValidate());
 
//        BranchNode bn =new BranchNode(btrfs);
//
//        bn.keys[0] = 1;
//        bn.keys[1] = 2;
//        bn.keys[2] = 3;
//        bn.childNodes[0] = 1;
//        bn.childNodes[1] = 2;
//        bn.childNodes[2] = 3;
//        bn.childNodes[3] = 4;
//        
//        bn.length = 3;
//        
//        System.out.println(bn); 
//        
//        System.out.println("--------------------"); 
//        bn.shift(1, 1);
//         
//        System.out.println(bn); 
//        LeafNode ln =new LeafNode(btrfs);
//
//        ln.keys[0] = 1;
//        ln.keys[1] = 2;
//        ln.keys[2] = 3;
//        ln.values[0] = 1;
//        ln.values[1] = 2;
//        ln.values[2] = 3;
//        
//        ln.length = 3;
//         
//        
//        
//        System.out.println(ln);
//        ln.shift(2, 1);
//        
//        System.out.println(ln); 
    }
}
