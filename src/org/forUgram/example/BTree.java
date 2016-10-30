package org.forUgram.example;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import org.forUgram.database.BTreeFileSystem;
import org.forUgram.database.NoSuchKeyException;
import org.forUgram.database.Serializer;

public class BTree {
    
    private static ArrayList<String> arr = new ArrayList<String>();

    public static void main(String args[]) throws IOException, InterruptedException {
        
        final BTreeFileSystem btrfs = new BTreeFileSystem(Serializer.INTEGER, "forUgram");
        
        //arr.add("BTreeFileSystem btrfs = new BTreeFileSystem(Serializer.INTEGER, \"forUgram\");");
                    
        Runtime.getRuntime().addShutdownHook(new Thread() {
            
            @Override
            public void run() {
                try {
                    File f = new File("debug.txt");
                    f.createNewFile();
                    
                    FileWriter fw = new FileWriter(f, false);
                    for(String str : arr) {
                        fw.append(str);
                        fw.append('\n');
                    }
                    fw.flush();
                }
                catch(IOException e) {
                    e.printStackTrace();
                }
            }
        });
        
        for(int n = 0; ; ++n) {
            
            int a = (int) (Math.random() * 20);
            
            if(btrfs.contains(a)) {
                btrfs.remove(a);
                arr.add("btrfs.remove(" + a + ");"); 
            }
            
            if(btrfs.length() <= 40) {
                int b = (int) (Math.random() * 20);
                
                btrfs.put(b, n);
                arr.add("btrfs.put(" + b + ", " + n + ");"); 
            }
            
            if(n % 10 == 0) {
                btrfs.sync();
                //arr.add("btrfs.sync();");
            }
            
            new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
            
            System.out.println(btrfs);
            
            if (!btrfs.isValidate()) {
                return;
            }
        
            Thread.sleep(30);
        }
    }
}
