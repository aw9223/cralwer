package org.forUgram.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import org.forUgram.database.BTreeFileSystem; 
import org.forUgram.database.Serializer;

public class BTreePut {
    
    private static ArrayList<String> arr = new ArrayList<String>();

    public static void main(String args[]) throws IOException, InterruptedException {
        
        while(true) {
            BTreeFileSystem btrfs = new BTreeFileSystem(Serializer.INTEGER, "forUgram");

            for(int n = 0; ; ++n) {

                int a = (int) (Math.random() * 10);

                if(btrfs.length() <= 40) {
                    int b = (int) (Math.random() * 10);

                    btrfs.put(b, n);
                    arr.add("btrfs.put(" + b + ", " + n + ");"); 
                } else {
                    arr.clear();
                    break;
                }

                new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();

                System.out.println(btrfs);

                if (!btrfs.isValidate()) { 
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
                    return;
                }

                Thread.sleep(1);
            }
        }
    }
}
