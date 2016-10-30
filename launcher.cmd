@echo off
SET CLASSPATH=dist/*;lib/*;
java -Xms256m -Xmx1024m -Xss128k -Djava.library.path=. org.forUgram.example.BTree1
@pause