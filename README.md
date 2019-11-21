# HadoopMapReduceDemo
## 方案一 概述
第一步，map方法读取文本数据，将数据按照TreeMap<人, 好友们[]>的结构进行存储；
第二步，找朋友，遍历TreeMap<人, 好友们[]>中的每一个"人-好友们"，得到两个人的共同好友；
第三部，输出结果。

具体如下：
map()方法
1.获取一行内容： A:B,C,D,F,E,O
2.用冒号":"进行切割，得到"A"和"B,C,D,F,E,O"；
3.将"A"作为Key，"B,C,D,F,E,O"切分成数组后作为value，放入TreeMap中。

cleanup()方法
1.获取TreeMap中所有的key并转换为String数组，这个数组是有序数组；
2.遍历上述的String数组，和其它人进行比较，获取到他们共同的好好友；
3.intersection()方法，获取两个String数组的交集，并以字符串的形式输出。

## 方案二 概述
1.//设置缓存文件 job.addCacheFile(new URI("file:///D:/test/friends/friends.txt"));
2.在Mapper类中，重写setup()方法，把缓存中数据取出，整理放入一个全局变量map中。
3.在map()方法中，把每行读取的数据 和map迭代 取交集。

具体流程：
在某人X的好友中寻找，若好友中包含A，把X放入“A区”
在某人X的好友中寻找，若好友中包含B，则把X放入“B区”
在某人X的好友中寻找，若好友中包含C，则把X放入 “C区”
同理，一直执行一遍后，汇总所有结果