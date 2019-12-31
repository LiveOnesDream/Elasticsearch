package Spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class TransformationExample {
    public static SparkConf sconf = new SparkConf().setAppName("TransformationExample").setMaster("local[2]").set("spark.testing.memory", "2147480000");
    public static SparkSession session = SparkSession.builder().config(sconf).getOrCreate();
    public static JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
    public static final String path = "C:\\Users\\zp244\\Desktop\\SparkData\\*";

    /**
     * spark RDD 的持久化
     */
    public static void Persist_Example() {

        long start1 = System.currentTimeMillis();
        JavaPairRDD pairRDD1 = readFileWordCount(path);
        pairRDD1.cache();
//        pairRDD1.persist(StorageLevel.DISK_ONLY());
        long sum = pairRDD1.count();
        System.out.println(sum);
        System.out.println(" end1\t" + (System.currentTimeMillis() - start1) + "ms");

        long start2 = System.currentTimeMillis();
        JavaPairRDD pairRDD2 = readFileWordCount(path);
        long sum2 = pairRDD2.count();
        System.out.println(sum2);
        System.out.println(" end2\t" + (System.currentTimeMillis() - start2) + "ms");
    }

    public static JavaPairRDD readFileWordCount(String path) {
        JavaRDD<String> lines = sc.textFile(path);

        JavaRDD<String> rdd = lines.flatMap(new FlatMapFunction<String,String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> pairRDD1 = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

        return pairRDD;
    }

    /**
     * 共享变量
     * 我们在dirver中声明的这些局部变量或者成员变量，可以直接在transformation中使用，
     * 但是经过transformation操作之后，是不会将最终的结果重新赋值给dirver中的对应的变量。
     * 因为通过action，触发了transformation的操作，transformation的操作，都是通过
     * DAGScheduler将代码打包 序列化 交由TaskScheduler传送到各个Worker节点中的Executor去执行，
     * 在transformation中执行的这些变量，是自己节点上的变量，不是dirver上最初的变量，我们只不过是将
     * driver上的对应的变量拷贝了一份而已。
     * 这个案例也反映出，我们需要有一些操作对应的变量，在driver和executor上面共享
     * spark给我们提供了两种解决方案——两种共享变量
     * 广播变量 注意事项
     *
     * 1、能不能将一个RDD使用广播变量广播出去？
     * 不能，因为RDD是不存储数据的。可以将RDD的结果广播出去。
     *
     * 2、 广播变量只能在Driver端定义，不能在Executor端定义。
     *
     * 3、 在Driver端可以修改广播变量的值，在Executor端无法修改广播变量的值。
     *
     * 4、如果executor端用到了Driver的变量，如果不使用广播变量在Executor有多少task就有多少Driver端的变量副本。
     *
     * 5、如果Executor端用到了Driver的变量，如果使用广播变量在每个Executor中只有一份Driver端的变量副本。
     *
     * 累加器注意：
     * 累加器只能在Driver定义初始化，在Executor端更新，不能再executor端定义，不能再executor端（.value）获取值
     */

    /**
     * 使用Spark广播变量
     * <p>
     * 需求：
     * 用户表：
     * id name age gender(0|1)
     * <p>
     * 要求，输出用户信息，gender必须为男或者女，不能为0,1
     */

    public static void Broadcast_Example() {

        List<String> list = Arrays.asList(
                "001,刘向前,18,0",
                "002,冯  剑,28,1",
                "003,李志杰,38,0",
                "004,郭  鹏,48,2");

        Map<String, String> map = new HashMap<>();
        map.put("0", "女");
        map.put("1", "男");

        Broadcast<Map<String, String>> broadcast = sc.broadcast(map);
        JavaRDD<String> userRDD = sc.parallelize(list);
        JavaRDD<String> result = userRDD.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String prefix = s.substring(0, s.indexOf(","));
                String gender = s.substring(s.lastIndexOf(",") + 1);
                Map<String, String> map1 = broadcast.value();
                String newGender = map1.getOrDefault(gender, "男");

                return prefix + ":" + newGender;
            }
        });

        result.foreach(s -> System.out.println(s));

    }

    /**
     * 累加器
     */
    public static void accumulator_Example() {

        Accumulator<Integer> count = sc.accumulator(0);
        List<String> list = Arrays.asList("haha", "", "test", "");

        JavaRDD<String> rdd = sc.parallelize(list, 2);
        rdd.mapToPair(x -> {
            if (x.equals("")) {
                count.add(1);
            }
            return new Tuple2<>(x, 1);
        }).collect();
        System.out.println(count.value());

    }

    public static void zip_Example() {

        ArrayList<Integer> list = new ArrayList<>();
        ArrayList<Integer> list2 = new ArrayList<>();
        for (int i = 0; i <= 200; i++) {
            if (i > 0 & i <= 100) {
                list.add(i);
            }
            if (i > 100 & i <= 200) {
                list2.add(i);
            }
        }

        JavaRDD<Integer> rdd1 = sc.parallelize(list, 2);
        JavaRDD<Integer> rdd2 = sc.parallelize(list2, 2);
        JavaPairRDD<Integer, Integer> zip = rdd1.zip(rdd2);
        zip.foreach(s -> System.out.println(s));

    }

    public static void checkpoint_Example() {
//        设置检查节点目录
        sc.setCheckpointDir("C:\\Users\\zp244\\Desktop\\check");

        JavaRDD<String> rdd = sc.textFile(path, 2);
        JavaRDD<String> lines = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).cache();

//        为lines 设置检查节点
        lines.checkpoint();
        System.out.println("isCheckpointed:" + lines.isCheckpointed());
        System.out.println("checkpoint:" + lines.getCheckpointFile());

        lines.collect();
        System.out.println("isCheckpointed:" + lines.isCheckpointed());
        System.out.println("checkpoint:" + lines.getCheckpointFile());

    }

    public static void zipPartitions_Example() {

        ArrayList<Integer> list = new ArrayList<>();
        ArrayList<Integer> list2 = new ArrayList<>();

        for (int i = 0; i <= 200; i++) {
            if (i > 0 & i <= 100) {
                list.add(i);
            }
            if (i > 100 & i <= 156) {
                list2.add(i);
            }
        }
        JavaRDD<Integer> rdd = sc.parallelize(list, 3);
        JavaRDD<Integer> rdd1 = sc.parallelize(list2, 3);

        //按照分区进行map操作，查看数据分区分布情况
//        List<String> collect = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer index, Iterator<Integer> iterator) throws Exception {
//                List<String> list = new ArrayList<>();
//                while (iterator.hasNext()) {
//                    list.add("patition" + index + ":" + iterator.next());
//                }
//                return list.iterator();
//            }
//        }, true).collect();


        List<String> collect = rdd.mapPartitionsWithIndex((index, y) -> {
            List<String> list1 = new ArrayList<>();
            while (y.hasNext()) {
                list1.add("partition" + index + ":" + y.next());
            }
            return list1.iterator();
        }, true).collect();


//        List<String> collect1 = rdd.zipPartitions(rdd1, new FlatMapFunction2<Iterator<Integer>, Iterator<Integer>, String>() {
//            @Override
//            public Iterator<String> call(Iterator<Integer> iterator, Iterator<Integer> iterator2) throws Exception {
//                List<String> list1 = new ArrayList<>();
//                while (iterator.hasNext() && iterator2.hasNext()) {
//                    list1.add(iterator.next().toString() + " - " + iterator2.next().toString());
//                }
//                return list1.iterator();
//            }
//        }).collect();

        List<String> collect1 = rdd.zipPartitions(rdd1, (i1, i2) -> {
            List<String> arrayList = new ArrayList<>();
            while (i1.hasNext() && i2.hasNext()) {
                arrayList.add(i1.next().toString() + "-" + i2.next().toString());
            }
            return arrayList.iterator();
        }).collect();

        for (String str : collect1) {
            System.out.println(str);
        }
    }

    /**
     * java 8 spark wordCount
     */
    public static void wordcount_lam() {

        JavaRDD<String> fileRdd = sc.textFile(path);
        JavaRDD<String> wordRdd = fileRdd.flatMap(s -> Arrays.asList(s.split(",")).iterator());
        JavaPairRDD<String, Integer> wordOneRdd = wordRdd.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCountRdd = wordOneRdd.reduceByKey((x, y) -> x + y);
        JavaPairRDD<Integer, String> count2WordRdd = wordCountRdd.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        JavaPairRDD<Integer, String> sortRDD = count2WordRdd.sortByKey(false);
        JavaPairRDD<String, Integer> result = sortRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        result.foreach(s -> System.out.println(s));

    }

    public static void main(String[] args) {

//        Persist_Example();
//        Broadcast_Example();
        wordcount_lam();
//        zip_Example();
//        zipPartitions_Example();
//        accumulator_Example();
//        checkpoint_Example();

        sc.close();
    }
}
