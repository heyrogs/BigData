package com.jiang.sparkstream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author jiang
 * <p>
 *     stream操作步骤:
 *     1、创建StreamingContext对象，注意创建过程要指定处理数据的时间间隔。
 *     2、创建InputDStream，指定输入源。
 *     3、操作DStream。
 *     4、启动stream。
 *
 * </p>
 * Create by 2018/5/21 13:51
 */
public class SparkStreamingT {


    public static void main(String[] args) throws Exception{

        /**
         *  1.
         *  spark straming 基于spark core计算，需要注意事项:
         *     1.设置本地master，如果需要指定local的话，必须指定
         *     最少两条线程，一条用来接收实时数据，另外一条则用来实现计算。
         *
         *     2.对于集群而言，每隔exccutor一般肯定不只一个Thread，
         *     那对于处理Spark Streaming应用程序而言，每个executor
         *     一般分配多少core比较合适？根据我们过去的经验，5个左右
         *     的core是最佳的（段子:分配为奇数个core的表现最佳,例如：分配3个、5个、7个core等）
         */
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("my first spark stream project");

        /**
         *
         *  2.
         *  创建SparkStreamingContext
         *  这个是SparkStreaming一切的开始，可以基于SparkConfig参数，
         *  也可以基于持久化的SparkStreamingContext进行状态修复。典型的
         *  场景是Driver奔溃以后由于Spark Streaming具有连续不断的24小时
         *  不间断地运行，所以需要自Driver重新启动后从上次运行的状态恢复过来
         *  此时的状态需要基于曾今的checkPoint
         */
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        /**
         * 3.
         * 创建输入来源,可以基于File、HDFS、flume、kafka、socket等。
         *
         * 这里指定的数据来源为网络的Socket端口，SparkStreaming连接上该端口，
         * 并且在运行的时候一直监听这个端口的数据，并且后续会根据业务需要不断的
         * 产生数据。
         *
         * 如果经常在每隔5秒没有数据就不断的启动空的job其实是会造成调度资源的浪费
         * ，因为没有接受到数据就提交了job。实际的企业级做法是提交job前判断时候有
         * 数据，没有的话就不会再提交。
         */
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("hadoop1",9999);

        /**
         * 4.
         * 基于DStream编程：
         *   原因是DStream是RDD产生的模板，在Spark Streaming发生计算前，其实是
         *   把每个Batch的DStream操作翻译成立RDD操作。
         *
         */
        //遍历每一行，并且将每行分割单词返回String的iterator
        JavaDStream<String> words = lines.flatMap(line
                -> Arrays.asList(line.split("\t")).iterator());
        /**
         * 将每一个单词的计数标记为1
         */
        JavaPairDStream<String,Integer> pairDStream = words.mapToPair(word
                -> new Tuple2(word,1));

        /**
         *  和reduce原理相似：
         *  先使用字典序进行排序，然后聚合在一组，这里变根据相同的key实现叠加。
         */
        JavaPairDStream<String,Integer> world_count = pairDStream.reduceByKey((v1,v2)
        ->(v1 + v2));

        /**
         *  此处的print()并不会触发job执行，因为目前的代码还在SparkStream的控制之下，
         *  具体触发取决于设置的Duration时间的间隔。
         */
        world_count.print();

        /**
         *  Spark Streaming引擎开始执行，也是Driver开始执行。
         *
         */
        jsc.start();

        /**
         * 等待程序执行结束
         */
        jsc.awaitTermination();
    }


}
