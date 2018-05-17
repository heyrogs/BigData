
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author jiang
 * <p>
 * Create by 2018/5/15 14:44
 */
public class HelloSpark {


   final SparkConf sparkConf =
            new SparkConf()
                    //.setMaster("spark://hadoop1:7077")
                    .setMaster("local")
                    .setAppName("hello spark");

   /**

    第一种数据输入方式:
    并行化数据集

    1.定义一个1-10的数据集
    val num = sc.parallelize(1 to 10)
    2.给数据集的每个数乘上2
    val doublenum = num.map(_*2)
    3.过滤掉被3整除的数
    val threenum = doublenum.filter(_%3 ==0)
    4.最后使用toDebugString显示RDD的LineAge
    threenum.toDebugString
    5.通过collect计算得出最终结果
    threenum.collect

    */
    public void collect() {
      //  System.setProperty("hadoop.home.dir","D:\\programmer\\server\\hadoop-2.7.5" );
        JavaSparkContext context = null;
        try {
            context = new JavaSparkContext(sparkConf);
            List<Integer> nums = Arrays.asList(1,2,3,4,5);
            JavaRDD<Integer> numsRdd = context.parallelize(nums);
            numsRdd.map(a -> a * 2);
            numsRdd.toDebugString();
            numsRdd.collect().forEach(num -> System.out.println("->" + num));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(context!=null)
                context.close();
        }
    }


    /**
     *
     *  第二种数据输入方式:
     *   外部数据集External Datasets
     *
     */
    public void externalDataSet(){

        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)){
            JavaRDD<String> distRdd
                    = sc.textFile("hdfs://hadoop1:9000/user/hadoop/data/core-site.xml");
           List<String> datas =  distRdd.collect();
           datas.forEach(data -> System.out.println("--> " + data));
        }
    }


    public static void main(String []args){

        new HelloSpark().collect();
    }


}
