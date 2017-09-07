package com.liuhuan.mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


    /**
     * MyMapper 将文本读取出来，处理成<A B C D,2>的形式
     */
    public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

        protected void map(Text value,Context context) throws IOException, InterruptedException {

            //拿到一行文本内容，转换成String 类型
            String line = value.toString();

            //将这行文本切分成维度和度量
            String keys = line.substring(0,line.length()-1);

            //取字符串中的最后一个字符转为int型作为value
            int values = Integer.valueOf(line.charAt(line.length()-1));

            //输出<key，value>
            context.write(new Text(keys), new IntWritable(values));
        }
    }

}
