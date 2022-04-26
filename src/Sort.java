import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class Sort {
    public static class InvertedIndexMapper extends Mapper<Object,Text,Text,IntWritable>{
        private Map<Text,IntWritable> word_int =new HashMap<>();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            StringTokenizer itr = new StringTokenizer(value.toString());
            for(; itr.hasMoreTokens();) {
                Text word=new Text(itr.nextToken());
                if(word_int.containsKey(word)){
                    word_int.put(word,new IntWritable(word_int.get(word).get()+1));
                }
                else{
                    word_int.put(word,new IntWritable(1));
                }
            }

            for(Text word:word_int.keySet()){
                Text keyout=new Text(word+"#"+fileName);
                IntWritable valueout=word_int.get(word);
                context.write(keyout,valueout);
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable value:values){
                sum=sum+value.get();
            }
            IntWritable intWritable=new IntWritable(sum);
            context.write(key,intWritable);
        }
    }

    public static class InvertedIndexPartitioner extends HashPartitioner<Text,IntWritable>{
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            Text word=new Text(key.toString().split("#")[0]);
            return super.getPartition(word,value,numReduceTasks);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text,IntWritable,Text,Text>{
        private Map<Text,IntWritable> word_sum=new TreeMap<>();
        private Map<Text,String> word_file=new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Text word=new Text(key.toString().split("#")[0]);
            String filename=new String(key.toString().split("#")[1]);
            StringBuilder stringBuilder=new StringBuilder();
            if(word_file.containsKey(word)){
                stringBuilder.append(word_file.get(word));
                stringBuilder.append(";");
                stringBuilder.append(filename);
                stringBuilder.append(":");
                for(IntWritable value:values) {
                    stringBuilder.append(value);
                    IntWritable intWritable=new IntWritable(word_sum.get(word).get() + value.get());
                    word_sum.put(word,intWritable);
                    word_file.put(word,stringBuilder.toString());
                }
            }
            else{
                stringBuilder.append(filename);
                stringBuilder.append(":");
                for(IntWritable value:values){
                    stringBuilder.append(value);
                    IntWritable intWritable=new IntWritable(value.get());
                    word_sum.put(word,intWritable);
                    word_file.put(word,stringBuilder.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Text word:word_sum.keySet()){
                double average=(double)word_sum.get(word).get()/word_file.get(word).split(";").length;
                Text keyout=new Text(word.toString());
                Text valueout=new Text(String.format("%.3f",average).toString());
                context.write(keyout,valueout);
            }
        }
    }

    public static class SortMapper extends Mapper<Object, Text, FloatWritable, Text> {
        @Override
        // (tf, word)
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] buffer = value.toString().split("\\t");
            context.write(new FloatWritable(Float.parseFloat(buffer[1])), new Text(buffer[0]));
        }
    }

    public static class SortReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
        @Override
        // (word, tf)
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, key);
            }
        }
    }


    public static class FloatWritableComparator extends WritableComparator {

        /*
         * 重写构造方法，定义比较类 IntWritable
         */
        public FloatWritableComparator() {
            super(FloatWritable.class, true);
        }
        /*
         * 重写compare方法，自定义比较规则
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            //向下转型
            FloatWritable ia = (FloatWritable) a;
            FloatWritable ib = (FloatWritable) b;
            return ib.compareTo(ia);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "index");
        job1.setJarByClass(Sort.class);
        job1.setMapperClass(InvertedIndexMapper.class);
        job1.setCombinerClass(InvertedIndexCombiner.class);
        job1.setPartitionerClass(InvertedIndexPartitioner.class);
        job1.setReducerClass(InvertedIndexReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));


        ControlledJob ctrljob1=new  ControlledJob(conf);
        ctrljob1.setJob(job1);
//        System.exit(job1.waitForCompletion(true) ? 0 : 1);

        Job job2 = Job.getInstance(conf, "sort");
        job2.setSortComparatorClass(Sort.FloatWritableComparator.class);
        job2.setJarByClass(Sort.class);
        job2.setMapperClass(Sort.SortMapper.class);
        job2.setReducerClass(Sort.SortReducer.class);
        job2.setMapOutputKeyClass(FloatWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        ControlledJob ctrljob2=new ControlledJob(conf);
        ctrljob2.setJob(job2);
        ctrljob2.addDependingJob(ctrljob1);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        JobControl joblists=new JobControl("my-list");

        //添加到总的JobControl里，进行控制
        joblists.addJob(ctrljob1);
        joblists.addJob(ctrljob2);

        Thread  t=new Thread(joblists);
        t.start();

        while(true){
            if(joblists.allFinished()){
                System.out.println(joblists.getSuccessfulJobList());
                joblists.stop();
                break;
            }
        }
    }
}