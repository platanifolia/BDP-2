package lab2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.*;

public class InvertedIndex {
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
                Text keyout=new Text(String.format("%s\t%.3f",word.toString(),average));
                Text valueout=new Text(word_file.get(word).toString());
                context.write(keyout,valueout);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");

        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setPartitionerClass(InvertedIndexPartitioner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
