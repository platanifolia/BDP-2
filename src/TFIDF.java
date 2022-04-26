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
import java.util.HashMap;
import java.util.StringTokenizer;

public class TFIDF {
    // (k,v) = word@filename(xxx@xxxx-x), tf
    public static class tfMapper extends Mapper<Object, Text, Text, IntWritable> {
        private HashMap<String, Integer> newkv = new HashMap<>();
        @Override
        protected void map (Object key, Text value, Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String k = itr.nextToken();
                if (newkv.containsKey(k)) {
                    Integer v = newkv.get(k);
                    newkv.put(k, ++v);
                } else {
                    newkv.put(k, 1);
                }
            }
            for (HashMap.Entry<String, Integer> entry : newkv.entrySet()) {
                context.write(new Text(entry.getKey() + "@" + filename), new IntWritable(entry.getValue()));
            }
        }
    }

    public static class TFPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            // 按照词语划分
            return super.getPartition(new Text(key.toString().split("@")[0]), value, numReduceTasks);
        }
    }

    public static class TFReducer extends Reducer<Text, IntWritable, Text, Text> {
        private HashMap<String, Float> result;
        private HashMap<String, Integer> tftmp;
        // 以下皆为上一项数据，因为需要二次比对
        private String lastword;
        private String lasttitle;
        private int filecount;
        private int lasttf;
//        private int allfilename;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            result = new HashMap<>();
            tftmp = new HashMap<>();
            lastword = "";
            lasttitle = "";
            filecount = 0;
            lasttf = 0;
//            Configuration conf = context.getConfiguration();
//            allfilename = Integer.parseInt(conf.get("totalfilenum"));
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String newword = key.toString().split("@")[0];
            String newtitle = key.toString().split("@")[1].split("-")[0];

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            if (!lasttitle.equals(newtitle) && !lasttitle.isEmpty()) {
                tftmp.put(lasttitle + ", " + lastword + ", ", lasttf);
                lasttf = 0;
            }
            if (!lastword.equals(newword) && !lastword.isEmpty()) {
                if (lasttitle.equals(newtitle) || lasttitle.isEmpty()) {
                    tftmp.put(lasttitle + ", " + lastword + ", ", lasttf);
                    lasttf = 0;
                }
                // 计算IDF，置入result
                double idf = Math.log((double) 260 / (filecount + 1));
                for(HashMap.Entry<String, Integer> iter : tftmp.entrySet()){
                    result.put(iter.getKey(), (float) (iter.getValue()*idf));
                }
                tftmp.clear();
                filecount = 0;
            }
            // 更新上一项
            lastword = newword;
            lasttitle = newtitle;
            filecount += 1;
            lasttf += sum;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(HashMap.Entry<String, Float> iter : result.entrySet()){
                context.write(new Text(iter.getKey()+String.format("%.3f",iter.getValue()).toString()), new Text());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        FileSystem hdfs = FileSystem.get(conf);
//        FileStatus[] stats = hdfs.listStatus(new Path(args[0]));
//        int DocSum = stats.length;
//        hdfs.close();
//
//        conf.set("totalfilenum", String.valueOf(DocSum));

        Job job = Job.getInstance(conf, "TFIDF");
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(tfMapper.class);
        job.setPartitionerClass(TFPartitioner.class);
        job.setReducerClass(TFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
