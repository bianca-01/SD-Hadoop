import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    public static class Map extends MapReduceBase implements 
    Mapper<LongWritable, Text, LongWritable, Text> {

        private LongWritable k = new LongWritable();
        private Text v = new Text();

        public void map(LongWritable key, Text value,
        OutputCollector<LongWritable, Text> output, Reporter reporter)
            throws IOException {
                String[] tokens = value.toString().split("\\s");
                

                if (tokens.length < 5) {
                    return; // Ignora a linha se não tiver elementos suficientes
                }    

                if (tokens[0].charAt(0) != '#') {
                    Long machine = new Long(tokens[1]);
                    if (tokens[2].equals("1")) {
                        k.set(machine);
                        v.set(tokens[3] + ":" + tokens[4]);
                        output.collect(k, v);
                    }
                }
        }
    }

    public static class Reduce extends MapReduceBase
    implements Reducer<LongWritable, Text, LongWritable, Text> {
    
        private Text val = new Text();

        public void reduce(LongWritable key, Iterator<Text> values,
        OutputCollector<LongWritable, Text> output, Reporter reporter)
            throws IOException {

                long sum = new Long(0);
                long traceStart = new Long(Long.MAX_VALUE);
                long traceEnd = new Long(0);
                long start = new Long(0);
                long end = new Long(0);

                while (values.hasNext()) {
                    String line = values.next().toString();
                    String[] tokens = line.split(":");
                    start = new Double(tokens[0]).longValue();
                    end = new Double(tokens[1]).longValue();

                    if (start < traceStart) {
                        traceStart = start;
                    }

                    if (end > traceEnd) {
                        traceEnd = end;
                    }
                    sum += (end - start);
                }
                val = new Text(Long.toString(sum));
                output.collect(key, val);
        }
    }

    public static void main (String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: WordCount <input path> <output path> <num reducers>");
            System.exit(1);
        }

        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("tempocount");

        conf.setNumReduceTasks(Integer.parseInt(args[2]));

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
