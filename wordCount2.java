import java.io.IOException;
import java.util.Iterator;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

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
                    Long machine = Long.parseLong(tokens[1]);
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

                long totalTime = 0;
                long traceStart = Long.MAX_VALUE;
                long traceEnd = 0;
                HashSet<Long> activeDays = new HashSet<>();

                while (values.hasNext()) {
                    String line = values.next().toString();
                    String[] tokens = line.split(":");
                    long start = Long.parseLong(tokens[0]);
                    long end = Long.parseLong(tokens[1]);

                    if (start < traceStart) {
                        traceStart = start;
                    }
                    if (end > traceEnd) {
                        traceEnd = end;
                    }
                    totalTime += (end - start);
                    
                    // Armazena os dias únicos em que a máquina esteve ativa
                    long day = start / (24 * 60 * 60); // Convertendo timestamp para dia
                    activeDays.add(day);
                }

                int totalDaysActive = activeDays.size();
                double avgTimePerDay = totalTime / (double) totalDaysActive;

                if (totalDaysActive >= 300 && avgTimePerDay >= 3600) {
                    val.set("OK " + totalTime + " " + traceStart + " " + traceEnd);
                } else {
                    val.set("NOK " + totalTime + " " + traceStart + " " + traceEnd);
                }
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
