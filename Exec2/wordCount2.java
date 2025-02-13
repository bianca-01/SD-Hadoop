import java.io.IOException;
import java.util.Iterator;
import java.util.HashSet;

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

                long totalTime = 0; // Tempo total que a máquina ficou ligada
                long traceStart = Long.MAX_VALUE; // Tempo de início mais antigo
                long traceEnd = 0; // Tempo de término mais recente
                HashSet<Long> activeDays = new HashSet<>(); // Dias únicos em que a máquina esteve ativa

                while (values.hasNext()) {
                    String line = values.next().toString();
                    String[] tokens = line.split(":");
                    
                    // Convertendo os timestamps para long (truncando a parte decimal)
                    long start = (long) Double.parseDouble(tokens[0]);
                    long end = (long) Double.parseDouble(tokens[1]);

                    // Atualiza o tempo de início e término do trace
                    if (start < traceStart) {
                        traceStart = start;
                    }
                    if (end > traceEnd) {
                        traceEnd = end;
                    }

                    // Soma o tempo total que a máquina ficou ligada
                    totalTime += (end - start);

                    // Calcula o dia em que a máquina esteve ativa
                    long day = start / (24 * 60 * 60); // Convertendo timestamp para dia
                    activeDays.add(day);
                }

                // Calcula o número de dias únicos em que a máquina esteve ativa
                int totalDaysActive = activeDays.size();

                // Calcula o tempo médio por dia (em segundos)
                double avgTimePerDay = totalTime / (double) totalDaysActive;

                // Verifica se a máquina esteve ativa por 300 dias ou mais e se o tempo médio é >= 1 hora (3600 segundos)
                if (totalDaysActive >= 300 && avgTimePerDay >= 3600) {
                    val.set("OK " + totalTime + " " + traceStart + " " + traceEnd);
                } else {
                    val.set("NOK " + totalTime + " " + traceStart + " " + traceEnd);
                }

                // Emite o resultado
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
