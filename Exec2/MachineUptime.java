import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MachineUptime {

    public static class Map extends MapReduceBase implements 
    Mapper<LongWritable, Text, Text, Text> {

        private Text machine = new Text();
        private Text eventInfo = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String[] tokens = value.toString().split("\\s+");

            if (tokens.length < 5) {
                return; // Ignorar linhas inválidas
            }

            if (tokens[0].charAt(0) != '#') {
                String nodeName = tokens[1]; 
                String eventType = tokens[2];
                String startTime = tokens[3];
                String endTime = tokens[4];

                if (eventType.equals("1")) { // Máquina ligada
                    machine.set(nodeName);
                    eventInfo.set(startTime + ":" + endTime);
                    output.collect(machine, eventInfo);
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase 
    implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, 
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            long totalTime = 0;
            long minStartTime = Long.MAX_VALUE;
            long maxEndTime = 0;
            Set<String> activeDays = new HashSet<>();

            while (values.hasNext()) {
                String[] tokens = values.next().toString().split(":");
                long start = Long.parseLong(tokens[0]);
                long end = Long.parseLong(tokens[1]);

                totalTime += (end - start);
                minStartTime = Math.min(minStartTime, start);
                maxEndTime = Math.max(maxEndTime, end);

                // Adiciona o dia correspondente ao conjunto de dias ativos
                activeDays.add(String.valueOf(start / (24 * 60 * 60))); // Convertendo timestamp para "dia"
            }

            long totalDays = activeDays.size();
            double avgTimePerDay = totalDays > 0 ? (double) totalTime / totalDays : 0;

            // Se a máquina ficou ativa por pelo menos 300 dias e teve média >= 1 hora por dia
            if (totalDays >= 300 && avgTimePerDay >= 3600) {
                output.collect(key, new Text("Tempo médio: " + (avgTimePerDay / 3600) + " horas/dia | Dias ativos: " + totalDays));
            } else {
                output.collect(key, new Text("Fora do intervalo | Tempo médio: " + (avgTimePerDay / 3600) + " horas/dia | Dias ativos: " + totalDays));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: MachineUptime <input path> <output path> <num reducers>");
            System.exit(1);
        }

        JobConf conf = new JobConf(MachineUptime.class);
        conf.setJobName("machine-uptime");

        conf.setNumReduceTasks(Integer.parseInt(args[2]));

        conf.setOutputKeyClass(Text.class);
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
