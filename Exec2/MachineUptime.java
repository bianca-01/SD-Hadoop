import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MachineUptime {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text machine = new Text();
        private Text eventInfo = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] tokens = value.toString().split("\\s+");
            if (tokens.length < 5) return; // Ignorar linhas inválidas

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

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private static int outOfRangeCount = 0;
        
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            long totalTime = 0;
            long minStartTime = Long.MAX_VALUE;
            long maxEndTime = 0;
            Set<String> activeDays = new HashSet<>();
            String machineName = key.toString(); // Nome da máquina
            long totalStartTime = Long.MAX_VALUE;
            long totalEndTime = 0;

            while (values.hasNext()) {
                String[] tokens = values.next().toString().split(":");
                try {
                    // Tratando a possibilidade de timestamp com casas decimais
                    double start = Double.parseDouble(tokens[0]); // Agora usamos Double
                    double end = Double.parseDouble(tokens[1]);

                    totalTime += (long) (end - start);
                    minStartTime = Math.min(minStartTime, (long) start);
                    maxEndTime = Math.max(maxEndTime, (long) end);
                    activeDays.add(String.valueOf((long) (start / (24 * 60 * 60)))); // Convertendo timestamp para "dia"

                    totalStartTime = Math.min(totalStartTime, (long) start);
                    totalEndTime = Math.max(totalEndTime, (long) end);

                } catch (NumberFormatException e) {
                    // Apenas ignora a exception.
                }
            }

            long totalDays = activeDays.size();
            double avgTimePerDay = totalDays > 0 ? (double) totalTime / totalDays : 0;

            if (totalDays >= 300 && avgTimePerDay >= 3600) {
                output.collect(key, new Text(machineName + " | Tempo medio: " + (avgTimePerDay / 3600) + " horas/dia | Dias ativos: " + totalDays
                        + " | Tempo de inicio: " + totalStartTime + " | Tempo de fim: " + totalEndTime));
            } 
            else {
                synchronized (Reduce.class) {
                    outOfRangeCount++;
                }
            }
        }

        @Override
        public void close() throws IOException {
            OutputCollector<Text, Text> output = new OutputCollector<Text, Text>() {
                @Override
                public void collect(Text key, Text value) throws IOException {
                    System.out.println(key.toString() + "\t" + value.toString());
                }
            };
            output.collect(new Text("Maquinas fora do intervalo: "), new Text(String.valueOf(outOfRangeCount)));
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
