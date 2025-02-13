import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// INFO - A Classe Foi refatorada com o seguinte requisitos

// - Ver
public class WordCount2 {

    /**
     * Maper do registro de entrada e saida
     */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

        private final Text machineName = new Text();
        private final LongWritable duration = new LongWritable();

        /**
         * Processa cada linha da entrada e emite os tempos de atividade das máquinas.
         */
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                throws IOException {
            String[] tokens = value.toString().split("\\s");
            if (isValidEntry(tokens)) {
                processEntry(tokens, output);
            }
        }

        /**
         * Verifica se a entrada é válida (tem pelo menos 5 colunas e não começa com '#').
         */
        private boolean isValidEntry(String[] tokens) {
            return tokens.length >= 5 && tokens[0].charAt(0) != '#';
        }

        /**
         * Processa a entrada e emite os tempos de atividade das máquinas ligadas.
         */
        private void processEntry(String[] tokens, OutputCollector<Text, LongWritable> output) throws IOException {
            String nodeName = tokens[1];
            if (tokens[2].equals("1")) { // INFO: aqui ta verificando apenas máquinas ligadas
                long timeOn = calculateTimeOn(tokens);
                machineName.set(nodeName);
                duration.set(timeOn);
                output.collect(machineName, duration);
            }
        }

        /**
         * Calcula o tempo de atividade da máquina (diferença entre horário final e inicial).
         */
        private long calculateTimeOn(String[] tokens) {
            long startTime = Long.parseLong(tokens[3]);
            long endTime = Long.parseLong(tokens[4]);
            return endTime - startTime;
        }
    }

    /**
     * Agrega os tempos de atividade e calcula a média diária.
     */
    public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, Text> {

        /**
         * Responsavel por reduzri os valores, calcula a média de horas por dia e filtra os resultados.
         */
        @Override
        public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            MachineStats stats = calculateStats(values);
            if (shouldOutput(stats)) {
                output.collect(key, new Text(stats.formatResult()));
            }
        }

        /**
         * Calcula estatísticas do tempo total de atividade e a média de horas por dia.
         */
        private MachineStats calculateStats(Iterator<LongWritable> values) {
            long totalUptime = 0;
            int daysActive = 0;
            while (values.hasNext()) {
                totalUptime += values.next().get();
                daysActive++;
            }
            // aqui tem o calculo do averege por dia aqui temos o status da maquina de status passando para o metodo do retorno o uptimeTotal , dias ativos e o avrg por dia
            double avgPerDay = (daysActive > 0) ? (totalUptime / (double) daysActive) / 3600 : 0;
            return new MachineStats(totalUptime, daysActive, avgPerDay);
        }

        /**
         * Determina se a máquina deve ser incluída no resultado final (mínimo de 300 dias e média ≥ 1h/dia).
         */
        private boolean shouldOutput(MachineStats stats) {
            return stats.daysActive >= 300 && stats.avgPerDay >= 1;
        }
    }

    /**
     * Classe auxiliar para armazenar estatísticas do tempo de atividade das máquinas.
     */
    private static class MachineStats {
        int daysActive;
        double avgPerDay;

        MachineStats(long totalUptime, int daysActive, double avgPerDay) {
            this.daysActive = daysActive;
            this.avgPerDay = avgPerDay;
        }

        /**
         * Formata o resultado para exibição.
         */
        String formatResult() {
            return String.format("Média: %.2f horas/dia, Dias Ativos: %d", avgPerDay, daysActive);
        }
    }

    /**
     * Método principal que configura e executa o job MapReduce.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: WordCount <input path> <output path> <num reducers>");
            System.exit(1);
        }   

        JobConf conf = configureJob(args);
        JobClient.runJob(conf);
    }

    /**
     *Main com a config de parâmetros do job -===== Aqui é a main do projeto executar ele vai executar o processo .
     */
    private static JobConf configureJob(String[] args) {
        JobConf conf = new JobConf(WordCount2.class);
        conf.setJobName("machine-uptime-analysis");
        conf.setNumReduceTasks(Integer.parseInt(args[2]));
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        return conf;
    }
}
