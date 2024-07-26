package it.unipi.hadoop.nightswatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class LetterFrequency {

    /*
     * In this code we defined a Mapper with In-Mapper Combining and also a Combiner
     * that is meant to be used with NO In-Mapper Combining, that's why we
     * defined a Mapper ("LetterMapper2") that is the one we use with this Combiner.
     * We decided to put both versions inside of this code to keep everything in one
     * file, and we modify the main function to use one of the two versions we
     * have, depending on the experiments we want to carry out.
     */

    // Mapper (In-Mapper Combining)
    public static class LetterMapper1 extends Mapper<Object, Text, Text, IntWritable> {

        private Map<Character, Integer> letterCounts;

        // in the setup() we initalize a new HashMap
        @Override
        protected void setup(Context context) {
            letterCounts = new HashMap<>();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // we make sure each letter in the line transformed to lower case
            String line = value.toString().toLowerCase();
            // for each character in the line, if it is a letter from the English alphabet,
            // we insert it into the HashMap
            for (char c : line.toCharArray()) {
                if (Character.isLetter(c)) {
                    // if the key (so the letter) is already present inside of the HashMap, then we
                    // increment the existing related value
                    // otherwise we just do "0 + 1", which means we are inserting a new key (new
                    // letter)
                    letterCounts.put(c, letterCounts.getOrDefault(c, 0) + 1);
                }
            }
        }

        // we emit at the end of the Map logic using the cleanup() method
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Character, Integer> entry : letterCounts.entrySet()) {
                // we emit the local aggregated key-value pairs
                context.write(new Text(entry.getKey().toString()), new IntWritable(entry.getValue()));
            }
        }
    }

    // Mapper (NO In-Mapper Combining)
    public static class LetterMapper2 extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text letter = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // we make sure each letter in the line transformed to lower case
            String line = value.toString().toLowerCase();
            // for each character in the line, if it is a letter from the English alphabet,
            // emit a pair which is <letter,1>
            for (char c : line.toCharArray()) {
                if (Character.isLetter(c)) {
                    letter.set(Character.toString(c));
                    context.write(letter, one);
                }
            }
        }
    }

    // Combiner: to use only WITHOUT In-Mapper Combining
    // The input of the Combiner equals the output of the Mapper (Text, IntWritable)
    // and the output has to be the same as the Mapper's
    // because we are emitting to the Reducer, that is expecting (Text, IntWritable)
    public static class LetterCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        // The Combiner locally aggregates the output of each Mapper (which is <key,
        // list of values>)
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // for each letter we compute the sum of the values in the list (the occurrence
            // of the letter in each split)
            for (IntWritable val : values) {
                sum += val.get();
            }
            // we emit a pair consisting of the letter and its local occurrence
            context.write(key, new IntWritable(sum));
        }
    }

    /*
     * Reducer (frequency calculation in cleanup() method)
     * NOTE: This code works correctly only with a single reducer task.
     * Even though a key is guaranteed to go to only one reducer, the total count
     * calculated in each reducer will be partial
     * and only relative to the keys processed by that specific reducer. So we would
     * be able to compute the absoulte frequency, but not the relative one.
     */
    public static class LetterReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private long totalLetters = 0;
        private Map<Text, Integer> letterCounts;

        // we initialize a new HashMap inside of the setup() method
        // we use it to store the total letter occurrencies that is needed to calculate the frequency in the cleanup() method
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            letterCounts = new HashMap<>();
        }

        // the following reduce() method contains the same logic as the reduce() method
        // in the Combiner
        // but this time we also sum up the total number of letters of the input file
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            letterCounts.put(new Text(key), sum);
            totalLetters += sum;
        }

        // in the cleanup() method we compute the relative frequencies for each of the
        // 26 letters of the English alphabet
        // we then emit a pair that contains <letter, frequency>
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Text, Integer> entry : letterCounts.entrySet()) {
                double frequency = (double) entry.getValue() / totalLetters;
                context.write(entry.getKey(), new DoubleWritable(frequency));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // Command Line input
        // we check if the nummber of arguments is correct
        if (args.length != 2) {
            System.err.println("Usage: LetterFrequencyAnalyzer <input path> <output path>");
            System.exit(-1);
        }

        // we declare a configuration object
        Configuration conf = new Configuration();
        
        // we create a job object, configuring it with the "Letter Count" name
        Job job1 = Job.getInstance(conf, "Letter Count");
        
        // assign the jar executable to the job
        job1.setJarByClass(LetterFrequency.class);

        // Use this setting for In-Mapper Combining (disabling the Mapper and Combiner
        // below)
        job1.setMapperClass(LetterMapper1.class);

        /*
          // Use these settings for Mapper + Combiner (disabling the Mapper above)
          job1.setMapperClass(LetterMapper2.class);
          job1.setCombinerClass(LetterCombiner.class);
         */
        job1.setReducerClass(LetterReducer.class);

        // if we do not specify anything, the default number of reducer tasks will be 1
        // job1.setNumReduceTasks(1);
        
        // Mapper's output differ from Reducer's
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        // Combiner's output is the same as the Mapper's
        // Reducer's output (final output)
        // the relative frequency is of type "Double"
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        // if we do not specify anything, the default input and output Format Class will be Text
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // define I/O
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // we set a verbose execution with "true" inside waitForCompletion() 
        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }

}
