package it.unipi.hadoop.nightswatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

// this code has two MapReduce Jobs 
// we implemented this version in addition to the single job code in order to run experiments with different numbers of reducer tasks
public class LetterFrequencyMR {

    public static class LetterMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Map<Character, Integer> letterCounts;

        // we use the setup() method to initialize a new HashMap 
        @Override
        protected void setup(Context context) {
            letterCounts = new HashMap<>();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // we make sure each letter in the line transformed to lower case
            String line = value.toString().toLowerCase();
            // for each character in the line, if it is a letter from the English alphabet, we insert it into the HashMap
            for (char c : line.toCharArray()) {
                if (Character.isLetter(c)) {
                    // if the key (so the letter) is already present inside of the HashMap, then we increment the existing related value
                    // otherwise we just do "0 + 1", which means we are inserting a new key (new letter)
                    letterCounts.put(c, letterCounts.getOrDefault(c, 0) + 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Character, Integer> entry : letterCounts.entrySet()) {
                // we emit the local aggregated key-value pairs
                context.write(new Text(entry.getKey().toString()), new IntWritable(entry.getValue()));
            }
        }
    }

    public static class LetterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // The reducer (used in job1) locally aggregates the output of each Mapper (which is <key, list of values>)

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // for each letter we compute the sum of the values in the list (the occurrence of the letter in each split)
            for (IntWritable val : values) {
                sum += val.get();
            }
            // we emit a pair consisting of the letter and its total occurrencies
            context.write(key, new IntWritable(sum));
        }
    }

    // no Mapper is specified for the second job, as we will simply use the Identity Mapper (default Mapper that outputs what it gets as input)
    // the following Reducer is used in job2
    public static class FrequencyReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private long totalLetters = 0;
        private Map<Text, Integer> letterCounts;
        // we use the setup() method to initialize a new HashMap

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            letterCounts = new HashMap<>();
        }
        // the reduce() method sums up the letter occurrence for each letter, in order to get the total number of letters inside of the input file
        // we will then compute the relative frequency for each letter inside of the cleanup() method

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            
            // we need a for loop to access Iterable values. Even though we know that it only contains one value
            // since it was already reduced in job1, it's still inside a list which needs to be accessed through a for loop
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            letterCounts.put(new Text(key), sum);
            totalLetters += sum;
        }

        // the cleanup() method computes the relative frequencies
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
        // we check if the number of arguments is correct
        // in this case we need 3 arguments, as in addition to input and output paths we have an intermediate path
        // this intermediate path is needed because we have 2 MapReduce jobs, and the first job will use this intermediate path as output path
        // while the second job will use the intermediate path as input path 
        if (args.length != 3) {
            System.err.println("Usage: LetterFrequencyAnalyzer <input path> <intermediate path> <output path>");
            System.exit(-1);
        }
        // we declare a configuration object
        Configuration conf = new Configuration();
        // we create a job object (job1), configuring it with the "Letter Count" name
        Job job1 = Job.getInstance(conf, "Letter Count");
        // assign the jar executable to the job
        job1.setJarByClass(LetterFrequencyMR.class);

        job1.setMapperClass(LetterMapper.class);

        job1.setReducerClass(LetterReducer.class);
        // we set the number of reducer tasks in order to run multiple experiments
        job1.setNumReduceTasks(2);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        // Mapper's output is the same as Reducer's , so we do not need to set Map OutputKeyClass() and OutputValueClass()

        // if we do not specify anything, the default input and output Format Class will be Text
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // define I/O
        // the output path of job1 is the intermediate path passed as the second argument  
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // if job1 doesn't complete correctly, we exit with code 1 (abnormal termination)
        // if, instead, job1 terminates correctly, we do not exit the program as we need to go on with job2
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // we create another job object (job2), configuring it with the "Letter Frequency" name
        Job job2 = Job.getInstance(conf, "Letter Frequency");
        // assign the jar executable to the job
        job2.setJarByClass(LetterFrequencyMR.class);

        // if we do not specify anything, the default input and output Format Class will be Text
        // the KeyValueTextInputFormat is used as the input of job2 comes from the previous job (job1) 
        // the output generated by job1 is a file that is structured as <key, value> 
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // We do not specify a Mapper for job2, and Hadoop will assign the default Mapper, which is the identity Mapper 
        job2.setReducerClass(FrequencyReducer.class);
        // in job2 we need to use only one reducer task to get the final results for the frequencies 
        job2.setNumReduceTasks(1); // we cannot set this in a different way

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        // Mapper's output differs from Reducer's, so we have to set the Mapper's output 
        // if we do not specify the different output, Hadoop would set an output matching the Reducer's one
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        // define I/O
        // the input path of job2 is the intermediate path, passed as second argument 
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // we set a verbose execution with "true" inside waitForCompletion() method 
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}