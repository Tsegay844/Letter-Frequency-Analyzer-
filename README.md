# Letter Frequency Analyzer

This repository contains implementations of a Letter Frequency Analyzer using Hadoop MapReduce and Spark.

## Repository Structure

- `letter_frequency in Sparck.py`: Implementation of the Letter Frequency Analyzer using PySpark.
- `LetterFrequency in Hadoop.java`: Implementation of the Letter Frequency Analyzer using Hadoop with a single MapReduce job.
- `LetterFrequency2 in Hadoop.java`: Implementation of the Letter Frequency Analyzer using Hadoop with two MapReduce jobs for experimentation.
- `single_machine.py`: This implementation is used to compare the performance of the distributed implementations with a single machine implementation.

Please refer to the [Documentation.pdf](Documentation.pdf) file for more information about the implementations and the results of the experiments.

The cluster used for the experiments consisted of 3 nodes, on which Hadoop and Spark were installed.