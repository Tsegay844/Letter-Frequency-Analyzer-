# Import necessary modules from PySpark
from pyspark import SparkContext, SparkConf
# Import the regular expression module to deal with characters
import re 
# importing the defaultdict, that acts similarly to the HashMap in Java
from collections import defaultdict

# in this code we wanted to implement (in Spark) the same logic we used in our Java code in Hadoop
# to do this we divided the code into phases: Map, Reduce and Clean-Up

# Define the main function that will process the input files and calculate letter frequencies
def main(input_files, final_output):
    # Initialize a SparkContext with a given application name
    sc = SparkContext(appName="LetterFrequency")

    # Read the input text files into an RDD (Resilient Distributed Dataset)
    # If we have more input files (a whole directory as input path) then the files are concatenated together
    text_rdd = sc.textFile(",".join(input_files))

    # Map Phase with In-Mapper Combining: Extract and count letters in each partition
    def map_with_in_mapper_combining(partition):
        # Initialize a dictionary to count letters within each partition
        letter_counts = defaultdict(int)
        # Iterate over each line in the partition
        for line in partition:
            # Convert letters to lowercase and count them 
            # we only count the 26 letters of the English alphabet (no special characters or numbers) in the line
            letters = re.findall(r'[a-z]', line.lower())
            # Increment the count for each letter in the dictionary
            for letter in letters:
                letter_counts[letter] += 1
        # Collect the letter counts as (letter, count) tuples in a list
        # to return a list of pairs <key, value> is a good way to stick to the MapReduce paradigm 
        results = [(letter, count) for letter, count in letter_counts.items()]
        return results

    # Apply the in-mapper combining map function to each partition
    letter_tuples = text_rdd.mapPartitions(map_with_in_mapper_combining)

    # Reduce Phase: Count the occurrences of each letter across partitions
    # Reduce by key to aggregate the counts of each letter
    letter_counts = letter_tuples.reduceByKey(lambda a, b: a + b)

    # "Clean-Up" Phase: Calculate total number of letters
    # Sum up all the letter counts
    total_letters = letter_counts.map(lambda x: x[1]).sum()

    # "Clean-Up" Phase: Calculate letter frequencies
    # Map each letter and its count to a tuple containing the letter, the absolute frequency and the relative frequency
    letter_frequencies = letter_counts.map(lambda x: (x[0], (x[1], x[1] / total_letters)))

    # "Clean-Up" Phase: Save the final letter frequencies to the specified output path
    letter_frequencies.map(lambda x: f"{x[0]}\t{x[1][0]}\t{x[1][1]}").saveAsTextFile(final_output)

    # Stop the SparkContext to free up resources
    sc.stop()


# Check if the script is run as the main program
if __name__ == "__main__":
    import sys  # Import the sys module to access command-line arguments

    # Ensure there are enough command-line arguments (at least 3)
    if len(sys.argv) < 3:
        print("Usage: letter_frequency.py <input_files> <final_output>")  # Print usage message if arguments are insufficient
        sys.exit(-1)  # Exit the script with an error code

    # Get the input file paths from command-line arguments (all except the last one)
    input_files = sys.argv[1:-1]

    # Get the output path from the last command-line argument
    final_output = sys.argv[-1]

    # Call the main function with the provided input files and output path
    main(input_files, final_output)
