# Importing the necessary libraries
import collections
import string
import pandas as pd
import time


start_time = time.time()


# Function to generate a dictionary with letter frequencies
def total_tokens(text):
    # Filtering only alphabetic characters and convert to lowercase
    text = ''.join(filter(str.isalpha, text)).lower()
    # Tokenize into individual characters
    tokens = list(text)
    # Create a counter for the tokens
    return collections.Counter(tokens), len(tokens)


# Function to create a dataframe with the relative frequency of letters
def make_df(counter, total_count):
    # Filtering the counter to include only the 26 letters of the English alphabet
    alphabet = string.ascii_lowercase
    filtered_counter = {letter: counter[letter] for letter in alphabet}

    # Create a dataframe with the absolute and relative frequencies
    df = pd.DataFrame(list(filtered_counter.items()), columns=["Letters", "Absolute Frequency"])
    df["Relative Frequency"] = df["Absolute Frequency"] / total_count
    df.set_index("Letters", inplace=True)

    return df


# Input file
input_file = "shakespeare_complete.txt"
# Output file
output_file = "shake_comp_output.txt"

# If the output file already exists, clear it using option "w" when opening it
open(output_file, 'w').close()

# Read the input text file
with open(input_file, "r", encoding="ascii", errors='ignore') as file:
    text = file.read()

# Tokenize and count letter frequencies
counter, total_letter_count = total_tokens(text)

# Get the dataframe of letter frequencies
df = make_df(counter, total_letter_count)

# Save the dataframe to the output file
df.to_csv(output_file)

end_time = time.time()

exec_time = end_time - start_time

print(f"Execution time: {exec_time:.2f} seconds")
