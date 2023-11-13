# Open the input file for reading
with open("output.txt", "r") as input_file:
    # Read all lines from the input file
    lines = input_file.readlines()

# Parse the lines into a list of (int, string) tuples
key_value_pairs = []
for line in lines:
    key, value = line.split(" ")
    key_value_pairs.append((int(key), value))

# Sort the list of tuples by the first element (the key)
key_value_pairs.sort(key=lambda tup: str(tup[0]))

# Open the output file for writing
with open("sorted_output.txt", "w") as output_file:
    # Write each tuple to a new line in the output file
    for key, value in key_value_pairs:
        output_file.write(f"{key} {value}")
