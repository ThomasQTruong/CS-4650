# Step 0: create a new file (lower cased + underscored).
"""row_column_min_max.py

For every column, print max and the row(s) that have the max.
For every row, print the min and the column(s) that have the min.
"""

# Step 1: import MRJob.
from mrjob.job import MRJob


# Step 2: create a class that inherits from MRJob.
class ColumnMinMax(MRJob):
  """Extracts data from a file.

  Prints the column with the largest value,
    and the row(s) that have the maximum value.
  Prints the row with the smallest value,
    and the column(s) that have the minimum value.
  """

  # Step 3: create mapper (input: file -> data).
  def mapper(self, _, line):
    """Extracts data from a line and maps it."""

    # Clean up the data.
    cleanedLine = line.replace(" ", "")      # Remove whitespaces.
    cleanedLine = cleanedLine.split(",")     # Split into array.
    cleanedLine[2] = int(cleanedLine[2])     # Make int.

    # Check for valid data.
    if (not cleanedLine[0].isalpha()):
      return
    if (not cleanedLine[1].isalpha()):
      return
    if (cleanedLine[2] < 0 or cleanedLine[2] > 999):
      return

    # Cleaning part 2: make uppercase if needed.
    cleanedLine[0] = cleanedLine[0].upper()
    cleanedLine[1] = cleanedLine[1].upper()

    # Check part 2: A-T only.
    if (ord(cleanedLine[1]) > 84):
      return

    # Return (column, row) with number.
    yield cleanedLine[0], [cleanedLine[1], cleanedLine[2]]
    yield cleanedLine[1], [cleanedLine[0], cleanedLine[2]]


  # Step 4: create reducer (output: data -> console/file/etc).
  def reducer(self, key, values):
    """Outputs a formatted result.
    
    Prints the column with the largest value,
      and the row(s) that has the maximum value.
    Prints the row with the smallest value,
      and the column(s) that have the minimum value.
    """

    # Converts key to ASCII.
    asciiKey = ord(key)
    # Indicates whether key is a column or a row.
    isKeyColumn = (asciiKey >= 65 and asciiKey <= 74)

    # Tracker variables.
    current_iterator = next(values)
    current_value = current_iterator[1]
    value_list = [current_iterator[0]]

    # For every value in values.
    for v in values:
      # Equal, append to value_list.
      if v[1] == current_value:
        value_list.append(v[0])
      # Is a column, check for max.
      elif (isKeyColumn):
        # v[1] is new max.
        if v[1] > current_value:
          current_value = v[1]
          value_list = [v[0]]  # Reset list to the new max only.
      # Is a row, check for min.
      else:
        # v[1] is new min.
        if v[1] < current_value:
          current_value = v[1]
          value_list = [v[0]]  # Reset list to the new min only.
    
    # Print based on column/row.
    if (isKeyColumn):
      yield key, {"value": current_value, "row(s)": value_list}
    else:
      yield key, {"value": current_value, "col(s)": value_list}


# Step 5: set up main to run program.
if __name__ == "__main__":
  ColumnMinMax.run()
