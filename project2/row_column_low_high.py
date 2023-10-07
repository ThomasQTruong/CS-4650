# Step 0: create a new file (lower cased + underscored).
"""row_column_low_high.py

For every column, print high; for every row, print low.
"""

# Step 1: import MRJob.
from mrjob.job import MRJob


# Step 2: create a class that inherits from MRJob.
class RowColumnLowHigh(MRJob):
  """Extracts data from a file.
  
  Prints the row/column with the low/high respectively.
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

    # Return column/row with number.
    yield cleanedLine[0], cleanedLine[2]
    yield cleanedLine[1], cleanedLine[2]


  # Step 4: create reducer (output: data -> console/file/etc).
  def reducer(self, key, values):
    """Prints the row/column with the low/high respectively.
    
    For every column, print high.
    For every row, print low.
    """

    # Convert and check letter based on ASCII.
    asciiKey = ord(key)
    # A-J = column (max).
    if (asciiKey >= 65 and asciiKey <= 74):
      yield key, max(values)
    # K-Z = row (min).
    else:
      yield key, min(values)


# Step 5: set up main to run program.
if __name__ == "__main__":
  RowColumnLowHigh.run()
