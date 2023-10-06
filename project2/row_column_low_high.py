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

    # Get rid of whitespace, split into array by comma, and int cast 3rd item.
    cleanedLine = line.replace(" ", "")
    cleanedLine = cleanedLine.split(",")
    cleanedLine[2] = int(cleanedLine[2])

    # Check for valid data.
    if (not cleanedLine[0].isalpha()):
      return
    if (not cleanedLine[1].isalpha()):
      return
    if (cleanedLine[2] < 0 or cleanedLine[2] > 999):
      return

    yield (cleanedLine[0], cleanedLine[1]), cleanedLine[2]


  # Step 4: create reducer (output: data -> console/file/etc).
  def reducer(self, key, values):
    """Prints the row/column with the low/high respectively.
    
    For every column, print high.
    For every row, print low.
    """
    for data in values:
      yield key, data 


# Step 5: set up main to run program.
if __name__ == "__main__":
  RowColumnLowHigh.run()
