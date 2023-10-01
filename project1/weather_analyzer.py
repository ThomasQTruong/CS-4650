"""weather_analyzer.py
  
Contains a class for inputting/outputting specific weather data.
"""

import re
import json
from mrjob.job import MRJob


# Global constant.
QUALITY_RE = re.compile(r"[01459]")


class WeatherAnalyzer(MRJob):
  """
  Contains functions for inputting/outputting specific weather data.
  """

  def mapper(self, _, line):
    """
    Input: extracts wind direction, wind quality, temperature,
      and temperature quality.
    """
    # Clean data.
    val = line.strip()

    # Extract and map data.
    wind_direction = val[60:63]
    wind_quality = val[63]
    temperature = val[87:92]
    temperature_quality = val[92]

    # Sad path: do not keep unknown values/mismatched qualities.
    if (wind_direction == "999" or not re.match(QUALITY_RE, wind_quality)):
      return
    if(temperature == "+9999" or not re.match(QUALITY_RE, temperature_quality)):
      return

    # Yield the valid direction and dictionary of data.
    yield wind_direction, {
                            "wind_quality": wind_quality,
                            "temperature": temperature,
                            "temperature_quality": temperature_quality
                          }


  def reducer(self, key, values):
    """
    Output: shows the low/high temperature and count for each
      valid wind direction.
    """
    # Store all the temperature values for the key (wind direction).
    temperatures = []
    for data in values:
      temperatures.append(int(data["temperature"]))

    # Yields the key and a dictionary with the low, high, and count.
    yield int(key), {"low": min(temperatures), "high": max(temperatures),
                     "count": len(temperatures)}



if __name__ == "__main__":
  WeatherAnalyzer.run()
