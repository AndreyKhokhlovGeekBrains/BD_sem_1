import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor


def mapper(path):
    """
    The mapper function can process each JSON file and extract the 'movieIMDbRating' field.
    It should yield a tuple containing the score and the number 1 (to indicate the count).
    We return not only the score but also the square of the score for each file. This is done to facilitate the calculation of the variance in the reducer function.
    The calculation of the variance involves the sum of the squared scores, which is needed to compute the variance formula:
    Variance = (sum of (x^2) / n) - (mean)^2
    By returning the square of the score along with the score itself in the mapper function, we avoid the need to recalculate the square of the score in the reducer
    function. This makes the computation more efficient and allows us to directly use the squared score values to calculate the variance in the reducer.
    """
    with open(path, 'r') as f:
        info = json.load(f)
    score = float(info['movieIMDbRating'])
    yield (1, score, score**2)


def reducer(data):
    n, sum_score, sum_score_squared = 0, 0.0, 0.0
    for count, score, score_squared in data:
        n += count
        sum_score += score
        sum_score_squared += score_squared
    mean = sum_score / n
    variance = (sum_score_squared / n) - (mean ** 2)
    return (mean, variance ** 0.5)


# Get the list of JSON files
files = [str(path) for path in Path('imdb-user-reviews').glob('**/*.json')]

# Map phase
# This code block is using the ProcessPoolExecutor from the concurrent.futures module to parallelize the mapping process: ProcessPoolExecutor - this is a class
# that provides a way to create a pool of processes to execute calls asynchronously. Each process in the pool can handle a different task concurrently.

with ProcessPoolExecutor() as executor: # The with statement ensures that the resources used by the executor are properly managed and released after the block of code is executed
    mapped_data = executor.map(mapper, files) # executor.map() method is used to apply the mapper function to each element in the files list in parallel. It distributes the tasks across the available processes in the pool for concurrent execution

# Reduce phase
result = reducer(mapped_data)

print(result)
