# RecSys Relational Queries with Provenance

> A Python relational operator library for push or pull-based pipeline and data parallel computing of recommender system queries with provenance tracking. Implemented as part of the User-Centric Systems for Data Science course at Boston University.

## Prerequisites for running queries:

1. Python (3.7+)
2. [Pytest](https://docs.pytest.org/en/stable/)
3. [Ray](https://ray.io)

## Input Data

Queries of assignments 1 and 2 expect two space-delimited text files (similar to CSV files). 

The first file (friends) must include records of the form:

|UID1 (int)|UID2 (int)|
|----|----|
|1   |2342|
|231 |3   |
|... |... |

The second file (ratings) must include records of the form:

|UID (int)|MID (int)|RATING (int)|
|---|---|------|
|1  |10 |4     |
|231|54 |2     |
|...|...|...   |

## Assignment 1 - Main branch

### Likeness Prediction Query 
> Predict how much user A will like movie M as the average ratings of A's friends.

Query:
``` 
SELECT AVG(R.Rating)
FROM Friends as F, Ratings as R
WHERE F.UID2 = R.UID
    AND F.UID1 = 'A'
    AND R.MID = 'M'
```

Running:
```bash
$ python assignment_12.py --query 1 --ff [path_to_friends_file.txt] --mf [path_to_ratings_file.txt] --uid [user_id] --mid [movie_id] --pull [0 Push-based / 1 Pull-based]
```

### Movie Recommendation Query 
> Recommend to user A the movie with the highest likeness value.

Query:
``` 
SELECT R.MID
FROM ( SELECT R.MID, AVG(R.Rating) as score
        FROM Friends as F, Ratings as R
        WHERE F.UID2 = R.UID
                AND F.UID1 = 'A'
        GROUP BY R.MID
        ORDER BY score DESC
        LIMIT 1 )
```

Running:
```bash
$ python assignment_12.py --query 2 --ff [path_to_friends_file.txt] --mf [path_to_ratings_file.txt] --uid [user_id] --pull [0 Push-based / 1 Pull-based]
```

### Explanation Query 
> Explain the movie recommendation from the above query with a Histogram of user A's friends ratings.

Query:
``` 
SELECT HIST(R.Rating) as explanation
FROM Friends as F, Ratings as R
WHERE F.UID2 = R.UID
        AND F.UID1 = 'A'
        AND R.MID = 'M'
```

Running:
```bash
$ python assignment_12.py --query 3 --ff [path_to_friends_file.txt] --mf [path_to_ratings_file.txt] --uid [user_id] --mid [movie_id] --pull [0 Push-based / 1 Pull-based]
```

### Testing All Push and Pull-based Operators

```bash
$ pytest tests.py
```


## Assignment 1 - Ray branch 
> Pipeline and Data Parallel Operators using Ray Actors. Only implemented Pull-Based Operators.

### Likeness Prediction Query 

Running:
```bash
$ python assignment_12.py --query 1 --ff [path_to_friends_file.txt] --mf [path_to_ratings_file.txt] --uid [user_id] --mid [movie_id]
```

### Movie Recommendation Query 

Running:
```bash
$ python assignment_12.py --query 2 --ff [path_to_friends_file.txt] --mf [path_to_ratings_file.txt] --uid [user_id]
```

### Testing Ray Operators

```bash
$ pytest tests.py
```

## Assignment 2

> Provenance tracking for individual tuples, such as Lineage, Where and How provenance.

### Backward Tracing
> The Lineage of an output tuple t for a query q(D) is the collection of input tuples that contributed to having the tuple t in the output of the query. 

Running:
```bash
$ python assignment_12.py --query 2 --ff [path_to_friends_file.txt] --mf [path_to_ratings_file.txt] --uid [user_id] --pull [0 Push-based / 1 Pull-based] --lineage [output tuple index]
```

### Where Provenance
> Where-provenance of a tuple attribute t[a] is the list of attributes of the input tuples whose values contributed to t[a]’s value.

Running:
```bash
$ python assignment_12.py --query 1 --ff [path_to_friends_file.txt] --mf [path_to_ratings_file.txt] --uid [user_id] --mid [movie_id] --pull [0 Push-based / 1 Pull-based] --where-row [output tuple index] --where-attribute [attribute index]
```

### How Provenance
> How-provenance returns the provenance metadata in the same form as: `AVG( (f1*r1@5), (f2*r2@8), (f3*r3@2) )` where `fi,ri` are unique identifiers for tuples in Friends and Ratings respectively, `fi*ri` denotes co-existence of tuples `fi` and `ri` in the input, and `ri@c` means that “the actual rating in tuple with id `ri` is `c`”

Running:
```bash
$ python assignment_12.py --query 2 --ff [path_to_friends_file.txt] --mf [path_to_ratings_file.txt] --uid [user_id] --pull [0 Push-based / 1 Pull-based] --how [output tuple index]
```

## Most responsible tuples
> Given a movie recommendation, returns the input tuples whose responsibility ρ for that recommendation is at least 50%.

Running:
```bash
$ python assignment_12.py --query 2 --ff [path_to_friends_file.txt] --mf [path_to_ratings_file.txt] --uid [user_id] --pull [0 Push-based / 1 Pull-based] --responsibility [output tuple index]
```
