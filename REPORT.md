# Hands-on L9: Report

**Name: Albert Bastakoti**
**Student ID: 801291837**
**Email: abastako@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`: `801291837`

The commands I ran, in order:

```bash
python datagen.py 801291837
docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output
```

## Results

### Task 1: hashtag trends (DataFrame API)

```text
hashtag,count
#design,1720
#ux,1189
#fail,1177
#social,1163
#cleanui,1142
#tech,1135
#update,1090
#bug,470
#love,282
#mood,249
```

The most frequent hashtag was #design. Before lower and trim, it had two distinct spellings: #design and #DESIGN.

### Task 2: engagement by age group (Spark SQL)

```text
age_group,posts,avg_likes,avg_retweets
Teen,1034,65.01,15.42
Adult,1274,58.25,14.1
Senior,2692,57.8,13.88
```

Teen users had the highest average likes and retweets. Senior users created the most posts but had the lowest average engagement.

### Task 3: sentiment versus engagement (Spark SQL)

```text
sentiment,posts,avg_likes,avg_retweets
Negative,1052,29.0,7.08
Neutral,2219,54.04,12.98
Positive,1729,84.78,20.26
```

The higher the sentiment the better the reactions the posts get. Positive posts averaged 84.78 likes and 20.26 retweets, compared with 29.00 likes and 7.08 retweets for negative posts.

### Task 4: top verified users (DataFrame API)

```text
username,total_likes,total_retweets,reach
@techie13,22227,5435,27662
@night_coder45,21284,5044,26328
@meme_lord23,20159,5444,25603
@designer25,19566,4292,23858
@rage_user32,18996,4252,23248
```

## Two languages, one plan

The SQL and DataFrame physical plans were the same except for expression and plan IDs:

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes DESC NULLS LAST, 200), ENSURE_REQUIREMENTS
      +- HashAggregate(keys=[age_group], functions=[count(1), avg(likes), avg(retweets)])
         +- Exchange hashpartitioning(age_group, 200), ENSURE_REQUIREMENTS
            +- HashAggregate(keys=[age_group], functions=[partial_count(1), partial_avg(likes), partial_avg(retweets)])
               +- Project [likes, retweets, age_group]
                  +- BroadcastHashJoin [user_id], [user_id], Inner, BuildRight
                     :- Filter isnotnull(user_id)
                     :  +- FileScan csv [user_id,likes,retweets] ... posts.csv
                     +- BroadcastExchange HashedRelationBroadcastMode(...)
                        +- Filter isnotnull(user_id)
                           +- FileScan csv [user_id,age_group] ... users.csv
```

DataFrame API plan:

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes DESC NULLS LAST, 200), ENSURE_REQUIREMENTS
      +- HashAggregate(keys=[age_group], functions=[count(1), avg(likes), avg(retweets)])
         +- Exchange hashpartitioning(age_group, 200), ENSURE_REQUIREMENTS
            +- HashAggregate(keys=[age_group], functions=[partial_count(1), partial_avg(likes), partial_avg(retweets)])
               +- Project [likes, retweets, age_group]
                  +- BroadcastHashJoin [user_id], [user_id], Inner, BuildRight
                     :- Filter isnotnull(user_id)
                     :  +- FileScan csv [user_id,likes,retweets] ... posts.csv
                     +- BroadcastExchange HashedRelationBroadcastMode(...)
                        +- Filter isnotnull(user_id)
                           +- FileScan csv [user_id,age_group] ... users.csv
```

Spark used an inner BroadcastHashJoin and broadcast the smaller users table. The Exchange before the aggregation repartitions rows by age_group. The Exchange before the sort range-partitions rows by descending avg_likes. SQL and the DataFrame API are both translated into the same Spark execution plan, so the choice can be based on readability and personal preference.

## Views

createOrReplaceTempView("posts") registered the DataFrame's logical plan under the temporary name `posts`; it did not copy or materialize the CSV data. The view exists only during the Spark session and disappears when the program ends. To keep a table for another session, the data could be written with saveAsTable or a SQL CREATE TABLE AS SELECT statement.

## Problems and fixes

The first local syntax check failed with IndentationError: unexpected indent (main.py, line 98). I removed the extra indentation from the Task 2 DataFrame return statement and reran python -m py_compile main.py successfully.

I also initially ran Docker Compose from the parent directory and received no configuration file provided: not found. Rerunning it from the repository directory fixed the issue. The final Spark job completed successfully with exit code 0.
