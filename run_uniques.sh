#!/bin/bash

spark-submit --class com.wpcom.sparkuniques.SparkUniquesApp --master <spark-master> sparkuniquesapp-0.0.1-SNAPSHOT.jar <source dir> <use parquet>
