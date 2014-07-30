#!/bin/bash

spark-submit --class com.wpcom.sparkuniques.SparkUniquesApp --master <spark-master> sparkuniquesapp-0.0.1-SNAPSHOT.jar <input tab-delimited files> <output file>
