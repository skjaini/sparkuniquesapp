### Spark Uniques App

#### Build
Download the repo and run: mvn package

#### Run
Use spark-submit and pass the directory containing raw data files:

    spark-submit 
      --class com.wpcom.sparkuniques.SparkUniquesApp 
      --master <spark-master> sparkuniquesapp-0.0.1-SNAPSHOT.jar 
      <source dir>    // root directory of raw data files eg: /data/2014/07/01
      <use parquet>   // true|false to use parquet format or not
