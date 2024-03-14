## intsurfing_scala_notebook_test
# Task
To complete this test you need to:
- Setup single node Hadoop cluster
- Setup Spark 3.5
- Setup Jupyter Notebooks
- Use file people.csv as data source.

The task is to create a notebook using Scala which:
- Reads people.csv
- Counts all people with the same last name by every state from people.csv file and displays the result as a percentage for every state.
- Saves results to different files for each state (for example, peopleByLastnameFromFlorida.csv, peopleByLastnameFromCalifornia.csv)
- Give us your notebook’s code   
- It will be a plus if code produces results as a json files.

# Solution

I'll be using [Almond Scala kernel for Jupyter](https://almond.sh), using `/home/jovyan/work` inner working dir
```shell
mkdir ~/scala/almond
cp people.csv ~/scala/almond
docker run -it --rm -p 8888:8888 -p 8889:8889 -v ~/scala/almond:/home/jovyan/work almondsh/almond:latest
```

`people.csv` example:
```csv
firstname,lastname,birthday,state
Joe,Watson,19631001,California
Camila,Watson,19901105,California
Jeleesa,Johnson,19890404,California
Jimmie,Turner,19900220,California
Courtney,Walker,19900831,California
Erica,Batres,19950716,California
Stephanie,Walker,19881101,California
Alexis,Turner,19950923,California
Kaitlin,Turner,19901203,California
Ray,Turner,19911016,California
Danielle,Harrison,19801211,Florida
Moronnie,Jones,19770401,Florida
Sheletha,Myers,19771210,Florida
Alecia,Harrison,19851006,Florida
Nan,Ratchford,19790318,Florida
Alan,Harrison,19790601,Florida
Ellis,Baird,19780401,Florida
Michael,Baker,19851029,Florida
Sara,Baker,19840918,Florida
Betty,Foster,19451001,Florida
```


`intsurfing test jupyter notebook` code:


```scala
import $ivy.`org.apache.spark::spark-sql:2.4.0`
import $ivy.`sh.almond::almond-spark:0.14.0-RC8`

import org.apache.log4j.{Level, Logger}
Logger.getLogger("org").setLevel(Level.OFF)

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.nio.file.{DirectoryStream, Files, Path, Paths, FileSystems}
import scala.collection.JavaConverters._

//saving csv by spark (parts in temp dir moved to result file)
def saveDfToCsv(df: DataFrame, csvOutput: String, sep: String = ",", header: Boolean = false): Unit = {
    val tmpOutDir = "/home/jovyan/work/tmp.csv"
  
    //leaving single part
    df.repartition(1).write
        .format("com.databricks.spark.csv")
        .option("header", header.toString)
        .option("delimiter", sep)
        .save(tmpOutDir)
  
    val dir = new File(tmpOutDir)
    val tmpPartName = Files.newDirectoryStream(FileSystems.getDefault().getPath(tmpOutDir))
                        .asScala
                        .filter(_.getFileName.toString.startsWith("part-00000"))
                        .map(_.toAbsolutePath).head.toString // headOption is skipped for simplicity
    
    (new File(tmpPartName)).renameTo(new File(csvOutput))
  
    dir.listFiles.foreach( f => f.delete )
    dir.delete
}

//saving json by jdk8
def saveDfToJson(df: DataFrame, jsonOutput: String): Unit = {
    //string representation including json array
    val repr = df.toJSON.collect.mkString("[\n", ",\n" , "\n]" )

    Files.write(Paths.get(new File(jsonOutput).toURI()), repr.getBytes());
}

//-=================================================================-\\
//spark infra
val spark = {
    NotebookSparkSession.builder()
    .master("local[*]")
    .getOrCreate()
}
val sc = spark.sparkContext


// Reads people.csv
val df = spark
          .read
          .option("header", "true")
          .csv("people.csv")
//df.show()

// Counts all people with the same last name by every state from people.csv file
val countDF = df.groupBy("state", "lastname").count().withColumnRenamed("count", "lastnames_count")
//countDF.show()

// adds counts by state
val countByState = countDF.groupBy("state").agg(sum("lastnames_count").alias("total_by_state"))
val withCountDF = countDF.join(countByState, "state")
//withCountDF.show()

// and displays the result as a percentage for every state
val percentDF = withCountDF.withColumn("percentage", (col("lastnames_count") / col("total_by_state")) * 100)
//percentDF.show()

// 3) Save results to different files for each state
val states = percentDF.select("state").distinct().collect()

states.foreach { state_elm =>
    val state = state_elm.getString(0)
    val stateDF = percentDF.filter(col("state") === state)
//    println(state)
//    stateDF.show()
    saveDfToCsv(stateDF, s"/home/jovyan/work/peopleByLastnameFrom${state}.csv", header = true)
    saveDfToJson(stateDF, s"/home/jovyan/work/peopleByLastnameFrom${state}.json")
}
```

## Results
```shell
➜ cat peopleByLastnameFromCalifornia.csv
state,lastname,lastnames_count,total_by_state,percentage
California,Watson,2,10,20.0
California,Walker,2,10,20.0
California,Batres,1,10,10.0
California,Johnson,1,10,10.0
California,Turner,4,10,40.0


➜ cat peopleByLastnameFromCalifornia.json
[
{"state":"California","lastname":"Watson","lastnames_count":2,"total_by_state":10,"percentage":20.0},
{"state":"California","lastname":"Walker","lastnames_count":2,"total_by_state":10,"percentage":20.0},
{"state":"California","lastname":"Batres","lastnames_count":1,"total_by_state":10,"percentage":10.0},
{"state":"California","lastname":"Johnson","lastnames_count":1,"total_by_state":10,"percentage":10.0},
{"state":"California","lastname":"Turner","lastnames_count":4,"total_by_state":10,"percentage":40.0}
]

➜ cat peopleByLastnameFromFlorida.csv
state,lastname,lastnames_count,total_by_state,percentage
Florida,Baker,2,10,20.0
Florida,Myers,1,10,10.0
Florida,Baird,1,10,10.0
Florida,Jones,1,10,10.0
Florida,Ratchford,1,10,10.0
Florida,Foster,1,10,10.0
Florida,Harrison,3,10,30.0


➜ cat peopleByLastnameFromFlorida.json
[
{"state":"Florida","lastname":"Baker","lastnames_count":2,"total_by_state":10,"percentage":20.0},
{"state":"Florida","lastname":"Myers","lastnames_count":1,"total_by_state":10,"percentage":10.0},
{"state":"Florida","lastname":"Baird","lastnames_count":1,"total_by_state":10,"percentage":10.0},
{"state":"Florida","lastname":"Jones","lastnames_count":1,"total_by_state":10,"percentage":10.0},
{"state":"Florida","lastname":"Ratchford","lastnames_count":1,"total_by_state":10,"percentage":10.0},
{"state":"Florida","lastname":"Foster","lastnames_count":1,"total_by_state":10,"percentage":10.0},
{"state":"Florida","lastname":"Harrison","lastnames_count":3,"total_by_state":10,"percentage":30.0}
]
```

P.S. I'm more used to using separate Scala project utilizing embedded Spark engine. However, using Notebook is fine as well.
P.P.S. I am also going to implement this test assignment in [Rust](https://www.rust-lang.org) using [Apache Arrow Datafusion](https://arrow.apache.org/datafusion/)
