# SparkEnron


	// Enter the Spark REPL
	/opt/spark/bin/spark-shell 

	// pull data from HDFS
	val seq20 = sc.wholeTextFiles("hdfs://localhost:54310/user/hduser/in/enron20.seq") 
	seq20.show()  
	
	val fullPositions = spark.read.format("csv").option("header","true").option("inferSchema","true").load("hdfs://localhost:54310/user/hduser/in/full-positions.csv")

	fullPositions.select("Id","Email1")
		
	// WholeTextFiles returns a key value RDD 
	// The key is the file path. The value is the contents of enron20.seq 
	// We only need the value 
	// And seq20 contains only one tuple. 
	val seq1 = seq20.first  

	//Everything is on one line. We split onto several lines.
	val seq1_lines = seq1.split("\n\n") 

	//We only need header info. Filter out message body.
	Val seq1_header = seq1_lines.filter(line = line.contains("From:")) 

	//Its more concise to write:
	val seq20 = sc.wholeTextFiles("hdfs://localhost:54310/user/hduser/in/enron20.seq").first._2.split("\n\n").filter(line => line.contains("Date:")) 

	//Let's just work with only one tuple for now  
	val rgx = seq20(0) 

	// These prefixes denote recipients.
	var recipPrefix = Seq("To:","Cc:","Bcc:") 
	 
	 
	// this comprehension is an anonymous function that returns a Seq of recipients. It's pretty good.
	val recipList = (
		for {
		prefix <- recipPrefix
		x <- rgx. // this is a flatmap
			replace("\n\t","").
			split("\n").
			filter(line => line.startsWith(prefix)).
			collect { case s if (s.contains(":")) =>
				s.substring(prefix.length + 1)
			}
		y <- x.split(", ") //another flatmap
		} yield 
		{
			y
		}// comprehension "body"	
	).toSet

	val recipList = for (prefix <- recipPrefix) yield {
			rgx.
			replace("\n\t","").
			split("\n").
			mkString
		
		}     		
		
	// but we still need to split the above resukt.
	val recipientSet = recipList.flatMap(_.split(", ")).toSet

	
	// instead of a comprehension we could use map, map, and flatMap	
	val recipientSet = recipPrefix.map(e => rgx.replace("\n\t","").split("\n").filter(line => line.startsWith(e)).mkString).filter(_.nonEmpty).map(_.split(":")(1).trim()).flatMap(_.split(", ")).toSet
		// but how is this readable?
	
	// Parse sender
	val sender = rgx.replace("\n\t","").split("\n").filter(line => line.startsWith("From:"))(0).substring(6).trim()

	//Parse Date
	val date = rgx.replace("\n\t","").split("\n").filter(line => line.startsWith("Date:"))(0).substring(6).trim()
	
	
	
	// we don't need dataframes, do we?
	// create dataframes
	
	val recipientDF = sc.parallelize(recipientSet.toSeq).toDF
	val senderDF = sc.parallelize(List(sender)).toDF
	val dateDF = sc.parallelize(List(date)).toDF
	
	val emitDF = dateDF.crossJoin(senderDF).crossJoin(recipientDF)
	
	
	def timesliceMapper (rgx:String) : org.apache.spark.sql.DataFrame = {
			var recipPrefix = Seq("To:","Cc:","Bcc:")
			val recipientSet = recipPrefix.map(e => rgx.replace("\n\t","").split("\n").filter(line => line.startsWith(e)).mkString).filter(_.nonEmpty).map(_.split(":")(1).trim()).flatMap(_.split(", ")).toSet
			
			// Parse sender
			val sender = rgx.replace("\n\t","").split("\n").filter(line => line.startsWith("From:"))(0).substring(6).trim()

			//Parse Date
			val date = rgx.replace("\n\t","").split("\n").filter(line => line.startsWith("Date:"))(0).substring(6).trim()
			
			// create dataframes
			
			val recipientDF = sc.parallelize(recipientSet.toSeq).toDF
			val senderDF = sc.parallelize(List(sender)).toDF
			val dateDF = sc.parallelize(List(date)).toDF
			
			val emitDF = dateDF.crossJoin(senderDF).crossJoin(recipientDF)
			
			return emitDF		
	}
	
	val schema = emitDF.schema
	
	var emptyDF = spark.createDataFrame(sc.emptyRDD[org.apache.spark.sql.Row],schema)
	
	for (line <- seq20){
		emptyDF = emptyDF.union(timesliceMapper(line))	
	}
	
	
