val inputDf = spark.read.format("json").load("/opt/bundle-validation/data/utilities/data")
hudiDf.registerTempTable("hudi_tbl")
inputDf.registerTempTable("src_tbl")
val hudiCount = spark.sql("select distinct date, key from hudi_tbl").count()
val srcCount = spark.sql("select distinct date, key from src_tbl").count()
println(s"::debug::hudiCount $hudiCount")
println(s"::debug::srcCount $srcCount")
if (hudiCount == srcCount) System.exit(0)
println(s"Counts don't match hudiCount: $hudiCount, srcCount: $srcCount")
System.exit(1)
