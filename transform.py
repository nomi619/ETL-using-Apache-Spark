import urllib.request
import gzip
from pyspark.sql import SparkSession
import pyspark.sql.column
import findspark
findspark.init()

spark=SparkSession.builder.getOrCreate()

title_ratings=spark.read.csv("title.ratings.tsv.gz",sep='\t',header=True)
name_basics=spark.read.csv("name.basics.tsv.gz",sep='\t',header=True)
title_ratings.createOrReplaceTempView("title_ratings")

name_basics=name_basics.repartition(20)
title_ratings=title_ratings.repartition(20)

title_ratings=spark.sql("Select * from title_ratings where numVotes>50000")
title_ratings.createOrReplaceTempView("title_ratings")

name_basics=name_basics.selectExpr("nconst","primaryName","birthYear","deathYear","PrimaryProfession",\
                                        'substring(knownForTitles, 1,9) as knownForTitle1',
                                        'substring(knownForTitles, 11,9) as knownForTitle2',\
                                        'substring(knownForTitles, 21,9) as knownForTitle3',\
                                        'substring(knownForTitles, 31,9) as knownForTitle4')
name_basics.createOrReplaceTempView("name_basics")
name_basics=spark.sql("Select * from name_basics where deathYear Like '%N%'")
name_basics.createOrReplaceTempView("name_basics")
name_basics=spark.sql("Select * from name_basics where birthYear>1970")
name_basics.createOrReplaceTempView("name_basics")

reduced_table1=spark.sql("select nconst,primaryName,birthYear,deathYear,PrimaryProfession,rt.averageRating,rt.numVotes\
    from name_basics nm \
     inner join title_ratings rt on nm.knownForTitle1=rt.tconst")
reduced_table1.createOrReplaceTempView("reduced_table1")

reduced_table2=spark.sql("select nconst,primaryName,birthYear,deathYear,PrimaryProfession,rt.averageRating,rt.numVotes\
    from name_basics nm \
     inner join title_ratings rt on nm.knownForTitle2=rt.tconst where nm.knownForTitle2 is not Null")
reduced_table2.createOrReplaceTempView("reduced_table2")

reduced_table3=spark.sql("select nconst,primaryName,birthYear,deathYear,PrimaryProfession,rt.averageRating,rt.numVotes\
    from name_basics nm \
     inner join title_ratings rt on nm.knownForTitle3=rt.tconst where nm.knownForTitle3 is not Null")
reduced_table3.createOrReplaceTempView("reduced_table3")

reduced_table4=spark.sql("select nconst,primaryName,birthYear,deathYear,PrimaryProfession,rt.averageRating,rt.numVotes\
    from name_basics nm \
     inner join title_ratings rt on nm.knownForTitle4=rt.tconst where nm.knownForTitle4 is not Null")
reduced_table4.createOrReplaceTempView("reduced_table4")

reduced_table=reduced_table4.union(reduced_table3)
reduced_table=reduced_table.union(reduced_table1)
reduced_table=reduced_table.union(reduced_table2)
reduced_table.createOrReplaceTempView("reduced_table")

reduced_table=spark.sql("select * from reduced_table where nconst in\
    (select nconst from reduced_table group by nconst having count(nconst)=4)")
reduced_table.createOrReplaceTempView("reduced_table")

reduced_table=spark.sql("select primaryName,avg(averageRating),sum(numVotes) from reduced_table group by primaryName\
     order by sum(numVotes) desc")
reduced_table.createOrReplaceTempView("reduced_table")