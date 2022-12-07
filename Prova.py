# Import the basic spark library
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, collect_list, col, regexp_replace, size, expr, count

# Create an entry point to the PySpark Application
spark = SparkSession.builder \
    .master("local") \
    .appName("MyFirstSparkApplication") \
    .getOrCreate()
# master contains the URL of your remote spark instance or 'local'

vectASIA = ["Department of Statistics, Rajshahi University, Rajshahi, Bangladesh",
            "Department of Computer Science & Engineering, BUET, Dhaka, Bangladesh",
            'Department of System Design and Informatics, Kyushu Institute of Technology, Fukuka, Japan',
            "Corporate Research and Development Center, Toshiba Corporation, Kawasaki, Japan",
            "Institute of Industrial Science, University of Tokyo, Meguro-Ku, Japan", "continent",
            "Department of Human and Artificial Intelligence System, Graduate School of Engineering, University of Fukui, Japan",
            "Department of Computer Science, Faculty of Engineering, Yamanashi University, Kofu, Japan",
            "Nippon Hoso Kyokai", "Shinshu University", 'Nagano Prefectural College']

vectEU = ["Università della Calabria", "Università degli Studi di Modena e Reggio Emilia", "Politecnico di Milano",
          'Università degli Studi di Firenze'
          "University of Milano-Bicocca",
          "Cognitive Interaction Technology, Center of Excellence and Applied Informatics, Faculty of Technology, Bielefeld University, Bielefeld, Germany#TAB#",
          "Department of Information Systems and Business Administration, Johannes Gutenberg-Universität Mainz, Jakob-Welder-Weg 9, Mainz 55128, Germany",
          "Dept. of Computer Science, Uni. Politècnica de Catalunya, Spain#TAB#", "university of malaga",
          "Computer Laboratory, University Of Cambridge", "New England Aquarium",
          "Unconventional Computing Group, University of the West of England, Bristol, UK BS16 1QY#TAB#",
          "ESIEE-Paris", "UNIVERSITÉ LAVAL", "University of Southern Denmark", "University of Lisbon",
          "#N#University of Glasgow#N#"]

vectAMERICA = ['The University of Chicago Press, Chicago, USA']

author_df = spark.read.json("C:\\Users\\bsbar\\PycharmProjects\\pythonProject\\authors.json")
fos_df = spark.read.json("C:\\Users\\bsbar\\PycharmProjects\\pythonProject\\fos.json")
publication_df = spark.read.json("C:\\Users\\bsbar\\PycharmProjects\\pythonProject\\publications.json")
venue_df = spark.read.json("C:\\Users\\bsbar\\PycharmProjects\\pythonProject\\venues.json")
rel_df = spark.read.json("C:\\Users\\bsbar\\PycharmProjects\\pythonProject\\rel_dw.json")

print("Tabella autori ")
author_df.show()

print("Tabella fos")
#fos_df.show()

print("Tabella publication")
#publication_df.show()

print("Tabella venue")
#venue_df.show()

print("Tabella rel")
#rel_df.show()
creationalq = [False, False, False, False, False]
otherq = [False, False, False, False, False, True, False, False, False, False]

# rdd=spark.sparkContext.parallelize(author_df)
# "print(rdd.collect())


# CREATIONAL/UPDATE QUERIES:

if creationalq[0]:
    # 1 query creation of a row
    columns = ["date", "id", "number", "raw", "type", "volume"]
    newRow = spark.createDataFrame([("2022-12-05", 9999999, 99, "New venue raw", "C", 3)], columns)
    venue_df = venue_df.union(newRow)
    venue_df.show(truncate=False)

if creationalq[1]:
    # 2 Creation of the new column that counts the number of references
    publication_df.withColumn("references number", size(col("references"))).show(truncate=False)

if creationalq[2]:
    # 3 Creation of a new column that represents the region of the university
    author_df = author_df.withColumn('Continent',
                                     when(author_df.affiliation.isin(vectASIA), lit("Asia"))
                                     .when(author_df.affiliation.isin(vectAMERICA), lit("America"))
                                     .when(author_df.affiliation.isin(vectEU), lit("Europa"))
                                     .otherwise(lit("Rest of the world"))
                                     ).show(truncate=False)

if creationalq[3]:
    # 4 Update of fos names
    fos_df = fos_df.withColumn('name',
                               when(fos_df.name.contains("mathematics"),
                                    regexp_replace(fos_df.name, 'Discrete mathematics', 'Discrete Math'))
                               .when(fos_df.name.contains("Mathematics"),
                                     regexp_replace(fos_df.name, 'Mathematics', 'Math'))
                               .when(fos_df.name.contains("Artificial intelligence"),
                                     regexp_replace(fos_df.name, 'Artificial intelligence', 'AI'))
                               .otherwise(fos_df.name)).show()

if creationalq[4]:
    # 5 Delete every publication whose type is Conference
    publication_df = publication_df.where(publication_df.type != "Conference").show()


# OTHER QUERIES:
#WHERE, JOIN
if (otherq[0]):
    fos_df.filter(col("name") == "Artificial intelligence") \
    .join(rel_df, fos_df.id == rel_df.fos_id, "inner") \
    .join(publication_df, rel_df.pub_id == publication_df.id, "inner") \
    .select("title").show(truncate=False)



#WHERE, LIMIT, LIKE
if (otherq[1]):
    author_df.withColumnRenamed("id", "authorId").filter(col("affiliation").like("%Politecnico%")).limit(5).join(
        publication_df, expr(
            "array_contains(authors, authorId)")).select(col("title").alias("publicationTitle"),
                                                         col("name").alias("authorName"), "affiliation").show(truncate=False)

    author_df.withColumnRenamed("id", "authorId").filter(
        col("affiliation").like("%Politecnico%")).limit(5)
    #publication_df.select(count(.name)).show()



#WHERE, IN Nested Query
# Return the number of publications that have at least one fos whose name contains "Computer"
if (otherq[2]):
    computerScience_fos = fos_df.filter(col("name").contains("Computer")).select(col("id")).collect()
    computerScience_fos = [csf[0] for csf in computerScience_fos]

    count_cs_publications = rel_df.filter(col("fos_id").isin(computerScience_fos)).select(col("pub_id")).distinct().count()
    print("percentage of publications that have fos with Computer:" + str(count_cs_publications/2500))

#GROUP BY, 1 JOIN, AS
#The query counts for each name of the venue (that has different ID since it can have different editions or volumes)
# the number of papers that were presented there
if (otherq[3]):
    venue_df.join(publication_df, venue_df.id == publication_df.venue, "inner")\
        .groupby("raw")\
            .count()\
                .select(venue_df.raw, col("count").alias("Number of papers for every raw")).show()

#WHERE, GROUP BY
#It filters the papers that have at least 3 authors, then it shows for every publisher
# the max number of pages between the papers he published, shown in descending order

if (otherq[4]):
    publication_df.filter(size('authors') >= 3)\
        .groupBy('publisher').max('pages')\
            .select(publication_df.publisher, col("max(pages)").alias("Maxpages"))\
                .orderBy(col("Maxpages").desc()).show(50)

#GROUP BY, HAVING, AS

#The query groups the venues by affiliation, for each of them it counts the number of authors by id and collects
#the names of the authors in a list, then it filters (HAVING) the affiliations by the number of authors in the list
#that should be between 5 and 15. We order the table by descending order for the number of authors, and in case of tie
#they show the affiliation name in alphabetic order

if (otherq[5]):
    author_df.groupBy("affiliation")\
        .agg(
            countDistinct("id").alias("Number of Authors"),
            collect_list(author_df.name).alias("Authors list")
            )\
                .filter((col("Number of Authors") < 15) & (col("Number of Authors") > 5))\
                    .orderBy(col("Number of Authors").desc(), col("affiliation").asc())\
                        .show(truncate=False)


#WHERE, GROUP BY, HAVING, AS
# if (otherq[6]):

#WHERE, Nested Query(i.e., 2-step Queries), GROUP BY
# if (otherq[7]):

#WHERE, GROUP BY, HAVING, 1 JOIN
# if (otherq[8]):

#WHERE, GROUP BY, HAVING, 2 JOINs
# if (otherq[9]):

