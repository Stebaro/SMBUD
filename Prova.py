# Import the basic spark library
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, collect_list, col, regexp_replace, size

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
# author_df.show()
# fos_df.show()
#publication_df.show()
# venue_df.show()
#rel_df.show()
creationalq = [False, False, False, False, True]
otherq = [False, False, False, False, False, False, False, False, False, False]

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
# if (otherq[0]):

# if (otherq[1]):

# if (otherq[2]):

# if (otherq[3]):

# if (otherq[4]):

# if (otherq[5]):

# if (otherq[6]):

# if (otherq[7]):

# if (otherq[8]):

# if (otherq[9]):
