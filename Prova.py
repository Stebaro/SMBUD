# Import the basic spark library
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, collect_set, collect_list, col

# Create an entry point to the PySpark Application
spark = SparkSession.builder \
      .master("local") \
      .appName("MyFirstSparkApplication") \
      .getOrCreate()
# master contains the URL of your remote spark instance or 'local'

vectASIA= ["Department of Statistics, Rajshahi University, Rajshahi, Bangladesh",
             "Department of Computer Science & Engineering, BUET, Dhaka, Bangladesh",
             'Department of System Design and Informatics, Kyushu Institute of Technology, Fukuka, Japan',
             "Corporate Research and Development Center, Toshiba Corporation, Kawasaki, Japan",
             "Institute of Industrial Science, University of Tokyo, Meguro-Ku, Japan", "continent",
             "Department of Human and Artificial Intelligence System, Graduate School of Engineering, University of Fukui, Japan",
             "Department of Computer Science, Faculty of Engineering, Yamanashi University, Kofu, Japan", "Nippon Hoso Kyokai","Shinshu University",'Nagano Prefectural College' ]

vectEU = ["Università della Calabria","Università degli Studi di Modena e Reggio Emilia","Politecnico di Milano", 'Università degli Studi di Firenze'
          "University of Milano-Bicocca", "Cognitive Interaction Technology, Center of Excellence and Applied Informatics, Faculty of Technology, Bielefeld University, Bielefeld, Germany#TAB#", "Department of Information Systems and Business Administration, Johannes Gutenberg-Universität Mainz, Jakob-Welder-Weg 9, Mainz 55128, Germany",
          "Dept. of Computer Science, Uni. Politècnica de Catalunya, Spain#TAB#","university of malaga", "Computer Laboratory, University Of Cambridge", "New England Aquarium", "Unconventional Computing Group, University of the West of England, Bristol, UK BS16 1QY#TAB#",
          "ESIEE-Paris","UNIVERSITÉ LAVAL", "University of Southern Denmark","University of Lisbon","#N#University of Glasgow#N#"]

vectAMERICA = ['The University of Chicago Press, Chicago, USA']

author_df = spark.read.json("C:\\Users\\bsbar\\PycharmProjects\\pythonProject\\authors.json")
fos_df = spark.read.json("C:\\Users\\bsbar\\PycharmProjects\\pythonProject\\fos.json")
publication_df = spark.read.json("C:\\Users\\bsbar\\PycharmProjects\\pythonProject\\publications.json")
venue_df = spark.read.json("C:\\Users\\bsbar\\PycharmProjects\\pythonProject\\venues.json")
rel_df = spark.read.json("C:\\Users\\bsbar\\PycharmProjects\\pythonProject\\rel_dw.json")
#author_df.show()
#fos_df.show()
#publication_df.show()
#venue_df.show()
#rel_df.show()
creationalq = [False, False, True, False, False]
normalq = [False, False, False, False, False, False, False, False, False, False]

#rdd=spark.sparkContext.parallelize(author_df)
#"print(rdd.collect())

# if (creationalq[1]):
      #1 query creation of a row

list=collect_list(author_df.affiliation)
print(list)

if (creationalq[2]):
      #3 Creation of a new column that represents the region of the university
      author_df = author_df.withColumn('Continent',
                                       when(author_df["affiiliation"] in vectASIA) then lit( "Asia"),
                                                                                        
                                       (when(author_df["affiiliation"] in vectAMERICA, "America")),
                                       (when(author_df["affiiliation"] in vectEU, "Europa"))

                                       )
# if (creationalq[3]):
