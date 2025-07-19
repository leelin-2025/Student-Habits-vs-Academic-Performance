#instaler pyspark
!pip install findspark
!pip install pyspark
import findspark
import pyspark
findspark.init()

from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder \
    .appName("ProjetFinal") \
    .getOrCreate()
import kagglehub

# Download latest version
path = kagglehub.dataset_download("jayaantanaath/student-habits-vs-academic-performance")

# Chemin du fichier CSV
csv_file_path = path+"/student_habits_performance.csv"

print("Chemin du fichier CSV:", csv_file_path)

# installer pandas
!pip install pandas
import pandas as pd

# Lecture du fichier CSV
df = spark.read.csv(csv_file_path, header=True,inferSchema=True)

# Afficher les 20 premières lignes
df.show()

#supression des doublons
df = df.distinct()

# Supression des données aberente (inferieur à 0)
liste  = ['age','study_hours_per_day','social_media_hours','netflix_hours',
          'attendance_percentage','sleep_hours','exercise_frequency',
          'mental_health_rating','exam_score']
for col in liste:
  df = df.filter(df[col]>=0)

#noms et type des colone
print("Noms et types des colones")

df.toPandas().info()

