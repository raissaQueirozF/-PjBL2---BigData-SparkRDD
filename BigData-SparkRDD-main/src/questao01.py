from pyspark.sql import SparkSession

#verifica se a linha é o header do arquivo
def is_header(line):
    return line.startswith("Country;Year")

#verifica se os campos são validos, ou seja, se possuem pelo menos 10 campos e se os campos necessários não estão vazios
def is_valid(fields):

    if len(fields) < 10:
        return False

    return (
        fields[0].strip() != "" and
        fields[1].strip() != "" and
        fields[4].strip() != "" and
        fields[9].strip() != ""
    )


spark = SparkSession.builder \
    .appName("Questao01") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

#carrega o arquivo CSV como um RDD
rdd = sc.textFile("in/operacoes_comerciais_inteira.csv")


rdd_sem_header = rdd.filter(
    lambda line: not is_header(line)
)


rdd_split = rdd_sem_header.map(
    lambda line: line.split(";")
)


rdd_valido = rdd_split.filter(
    lambda fields: is_valid(fields)
)


rdd_brazil = rdd_valido.filter(
    lambda fields: fields[0] == "Brazil"
)


resultado = rdd_brazil.count()


print("Número de transações envolvendo o Brasil:")
print(resultado)


with open("output/resultado-01.txt", "w") as f:
    f.write(f"Número de transações envolvendo o Brasil: {resultado}")


spark.stop()