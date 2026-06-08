from pyspark.sql import SparkSession

# verifica se a linha é o header do arquivo
def is_header(line):
    return line.startswith("Country;Year")


# verifica se os campos são válidos
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
    .appName("Questao03") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


# carrega o arquivo CSV
rdd = sc.textFile("in/operacoes_comerciais_inteira.csv")


# remove header
rdd_sem_header = rdd.filter(
    lambda line: not is_header(line)
)


# separa os campos
rdd_split = rdd_sem_header.map(
    lambda line: line.split(";")
)


# mantém apenas linhas válidas
rdd_valido = rdd_split.filter(
    lambda fields: is_valid(fields)
)


# filtra transações do Brasil
rdd_brazil = rdd_valido.filter(
    lambda fields: fields[0] == "Brazil"
)


# filtra transações do Brasil em 2016
rdd_brazil_2016 = rdd_brazil.filter(
    lambda fields: fields[1] == "2016"
)


# conta as transações
resultado = rdd_brazil_2016.count()


print("Número de transações do Brasil em 2016:")
print(resultado)


# salva resultado
with open("output/resultado-03.txt", "w") as f:
    f.write(f"Número de transações do Brasil em 2016: {resultado}")


spark.stop()