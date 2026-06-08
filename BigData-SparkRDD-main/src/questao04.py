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


# cria a chave do PairRDD
def create_pair(fields):
    return (("Brazil", 2016), 1)


# soma os valores
def sum_values(a, b):
    return a + b


spark = SparkSession.builder \
    .appName("Questao04") \
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


# cria PairRDD
pair_rdd = rdd_brazil_2016.map(
    create_pair
)


# soma as ocorrências
resultado_pair = pair_rdd.reduceByKey(
    sum_values
)


# coleta resultado
resultado = resultado_pair.collect()[0]

(chave, valor) = resultado
(pais, ano) = chave


print("Número de transações do Brasil em 2016 usando PairRDD:")
print(pais, ano, valor)


# salva resultado
with open("output/resultado-04.txt", "w") as f:
    f.write(f"{pais},{ano}\t{valor}")


spark.stop()