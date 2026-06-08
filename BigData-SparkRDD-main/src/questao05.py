from pyspark.sql import SparkSession


# verifica se a linha é o cabeçalho
def is_header(line):
    return line.startswith("Country;Year")


# verifica se a linha possui os campos necessários
def is_valid(fields):

    if len(fields) < 10:
        return False

    return (
        fields[0].strip() != "" and  # country
        fields[1].strip() != "" and  # year
        fields[4].strip() != "" and  # flow
        fields[5].strip() != "" and  # price
        fields[9].strip() != ""      # category
    )


# cria a sessão Spark
spark = SparkSession.builder \
    .appName("Questao05") \
    .master("local[*]") \
    .getOrCreate()


# cria o SparkContext
sc = spark.sparkContext


# lê o CSV como RDD
rdd = sc.textFile("in/operacoes_comerciais_inteira.csv")


# remove cabeçalho
rdd_sem_header = rdd.filter(
    lambda line: not is_header(line)
)


# separa as colunas
rdd_split = rdd_sem_header.map(
    lambda line: line.split(";")
)


# remove linhas inválidas
rdd_valido = rdd_split.filter(
    lambda fields: is_valid(fields)
)


# mantém apenas transações do Brasil
rdd_brazil = rdd_valido.filter(
    lambda fields: fields[0] == "Brazil"
)


# cria PairRDD no formato:
# (ano, (preço, 1))
pair_rdd = rdd_brazil.map(
    lambda fields: (
        fields[1],
        (float(fields[5]), 1)
    )
)


# soma os preços e as quantidades
resultado = pair_rdd.reduceByKey(
    lambda a, b: (
        a[0] + b[0],  # soma dos preços
        a[1] + b[1]   # soma das quantidades
    )
)


# calcula a média
media_por_ano = resultado.mapValues(
    lambda x: x[0] / x[1]
)


# ordena pelos anos
resultado_ordenado = media_por_ano.sortByKey()


# traz resultado para memória local
resultado_final = resultado_ordenado.collect()


# exibe no terminal
print("Valor médio das transações por ano no Brasil:\n")

for ano, media in resultado_final:
    print(f"{ano} -> {media}")


# salva em txt
with open("output/resultado-05.txt", "w") as f:

    for ano, media in resultado_final:
        f.write(f"{ano} -> {media}\n")


# encerra sessão Spark
spark.stop()