from pyspark.sql import SparkSession


# verifica se a linha é o cabeçalho do CSV
def is_header(line):
    return line.startswith("Country;Year")


# verifica se a linha possui os dados mínimos necessários
def is_valid(fields):

    # o dataset deve possuir pelo menos 10 colunas
    if len(fields) < 10:
        return False

    # verifica se os campos obrigatórios não estão vazios
    return (
        fields[0].strip() != "" and  # country
        fields[1].strip() != "" and  # year
        fields[4].strip() != "" and  # flow
        fields[9].strip() != ""      # category
    )


# cria a sessão Spark
spark = SparkSession.builder \
    .appName("Questao02") \
    .master("local[*]") \
    .getOrCreate()


# cria o SparkContext, responsável por manipular RDDs
sc = spark.sparkContext


# lê o arquivo CSV como um RDD de linhas
rdd = sc.textFile("in/operacoes_comerciais_inteira.csv")


# remove o cabeçalho do arquivo
rdd_sem_header = rdd.filter(
    lambda line: not is_header(line)
)


# separa cada linha em colunas usando ";"
rdd_split = rdd_sem_header.map(
    lambda line: line.split(";")
)


# remove linhas inválidas ou com dados faltantes
rdd_valido = rdd_split.filter(
    lambda fields: is_valid(fields)
)


# cria um PairRDD no formato:
# (ano, 1)
pair_rdd = rdd_valido.map(
    lambda fields: (fields[1], 1)
)


# agrupa pela chave (ano)
# e soma a quantidade de transações
resultado = pair_rdd.reduceByKey(
    lambda a, b: a + b
)


# ordena o resultado pelos anos
resultado_ordenado = resultado.sortByKey()


# traz o resultado distribuído para memória local
resultado_final = resultado_ordenado.collect()


# salva o resultado em arquivo txt
with open("output/resultado-02.txt", "w") as f:

    for ano, quantidade in resultado_final:
        f.write(f"{ano} -> {quantidade}\n")


# encerra a sessão Spark
spark.stop()