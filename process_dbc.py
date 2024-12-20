from pyspark.sql import SparkSession
from dbc_reader import DbcReader
import pandas as pd
import os
import logging
import shutil

# Diretórios
INPUT_DIR = "input"
OUTPUT_DIR = "output"

# Configura o logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def process_dbc_with_dbc_reader_and_spark(spark, file_name):
    # Caminho do arquivo DBC
    input_path = os.path.join(INPUT_DIR, file_name)
    if not os.path.exists(input_path):
        logging.error(f"Arquivo {file_name} não encontrado em {INPUT_DIR}")
        return

    # Caminho do arquivo de saída
    output_file_name = file_name.replace(".dbc", ".csv")
    output_path = os.path.join(OUTPUT_DIR, output_file_name)

    logging.info(f"Lendo o arquivo DBC: {input_path}")
    try:
        # Lê o arquivo DBC usando DbcReader diretamente pelo caminho do arquivo
        rows = [row for row in DbcReader(input_path)]

        # Converte para Pandas DataFrame
        pandas_df = pd.DataFrame(rows)

        # Converte para Spark DataFrame
        spark_df = spark.createDataFrame(pandas_df)

        logging.info(f"Arquivo DBC lido com sucesso. Total de linhas: {spark_df.count()}")

        # Diretório temporário para salvar os arquivos
        temp_dir = output_path + "_tmp"

        # Remove o diretório temporário se já existir
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        # Salva como CSV usando Spark, garantindo apenas um arquivo de saída
        logging.info(f"Salvando o arquivo CSV em: {output_path}")
        spark_df.coalesce(1).write.csv(temp_dir, header=True, mode='overwrite', encoding='utf-8')

        # Move o arquivo único para o local final
        for temp_file in os.listdir(temp_dir):
            if temp_file.endswith(".csv"):
                shutil.move(os.path.join(temp_dir, temp_file), output_path)
                break

        # Remove o diretório temporário
        shutil.rmtree(temp_dir)
        logging.info(f"Arquivo CSV gerado com sucesso: {output_path}")
    except Exception as e:
        logging.error(f"Erro ao processar o arquivo DBC: {e}")

# Configura e executa o processamento
def main():
    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR)
        logging.info(f"Criada pasta de entrada: {INPUT_DIR}")

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logging.info(f"Criada pasta de saída: {OUTPUT_DIR}")

    # Inicia sessão Spark
    spark = SparkSession.builder.appName("ProcessDBCWithDbcReader").getOrCreate()

    # Pega os arquivos .dbc na pasta de entrada
    dbc_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.dbc')]
    if not dbc_files:
        logging.warning(f"Nenhum arquivo .DBC encontrado em {INPUT_DIR}")
        return

    # Processa todos os arquivos .dbc encontrados
    for index, file_name in enumerate(dbc_files, start=1):
        logging.info(f"Processando arquivo {index}/{len(dbc_files)}: {file_name}")
        process_dbc_with_dbc_reader_and_spark(spark, file_name)

    spark.stop()
    logging.info("Processamento concluído.")

if __name__ == "__main__":
    main()
