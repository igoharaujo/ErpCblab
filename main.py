import logging
import os
import sqlite3
from typing import Optional
from processamento.bronze import BronzeLayerProcessor
from processamento.silver import SilverLayerProcessor
from pyspark.sql import SparkSession

def configurar_logging() -> None:
    """Configura o sistema de logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def criar_sqlite_database(spark: SparkSession) -> bool:
    """
    Cria e popula o banco SQLite com os dados da camada Silver
    """
    try:
        logging.info("Iniciando criação do banco SQLite...")
        
        # Carregar os arquivos Parquet
        tabelas = {
            'menu_items': spark.read.parquet('DataLake/landing-zone/Prata/erp/menu_items'),
            'guest_checks': spark.read.parquet('DataLake/landing-zone/Prata/erp/guest_checks'),
            'taxes': spark.read.parquet('DataLake/landing-zone/Prata/erp/taxes'),
            'detail_lines': spark.read.parquet('DataLake/landing-zone/Prata/erp/detail_lines')
        }
        
        # Criar conexão com SQLite
        conn = sqlite3.connect('erp_database.db')
        cursor = conn.cursor()
        
        for nome_tabela, df in tabelas.items():
            logging.info(f"Processando tabela {nome_tabela}...")
            
            # Criar tabela
            colunas = [f"{col} TEXT" for col in df.columns]
            create_query = f"CREATE TABLE IF NOT EXISTS {nome_tabela} ({', '.join(colunas)})"
            cursor.execute(create_query)
            
            # Inserir dados
            for row in df.collect():
                valores = tuple(str(v) if v is not None else None for v in row)
                placeholders = ", ".join(["?" for _ in valores])
                insert_query = f"INSERT INTO {nome_tabela} VALUES ({placeholders})"
                cursor.execute(insert_query, valores)
            
            conn.commit()
            logging.info(f"Tabela {nome_tabela} processada com sucesso")
        
        conn.close()
        logging.info("Banco SQLite criado e populado com sucesso")
        return True
        
    except Exception as e:
        logging.error(f"Erro ao criar banco SQLite: {str(e)}")
        return False

def executar_processamento() -> Optional[bool]:
    """
    Executa o pipeline completo: Bronze -> Silver
    Returns:
        bool: True se todo o processamento foi bem sucedido, False caso contrário
    """
    try:
        # Processamento Bronze
        logging.info("Iniciando processamento da camada Bronze")
        bronze_processor = BronzeLayerProcessor()
        
        logging.info("Criando database Bronze...")
        bronze_processor.create_database()
        
        logging.info("Processando dados Bronze...")
        resultado_bronze = bronze_processor.process_bronze_layer()
        
        if not resultado_bronze:
            logging.error("Falha no processamento da camada Bronze")
            return False
        
        # Processamento Silver
        logging.info("Iniciando processamento da camada Silver")
        silver_processor = SilverLayerProcessor()
        
        # Pega a data atual que foi usada no Bronze
        data_atual = bronze_processor._get_current_date()
        
        logging.info(f"Processando dados Silver para a data {data_atual}...")
        resultado_silver = silver_processor.process_silver_layer(data_atual)
        
        if not resultado_silver:
            logging.error("Falha no processamento da camada Silver")
            return False
        
        # Criar banco SQLite
        resultado_sqlite = criar_sqlite_database(silver_processor.spark)
        
        if not resultado_sqlite:
            logging.error("Falha na criação do banco SQLite")
            return False
            
        logging.info("Pipeline completo executado com sucesso")
        return True
        
    except Exception as e:
        logging.error(f"Erro durante o processamento do pipeline: {str(e)}")
        return False

def main() -> None:
    configurar_logging()
    executar_processamento()

if __name__ == "__main__":
    main()