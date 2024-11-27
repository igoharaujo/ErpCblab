from config import get_spark_session
from pyspark.sql.functions import current_date
from pathlib import Path
import datetime
import os

class BronzeLayerProcessor:
    def __init__(self):
        self.spark = get_spark_session()
        self.base_path = Path(os.getcwd())
        self.database_name = "DB_ERP"
    
    def _get_paths(self):
        """Define os caminhos utilizados no processamento"""
        return {
            'raw_path': f"file:{self.base_path}/DataLake/raw/erp.json",
            'bronze_base': f"file:{self.base_path}/DataLake/landing-zone/Bronze/erp",
            'bronze_table': f"file:{self.base_path}/DataLake/landing-zone/Bronze/erp/erp_{self._get_current_date()}"
        }
    
    @staticmethod
    def _get_current_date():
        """Retorna a data atual no formato YYYYMMDD"""
        return datetime.datetime.now().strftime("%Y%m%d")
    
    @staticmethod
    def _get_custom_date(data: datetime.datetime):
        """Retorna uma data específica no formato YYYYMMDD"""
        return data.strftime("%Y%m%d")
    
    def create_database(self):
        """Cria o database Bronze se não existir"""
        paths = self._get_paths()
        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {self.database_name} "
            f"LOCATION '{paths['bronze_base']}'"
        )
    
    def process_bronze_layer(self):
        """Processa os dados da camada Raw para Bronze"""
        try:
            paths = self._get_paths()
            
            # Leitura do arquivo JSON
            df = self.spark.read.option("multiline", "true").json(paths['raw_path'])
            
            # Adiciona coluna de data de ingestão
            df_bronze = df.withColumn("ingestion_date", current_date())
            
            # Salva no formato Parquet
            df_bronze.write.mode("overwrite").parquet(paths['bronze_table'])
            
            # Remove o arquivo da Raw após processamento
            raw_file_path = paths['raw_path'].replace('file:', '')
            if os.path.exists(raw_file_path):
                os.remove(raw_file_path)
                print(f"Arquivo {raw_file_path} removido com sucesso")
            
            return True
        except Exception as e:
            print(f"Erro no processamento da camada Bronze: {str(e)}")
            return False
