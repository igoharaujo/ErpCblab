import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, 
    StringType, DoubleType, BooleanType
)
from pyspark.sql import Row, DataFrame

class SilverLayerProcessor:
    def __init__(self):
        self.spark = self._get_spark_session()
        self._init_schemas()
    
    def _get_spark_session(self):
        return SparkSession.builder \
            .appName("SilverLayerProcessor") \
            .getOrCreate()
    
    def _init_schemas(self):
        """Inicializa todos os schemas das tabelas"""
        self.guest_check_schema = StructType([
        StructField("guestCheckId", LongType(), True),
        StructField("chkNum", IntegerType(), True),
        StructField("opnBusDt", StringType(), True),
        StructField("opnUTC", StringType(), True),
        StructField("opnLcl", StringType(), True),
        StructField("clsdBusDt", StringType(), True),
        StructField("clsdUTC", StringType(), True),
        StructField("clsdLcl", StringType(), True),
        StructField("lastTransUTC", StringType(), True),
        StructField("lastTransLcl", StringType(), True),
        StructField("lastUpdatedUTC", StringType(), True),
        StructField("lastUpdatedLcl", StringType(), True),
        StructField("clsdFlag", BooleanType(), True),
        StructField("gstCnt", IntegerType(), True),
        StructField("subTtl", DoubleType(), True),
        StructField("nonTxblSlsTtl", DoubleType(), True),
        StructField("chkTtl", DoubleType(), True),
        StructField("dscTtl", IntegerType(), True),
        StructField("payTtl", DoubleType(), True),
        StructField("balDueTtl", DoubleType(), True),
        StructField("rvcNum", IntegerType(), True),
        StructField("otNum", IntegerType(), True),
        StructField("ocNum", IntegerType(), True),
        StructField("tblNum", IntegerType(), True),
        StructField("tblName", StringType(), True),
        StructField("empNum", IntegerType(), True),
        StructField("numSrvcRd", IntegerType(), True),
        StructField("numChkPrntd", IntegerType(), True),
    ])
        
        self.taxes_schema = StructType([
            StructField("taxNum", IntegerType(), True),
            StructField("txblSlsTtl", DoubleType(), True),
            StructField("taxCollTtl", DoubleType(), True),
            StructField("taxRate", IntegerType(), True),
            StructField("type", IntegerType(), True)
        ])
        
        self.detail_lines_schema = StructType([
            StructField("guestCheckLineItemId", LongType(), True),
            StructField("rvcNum", IntegerType(), True),
            StructField("dtlOtNum", IntegerType(), True),
            StructField("dtlOcNum", IntegerType(), True),
            StructField("lineNum", IntegerType(), True),
            StructField("dtlId", IntegerType(), True),
            StructField("detailUTC", StringType(), True),
            StructField("detailLcl", StringType(), True),
            StructField("lastUpdateUTC", StringType(), True),
            StructField("lastUpdateLcl", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("wsNum", IntegerType(), True),
            StructField("dspTtl", DoubleType(), True),
            StructField("dspQty", IntegerType(), True),
            StructField("aggTtl", DoubleType(), True),
            StructField("aggQty", IntegerType(), True),
            StructField("chkEmpId", LongType(), True),
            StructField("chkEmpNum", IntegerType(), True),
            StructField("svcRndNum", IntegerType(), True),
            StructField("seatNum", IntegerType(), True)
        ])
        
        self.menu_item_schema = StructType([
            StructField("miNum", IntegerType(), True),
            StructField("modFlag", BooleanType(), True),
            StructField("inclTax", DoubleType(), True),
            StructField("activeTaxes", StringType(), True),
            StructField("prcLvl", IntegerType(), True),
            StructField("discount", IntegerType(), True),
            StructField("serviceCharge", IntegerType(), True),
            StructField("enderMedia", IntegerType(), True),
            StructField("errorCode", IntegerType(), True),
        ])
    
    def _map_data_to_schema(self, data_item: dict, schema: StructType) -> Row:
        """Mapeia os dados para o schema definido"""
        schema_fields = [field.name for field in schema]
        filtered_data = {col: data_item.get(col, None) for col in schema_fields}
        return Row(**filtered_data)
    
    def _process_guest_checks(self, bronze_df: DataFrame) -> DataFrame:
        """Processa a tabela GuestChecks"""
        exploded_df = bronze_df.select(explode(col("guestChecks")).alias("guestCheck"))
        data = exploded_df.select("guestCheck.*").collect()
        rows = [self._map_data_to_schema(row.asDict(), self.guest_check_schema) for row in data]
        return self.spark.createDataFrame(rows, schema=self.guest_check_schema)
    
    def _process_taxes(self, bronze_df: DataFrame) -> DataFrame:
        """Processa a tabela Taxes"""
        df_tax_exploded = bronze_df.select(explode(col("guestChecks")).alias("guestCheck"))
        exploded_taxes = df_tax_exploded.select(explode(col("guestCheck.taxes")).alias("tax"))
        data_taxes = exploded_taxes.select("tax.*").collect()
        rows = [self._map_data_to_schema(row.asDict(), self.taxes_schema) for row in data_taxes]
        return self.spark.createDataFrame(rows, schema=self.taxes_schema)
    
    def _process_detail_lines(self, bronze_df: DataFrame) -> DataFrame:
        """Processa a tabela DetailLines"""
        df_detail_exploded = bronze_df.select(explode(col("guestChecks")).alias("guestCheck"))
        exploded_lines = df_detail_exploded.select(explode(col("guestCheck.detailLines")).alias("detailLine"))
        data = exploded_lines.select("detailLine.*").collect()
        rows = [self._map_data_to_schema(row.asDict(), self.detail_lines_schema) for row in data]
        return self.spark.createDataFrame(rows, schema=self.detail_lines_schema)
    
    def _process_menu_items(self, bronze_df: DataFrame) -> DataFrame:
        """Processa a tabela MenuItem"""
        df_detail_exploded = bronze_df.select(explode(col("guestChecks")).alias("guestCheck"))
        exploded_detail_lines = df_detail_exploded.select(explode(col("guestCheck.detailLines")).alias("detailLine"))
        
        exploded_menu_item = exploded_detail_lines.select(
            col("detailLine.menuItem.miNum").alias("miNum"),
            col("detailLine.menuItem.modFlag").alias("modFlag"),
            col("detailLine.menuItem.inclTax").alias("inclTax"),
            col("detailLine.menuItem.activeTaxes").alias("activeTaxes"),
            col("detailLine.menuItem.prcLvl").alias("prcLvl")
        )
        
        data_menu_item = exploded_menu_item.collect()
        rows_menu_item = [self._map_data_to_schema(row.asDict(), self.menu_item_schema) for row in data_menu_item]
        return self.spark.createDataFrame(rows_menu_item, schema=self.menu_item_schema)
    
    def create_silver_tables(self):
        """Cria as tabelas Silver se não existirem"""
        database_name = "DB_ERP_SILVER"
        silver_base = "DataLake/landing-zone/Prata/erp"
        
        # Cria o database se não existir
        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {database_name} "
            f"LOCATION 'file:{os.getcwd()}/{silver_base}'"
        )
        
        # Usa o database
        self.spark.sql(f"USE {database_name}")
        
        # Define e cria as tabelas
        tables = {
            'menu_items': self.menu_item_schema,
            'guest_checks': self.guest_check_schema,
            'taxes': self.taxes_schema,
            'detail_lines': self.detail_lines_schema
        }
        
        for table_name, schema in tables.items():
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                    {', '.join([f"{field.name} {self._get_spark_type(field.dataType)}" 
                              for field in schema.fields])}
                )
                USING PARQUET
                LOCATION 'file:{os.getcwd()}/{silver_base}/{table_name}'
            """)
    
    def _get_spark_type(self, data_type):
        """Converte tipos PySpark para strings SQL"""
        type_mapping = {
            IntegerType: "INT",
            LongType: "BIGINT",
            StringType: "STRING",
            DoubleType: "DOUBLE",
            BooleanType: "BOOLEAN"
        }
        return type_mapping.get(type(data_type), "STRING")
    
    def process_silver_layer(self, date_str: str = None):
        try:
            self.create_silver_tables()
            
            bronze_path = f'DataLake/landing-zone/Bronze/erp/erp_{date_str}'
            bronze_df = self.spark.read.parquet(bronze_path)
            
            tables_data = {
                'menu_items': self._process_menu_items(bronze_df),
                'guest_checks': self._process_guest_checks(bronze_df),
                'taxes': self._process_taxes(bronze_df),
                'detail_lines': self._process_detail_lines(bronze_df)
            }
            
            for table_name, df in tables_data.items():
                df.createOrReplaceTempView(f"temp_{table_name}")
                self.spark.sql(f"""
                    INSERT INTO DB_ERP_SILVER.{table_name}
                    SELECT * FROM temp_{table_name}
                """)
                print(f"Dados inseridos com sucesso na tabela {table_name}")
            
            return True
            
        except Exception as e:
            print(f"Erro no processamento da camada Silver: {str(e)}")
            return False

#def main():
#    processor = SilverLayerProcessor()
#    processor.process_silver_layer("20241127")
#
#if __name__ == "__main__":
#    main()