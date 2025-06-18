import os
import sys
import pandas as pd
import pymysql
from dotenv import load_dotenv


class CSVToMariaDBLoader:
    def __init__(self, csv_folder=None):
        # Ruta al directorio raíz del proyecto
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

        # Cargar variables de entorno desde .env en la raíz del proyecto
        dotenv_path = os.path.join(base_dir, ".env")
        load_dotenv(dotenv_path)

        # Configuración de conexión a la base de datos
        self.host = "localhost"
        self.user = os.getenv("MARIADB_USER")
        self.password = os.getenv("MARIADB_PASSWORD")
        self.database = os.getenv("MARIADB_DATABASE")
        self.csv_folder = csv_folder or os.path.join(base_dir, "csv-data")

        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database, autocommit=True)
        self.cursor = self.conn.cursor()

    def infer_sql_type(self, pandas_dtype):
        if pd.api.types.is_integer_dtype(pandas_dtype):
            return "INT"
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return "FLOAT"
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            return "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            return "DATETIME"
        else:
            return "TEXT"

    def create_table_from_csv(self, table_name, df):
        self.cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        columns = [f"`{col}` {self.infer_sql_type(df[col].dtype)}" for col in df.columns]
        create_stmt = f"CREATE TABLE `{table_name}` ({', '.join(columns)})"
        self.cursor.execute(create_stmt)

    def insert_data(self, table_name, df):
        placeholders = ", ".join(["%s"] * len(df.columns))
        columns = ", ".join([f"`{col}`" for col in df.columns])
        insert_stmt = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        self.cursor.executemany(insert_stmt, df.values.tolist())

    def load_csv(self, csv_file):
        table_name = os.path.splitext(os.path.basename(csv_file))[0]
        df = pd.read_csv(csv_file)
        self.create_table_from_csv(table_name, df)
        self.insert_data(table_name, df)
        print(f"✔ Tabla `{table_name}` cargada desde `{csv_file}`")

    def run(self, csv_files=None):
        if csv_files:
            for filename in csv_files:
                file_path = os.path.join(self.csv_folder, filename.strip())
                if os.path.isfile(file_path):
                    self.load_csv(file_path)
                else:
                    print(f"Archivo no encontrado: {filename}")
        else:
            for filename in os.listdir(self.csv_folder):
                if filename.endswith(".csv"):
                    self.load_csv(os.path.join(self.csv_folder, filename))


if __name__ == "__main__":
    loader = CSVToMariaDBLoader()

    # === EJEMPLOS DE USO DESDE LA RAÍZ DEL PROYECTO ===
    # Cargar archivos específicos:
    # python scripts/csv_to_mariadb_loader.py test.csv test_2.csv
    #
    # Cargar todos los archivos:
    # python scripts/csv_to_mariadb_loader.py

    if len(sys.argv) > 1:
        csv_files = [arg for arg in sys.argv[1:] if arg.endswith(".csv")]
        loader.run(csv_files)
    else:
        loader.run()
