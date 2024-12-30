import pymssql
import logging

class SQLServerManager:
    def __init__(self, host, user, password, database, port=1433):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        """
        Establece la conexión con la base de datos SQL Server.
        """
        try:
            self.connection = pymssql.connect(
                server=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            logging.info("Conexión exitosa a la base de datos SQL Server.")
        except pymssql.DatabaseError as db_err:
            logging.error(f"Error al conectar a la base de datos: {str(db_err)}")
            raise

    def execute_query(self, query):
        """
        Ejecuta una consulta SQL y devuelve los resultados.

        Args:
            query (str): Consulta SQL a ejecutar.

        Returns:
            list: Resultados de la consulta.
        """
        try:
            if not self.connection:
                raise ValueError("La conexión no está establecida. Llama a connect() primero.")

            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()

            logging.info(f"Consulta ejecutada correctamente: {query}")
            return results
        except Exception as e:
            logging.error(f"Error al ejecutar la consulta: {str(e)}")
            raise

    def close(self):
        """
        Cierra la conexión a la base de datos.
        """
        try:
            if self.connection:
                self.connection.close()
                logging.info("Conexión cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión: {str(e)}")
            raise
