import pymssql
import codecs
import chardet
import os


if __name__ == '__main__':
    print('')
    print('No es ejecutable')
    print("Módulo de funciones para simplificar el acceso a SQL Server")
    print('')


class Hsql:
    """
    Clase para simplificar el acceso a la Base de Datos SQL Server
    Cada llamada es una Conexión a la BD
    """

    def __init__(self, server: str, user: str, pwd: str, dbname: str):
        """
        Prepara la información de conexión a la Base de Datos.
        """
        self.server = server
        self.user = user
        self.pwd = pwd
        self.dbname = dbname
        self.conn = None
        self.error = None

    def has_error(self):
        """Retorna True si la ultima sentencia genero un Error"""
        return False if self.error is None else True

    def clear_error(self):
        """Limpia el Estado de Error"""
        self.error = None

    def print_error(self):
        """Imprime el Ultimo Error SQL que se ha producido."""

        if self.error is not None:
            print()
            print(
                f'Error SQL : {self.error["Database"]}\nServer    : {self.server}\nMensaje   : {self.error["Mensaje"]}')
            print()

    def connect(self) -> bool:
        if self.conn is not None:
            return True

        try:
            self.conn = pymssql.connect(self.server, self.user, self.pwd, self.dbname)
            self.conn.autocommit(True)
        except pymssql.StandardError as e:
            self.error = {"Database": self.dbname,
                          "Mensaje": str(e)}
            return False
        return True

    def disconnect(self):
        try:
            self.conn.close()
            self.conn = None
        except:
            pass

    def get_object(self, objeto, database='') -> str:
        """
        Obtiene el texto de definicion de un objeto de la Base de Datos
        (Procedure, Function, View, Trigger)
        """
        if self.error is not None:
            print('SQL En estado de Error')
            return ''

        if self.conn is None:
            self.connect()

        if self.has_error():
            return 'Error'

        _database = self.dbname
        if database != '' and _database != database:
            self.use_db(database)

        salida = ""
        query = "SELECT OBJECT_DEFINITION(OBJECT_ID(N'" + objeto + "'))"
        # print("getObject " + query)
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(query)
                for row in cursor:
                    salida += row[0] if row[0] else ''
            except pymssql.StandardError as e:
                self.error = {"Database": self.dbname,
                              "Mensaje": str(e)}
                return 'Error'

        salida = salida.replace('\r', '')
        return salida.strip().replace('\n\n', '\n')

    def exec_dictionary(self, comando, database=''):
        """
        Ejecuta un comando SQL y retorna la salida como diccionario
        """
        if self.error is not None:
            print('SQL En estado de Error')
            return dict()

        if self.conn is None:
            self.connect()

        _database = self.dbname
        if database != '' and _database != database:
            self.use_db(database)

        # print(f'Conexión: {self.server} {self.user} {self.pwd}')
        cursor = self.conn.cursor(as_dict=True)

        try:
            cursor.execute(comando)
        except pymssql.StandardError as e:
            self.error = {"Database": self.dbname, "Mensaje": str(e)}
            return []

        rows = []
        for row in cursor:
            nrow = {k.lower(): v for k, v in row.items()}
            rows.append(nrow)

        return rows

    def exec_dictionary_multirs(self, comando, database=''):
        if self.error is not None:
            print('SQL En estado de Error')
            return dict()

        if self.conn is None:
            self.connect()

        _database = self.dbname
        if database != '' and _database != database:
            self.use_db(database)

        # print(f'Conexión: {self.server} {self.user} {self.pwd}')
        cursor = self.conn.cursor(as_dict=True)

        try:
            cursor.execute(comando)
        except pymssql.StandardError as e:
            self.error = {"Database": self.dbname,
                          "Mensaje": str(e)
                          }
            return []

        result_sets = 0
        multiple_rs = []
        while True:
            rows = []
            for row in cursor:
                nrow = {k.lower(): v for k, v in row.items()}
                rows.append(nrow)
            print(f'Rows {rows}')
            multiple_rs.append(rows)
            if not cursor.nextset():
                result_sets += 1
                break

        return result_sets, multiple_rs

    def use_db(self, dbname: str) -> bool:

        if self.error is not None:
            print('SQL En estado de Error')
            return False

        if self.conn is None:
            self.connect()

        comando = "USE " + dbname

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(comando)
            except pymssql.StandardError as e:
                self.error = {"Database": self.dbname,
                              "Mensaje": str(e)
                              }
                return False

        return True

    def exec_no_result(self, comando, database='') -> bool:
        """
        Ejecuta un comando SQL que no trae resultados
        Retorna: None
        """

        if self.error is not None:
            print('SQL En estado de Error')
            return False

        if self.conn is None:
            self.connect()

        _database = self.dbname
        if database != '' and _database != database:
            self.use_db(database)

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(comando)
            except pymssql.StandardError as e:
                self.error = {"Database": self.dbname,
                              "Mensaje": str(e)
                              }
                return False

        return True

    def get_dbnames(self, ):
        """
        Retorna el nombre de todas las BD's del Sistema
        """
        databases = []
        comando = "SELECT name FROM sys.databases"
        if self.error is not None:
            print('SQL En estado de Error')
            return []

        if self.conn is None:
            self.connect()
        try:
            res = self.exec_dictionary(comando)
            databases = [k['name'] for k in res]

        except pymssql.StandardError as e:
            self.error = {"Database": self.dbname,
                          "Mensaje": str(e)
                          }
            return 'Error'

        return databases

    def get_date(self, ):
        """
        Retorna la Fecha desde SQL Server
        """

        if self.error is not None:
            print('SQL En estado de Error')
            return ''

        if self.conn is None:
            self.connect()

        if self.conn is None:
            self.error = f'No es posible conectarse al servidor {self.dbname}'
            return ''

        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT getdate() as fecha")
            res = cursor.fetchone()
            return res[0]
        except pymssql.StandardError as e:
            self.error = {"Database": self.dbname,
                          "Mensaje": str(e)}
            return ''


def read_sqlfile(file_name):
    """
    Lee un archivo de texto retornando las secciones de este que están separadas
    por una línea que contiene la palabra "GO"
    """

    # Para Azure SQL
    to_replace = f"{chr(226)}{chr(8364)}{chr(8221)}"

    if not os.path.isfile(file_name):
        return []

    rawdata = open(file_name, 'rb').read()
    result = chardet.detect(rawdata)
    charenc = result['encoding']
    del rawdata

    result = []
    with codecs.open(file_name, 'r', charenc) as f:
        comando = ""
        for line in f:
            line = line.replace(to_replace, '-')
            if line.strip().upper() == "GO":
                result.append(comando)
                comando = ""
            else:
                comando += line

        if not comando == "":
            result.append(comando)

    return result


def parse_command(comando):
    result = []
    to_replace = ['go', 'Go', 'gO']
    for val in to_replace:
        comando = comando.replace(f"\n{val}\n", "\nGO\n")

    result = comando.split("\nGO\n")
    return result

# FIN DEL ARCHIVO
