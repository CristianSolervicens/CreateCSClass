import modules.sql as Hsql
from modules.config import Config


def main():
    print("")
    hsql.connect()
    if hsql.has_error():
        print(f"Error conectándose a {cfg.server}")
        hsql.print_error()
        hsql.clear_error()
        return
    else:
        print(f"Conectado a {cfg.server}:{cfg.database}")

    base = input("Indique Base de Datos >> ")
    print(f"Base de Datos: {base}")
    hsql.use_db(base)

    seguir = True
    while seguir:
        tabla = input(" Indique una Tabla >> ")

        if tabla == '':
            seguir = False
            continue

        columnas = get_table_columns(tabla)
        if not columnas:
            print(f"Tabla [{tabla}] No encontrada")
            input(">> ")
            continue

        table_name = tabla if '.' not in tabla else tabla.split('.')[1]

        print("")
        print(f"private class {table_name}")
        print("{")
        for col, col_def in columnas.items():
            print(f"    public {map_column(col_def):12} {col} {{get; set;}}")
        print("}")
        print("")
        input(">> ")


def map_column(column: dict):
    match_type = ''
    match column["data_type"].casefold():
        case "char" | "varchar" | "nvarchar":
            match_type = ' string'
        case "smallint" | "int":
            match_type = ' int32'
        case "byte":
            match_type = " bool"
        case "datetime" | "smalldatetime":
            match_type = ' datetime'
        case "image" | "varbinary":
            match_type = ' bytearray'
        case "numeric":
            if column["scale"] == 0:
                if column["precision"] < 7:
                    match_type = " int32"
                else:
                    match_type = " int64"
            else:
                match_type = ' double'

    return match_type


def get_table_columns(table):
    schema = ''
    table_name = ''
    if '.' in table:
        schema, table_name = table.split('.')
    else:
        schema, table_name = 'dbo', table

    comando = f"""
    select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
       NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, 
       IS_NULLABLE 
    from INFORMATION_SCHEMA.COLUMNS
    where TABLE_NAME = '{table_name}'
      and table_schema = '{schema}'
    ORDER BY ordinal_position
    """
    columns = {}
    res = hsql.exec_dictionary(comando)
    for row in res:
        columns[row['column_name']] = {
            "data_type": row["data_type"],
            "length": row["character_maximum_length"],
            "precision": row["numeric_precision"],
            "scale": row["numeric_scale"]
        }
    return columns


if __name__ == "__main__":
    cfg = Config()
    hsql = Hsql.Hsql(cfg.server, cfg.user, cfg.passwd, cfg.database)
    main()
