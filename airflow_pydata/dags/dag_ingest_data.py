from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator


import subprocess
import pandas as pd
from datetime import datetime
import time
import os
import io
import json
import sys



def global_data_ingesta():

    import pymssql
    import pandas as pd
    import pandasql as ps
    import csv
    import requests
    from datetime import datetime
    from airflow.models import Variable

    DATA_CONNECTION = {
    "SQL_SERVER_IP":"sql-server",
    "USER_SQL_SERVER":"sa",
    "PASSWORD_SQL_SERVER":Variable.get("password_sql_server_secret"),
    "DATABASE_SQL_SERVER":"Testing_ETL"
    }

    ID_DAG_PROCESS = int(str(datetime.now()).replace(':', '').replace('.', '').replace('-', '').replace(' ', ''))


    def get_data_database(DATA_CONNECTION):

        server = DATA_CONNECTION['SQL_SERVER_IP']
        print(DATA_CONNECTION['SQL_SERVER_IP'])
        user = DATA_CONNECTION['USER_SQL_SERVER']
        print(DATA_CONNECTION['USER_SQL_SERVER'])
        password = DATA_CONNECTION['PASSWORD_SQL_SERVER']
        print(DATA_CONNECTION['PASSWORD_SQL_SERVER'])
        conn = pymssql.connect(server, user, password, DATA_CONNECTION['DATABASE_SQL_SERVER'])
        print(DATA_CONNECTION['DATABASE_SQL_SERVER'])
        cursor = conn.cursor()

        print('Arranco el Proceso')

        # GET DATA FROM DATABASE
        Unificado_headers = ['CHROM',
                             'POS',
                             'ID',
                             'REF',
                             'ALT',
                             'QUAL',
                             'FILTER',
                             'INFO',
                             'FORMAT',
                             'MUESTRA',
                             'VALOR',
                             'ORIGEN',
                             'FECHA_COPIA',
                             'RESULTADO']

        cursor.execute("""

            SELECT * FROM Unificado    

        """)

        df = pd.DataFrame(cursor, columns=Unificado_headers)

        # GET DATA DATAFRAME FROM QUERY
        pysqldf = lambda q: sqldf(q, locals())
        query_select_all_records = "SELECT * FROM df"
        df_sql_db = ps.sqldf(query_select_all_records, locals())

        return df_sql_db

    def get_data_from_api_csv():

        print('GET DATA FROM CSV API')
        with requests.Session() as s:
            download = s.get(
                'https://francisadbteststorage.blob.core.windows.net/challenge/nuevas_filas.csv?sp=r&st=2021-07-08T18:53:40Z&se=2022-12-09T02:53:40Z&spr=https&sv=2020-08-04&sr=b&sig=AK7dCkWE1xR28ktHfdSYU2RSZITivBQmv83U51pyJMo%3D')

            decoded_content = download.content.decode('utf-8')

            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)

        get_headers = list(pd.DataFrame(my_list).head(1).values[0])
        df_csv = pd.DataFrame(my_list, columns=get_headers)

        if df_csv.shape[1]>10:
            df_csv = df_csv.iloc[1:]
            df_csv['FECHA_COPIA'] = str(datetime.now())
            df_csv = df_csv.iloc[:, :14]
            #print(df_csv)
            return [True,df_csv]
        else:
            return [False]

    def agrupar_datos_fecha_maxima(df):


        query_group_by_for_fecha_copia = """
                      SELECT CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO, FORMAT, MUESTRA, VALOR, ORIGEN, max(FECHA_COPIA) as FECHA_COPIA, RESULTADO
                        FROM df 
                    GROUP BY CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO, FORMAT, MUESTRA, VALOR, ORIGEN, RESULTADO

            """

        df_sql = ps.sqldf(query_group_by_for_fecha_copia, locals())
        df_sql

        df_sql['FECHA_COPIA'] = df_sql.FECHA_COPIA.map(lambda x: formatDatetime(str(x)))

        print(df_sql)
        return df_sql

    def formatDatetime(CADENA):
        end = CADENA.find('.')
        return CADENA[:][0:end]

    def eliminar_data_sql_server(DATA_CONNECTION):

        server = DATA_CONNECTION['SQL_SERVER_IP']
        user = DATA_CONNECTION['USER_SQL_SERVER']
        password = DATA_CONNECTION['PASSWORD_SQL_SERVER']
        conn = pymssql.connect(server, user, password, DATA_CONNECTION['DATABASE_SQL_SERVER'])
        cursor = conn.cursor()

        try:
            cursor.execute("""

                DELETE Unificado    

            """)

            conn.commit()
            conn.close()
            return True

        except Exception as e:

            print('FALLO EL PROGRAMA AL ELIMINAR LOS DATOS DE LA BASE')
            codeError = str(ID_DAG_PROCESS)
            messageError = str(e.args[0]) + ' - Error al eliminar datos en la Base de Datos'
            sysdateError = formatDatetime(str(datetime.now()))
            system_log(codeError, messageErrorm, sysdateError)

            return False

    def insert_data_sql_server(df_sql,DATA_CONNECTION):

        server = DATA_CONNECTION['SQL_SERVER_IP']
        user = DATA_CONNECTION['USER_SQL_SERVER']
        password = DATA_CONNECTION['PASSWORD_SQL_SERVER']
        conn = pymssql.connect(server, user, password, DATA_CONNECTION['DATABASE_SQL_SERVER'])
        cursor = conn.cursor()


        for i in df_sql.values:
            try:
                v_CHROM = "'" + i[0] + "'"
                v_POS = "'" + i[1] + "'"
                v_ID = "'" + i[2] + "'"
                v_REF = "'" + i[3] + "'"
                v_ALT = "'" + i[4] + "'"
                v_QUAL = "'" + i[5] + "'"
                v_FILTER = "'" + i[6] + "'"
                v_INFO = "'" + i[7] + "'"
                v_FORMAT = "'" + i[8] + "'"
                v_MUESTRA = "'" + i[9] + "'"
                v_VALOR = "'" + i[10] + "'"
                v_ORIGEN = "'" + i[11] + "'"
                v_FECHA_COPIA = "'" + i[12] + "'"
                v_RESULTADO = "'" + i[13] + "'"
                cursor.execute("""
                    INSERT INTO  Unificado (CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO, FORMAT, MUESTRA, VALOR, ORIGEN, FECHA_COPIA, RESULTADO)
                    VALUES( """ + v_CHROM + """,
                            """ + v_POS + """,
                            """ + v_ID + """,
                            """ + v_REF + """,
                            """ + v_ALT + """,
                            """ + v_QUAL + """,
                            """ + v_FILTER + """,
                            """ + v_INFO + """,
                            """ + v_FORMAT + """,
                            """ + v_MUESTRA + """,
                            """ + v_VALOR + """,
                            """ + v_ORIGEN + """,
                            """ + v_FECHA_COPIA + """,
                            """ + v_RESULTADO + """)

                """)
            except Exception as e:

                print('FALLO EL PROGRAMA AL INSERTAR')
                codeError = str(ID_DAG_PROCESS)
                messageError= str(e.args[0])+' - Error al insertar datos en la Base de Datos'
                sysdateError = formatDatetime(str(datetime.now()))
                system_log(codeError,messageErrorm,sysdateError)

        conn.commit()
        conn.close()

    def system_log(LOG_ID, LOG_DESCRIPCION, LOG_SYSDATE):
        server = DATA_CONNECTION['SQL_SERVER_IP']
        user = DATA_CONNECTION['USER_SQL_SERVER']
        password = DATA_CONNECTION['PASSWORD_SQL_SERVER']
        conn_error = pymssql.connect(server, user, password, DATA_CONNECTION['DATABASE_SQL_SERVER'])
        cursor_error = conn_error.cursor()

        cursor_error.execute("""

            INSERT INTO Unificado_System_Log (ID, DESCRIPTION, DATE)
            VALUES('""" + LOG_ID + """','""" + LOG_DESCRIPCION + """','""" + LOG_SYSDATE + """')

        """)
        conn_error.commit()
        conn_error.close()




    print('------------------------START PROCESS-----------------------')


    print('ID_DAG_PROCESS: ',str(ID_DAG_PROCESS))


    print('GET DATA FROM DATABASE')
    df_sql_db = get_data_database(DATA_CONNECTION)
    print(df_sql_db)

    # LOGGING ON DATABASE
    system_log(str(ID_DAG_PROCESS),'Se han verificado '+str(len(df_sql_db))+' registros en la base de datos - Comienzo de proceso',formatDatetime(str(datetime.now())))



    print('GET DATA FROM CSV API')
    df_csv = get_data_from_api_csv()
    # LOGGING ON DATABASE
    system_log(str(ID_DAG_PROCESS), 'Se han consultado ' + str(len(df_csv[1])) + ' nuevos registros en la API-CSV - Nuevos Registros', formatDatetime(str(datetime.now())))

    if df_csv[0]:

        print(df_csv[1])


        print('JOIN DATA FROM DATABASE AND CSV')
        df = pd.concat([df_sql_db, df_csv[1]], axis=0)
        print(df)
        # LOGGING ON DATABASE
        system_log(str(ID_DAG_PROCESS), 'Total de registros sin quitar duplicados: ' + str(len(df)) + ' registros en la Base de Datos - Antes de Quitar Duplicados', formatDatetime(str(datetime.now())))


        print('AGRUPAR DATOS DUPLICADOS TOMANDO LA FECHA MAXIMA')
        df_sql = agrupar_datos_fecha_maxima(df)
        print(df_sql)

        # LOGGING ON DATABASE
        system_log(str(ID_DAG_PROCESS), 'Se han quitado registros duplicados y quedo un total de  ' + str(len(df_sql)) + ' registros', formatDatetime(str(datetime.now())))


        print('ACTUALIZAR DATOS EN LA BASE DE DATOS')
        validate_delete = eliminar_data_sql_server(DATA_CONNECTION)

        # LOGGING ON DATABASE
        system_log(str(ID_DAG_PROCESS), 'Se han eliminado '+str(len(df_sql_db))+' registros de la base de Datos para realizado el volcado de nuevos registros', formatDatetime(str(datetime.now())))

        if validate_delete:
            print('VALIDAR QUE SE HAN ELIMINADO BIEN LOS DATOS')
            insert_data_sql_server(df_sql,DATA_CONNECTION)
            # LOGGING ON DATABASE
            system_log(str(ID_DAG_PROCESS), 'Se han insertado '+str(len(df_sql))+' nuevos registros de la base de Datos', formatDatetime(str(datetime.now())))
        else:
            print('ERROR AL ELIMINAR DATOS DE LA BASE')
            # LOGGING ON DATABASE
            system_log(str(ID_DAG_PROCESS), 'Fallo el proceso Actualizacion en la Base de Datos', formatDatetime(str(datetime.now())))

    else:
        print('ERROR AL CONSULTAR API')
        # LOGGING ON DATABASE
        system_log(str(ID_DAG_PROCESS), 'Fallo la consulta a la API de nuevos Datos', formatDatetime(str(datetime.now())))


        print('--------------------------END PROCESS--------------------------')










default_args = {
    'owner': 'Emanuel Baquero'
}


def hello_world_loop():
    for palabra in ['hello', 'world']:
        print(palabra)


with DAG(dag_id='dag_ingest_data',
         default_args=default_args,
         start_date = datetime(2021, 7, 1, 5, 0, 0),
         schedule_interval='0 5 * * 1',
         catchup=False

         ) as dag:


    start = DummyOperator(task_id='start')


    get_data_task = PythonVirtualenvOperator(
                                                task_id='get_data_task',
                                                python_callable=global_data_ingesta,
                                                requirements=["pymssql","pandas","pandasql","requests"]
                                            )



    test_bash = BashOperator(
                                    task_id='test_bash',
                                    bash_command='echo test_bash'
                                )

start >> get_data_task >> test_bash