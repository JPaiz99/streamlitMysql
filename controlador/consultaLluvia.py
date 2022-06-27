# created by Joel Paiz: hpaiz@imsa.com.gt / jpaizb91@gmail.com
# Automatizacion agricola 2022

import variables.mysqldb as dbu
import streamlit as st
import pandas as pd
import numpy as np
import pymysql


def consulta_lluvia(dia, mes, año, ciclo):
    total = 0
    m = mes
    a = año
    d = dia

    hi = "00:00:00"
    hf = "23:59:59"
    ar = []
    df = pd.DataFrame(index=('%d' % i for i in range(dia, ciclo + 1)))
    while d <= ciclo:
        f = str(a) + "-" + str(m) + "-" + str(d)
        try:
            conn = pymysql.connect(host=dbu.host, user=dbu.user, password=dbu.passw, db=dbu.db, port=int(dbu.port))
            cur = conn.cursor()

            sql = '''
                select dato1 
                from prueba 
                where fecha_cap between ''' + "'" + f + " " + hi + "'" + ''' and ''' + "'" + f + " " + hf + "'" + '''
                and dato1 !=0 and dato1<100;
            '''
            cur.execute(sql)
            for data in cur.fetchall():
                data_list = list(data)
                lluvia = data_list[0]
                total = total + float(lluvia)

            ar.append(total)
            conn.close()
            d = d + 1

            total = 0
        except pymysql.Error as ERROR:
            conn.close()
    df['Pluviometro optico'] = ar
    newdf = df.transpose()
    st.dataframe(newdf)
    st.bar_chart(df)
    csv = convert_df(newdf)
    st.download_button(
        label="Descargar como CSV",
        data=csv,
        file_name='lamina de lluvia de junio .csv',
        mime='text/csv',
    )


def consulta_lluvia2(dia, mes, año, ciclo):
    total = 0
    m = mes
    a = año
    d = dia

    hi = "00:00:00"
    hf = "23:59:59"
    ar = []
    ar2 = []
    # df = pd.DataFrame(index=('%d' % i for i in range(dia, ciclo + 1)))

    # while d <= ciclo:
    f = str(a) + "-" + str(m) + "-" + str(d)
    try:
        conn = pymysql.connect(host=dbu.host, user=dbu.user, password=dbu.passw, db=dbu.db, port=int(dbu.port))
        cur = conn.cursor()

        sql = '''
            select dato1, fecha_cap 
            from prueba 
            where fecha_cap between ''' + "'" + f + " " + hi + "'" + ''' and ''' + "'" + f + " " + hf + "'" + '''
            and dato1<100;
        '''
        cur.execute(sql)
        for data in cur.fetchall():
            data_list = list(data)
            lluvia = data_list[0]
            fecha = data_list[1]
            # total = total + float(lluvia)
            ar.append(lluvia)
            df = pd.DataFrame(ar)

        ar.append(total)
        conn.close()
        d = d + 1

        total = 0
    except pymysql.Error as ERROR:
        conn.close()

    # df['Pluviometro optico'] = ar
    newdf = df.transpose()
    st.dataframe(newdf)
    st.bar_chart(df)
    csv = convert_df(newdf)
    st.download_button(
        label="Descargar como CSV",
        data=csv,
        file_name='lamina de lluvia de junio .csv',
        mime='text/csv',
    )


def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


def main():
    st.title('Reporte express')

    d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
    d2 = st.date_input(label="INGRESE FECHA FINAL", key="fecha_fin")

    if st.button('Aceptar'):
        with st.spinner('Cargando...'):
            consulta_lluvia2(d.day, d.month, d.year, d2.day)

        # st.balloons()
