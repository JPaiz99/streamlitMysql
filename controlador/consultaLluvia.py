# created by Joel Paiz: hpaiz@imsa.com.gt / jpaizb91@gmail.com
# Automatizacion agricola 2022

import pandas as pd
import pymysql
import streamlit as st
from datetime import datetime, date, time, timedelta
import variables.mysqldb as dbu


def consulta_lluvia_administrativo(dia, mes, año, ciclo):
    total = 0.0
    m = mes
    a = año
    d = dia
    clicks_ant = 0.0
    hi = "00:00:00"
    hf = "23:59:59"
    ar = []
    ar2 = []
    fecha = ''
    cont = 0
    while d <= ciclo:
        f = str(a) + "-" + str(m) + "-" + str(d)
        try:
            conn = pymysql.connect(host=dbu.host, user=dbu.user, password=dbu.passw, db=dbu.db, port=int(dbu.port))
            cur = conn.cursor()

            sql = '''
                   select clicks, fecha_cap 
                   from data_sensor_lluviaradiacion 
                   where fecha_cap between ''' + "'" + f + " " + hi + "'" + ''' and ''' + "'" + f + " " + hf + "'" + ''' 
                   order by fecha_cap;
               '''
            cur.execute(sql)
            for data in cur.fetchall():
                data_list = list(data)
                if cont == 0:
                    clicks_ant = data_list[0]
                    cont = 1
                else:
                    if data_list[0] == fecha:
                        fecha = data_list[1] + timedelta(days=1)
                        total = 0.0

                    else:
                        clicks = data_list[0]
                        fecha = data_list[1]
                        lluvia = float(clicks) - float(clicks_ant)
                        lluvia = lluvia * 0.1
                        total = total + lluvia
                        clicks_ant = clicks
            # st.write(str(total) + " " + str(fecha))
            ar.append(total)
            ar2.append(fecha.strftime('%d/%m/%Y'))
            conn.close()
            d = d + 1
            df = pd.DataFrame(data=ar)
            total = 0
        except pymysql.Error as ERROR:
            conn.close()
        # df['Pluviometro optico'] = ar
    newdf = df.transpose()
    st.dataframe(newdf)
    st.line_chart(df)
    csv = convert_df(newdf)
    st.download_button(
        label="Descargar como CSV",
        data=csv,
        file_name='lamina de lluvia de junio .csv',
        mime='text/csv',
    )


def consulta_lluvia_optico(dia, mes, año, ciclo):
    total = 0
    m = mes
    a = año
    d = dia

    hi = "00:00:00"
    hf = "23:59:59"
    ar = []
    ar2 = []
    df = pd.DataFrame(index=('%d' % i for i in range(dia, ciclo + 1)))
    while d <= ciclo:
        f = str(a) + "-" + str(m) + "-" + str(d)
        try:
            conn = pymysql.connect(host=dbu.host, user=dbu.user, password=dbu.passw, db=dbu.db, port=int(dbu.port))
            cur = conn.cursor()

            sql = '''
                select dato1, fecha_cap 
                from prueba 
                where fecha_cap between ''' + "'" + f + " " + hi + "'" + ''' and ''' + "'" + f + " " + hf + "'" + '''
                and dato1 !=0 and dato1<100;
            '''
            cur.execute(sql)
            for data in cur.fetchall():
                data_list = list(data)
                lluvia = data_list[0]
                fecha = data_list[1]
                total = total + float(lluvia)

            ar.append(total)
            ar2.append(fecha.strftime('%d/%m/%Y'))
            conn.close()
            d = d + 1
            # df = pd.DataFrame(data=ar)
            total = 0
        except pymysql.Error as ERROR:
            conn.close()
    df['Optico'] = ar
    newdf = df.transpose()
    st.dataframe(newdf)
    st.line_chart(df)
    csv = convert_df(newdf)
    st.download_button(
        label="Descargar como CSV",
        data=csv,
        file_name='lamina de lluvia de junio .csv',
        mime='text/csv',
    )


def consulta_porDia(dia, mes, año):
    total = 0
    m = mes
    a = año
    d = dia
    cont = 0
    hi = "00:00:00"
    hf = "23:59:59"
    ar = []
    ar2 = []
    valor = ""
    df = pd.DataFrame()

    # while d <= ciclo:
    f = str(a) + "-" + str(m) + "-" + str(d)
    fechita = []
    fechita.append(f)
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
            if cont == 0:
                ar.append(lluvia)
                ar2.append(fecha.strftime('%H:%M:%S'))
                df = pd.DataFrame(data=ar, index=ar2)
                valor = fecha.strftime('%H:%M:%S')
                # st.write('primero')
                cont = 1
            else:
                if fecha.strftime('%H:%M:%S') == valor:
                    ar.append(lluvia)
                    ar2.append(fecha.strftime('%H:%M:%S mismo'))
                    df = pd.DataFrame(data=ar, index=ar2)
                    valor = fecha.strftime('%H:%M:%S')
                    # st.write('iguales')
                else:
                    ar.append(lluvia)
                    ar2.append(fecha.strftime('%H:%M:%S'))
                    df = pd.DataFrame(data=ar, index=ar2)
                    valor = fecha.strftime('%H:%M:%S')
                    # st.write('paso bien')

        ar.append(total)

        conn.close()
        d = d + 1

        total = 0
    except pymysql.Error as ERROR:
        print(ERROR)

    # df['Pluviometro optico'] = ar2
    newdf = df.transpose()
    st.dataframe(newdf)
    # st.bar_chart(df)
    csv = convert_df(newdf)
    st.download_button(
        label="Descargar como CSV",
        data=csv,
        file_name='lamina de lluvia.csv',
        mime='text/csv',
    )


def PaquetesXdia(dia, mes, año, ciclo):
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
                select fecha_cap
                from prueba 
                where fecha_cap between ''' + "'" + f + " " + hi + "'" + ''' and ''' + "'" + f + " " + hf + "'" + '''         
            '''
            # st.write(f)
            cur.execute(sql)
            data = cur.fetchall()
            # st.write(data)
            total = len(data)

            ar.append(total)
            # st.write(ar)
            conn.close()
            d = d + 1
            #total = 0

        except pymysql.Error as ERROR:
            print(ERROR)

    df['Pluviometro optico'] = ar
    newdf = df.transpose()
    st.dataframe(newdf)
    # st.bar_chart(df)
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
    ch2 = st.radio("Selecione", ('seleccione', 'Optico', 'Administrativo'))

    if ch2 == 'Optico':
        ch = st.radio("Periodo", ('por dia', 'rango de fechas', 'paquetes por dia'))
        if ch == 'por dia':
            d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
            if st.button('Aceptar'):
                with st.spinner('Cargando...'):
                    consulta_porDia(d.day, d.month, d.year)
        elif ch == 'paquetes por dia':
            d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
            d2 = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_fin")
            if st.button('Paquetes'):
                with st.spinner('Cargando...'):
                    PaquetesXdia(d.day, d.month, d.year, d2.day)
        else:
            d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
            d2 = st.date_input(label="INGRESE FECHA FINAL", key="fecha_fin")
            if st.button('Rango fechas'):
                with st.spinner('Cargando...'):
                    consulta_lluvia_optico(d.day, d.month, d.year, d2.day)
    elif ch2 == 'Administrativo':
        ch = st.radio("Periodo", ('por dia', 'rango de fechas'))
        if ch == 'por dia':
            d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
            if st.button('Aceptar'):
                with st.spinner('Cargando...'):
                    consulta_porDia(d.day, d.month, d.year)
        else:
            d = st.date_input(label="INGRESE FECHA INICIAL", key="fecha_ini")
            d2 = st.date_input(label="INGRESE FECHA FINAL", key="fecha_fin")

            if st.button('admin'):
                with st.spinner('Cargando...'):
                    consulta_lluvia_administrativo(d.day, d.month, d.year, d2.day)

        # st.balloons()
