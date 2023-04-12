import streamlit as st
import datetime
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import SparkSession
import plotly.express as px
#import aspose.pdf as ap
#import altair as alt
#from streamlit_folium import st_folium
#import folium 
#from geopy.geocoders import Nominatim

spark = SparkSession.builder.appName('one').getOrCreate() 

def grafico(cod, dataframe, atributo, atributo_apoio, var_y):
    if cod == 'parcial':
        relatorio = dataframe.groupby(atributo)[var_y].count()
        relatorio = relatorio.reset_index()
        st.write(relatorio)
        line_fig = px.bar(relatorio, x=atributo, y=var_y, color=atributo,title="Quantidade de "+var_y+" por "+atributo, width=750, height=500)
        st.plotly_chart(line_fig, use_container_width=False, sharing='streamlit')

    elif cod == 'tudo':
         for i in range(len(atributo_apoio)):
            #st.write(atributo_apoio[i])
            relatorio = dataframe.groupby(atributo_apoio[i])[var_y].count()
            relatorio = relatorio.reset_index()
            st.write(relatorio)
                      
            line_fig = px.bar(relatorio, x=atributo_apoio[i], y=var_y, color=atributo_apoio[i],title="Quantidade de "+var_y+" por "+atributo_apoio[i], width=750, height=500)
            st.plotly_chart(line_fig, use_container_width=False, sharing='streamlit')
    elif cod == 'customizada':
        try:
            atributo1 = st.selectbox('ATRIBUTO ALVO',(atributo_apoio))
            atributo2 = st.selectbox('ATRIBUTO LEGENDA',(atributo_apoio))
              
            
            relatorio = dataframe.groupby([atributo1,atributo2])[var_y].count()
            relatorio = relatorio.reset_index()
            st.write(relatorio)
              
            line_fig = px.bar(relatorio, x=atributo1, y=var_y, color=atributo2,title="Quantidade de "+var_y+" por "+atributo1+" e "+atributo2,width=750, height=500)
            st.plotly_chart(line_fig, use_container_width=False, sharing='streamlit')           


        except:
            st.write(f'<h1 style="color:#FF0000;font-size:18px;">{"Não é possível efetuar essa customização"}</h1>', unsafe_allow_html=True)


              
             
def diaria (servico_res, periodo, data_d_str, atributo, atributo_apoio, var_y, schema_def,cod):
    try:
        dataframe_spark = spark.read.csv("hdfs://localhost:9000//root/NOKIA_Analytics/reports"+"/"+servico_res+"/"+periodo+"/"+"report"+"_"+data_d_str+"_"+servico_res,sep=',',schema=schema_def)
        dataframe = dataframe_spark.toPandas()
        
        if servico_res == 'BB':
            
            dataframe.loc[dataframe['performance_good']=='22','performance_good']='Sim'
            dataframe.loc[dataframe['performance_good']=='0','performance_good']='Não'

            dataframe.loc[dataframe['performance_bad']=='22','performance_bad']='Sim'
            dataframe.loc[dataframe['performance_bad']=='0','performance_bad']='Não'

        grafico(cod, dataframe, atributo, atributo_apoio, var_y)
                           
    except:
        st.write(f'<h1 style="color:#FF0000;font-size:18px;">{"Não há dados!"}</h1>', unsafe_allow_html=True)


               
            
def semanal (servico_res, periodo, data_s_1_str, data_s_2_str, atributo, atributo_apoio, var_y, schema_def,cod):
    try:
        dataframe_spark = spark.read.csv("hdfs://localhost:9000//root/NOKIA_Analytics/reports"+"/"+servico_res+"/"+periodo+"/"+"report"+"_"+data_s_1_str+"_"+data_s_2_str+"_"+servico_res,sep=',',schema=schema_def)
        dataframe = dataframe_spark.toPandas()
        
        if servico_res == 'BB':
            
            dataframe.loc[dataframe['performance_good']=='22','performance_good']='Sim'
            dataframe.loc[dataframe['performance_good']=='0','performance_good']='Não'

            dataframe.loc[dataframe['performance_bad']=='22','performance_bad']='Sim'
            dataframe.loc[dataframe['performance_bad']=='0','performance_bad']='Não'
            
        grafico(cod, dataframe, atributo, atributo_apoio, var_y)

    except:
            st.write(f'<h1 style="color:#FF0000;font-size:18px;">{"Não há dados!"}</h1>', unsafe_allow_html=True)


def mensal (servico_res, periodo, mes, ano, atributo, atributo_apoio, var_y, schema_def,cod):
    try:
        dataframe_spark = spark.read.csv("hdfs://localhost:9000//root/NOKIA_Analytics/reports"+"/"+servico_res+"/"+periodo+"/"+"report"+"_"+mes+"_"+ano+"_"+servico_res,sep=',',schema=schema_def)            
        dataframe = dataframe_spark.toPandas()

        if servico_res == 'BB':
            dataframe.loc[dataframe['performance_good']=='22','performance_good']='Sim'
            dataframe.loc[dataframe['performance_good']=='0','performance_good']='Não'

            dataframe.loc[dataframe['performance_bad']=='22','performance_bad']='Sim'
            dataframe.loc[dataframe['performance_bad']=='0','performance_bad']='Não'
            
            
        grafico(cod, dataframe, atributo, atributo_apoio, var_y)

    except:
        st.write(f'<h1 style="color:#FF0000;font-size:18px;">{"Não há dados!"}</h1>', unsafe_allow_html=True)
         

def analise():
    st.image("img/analise.jpg",width=740,use_column_width=True)

    servico = st.selectbox('SERVIÇO',
                              ('AAA', 'Broadband'))
    if servico == 'AAA':
        servico_res = 'AAA'
    elif servico == 'Broadband':
        servico_res = 'BB'
    
    periodo = st.selectbox('PERÍODO',
                              ('Diária','Semanal','Mensal'))
    if periodo=='Diária':
        data_d = st.date_input("DIA",
                                 datetime.date(2023, 1, 1))
    elif periodo == 'Semanal':
        data_s_1 = st.date_input("DIA INICIAL",
                                 datetime.date(2023, 1, 1))
        data_s_2 = st.date_input("DIA FINAL",
                                 datetime.date(2023, 1, 1))
    elif periodo == 'Mensal':  
        mes = st.selectbox('MÊS',
                       ('Janeiro', 'Fevereiro', 'Março','Abril','Maio','Junho','Julho','Agosto',
                        'Setembro','Outubro','Novembro','Dezembro'))
        ano = st.selectbox('ANO',range(2023,2051))
        ano = str(ano) 
        
    if servico_res == 'AAA':
        atributo_apoio=['technology','sub_technology','region','city','equip_vendor','subs_type',
                        'customer_segmentation','customer_subcategory','group_name','parent_group_name',
                        'association_name','total_failures','session_duration']
        #x (atributo_apoio)

    elif servico_res == 'BB':
        atributo_apoio=['equip_id_seq','city','equip_vendor','device_capability','subscriber_id','subs_type','customer_segmentation','customer_subcategory','group_name',
                        'parent_group_name','association_name','flbb_total_up_power_signal_level',
                        'flbb_up_power_signal_level_count','flbb_min_up_power_signal_level',
                        'flbb_max_up_power_signal_level','flbb_total_down_power_signal_level',
                        'flbb_down_power_signal_level_count','flbb_min_down_power_signal_level',
                        'flbb_max_down_power_signal_level','performance_good','performance_bad',
                        'fiber_link_status','fiber_link_quality','technology','region']
        #x (atributo_apoio)
    
    atributo = st.selectbox('ATRIBUTO',(atributo_apoio))


    def schema(cod):
        if servico_res =='AAA':
                var_y='user_id'
                schema_def = StructType()
                schema_def.add("user_id","string",True)      
                schema_def.add("technology","string",True)     
                schema_def.add("sub_technology","string",True)  
                schema_def.add("region","string",True)      
                schema_def.add("city_id","string",True)     
                schema_def.add("city","string",True)
                schema_def.add("exchange_name","string",True)      
                schema_def.add("equip_vendor","string",True)     
                schema_def.add("subs_type","string",True)
                schema_def.add("customer_segmentation","string",True)     
                schema_def.add("customer_subcategory","string",True) 
                schema_def.add("group_name","string",True)   
                schema_def.add("parent_group_name","string",True)
                schema_def.add("association_name","string",True)
                schema_def.add("total_failures","integer",True)
                schema_def.add("session_duration","integer",True) 

                

        elif servico_res == 'BB':
                var_y='equip_id'
                schema_def = StructType()
                schema_def.add("equip_id","string",True)      
                schema_def.add("equip_id_seq","string",True)               
                schema_def.add("city","string",True)
                schema_def.add("equip_vendor","string",True) 
                schema_def.add("device_capability","string",True)
                schema_def.add("subscriber_id","string",True)    
                schema_def.add("subs_type","string",True)
                schema_def.add("customer_segmentation","string",True)     
                schema_def.add("customer_subcategory","string",True) 
                schema_def.add("group_name","string",True)   
                schema_def.add("parent_group_name","string",True)
                schema_def.add("association_name","string",True)
                schema_def.add("flbb_total_up_power_signal_level",'float',True)
                schema_def.add("flbb_up_power_signal_level_count","integer",True)
                schema_def.add("flbb_min_up_power_signal_level","float",True)
                schema_def.add("flbb_max_up_power_signal_level","float",True)
                schema_def.add("flbb_total_down_power_signal_level","float",True)
                schema_def.add("flbb_down_power_signal_level_count","integer",True)
                schema_def.add("flbb_min_down_power_signal_level","float",True)
                schema_def.add("flbb_max_down_power_signal_level","float",True)                
                schema_def.add("performance_good","string",True)
                schema_def.add("performance_bad","string",True)
                schema_def.add("fiber_link_status","string",True)
                schema_def.add("fiber_link_quality","string",True)
                schema_def.add("technology","string",True)
                schema_def.add("region","string",True)

        if periodo == 'Diária':
           data_d_str = data_d.strftime('%d_%m_%Y')
           diaria (servico_res, periodo, data_d_str, atributo, atributo_apoio, var_y, schema_def,cod)
        elif periodo == 'Semanal':
           data_s_1_str = data_s_1.strftime('%d_%m_%Y')
           data_s_2_str = data_s_2.strftime('%d_%m_%Y')
           semanal (servico_res, periodo, data_s_1_str, data_s_2_str, atributo, atributo_apoio, var_y, schema_def,cod)
        elif periodo == 'Mensal':
           mensal (servico_res, periodo, mes, ano, atributo, atributo_apoio, var_y, schema_def,cod)
    
    

    tudo_atributo = st.checkbox("Todos atributos",key=1)
    if tudo_atributo:
        cod = 'tudo'
        schema(cod)  
         #customizada = st.checkbox("Análises customizadas",value=tudo_atributo,disabled=True,key='a')
              
    customizada = st.checkbox("Análises customizadas",key=2)  
    if customizada:
        cod = 'customizada'
        schema(cod)

    if st.button('Analisar',key=3):
        cod = 'parcial'
        schema(cod)     
    
    
    

          

    
    
    #if customizada: 
     #    
    #if st.button('Analisar'):
     #    cod = 'parcial'
      #   schema(cod)
                        

            #st.write(dataframe_spark)
            
            #dataframe['satisfacao'] = dataframe['satisfacao'].astype(int)
            #agrup_valor_por_regiao_sum = dataframe.groupby('regiao')['valor'].sum()
            #valor_per_regiao_sum = dataframe.groupBy('regiao').sum('valor')

            #agrup_regiao_por_usuario_count = dataframe.groupby('regiao')['user'].count()
            #regiao_per_user_count = dataframe.groupby('customer_subcategory')['user_id'].count()
            #regiao_per_user_count = dataframe_spark.groupBy('customer_subcategory').count()
            #regiao_per_user_count.reset_index(inplace=True)
            #regiao_per_user_count = regiao_per_user_count.reset_index()
            #st.write(regiao_per_user_count)

            #agrup_satisfacao = dataframe.groupby('leg_satisfacao')['satisfacao'].count()

            #RESET INDEX
            
            #agrup_valor_por_regiao_sum = agrup_valor_por_regiao_sum.reset_index()
            #agrup_regiao_por_usuario_count = agrup_regiao_por_usuario_count.reset_index()
            #agrup_satisfacao = agrup_satisfacao.reset_index()

            

            #st.markdown("<h7 style='text-align: center; color: white;'>Quantidade de clientes por região</h7>", unsafe_allow_html=True)
            #st.bar_chart(agrup_regiao_por_usuario_count,x='regiao',y='user')
            #c = alt.Chart(agrup_regiao_por_usuario_count).mark_bar().encode(
            #x=alt.X('user:Q'),y=alt.Y('regiao:N')).properties(height=500)
            #st.altair_chart(c, use_container_width=True)

            #st.markdown("<h7 style='text-align: center; color: white;'>Distribuição da satisfação dos clientes</h7>", unsafe_allow_html=True)
            #pie_fig = px.pie(agrup_satisfacao, values='satisfacao', names='leg_satisfacao')
            #st.plotly_chart(pie_fig)         

            #elif periodo == 'Semanal':
             #   data_s_1_str = data_s_1.strftime('%d_%m_%Y')
             #   data_s_2_str = data_s_2.strftime('%d_%m_%Y')
             
             #  dataframe_spark = spark.read.csv("hdfs://localhost:9000//root/NOKIA_Analytics/reports"+"/"+servico_res+"/"+periodo+"/"+"report"+"_"+data_s_1_str+"_"+data_s_2_str+"_"+servico_res,sep=',',schema=schema_def)
            #elif periodo == 'Mensal':
               # dataframe_spark = spark.read.csv("hdfs://localhost:9000//root/NOKIA_Analytics/reports"+"/"+servico_res+"/"+periodo+"/"+"report"+"_"+mes+"_"+servico_res,sep=',',schema=schema_def)

            

            
                
                #RELATÓRIO
            
            

            #st.markdown("<h7 style='text-align: center; color: white;'>Quantidade de usuário por sub-tipo</h7>", unsafe_allow_html=True)
            


                #QUANTIDADE REGIÃO
               ### qtd_regiao = dataframe.groupby('region')['user_id'].count()
                #qtd_regiao = qtd_regiao.reset_index()

                #st.markdown("<h7 style='text-align: center; color: white;'>Quantidade de clientes por região</h7>", unsafe_allow_html=True)
                #st.bar_chart(qtd_regiao,x='region',y='user_id')
                
                #c = alt.Chart(qtd_regiao).mark_bar().encode(x=alt.X('user_id:Q'),y=alt.Y('region:N')).properties(height=500)
                #st.altair_chart(c, use_container_width=True)

                #QUANTIDADE EQUIPE VENDOR
                #qtd_vendor = dataframe.groupby('equip_vendor')['user_id'].count()
                #qtd_vendor = qtd_vendor.reset_index()

                #st.write(qtd_vendor)

                #QUANTIDADE DE SEGMENTO DE CLIENTE
                #qtd_vendor1 = dataframe.groupby('customer_segmentation')['user_id'].count()
                #qtd_vendor1 = qtd_vendor1.reset_index()

                #st.write(qtd_vendor1)    