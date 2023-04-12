import streamlit as st
import datetime
import pandas as pd
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def ingestao_servicos_diaria(dataframe_spark,etl_dataframe_spark,periodo,servico_res,data_d_str):
    dataframe_spark.write.csv("hdfs://localhost:9000//root/NOKIA_Analytics/raw"+"/"+servico_res+"/"+periodo+"/"+"raw"+"_"+data_d_str+"_"+servico_res)
    etl_dataframe_spark.write.csv("hdfs://localhost:9000//root/NOKIA_Analytics/reports"+"/"+servico_res+"/"+periodo+"/"+"report"+"_"+data_d_str+"_"+servico_res)
    st.write(dataframe_spark)
    st.write(etl_dataframe_spark)
    st.write(f'<h1 style="color:#33ff33;font-size:18px;">{"Base de Dados enviada com sucesso!"}</h1>', unsafe_allow_html=True)

def ingestao_servicos_semanal(dataframe_spark,etl_dataframe_spark,periodo,servico_res,data_s_1_str,data_s_2_str):
    dataframe_spark.write.csv("hdfs://localhost:9000//root/NOKIA_Analytics/raw"+"/"+servico_res+"/"+periodo+"/"+"raw"+"_"+data_s_1_str+"_"+data_s_2_str+"_"+servico_res)
    etl_dataframe_spark.write.csv("hdfs://localhost:9000//root/NOKIA_Analytics/reports"+"/"+servico_res+"/"+periodo+"/"+"report"+"_"+data_s_1_str+"_"+data_s_2_str+"_"+servico_res)  
    st.write(dataframe_spark)
    st.write(etl_dataframe_spark)
    st.write(f'<h1 style="color:#33ff33;font-size:18px;">{"Base de Dados enviada com sucesso!"}</h1>', unsafe_allow_html=True)


def ingestao_servicos_mensal(dataframe_spark,etl_dataframe_spark,periodo,servico_res,mes,ano):
    dataframe_spark.write.csv("hdfs://localhost:9000//root/NOKIA_Analytics/raw"+"/"+servico_res+"/"+periodo+"/"+"raw"+"_"+mes+"_"+ano+"_"+servico_res)
    etl_dataframe_spark.write.csv("hdfs://localhost:9000//root/NOKIA_Analytics/reports"+"/"+servico_res+"/"+periodo+"/"+"report"+"_"+mes+"_"+ano+"_"+servico_res)
    st.write(dataframe_spark)
    st.write(etl_dataframe_spark)
    st.write(f'<h1 style="color:#33ff33;font-size:18px;">{"Base de Dados enviada com sucesso!"}</h1>', unsafe_allow_html=True)


#def etl(dataframe_spark,lista_colunas_drop,periodo,servico_res,data_d_str,data_s_1_str,data_s_2_str,mes):
 #   etl_dataframe_spark=dataframe_spark.drop(*lista_colunas_drop)
  #  if periodo == 'Diária':
   #     ingestao_servicos_diaria(dataframe_spark,etl_dataframe_spark,periodo,servico_res,data_d_str)
    #elif periodo == 'Semanal':
     #   ingestao_servicos_semanal(dataframe_spark,etl_dataframe_spark,periodo,servico_res,data_s_1_str,data_s_2_str)
    #elif periodo== 'Mensal':
     #   ingestao_servicos_mensal(dataframe_spark,etl_dataframe_spark,periodo,servico_res,mes)
    
    #st.write(dataframe_spark)
    #st.write(etl_dataframe_spark)
    

def ingestao():
    col1, col2, col3 = st.columns([1,6,1])
    with col1:
        st.write("")
    with col2:
        st.image("img/xlsx_csv_parquet.jpg")
    with col3:
        st.write("")

    servico = st.radio("SERVIÇO",
                       ('AAA', 'Broadband'))
    if servico == 'AAA':
        servico_res = 'AAA'
    elif servico == 'Broadband':
        servico_res = 'BB'
    
    periodo = st.selectbox('PERÍODO',
                       ('Diária', 'Semanal', 'Mensal')) 
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

        
    carregamento_arquivo = st.file_uploader("Anexe o arquivo para ingestão de dados")
    if carregamento_arquivo is not None:
        #bytes_arquivo = carregamento_arquivo.getvalue()
        dataframe = pd.read_csv(carregamento_arquivo, encoding = "latin-1", on_bad_lines='skip', sep=';')
        #pd.set_option("display.precision", 2)
        #dataframe['flbb_total_up_power_signal_level'] = dataframe['flbb_total_up_power_signal_level'].astype()        

        if st.button('Ingerir Dados'):
                #try:
                    spark = SparkSession.builder.appName('one').getOrCreate()  
                    dataframe_spark=spark.createDataFrame(dataframe)
        #udf = F.UserDefinedFunction(lambda x: x.replace(",","."), T.StringType())
        #out = dataframe_spark.withColumn("valor", udf(F.col("valor")).cast(T.FloatType()))
        #etl_dataframe_spark = out.withColumn("valor",F.round(out["valor"]))

    
    
        #st.write("Base de Dados enviada com sucesso!")      
        
                    if servico_res == 'AAA':
                        lista_colunas_drop=('sub_technology_id','area_id','area','access_node_id',
                                'access_node_name','access_node_latitude','access_node_longitude',
                                'hub_id','hub_name','hub_latitude','hub_longitude','exchange_id',
                                'exchange_latitude','exchange_longitude','equip_id','equip_latitude',
                                'equip_longitude','equip_model','device_capability','device_equip_id',
                                'device_softwareversion','subscriber_id','spare_dim_1','spare_dim_2',
                                'spare_dim_3','spare_dim_4','spare_dim_5','spare_dim_6','spare_dim_7',
                                'spare_dim_8','spare_dim_9','spare_dim_10','sessions_established_orig',
                                'sessions_established','sessions_dropped','sessions_terminated_by_user',
                                'authentication_attempts','authentication_failures','redirects_to_landing_page',
                                'sessions_count','dt','tz')
            
                    elif servico_res == 'BB':
                        lista_colunas_drop=('technology_id','city_id','area_id','area','access_node_id','access_node_name','access_node_latitude',
                                'access_node_longitude','hub_id','hub_name','hub_latitude','hub_longitude',
                                'hub_interface_name','exchange_id','exchange_name','equip_model','cust_first_nm','cust_last_nm',
                                'subscriber_address','contract_id','contract_category','equip_latitude','equip_longitude',
                                'exchange_latitude','exchange_longitude','source_id','spare_ps_bb_segg_1','spare_ps_bb_segg_2',
                                'spare_ps_bb_segg_3','spare_ps_bb_segg_4','spare_ps_bb_segg_5','spare_ps_bb_segg_6','spare_ps_bb_segg_7',
                                'spare_ps_bb_segg_8','spare_ps_bb_segg_9','spare_ps_bb_segg_10','flbb_up_traffic_bytes','flbb_up_traffic_pkts',
                                'flbb_down_traffic_bytes','flbb_down_traffic_pkts','flbb_up_drop_pkts','flbb_down_drop_pkts','flbb_up_delay_pkts',
                                'flbb_down_delay_pkts','flbb_total_up_snr','flbb_up_snr_count','flbb_min_up_snr','flbb_max_up_snr','flbb_total_down_snr',
                                'flbb_down_snr_count','flbb_min_down_snr','flbb_max_down_snr','flbb_corrected_codewords','flbb_uncorrected_codewords',
                                'flbb_unerrored_codewords','flbb_max_subs_equip_uptime','flbb_subs_equip_restarts','flbb_sessions','kpi_spare_1',
                                'sub_technology_id','sub_technology','mtbe','mtbe_count','mtbe_ds','mtbe_ds_count','mtbe_us','mtbe_us_count','mtbr','mtbr_count',
                                'attenuation_ds','attenuation_ds_count','attenuation_us','attenuation_us_count','estim_line_dist','elec_length','elec_length_count',
                                'actual_bitrate_ds','actual_bitrate_ds_count','actual_bitrate_us','actual_bitrate_us_count','attainable_bitrate_ds',
                                'attainable_bitrate_ds_count','attainable_bitrate_us','attainable_bitrate_us_count','bec_us','bec_ds','stability_stable',
                                'stability_unstable','stability_overall','performance_overall','quality_score',
                                'quality_score_count','device_equip_id','device_softwareversion','flbb_down_power_signal_level_poor','flbb_down_power_signal_level_satisfactory','flbb_down_power_signal_level_good',
                                'flbb_up_power_signal_level_poor','flbb_up_power_signal_level_satisfactory','flbb_up_power_signal_level_good','dt','tz')
    
                    etl_dataframe_spark=dataframe_spark.drop(*lista_colunas_drop)
                    

                    if periodo == 'Diária':
                        data_d_str = data_d.strftime('%d_%m_%Y')
                        ingestao_servicos_diaria(dataframe_spark,etl_dataframe_spark,periodo,servico_res,data_d_str)
                    elif periodo == 'Semanal':
                        data_s_1_str = data_s_1.strftime('%d_%m_%Y')
                        data_s_2_str = data_s_2.strftime('%d_%m_%Y')
                        ingestao_servicos_semanal(dataframe_spark,etl_dataframe_spark,periodo,servico_res,data_s_1_str,data_s_2_str)
                    elif periodo == 'Mensal':
                        ingestao_servicos_mensal(dataframe_spark,etl_dataframe_spark,periodo,servico_res,mes,ano)

            
                #except:
                 #   st.write("Há inconsistência de informações e/ou modelagem de dados!")


    
            
            
            #etl_dataframe_spark.write.csv("hdfs://localhost:9000//root/NOKIA_Analytics/report/diaria/"+"report"+"_"+data_d_str+"_"+servico)
        #novo = out1.withColumn("valor",F.round(out1["valor"]))
        #novo = out.withColumn("valor",F.round(out.valor.cast(T.DoubleType())))
            #
        
            