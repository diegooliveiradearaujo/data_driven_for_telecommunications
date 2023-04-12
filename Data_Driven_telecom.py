import streamlit as st
import telas.ingestao as ingestao
import telas.home as home
import telas.analise as analise 
import telas.ia as ia

st.sidebar.title("Seja bem-vindo ao Sistema que revoluciona a Telecomunicações")
selection = st.sidebar.selectbox('Escolha uma opção',['Início','Ingestão de Dados','Análise de Dados','Inteligência Artificial'])

if selection == 'Início':
    home.home() 
elif selection == 'Ingestão de Dados':
    ingestao.ingestao()      
elif selection == 'Análise de Dados':
    analise.analise()    
elif selection == 'Inteligência Artificial':
    ia.ia()    
     