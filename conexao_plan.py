import gspread
from google.oauth2 import service_account
import pandas as pd
from datetime import date, timedelta
from pandas.tseries.offsets import BDay
import numpy as np
import requests
import os
import time
import re



#Configuração inicial
service_account_info = ["GOOGLE_SERVICE_ACCOUNT"]
scope = ['https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive"]


credentials = service_account.Credentials.from_service_account_file('credentials.json', scopes=scope)

client = gspread.authorize(credentials)


def busca_cargas(data_inicio,data_final):

    #Nessa função vou ter retorno de todas as carretas e a quantidade que foram filtradas com o intervalo de datas proposto

    # url = "https://cemagprod.onrender.com/api/publica/apontamento/tempo-processo-montagem"

    # response = requests.get(url)

    # if response.status_code == 200:
    #     dados = response.json()
    #     print(dados)
    # else:
    #     print(f'Erro na requisição: {response.status_code}')

    #ID planilha
    sheet_id = '1olnMhK7OI6W0eJ-dvsi3Lku5eCYqlpzTGJfh1Q7Pv9I'

    #Abrindo a planilha pelo ID

    sh = client.open_by_key(sheet_id)
    #worksheet_name
    wks = sh.worksheet('Importar Dados')
    list1 = wks.get_all_values()

    itens = pd.DataFrame(list1)
    itens.columns = itens.iloc[0]
    itens = itens.drop(index=0)


    #Pegando somente as colunas de interesse
    itens = itens[['PED_PREVISAOEMISSAODOC','PED_RECURSO.CODIGO','PED_QUANTIDADE','Carga']]
    itens.replace("", np.nan, inplace=True)  # Substitui string vazia por NaN
    itens = itens.dropna(subset=['PED_PREVISAOEMISSAODOC'])
    itens['PED_PREVISAOEMISSAODOC'] = pd.to_datetime(itens['PED_PREVISAOEMISSAODOC'], dayfirst=True, errors='coerce')
    itens['PED_QUANTIDADE'] = pd.to_numeric(itens['PED_QUANTIDADE'].str.replace(',','.'))
    
    # Agrupando pela Data e pelo Codigo Carreta

    # Filtrando o DataFrame para pegar as linhas dentro do intervalo de datas
    itens_filtrados = itens[(itens['PED_PREVISAOEMISSAODOC'] >= data_inicio) & (itens['PED_PREVISAOEMISSAODOC'] <= data_final)]

    itens_filtrados['PED_RECURSO.CODIGO'] = itens_filtrados['PED_RECURSO.CODIGO'].str.upper()
    itens_filtrados['Carga'] = itens_filtrados['Carga'].str.upper()

    itens_filtrados = itens_filtrados.groupby(['PED_RECURSO.CODIGO','PED_PREVISAOEMISSAODOC','Carga']).agg({
            'PED_QUANTIDADE': 'sum'
        }).reset_index()
    
    print('itens-------')
    print(itens_filtrados)

    # itens_filtrados.to_excel(r'C:\Users\TIDEV\Desktop\cargas_filtradas.xlsx',index=False)
    #Desconsiderar os códigos de cores VJ, VM, AN, LC, LJ, AM
    codigos_desconsiderados = ['VJ', 'VM', 'AN', 'LC', 'LJ', 'AM','AV']

    # Criando o padrão regex para corresponder a qualquer um desses códigos no final da string
    padrao = r'(' + '|'.join(codigos_desconsiderados) + r')$'

    # Remover os códigos indesejados (substituindo por uma string vazia)
    itens_filtrados.loc[:, 'PED_RECURSO.CODIGO'] = itens_filtrados['PED_RECURSO.CODIGO'].str.replace(padrao, '', regex=True)
    # Removendo espaços que ficaram
    itens_filtrados['PED_RECURSO.CODIGO'] = itens_filtrados['PED_RECURSO.CODIGO'].str.strip()

    # itens_filtrados.to_excel(r'C:\Users\TIDEV\Desktop\cargas_filtradas.xlsx',index=False)

    return itens_filtrados

def conectar_com_base(cargas_filtradas):
    #Nessa função vou ter retorno de tudo (com as quantidades) o que é preciso pra fazer todas as carretas que foram filtradas entre a o intervalo de datas proposto

    #ID planilha
    sheet_id = '1n2J6n_VxOsVxY5ikjJeDGva7oHTUJOlzadFfUbJnaSE'

    #Abrindo a planilha pelo ID

    sh = client.open_by_key(sheet_id)
    #worksheet_name
    wks = sh.worksheet('BASE')
    list1 = wks.get_all_values()

    itens = pd.DataFrame(list1)
    itens.columns = itens.iloc[0]
    itens = itens.drop(index=0)

    itens = itens.drop_duplicates()
    
    itens = pd.merge(cargas_filtradas,itens,left_on='PED_RECURSO.CODIGO',right_on='carreta',how='left')

    itens = itens[itens['PRIMEIRO PROCESSO'] == 'MONTAR']

    #Desconsiderar o que tiver COMPLETA E ACESSORIOS na descricao
    # itens[','] = itens[','].str.upper()
    itens['DESCRICAO'] = itens['DESCRIÇÃO']
    itens = itens[(~itens['DESCRICAO'].str.contains('ACESSORIO',regex=True)) & (~itens['DESCRICAO'].str.contains('COMPLETA',regex=True)) & (~itens['DESCRICAO'].str.contains('ACESSÓRIO',regex=True)) & (~itens['DESCRICAO'].str.contains('COMPLETO',regex=True))]

    itens['TOTAL'] = pd.to_numeric(itens['TOTAL'])
    itens['QTD'] = itens['PED_QUANTIDADE'] * itens['TOTAL']
    itens['QTD_ORIGINAL'] = itens['PED_QUANTIDADE'] * itens['TOTAL']
    

    colunas_desejadas = ['PED_PREVISAOEMISSAODOC','carreta','DESCRICAO','QTD','QTD_ORIGINAL','COD','Carga']

    conjuntos_filtrados = itens[colunas_desejadas]

    # conjuntos_filtrados.to_excel(r'C:\Users\TIDEV\Desktop\conjuntos_filtrados.xlsx',index=False)
    #Colunas Finais: Código, Descrição, quantidade de conjunto, data da carga
    return conjuntos_filtrados

def parse_data_condicional(data_str):
    if pd.isna(data_str):
        return pd.NaT

    data_str = str(data_str).strip()

    if data_str in ["?", "N/A", "Erro", "", "nan", "-"]:
        return pd.NaT

    if 'T' in data_str:
        data = pd.to_datetime(data_str.replace('Z', ''), utc=True, errors='coerce')
    else:
        data = pd.to_datetime(data_str, dayfirst=True, errors='coerce')

    if pd.isna(data):
        return pd.NaT

    # Remove o timezone se houver
    if data.tzinfo:
        data = data.tz_convert(None)

    # Normaliza para remover hora
    return data.normalize()


# Função que simula o DIATRABALHO (conta só dias úteis)
def dias_uteis(data_inicial, dias):
    data = data_inicial
    cont = 0
    passo = 1 if dias >= 0 else -1
    while cont < abs(int(dias)):
        data += timedelta(days=passo)
        if data.weekday() < 5:  # Segunda a sexta (0 a 4)
            cont += 1
    return data

# Função que replica a lógica da fórmula do Sheets de cores
def calcular_cor(row):

    etapa = row['Local']
    data = pd.to_datetime(row['Data_COR'], dayfirst=True, errors='coerce')
    emissao = pd.to_datetime(row['OPCIONAL_6_COR'], dayfirst=True, errors='coerce')
    entrega = pd.to_datetime(row['Data_de_Entrega_COR'], dayfirst=True, errors='coerce')

    # Define qual opcional usar
    if etapa == 'MONTAGEM':
        dias_op = pd.to_numeric(row['OPCIONAL_3_COR'])
    elif etapa == 'SOLDA':
        dias_op = pd.to_numeric(row['OPCIONAL_4_COR'])
    elif etapa == 'PINTURA':
        dias_op = pd.to_numeric(row['OPCIONAL_5_COR'])
    else:
        return np.nan

    if dias_op == 0:
        return '6.CINZA'
    if pd.isna(data) or pd.isna(emissao) or pd.isna(entrega):
        return ''
    if data < emissao:
        return '5.AZUL'
    if emissao <= data <= dias_uteis(emissao, dias_op * 0.33):
        return '4.VERDE'
    if dias_uteis(emissao, dias_op * 0.33) < data <= dias_uteis(emissao, dias_op * 0.66):
        return '3.AMARELO'
    if dias_uteis(emissao, dias_op * 0.66) < data <= dias_uteis(emissao, dias_op * 1) and data <= entrega:
        return '2.VERMELHO'
    if data > entrega:
        return '1.PRETO'
    return ''

def definir_leadtime(conjuntos):

    print(conjuntos)

    time.sleep(1)

    if conjuntos.empty:
        return conjuntos


    #ID planilha lead time
    sheet_id = '1yTQE0tUxiYHKXaACfay5iqGYzl-E01CMMio9Ou3uK2w'

    #ID planilha apontamento
    sheet_id_apontamento = '1x26yfwoF7peeb59yJuJuxCQNlqjCjh65NYS1RIrC0Zc'

    #ID planilha tempos montagem/solda - Pintura
    sheet_id_tempos_montagem = '12o38c0nYy4VEhtEu7ixuEyUOLT9ms13sUaPnlEniAGM'

    #ID planilha APONTAMENTO SOLDA
    sheet_id_apontamento_solda = '1XNuXhsDrOUjV0JWuZgo584izugNDgVhY06AIllkwspk'


    #Abrindo a planilha lead time
    sh_leadtime = client.open_by_key(sheet_id)

    # Abrindo a planilha de Apontamento Montagem
    sh_apontamento = client.open_by_key(sheet_id_apontamento)

    # Abrindo a planilha de tempos montagem/solda
    sh_tempos_montagem_solda = client.open_by_key(sheet_id_tempos_montagem)

    #Abrindo a planilha apontamento de solda
    sh_apontamento_solda = client.open_by_key(sheet_id_apontamento_solda)
    
    #PLANILHA APONTAMENTO SOLDA
    wks_apontamento_solda = sh_apontamento_solda.worksheet('RQ PCP-031-000 (APONTAMENTO SOLDA)')
    list_apontamento_solda = wks_apontamento_solda.get_all_values()

    # PLANILHA TEMPOS SOLDA/MONTAGEM (ABA dados)
    wks_tempos_solda_montagem = sh_tempos_montagem_solda.worksheet('dados')
    list_tempos_solda_montagem = wks_tempos_solda_montagem.get_all_values()

    # PLANILHA TEMPOS SOLDA/MONTAGEM (ABA dados pintura)
    wks_tempos_pintura = sh_tempos_montagem_solda.worksheet('dados pintura')
    list_tempos_pintura = wks_tempos_pintura.get_all_values()

    #Tratando o df para pegar as colunas da TEMPOS SOLDA/MONTAGEM
    itens_tempos_montagem = pd.DataFrame(list_tempos_solda_montagem)
    itens_tempos_montagem.columns = itens_tempos_montagem.iloc[0]
    itens_tempos_montagem = itens_tempos_montagem.drop(index=0)

    itens_tempos_montagem['data_carga'] = pd.to_datetime(itens_tempos_montagem['data_carga'],dayfirst=True)

    itens_tempos_montagem = itens_tempos_montagem[['codigo','data_inicio','data_fim_tratada','data_carga','qt_planejada','qt_apontada','status']]
    itens_tempos_montagem['codigo'] = itens_tempos_montagem['codigo'].str.split('-').str[0].str.strip()  # Pega somente o código antes do traço
    itens_tempos_montagem['id'] = ''

    #Tratando o df de pintura para pegar as colunas de dados Pintura

    itens_pintura = pd.DataFrame(list_tempos_pintura)
    itens_pintura.columns = itens_pintura.iloc[0]
    itens_pintura = itens_pintura.drop(index=0)


    # Trocando os valores das colunas de pintura para fazer o concat
    itens_pintura['id'] = itens_pintura['ID']
    itens_pintura['codigo'] = itens_pintura['CODIGO']
    itens_pintura['data_inicio'] = itens_pintura['DATA_INICIO']
    itens_pintura['data_fim_tratada'] = itens_pintura['DATA_FINALIZADA']
    itens_pintura['data_carga'] = itens_pintura['DATA_CARGA']
    itens_pintura['qt_planejada'] = itens_pintura['QT_PLAN']
    itens_pintura['qt_apontada'] = itens_pintura['QT_APONTADA']
    itens_pintura['status'] = ''

    itens_pintura['data_carga'] = pd.to_datetime(itens_pintura['data_carga'],dayfirst=True)

    itens_pintura = itens_pintura[['id','codigo','data_inicio','data_fim_tratada','data_carga','qt_planejada','qt_apontada','status']]

    itens_tempos_montagem['etapa'] = 'montagem'
    itens_pintura['etapa'] = 'pintura'

    itens_tempos_montagem['data_inicio'] = itens_tempos_montagem['data_inicio'].apply(parse_data_condicional)
    itens_pintura['data_inicio'] = itens_pintura['data_inicio'].apply(parse_data_condicional)
    itens_tempos_montagem['data_fim_tratada'] = itens_tempos_montagem['data_fim_tratada'].apply(parse_data_condicional)
    itens_pintura['data_fim_tratada'] = itens_pintura['data_fim_tratada'].apply(parse_data_condicional)
        

    #Tratando o df de solda para pegar as colunas de apontamento Solda
    itens_apontamento_solda = pd.DataFrame(list_apontamento_solda)
    itens_apontamento_solda.columns = itens_apontamento_solda.iloc[1]
    itens_apontamento_solda = itens_apontamento_solda.drop(index=[0,1])

    # Trocando os valores das colunas de solda para fazer o concat
    itens_apontamento_solda['id'] = ''
    itens_apontamento_solda['codigo'] = itens_apontamento_solda['Código']
    itens_apontamento_solda['data_inicio'] = itens_apontamento_solda['Data de apontamento inicial']
    itens_apontamento_solda['data_fim_tratada'] = itens_apontamento_solda['Data de apontamento final']
    itens_apontamento_solda['data_carga'] = itens_apontamento_solda['Data da carga']
    itens_apontamento_solda['qt_planejada'] = itens_apontamento_solda['Qtd prod']
    itens_apontamento_solda['qt_apontada'] = itens_apontamento_solda['Qtd prod']
    itens_apontamento_solda['status'] = ''


    itens_apontamento_solda['data_inicio'] = itens_apontamento_solda['data_inicio'].apply(parse_data_condicional)
    itens_apontamento_solda['data_fim_tratada'] = itens_apontamento_solda['data_fim_tratada'].apply(parse_data_condicional)
    itens_apontamento_solda['data_carga'] = itens_apontamento_solda['data_carga'].apply(parse_data_condicional)

    itens_apontamento_solda = itens_apontamento_solda[['codigo','data_inicio','data_fim_tratada','data_carga','qt_planejada','qt_apontada','status']]

    itens_apontamento_solda['etapa'] = 'solda' 
    itens_apontamento_solda = itens_apontamento_solda[~(itens_apontamento_solda['codigo'].isna() | (itens_apontamento_solda['codigo'] == ''))]


    #Concatenando as três planilhas de tempos
    # itens_tempos = pd.concat([itens_tempos_montagem,itens_pintura])
    itens_tempos = pd.concat([itens_tempos_montagem,itens_pintura,itens_apontamento_solda])
    
    itens_tempos['codigo'] = itens_tempos['codigo'].str.lstrip('0')
    # 1. Obter a primeira ocorrência de cada carga → para data_liberacao

    primeira_aparicao_montagem = (
        itens_tempos[(itens_tempos['etapa'] == 'montagem') & (itens_tempos['status'] != 'finalizada')]
        .groupby(['codigo', 'data_carga', 'etapa'], as_index=False)
        .agg({
            'data_inicio': 'first',
            'qt_planejada': 'first'
        })
    )

    primeira_aparicao_pintura = (
        itens_tempos[(itens_tempos['etapa'] == 'pintura')]
        .groupby(['codigo', 'data_carga', 'etapa'], as_index=False)
        .agg({
            'data_inicio': 'first',
            'qt_planejada': 'first'
        })
    )

    primeira_aparicao_solda = (
        itens_tempos[(itens_tempos['etapa'] == 'solda')]
        .groupby(['codigo', 'data_carga', 'etapa'], as_index=False)
        .agg({
            'data_inicio': 'first',
            'qt_planejada': 'first'
        })
    )



    # 2. Obter a última ocorrência de cada carga → para data_entrega
    # Para etapa = montagem e status = finalizado → soma qt_apontada
    itens_tempos['qt_planejada'] = pd.to_numeric(itens_tempos['qt_planejada'],errors='coerce').fillna(0)
    itens_tempos['qt_apontada'] = pd.to_numeric(itens_tempos['qt_apontada'],errors='coerce').fillna(0)

    montagem_finalizado = (
        itens_tempos[(itens_tempos['etapa'] == 'montagem') & (itens_tempos['status'] == 'finalizada')]
        .groupby(['codigo', 'data_carga', 'etapa'], as_index=False)
        .agg({
            'qt_apontada': 'sum',
            'data_fim_tratada': 'last'
        })
    )

    # Para outros casos → pega o último valor normalmente
    pintura = (
        itens_tempos[itens_tempos['etapa'] == 'pintura']
        .groupby(['codigo', 'data_carga', 'etapa'], as_index=False)
        .agg({
            'qt_apontada': 'sum',
            'data_fim_tratada': 'last'
        })
    )

    solda = (
        itens_tempos[itens_tempos['etapa'] == 'solda']
        .groupby(['codigo', 'data_carga', 'etapa'], as_index=False)
        .agg({
            'qt_apontada': 'sum',
            'data_fim_tratada': 'last'
        })
    )
    
    primeira_aparicao_montagem = primeira_aparicao_montagem[['codigo','data_carga','etapa','qt_planejada','data_inicio']]
    primeira_aparicao_pintura = primeira_aparicao_pintura[['codigo','data_carga','etapa','qt_planejada','data_inicio']]

    pintura = pintura[['codigo','data_carga','etapa','qt_apontada','data_fim_tratada']]
    montagem_finalizado = montagem_finalizado[['codigo','data_carga','etapa','qt_apontada','data_fim_tratada']]

    

    montagem_finalizado['codigo'] = montagem_finalizado['codigo'].str.lstrip('0')

    primeira_aparicao = pd.concat([primeira_aparicao_montagem, primeira_aparicao_pintura, primeira_aparicao_solda], ignore_index=True)

    # primeira_aparicao.to_excel(r'C:\Users\TIDEV\Desktop\primeira_aparicao.xlsx',index=False)
    # pintura.to_excel(r'C:\Users\TIDEV\Desktop\pintura_tempos.xlsx',index=False)

    ultima_aparicao = pd.concat([montagem_finalizado, pintura, solda], ignore_index=True)
  
    # print(primeira_aparicao.head())
    # print(primeira_aparicao.tail())
    # print(primeira_aparicao.sample(5))
    # print(primeira_aparicao.shape)
    # print(primeira_aparicao.columns)
    # print(primeira_aparicao.info())

    # print("--------------------------")
    # print(ultima_aparicao.head())
    # print(ultima_aparicao.tail())
    # print(ultima_aparicao.sample(5))
    # print(ultima_aparicao.shape)
    # print(ultima_aparicao.columns)
    # print(ultima_aparicao.info())

    # PEGANDO OS CODIGOS QUE CONTEM ZERO E ESTABELECENDO OUTRA COLUNA
    conjuntos['CODIG'] = conjuntos['COD'].str.lstrip('0')


    # 3. Juntar com a tabela A
    df_resultado = pd.merge(conjuntos, primeira_aparicao, left_on=['CODIG','PED_PREVISAOEMISSAODOC'], right_on=['codigo','data_carga'], how='left')

    # df_resultado['qt_planejada'] = pd.to_numeric(df_resultado['qt_planejada'],errors='coerce').fillna(0)
    # df_transformado['qt_apontada'] = pd.to_numeric(df_transformado['qt_apontada'],errors='coerce').fillna(0)
    
    # df_resultado = df_resultado.drop_duplicates(subset=['carreta', 'COD', 'data_carga', 'etapa'], keep='first')
    

    # df_resultado.to_excel(r'C:\Users\TIDEV\Desktop\df_resultado.xlsx',index=False)
    conjuntos_tempos = pd.merge(df_resultado, ultima_aparicao, left_on=['CODIG','PED_PREVISAOEMISSAODOC'], right_on=['codigo','data_carga'], how='left')
    # conjuntos_tempos.to_excel(r'C:\Users\TIDEV\Desktop\conjuntos_tempos.xlsx',index=False)


    # PLANILHA APONTAMENTO MONTAGEM
    wks_apontamento = sh_apontamento.worksheet('RQ PCP 002-000 (APONTAMENTO MONTAGEM)')
    list_montagem = wks_apontamento.get_all_values()

    #Tratando o df para pegar as colunas necessárias
    itens_montagem = pd.DataFrame(list_montagem)
    itens_montagem.columns = itens_montagem.iloc[4]
    itens_montagem = itens_montagem.drop(index=[0,1,2,3,4])

    itens_montagem = itens_montagem[['Código','Célula']]
    itens_montagem = itens_montagem[
        itens_montagem['Célula'].notna() & (itens_montagem['Célula'] != '') &
        itens_montagem['Código'].notna() & (itens_montagem['Código'] != '')
    ]
    itens_montagem = itens_montagem.groupby(['Código'],as_index=False).last().reset_index()

    # itens_montagem.to_excel(r'C:\Users\TIDEV\Desktop\celulas_montagem.xlsx',index=False)
    # print(itens_montagem)

    # itens_montagem.to_excel('itens_montagem.xlsx', index=False)


    conjuntos_tempos_montagem = pd.merge(conjuntos_tempos,itens_montagem,left_on='COD',right_on='Código',how='inner')

    # conjuntos_tempos_montagem.to_excel(r'C:\Users\TIDEV\Desktop\conjuntos_tempos_montagem.xlsx',index=False)



    #worksheet_name - LEADTIME
    wks = sh_leadtime.worksheet('Página3')
    list1 = wks.get_all_values()

    itens = pd.DataFrame(list1)
    itens.columns = itens.iloc[0]
    itens = itens.drop(index=0)


    itens = pd.merge(conjuntos_tempos_montagem,itens,left_on='COD',right_on='codigo_trat',how='inner')

    itens = itens[itens[['lead time montagem', 'lead time solda', 'lead time pintura']].notna().all(axis=1) &
              (itens[['lead time montagem', 'lead time solda', 'lead time pintura']] != '').all(axis=1)]

    # for coluna in itens.columns:
    #     print(coluna)

    # Lista para armazenar as novas linhas
    novas_linhas = []
    # print(itens)
    codigo_anterior = ''
    setores_adicionados = []

    # itens.to_excel(r'C:\Users\TIDEV\Desktop\conjuntos_itens.xlsx',index=False)

    itens.reset_index(drop=True, inplace=True)

    # Iterar sobre cada linha do DataFrame
    for i in range(len(itens) - 1):
    # for _, row in itens.iterrows():
        row = itens.loc[i]
        row_prox = itens.loc[i+1]

        if i + 1 > len(itens) - 1:
            row_prox = row

        
        if codigo_anterior != row['Código']:
            setores_adicionados.clear()
        
        codigo_anterior = row['Código']
        #VERIFICADORES DE MONTAGEM
        lead_time_montagem_check = row['lead time montagem'] != '0' and row['lead time montagem'] != '' and row['lead time montagem'] != '?' and row['lead time montagem'] != '#VALUE!' and row['lead time montagem'] != '#N/A'
        etapa_montagem_check = row['etapa_x'] == 'montagem' and row['etapa_y'] == 'montagem'

        #VERIFICADORES DE SOLDA
        lead_time_solda_check = row['lead time solda'] != '0' and row['lead time solda'] != '' and row['lead time solda'] != '?' and row['lead time solda'] != '#VALUE!' and row['lead time solda'] != '#N/A'
        etapa_solda_check = row['etapa_x'] == 'solda' and row['etapa_y'] == 'solda'

        #VERIFICADORES DE PINTURA
        lead_time_pintura_check = row['lead time pintura'] != '0' and row['lead time pintura'] != '' and row['lead time pintura'] != '?' and row['lead time pintura'] != '#VALUE!' and row['lead time pintura'] != '#N/A'
        etapa_pintura_check = row['etapa_x'] == 'pintura' and row['etapa_y'] == 'pintura'

        #VERIFICADOR DIFERENTE DE TUDO
        # etapa_diff_tudo = not etapa_montagem_check and not etapa_solda_check and not etapa_pintura_check

        #verificar 

        if lead_time_montagem_check and etapa_montagem_check:
            linha = {coluna: row[coluna] for coluna in itens.columns}
            linha['ETAPA'] = 'MONTAGEM'
            novas_linhas.append(linha)
            setores_adicionados.append('montagem')
            
            
        elif lead_time_montagem_check and 'montagem' not in setores_adicionados and row_prox['Código'] != row['Código']:
            linha = {coluna: row[coluna] for coluna in itens.columns}
            linha['ETAPA'] = 'MONTAGEM'
            linha['data_inicio'] = None
            linha['data_fim_tratada'] = None
            novas_linhas.append(linha)
            setores_adicionados.append('montagem')

        # MANTER PARA QUANDO FOR ESTABELECER OS TEMPOS DE SOLDA
        if  lead_time_solda_check and etapa_solda_check:
            linha = {coluna: row[coluna] for coluna in itens.columns}
            linha['ETAPA'] = 'SOLDA'
            novas_linhas.append(linha)
            setores_adicionados.append('solda')

        elif lead_time_solda_check and 'solda' not in setores_adicionados and row_prox['Código'] != row['Código']:
            linha = {coluna: row[coluna] for coluna in itens.columns}
            linha['ETAPA'] = 'SOLDA'
            linha['data_inicio'] = None
            linha['data_fim_tratada'] = None
            novas_linhas.append(linha)
            setores_adicionados.append('solda')
        # MANTER PARA QUANDO FOR ESTABELECER OS TEMPOS DE MONTAGEM DE MADEIRA
        # if row['lead time montar madeira'] != '0' and row['lead time montar madeira'] != '' and row['lead time montar madeira'] != '?' and row['lead time montar madeira'] != '#VALUE!':
        #     # colunas_excluidas = ['data_inicio','data_fim_tratada']
        #     linha = {coluna: row[coluna] for coluna in itens.columns}
        #     linha['ETAPA'] = 'MONTAR MADEIRA'
        #     linha['qt_planejada'] = None
        #     linha['qt_apontada'] = None
        #     linha['data_inicio'] = None
        #     linha['data_fim_tratada'] = None
        #     novas_linhas.append(linha)

        if lead_time_pintura_check and etapa_pintura_check:
            linha = {coluna: row[coluna] for coluna in itens.columns}
            linha['ETAPA'] = 'PINTURA'
            novas_linhas.append(linha)
            setores_adicionados.append('pintura')
        # Não está apontada na planilha
        elif lead_time_pintura_check and 'pintura' not in setores_adicionados and row_prox['Código'] != row['Código']:
            linha = {coluna: row[coluna] for coluna in itens.columns}
            linha['ETAPA'] = 'PINTURA'
            linha['data_inicio'] = None
            linha['data_fim_tratada'] = None
            novas_linhas.append(linha)
            setores_adicionados.append('pintura')
        


    df_transformado = pd.DataFrame(novas_linhas)

    # Convertendo a data para número serial de dias desde 01/01/1900

    # Tratando colunas de quantidade que estejam vazias
    df_transformado['qt_planejada'] = pd.to_numeric(df_transformado['qt_planejada'],errors='coerce').fillna(0)
    df_transformado['qt_apontada'] = pd.to_numeric(df_transformado['qt_apontada'],errors='coerce').fillna(0)

    condicoes_status_montagem = [
        (df_transformado['data_inicio'].isna()) & (df_transformado['data_fim_tratada'].isna()) & (df_transformado['ETAPA'] == 'MONTAGEM'),
        (df_transformado['data_inicio'].notna()) & (df_transformado['qt_planejada'] > df_transformado['qt_apontada']) & (df_transformado['qt_planejada'] > 0) & (df_transformado['ETAPA'] == 'MONTAGEM'),
        (df_transformado['data_inicio'].notna()) & (df_transformado['data_fim_tratada'].notna()) & (df_transformado['qt_planejada'] <= df_transformado['qt_apontada']) & (df_transformado['ETAPA'] == 'MONTAGEM') & (df_transformado['qt_planejada'] > 0)
    ]

    condicoes_status_pintura = [
        (df_transformado['data_inicio'].isna()) & (df_transformado['data_fim_tratada'].isna()) & (df_transformado['ETAPA'] == 'PINTURA'),
        # (df_transformado['data_inicio'].notna()) & (df_transformado['data_fim_tratada'].isna()) & (df_transformado['qt_planejada'] > 0) & (df_transformado['ETAPA'] == 'PINTURA'),
        (df_transformado['data_inicio'].notna()) & (df_transformado['qt_planejada'] >= df_transformado['qt_apontada']) & (df_transformado['qt_planejada'] > 0) & (df_transformado['ETAPA'] == 'PINTURA'),
        (df_transformado['data_inicio'].notna()) & (df_transformado['data_fim_tratada'].notna()) & (df_transformado['qt_planejada'] <= df_transformado['qt_apontada']) & (df_transformado['ETAPA'] == 'PINTURA') & (df_transformado['qt_planejada'] > 0)
    ]

    condicoes_status_solda = [
        (df_transformado['data_inicio'].isna()) & (df_transformado['ETAPA'] == 'SOLDA'),
        (df_transformado['data_inicio'].notna()) & (df_transformado['data_fim_tratada'].isna()) & (df_transformado['ETAPA'] == 'SOLDA'),
        (df_transformado['data_inicio'].notna()) & (df_transformado['data_fim_tratada'].notna()) & (df_transformado['ETAPA'] == 'SOLDA')
    ]

    valores_status = [
        'Aguardando Liberação',
        'Em Processo',
        'Finalizada'
    ]

    
    valor_lead_time_pintura = pd.to_timedelta(pd.to_numeric(df_transformado['pintura']),unit='D')
    valor_lead_time_montagem = pd.to_timedelta(pd.to_numeric(df_transformado['montagem']),unit='D')
    valor_lead_time_solda = pd.to_timedelta(pd.to_numeric(df_transformado['solda']),unit='D')

    valor_data_entrega_pintura = (df_transformado['PED_PREVISAOEMISSAODOC'] - BDay(1))
    valor_data_entrega_montagem = ((df_transformado['PED_PREVISAOEMISSAODOC'] - BDay(1)) - valor_lead_time_pintura - valor_lead_time_solda)
    valor_data_entrega_solda = ((df_transformado['PED_PREVISAOEMISSAODOC'] - BDay(1)) - valor_lead_time_pintura - valor_lead_time_montagem)

    condicoes_data_entrega = [
        (df_transformado['ETAPA'] == 'PINTURA'),
        (df_transformado['ETAPA'] == 'MONTAGEM'),
        (df_transformado['ETAPA'] == 'SOLDA')
    ]
    
    valores_data_entrega = [
        (valor_data_entrega_pintura.dt.strftime('%d/%m/%Y')),
        (valor_data_entrega_montagem.dt.strftime('%d/%m/%Y')),
        (valor_data_entrega_solda.dt.strftime('%d/%m/%Y'))
    ]

    # DEFININDO POSSIBILIDADES PARA OS VALORES
    condicoes_opcional_6 = [
        (df_transformado['ETAPA'] == 'PINTURA'),
        (df_transformado['ETAPA'] == 'MONTAGEM'),
        (df_transformado['ETAPA'] == 'SOLDA')
    ]
    
    valores_opcional_6 = [
        ((valor_data_entrega_pintura - valor_lead_time_pintura).dt.strftime('%d/%m/%Y')),
        ((valor_data_entrega_montagem - valor_lead_time_montagem).dt.strftime('%d/%m/%Y')),
        ((valor_data_entrega_solda - valor_lead_time_solda).dt.strftime('%d/%m/%Y'))
    ]


    # Garantir que a coluna esteja em formato datetime
    df_transformado['PED_PREVISAOEMISSAODOC'] = pd.to_datetime(df_transformado['PED_PREVISAOEMISSAODOC'], errors='coerce')
    df_transformado['data_inicio'] = pd.to_datetime(df_transformado['data_inicio'], errors='coerce',dayfirst=True)
    df_transformado['data_fim_tratada'] = pd.to_datetime(df_transformado['data_fim_tratada'], errors='coerce',dayfirst=True)

    # Calcular dias desde 1900-01-01

    #DEFININDO COLUNAS NOVAS
    df_transformado['Data'] = date.today().strftime('%d/%m/%Y')
    df_transformado['data_fim_tratada'] = df_transformado['data_fim_tratada'].dt.strftime('%d/%m/%Y')

    df_transformado['Ordem de Produção'] = (
        (df_transformado['PED_PREVISAOEMISSAODOC'] - pd.Timestamp('1900-01-01')).dt.days.astype(str)
        + df_transformado['Carga'].str.replace(' ', '')
    )
    df_transformado['Produto'] = df_transformado['COD']
    df_transformado['Cor'] = "UNICO"
    df_transformado['Tamanho'] = "UNICO"
    df_transformado['Descrição do Produto'] = df_transformado['DESCRICAO']
    df_transformado['Quantidade'] = df_transformado['QTD']
    df_transformado['Quantidade_Original'] = df_transformado['QTD_ORIGINAL']
    df_transformado['Cliente'] = 'EXPEDIÇÃO'
    df_transformado['Unidade Fabril'] = 'CEMAG'
    df_transformado['Local'] = df_transformado['ETAPA']
    df_transformado['Recurso'] = df_transformado['Célula']
    df_transformado['Grupo'] = df_transformado['subgrupo']
    df_transformado['SubGrupo'] = df_transformado['subgrupo']
    # df_transformado['Status'] = np.select(condicoes_status,valores_status,default='') if df_transformado['ETAPA'] != 'SOLDA' else np.select(condicoes_status_solda,valores_status,default='')
    # Primeiro aplica para quem é MONTAGEM
    mask_montagem = df_transformado['ETAPA'] == 'MONTAGEM'
    df_transformado.loc[mask_montagem, 'Status'] = np.select(
        condicoes_status_montagem, valores_status, default='')[mask_montagem]
    
    # Depois aplica para quem É PINTURA
    mask_pintura = df_transformado['ETAPA'] == 'PINTURA'
    df_transformado.loc[mask_pintura, 'Status'] = np.select(
        condicoes_status_pintura, valores_status, default='')[mask_pintura]

    # Depois aplica para quem É SOLDA
    mask_solda = df_transformado['ETAPA'] == 'SOLDA'
    df_transformado.loc[mask_solda, 'Status'] = np.select(
        condicoes_status_solda, valores_status, default='')[mask_solda]
    
    df_transformado['Data de Emissão'] = pd.Timestamp('1900-01-01').strftime('%d/%m/%Y')
    df_transformado['Data de Liberação'] = df_transformado['data_inicio'].dt.strftime('%d/%m/%Y')
    df_transformado['Data de Entrega'] = np.select(condicoes_data_entrega,valores_data_entrega,default='')
    df_transformado['Data de Encerramento'] = np.where(
        df_transformado['Status'] != 'Em Processo',
        df_transformado['data_fim_tratada'],
        ''
    )
    df_transformado['Valor Unitário'] = 1
    df_transformado['OPCIONAL 1'] = (df_transformado['PED_PREVISAOEMISSAODOC'] - BDay(1)).dt.strftime('%d/%m/%Y')
    df_transformado['OPCIONAL 2'] = df_transformado['PED_PREVISAOEMISSAODOC'].dt.strftime('%d/%m/%Y')
    df_transformado['OPCIONAL 3'] = df_transformado['montagem']
    df_transformado['OPCIONAL 4'] = df_transformado['solda']
    df_transformado['OPCIONAL 5'] = df_transformado['pintura']
    # SUGESTÃO DE INICIO
    df_transformado['OPCIONAL 6'] = np.select(condicoes_opcional_6,valores_opcional_6,default='')
    df_transformado['OPCIONAL 7'] = df_transformado['carreta']
    # SELECAO DA COR
    df_transformado['Data_COR'] = pd.to_datetime(df_transformado['Data'], dayfirst=True, errors='coerce')
    df_transformado['OPCIONAL_6_COR'] = pd.to_datetime(df_transformado['OPCIONAL 6'], dayfirst=True, errors='coerce')
    df_transformado['Data_de_Entrega_COR'] = pd.to_datetime(df_transformado['Data de Entrega'], dayfirst=True, errors='coerce')
    df_transformado['OPCIONAL_3_COR'] = pd.to_numeric(df_transformado['OPCIONAL 3'])
    df_transformado['OPCIONAL_4_COR'] = pd.to_numeric(df_transformado['OPCIONAL 4'])
    df_transformado['OPCIONAL_5_COR'] = pd.to_numeric(df_transformado['OPCIONAL 5'])

    df_transformado['COR PRIORIDADE'] = df_transformado.apply(calcular_cor, axis=1)

    df_transformado = df_transformado.sort_values(by='PED_PREVISAOEMISSAODOC')

    colunas_desejadas = ['Data','Ordem de Produção','Produto','Cor','Tamanho','Descrição do Produto','Quantidade','Quantidade_Original','Cliente','Unidade Fabril','Local',
                         'Recurso','Grupo','SubGrupo','Status','Data de Emissão','Data de Liberação','Data de Entrega','Data de Encerramento','Valor Unitário',
                         'OPCIONAL 1','OPCIONAL 2','OPCIONAL 3','OPCIONAL 4','OPCIONAL 5','OPCIONAL 6','OPCIONAL 7','COR PRIORIDADE']

    df_transformado = df_transformado[colunas_desejadas]

    # print(df_transformado)

    df_transformado = df_transformado.where(pd.notnull(df_transformado),None)

    # df_transformado = df_transformado[df_transformado['Recurso'].str.upper() == 'FUEIRO']


    return df_transformado


def base_levantamento_pecas(df_transformado):

    """
    Essa função vai puxar a a base de levantamento de peças para as cargas que ainda estão abertas, isso será feito apenas no arquivo do dia.
    """
    time.sleep(0.5)


    #ID planilha LEVANTAMENTO PARA CORTE
    # sheet_id = '1_-Ss5TVaIIGi8fx--_uUZFUOSk4cTe3lUF8t7qtEJNA'
    sheet_id = '1Hyc5E8OY9uxcZFXC4goK9D_dEh9-OTDLL2p3w4iZEFo'

    print('base_levantamento_pecas 1...')

    #Abrindo a planilha lead time
    sh_leadtime = client.open_by_key(sheet_id)
    #worksheet_name - LEADTIME
    wks = sh_leadtime.worksheet('tabela')
    list1 = wks.get_all_values()

    pecas = pd.DataFrame(list1)
    # print(itens)
    pecas.columns = pecas.iloc[1]
    pecas = pecas.drop(index=[0,1])

    pecas = pecas[['COD','QTD','DATA CARGA','CONJUNTO','DESCRIÇÃO','MP','1º PROCESSO','2º PROCESSO','SETOR','STATUS','OQUE IREI CONSUMIR']]
    # print(pecas)
    #OPCIONAL SE É PEÇA OU NÃO
    df_transformado['OPCIONAL 8'] = ""
    # base_com_pecas_dia = pd.merge(df_transformado, pecas, left_on=['OPCIONAL 2','Descrição do Produto'], right_on=['DATA CARGA','CONJUNTO'], how='left')


    print('base_levantamento_pecas...')

    novas_linhas = []

    data_base = ''

    for i in range(len(df_transformado)):
        linha_atual = df_transformado.iloc[i]
        novas_linhas.append(linha_atual) # adiciona a linha atual à lista

        if linha_atual['Status'] != 'Finalizada' and linha_atual['Local'] == 'MONTAGEM':
            # Filtra planilha de pecas por código e data iguais
            match_pecas = pecas[
                (pecas['DATA CARGA'] == linha_atual['OPCIONAL 2']) &
                (pecas['CONJUNTO'] == linha_atual['Descrição do Produto']) 
            ]
            

            if not match_pecas.empty:
                for _, peca in match_pecas.iterrows():
                    if 'OK' not in peca['STATUS'].strip().upper():  # Verifica se a peça não está finalizada
                        nova_linha = linha_atual.copy()
                        nova_linha['Produto'] = peca['COD']
                        nova_linha['Descrição do Produto'] = peca['DESCRIÇÃO']

                        status_split = peca['STATUS'].strip().upper().split(' ')
                        if len(status_split) > 1:
                            nova_linha['Quantidade'] = pd.to_numeric(peca['OQUE IREI CONSUMIR'], errors='coerce')
                            nova_linha['Quantidade_Original'] = pd.to_numeric(peca['QTD'],errors='coerce')
                            nova_linha['Local'] = peca['1º PROCESSO']
                            nova_linha['OPCIONAL 8'] = 'PEÇA'
                            nova_linha['quantidade_individual'] = pd.to_numeric(peca['QTD'],errors='coerce')
                            

                            # VERIFICANDO SE NO STATUS CONTEM VIRAR E SE A QUANTIDADE PLANEJADA DE PECAS É MAIOR QUE A QUANTIDADE DO STATUS
                            if status_split[0] == "ESTAMPAR":
                                nova_linha['Status'] = "Finalizada"
                            else:
                                if pd.to_numeric(peca['QTD'],errors='coerce') > nova_linha['Quantidade']:
                                    nova_linha['Status'] = "Em Processo"
                                else:
                                    nova_linha['Status'] = "Aguardando Liberação"
                        #DEFININDO A DATA DA SUGESTAO DE INICIO DAS PEÇAS
                        if peca['2º PROCESSO'].strip() != '': # Se houver um segundo processo
                            # nova_linha['OPCIONAL 6'] = (pd.to_datetime(linha_atual["OPCIONAL 6"], format="%d/%m/%Y") - pd.Timedelta(days=3)).strftime('%d/%m/%Y')
                            nova_linha['OPCIONAL 6'] = pd.to_datetime(linha_atual["OPCIONAL 6"], format="%d/%m/%Y") - BDay(3)
                            nova_linha['OPCIONAL 6'] = nova_linha['OPCIONAL 6'].strftime('%d/%m/%Y')
                        else: # Se não houver segundo processo
                            # nova_linha['OPCIONAL 6'] = (pd.to_datetime(linha_atual["OPCIONAL 6"], format="%d/%m/%Y") - pd.Timedelta(days=2)).strftime('%d/%m/%Y')
                            nova_linha['OPCIONAL 6'] = pd.to_datetime(linha_atual["OPCIONAL 6"], format="%d/%m/%Y") - BDay(2)
                            nova_linha['OPCIONAL 6'] = nova_linha['OPCIONAL 6'].strftime('%d/%m/%Y')

                        nova_linha['Data de Entrega'] = pd.to_datetime(nova_linha['OPCIONAL 6'], format="%d/%m/%Y") + BDay(1)
                        nova_linha['Data de Entrega'] = nova_linha['Data de Entrega'].strftime('%d/%m/%Y')

                        novas_linhas.append(nova_linha)

                        if peca['2º PROCESSO'].strip() != '':
                            nova_linha_2 = nova_linha.copy()
                            nova_linha_2['Local'] = peca['2º PROCESSO']
                            nova_linha_2['OPCIONAL 6'] = pd.to_datetime(linha_atual["OPCIONAL 6"], format="%d/%m/%Y") - BDay(1)
                            nova_linha_2['OPCIONAL 6'] = nova_linha_2['OPCIONAL 6'].strftime('%d/%m/%Y')
                            nova_linha_2['Data de Entrega'] = pd.to_datetime(nova_linha_2["OPCIONAL 6"], format="%d/%m/%Y") + BDay(1)
                            nova_linha_2['Data de Entrega'] = nova_linha_2['Data de Entrega'].strftime('%d/%m/%Y')

                            if status_split[0] == "ESTAMPAR":
                                if pd.to_numeric(peca['QTD'],errors='coerce') > nova_linha['Quantidade']:
                                    nova_linha_2['Status'] = "Em Processo"
                                else:
                                    nova_linha_2['Status'] = "Aguardando Liberação"
                            else:
                                nova_linha_2['Status'] = "Aguardando Liberação"

                            # Se a peça já estiver finalizada,remove a ultima linha adicionada, dado que não é preciso mapear peças finalizadas
                            if nova_linha['Status'] == "Finalizada":
                                novas_linhas.pop() 
                            novas_linhas.append(nova_linha_2)
                    
    base_com_pecas_dia = pd.DataFrame(novas_linhas)

    data_primeira_peca_nao_finalizada = base_com_pecas_dia[base_com_pecas_dia['OPCIONAL 8'].notna() & (base_com_pecas_dia['OPCIONAL 8'].str.strip() != '')]

    if not data_primeira_peca_nao_finalizada.empty:
        data_primeira_peca_nao_finalizada = data_primeira_peca_nao_finalizada.sort_values(by='OPCIONAL 8')
        data_base = data_primeira_peca_nao_finalizada.iloc[0]['OPCIONAL 6']
        data_base = pd.to_datetime(data_base, format="%d/%m/%Y") - pd.Timedelta(days=1)
        data_base_dt = data_base.date()
        print(data_base_dt)

        apontamento = apontamento_pecas(data_base_dt)

        apontamento['qt_plan'] = pd.to_numeric(apontamento['qt_plan'], errors='coerce').fillna(0).astype(int)

        apontamento_agrupado = apontamento.groupby('peca')['qt_plan'].sum().reset_index()
        

        check_peca_programado = pd.merge(base_com_pecas_dia,apontamento_agrupado,left_on=base_com_pecas_dia['Produto'],right_on=apontamento_agrupado['peca'],how='left')

        # print(check_peca_programado)

        apontamento_agrupado['modificado'] = ''
        check_peca_programado['OPCIONAL 9'] = ''
        # Pegando so as peças
        for i in range(len(check_peca_programado)):
            linha_atual = check_peca_programado.iloc[i]

            if pd.notna(linha_atual['peca']) and str(linha_atual['peca']).strip() != '':
                filtro = apontamento_agrupado[apontamento_agrupado['peca'] == linha_atual['peca']]
                
                if not filtro.empty:
                    filtro = filtro.iloc[0]
                    # print(filtro)
                    # print(apontamento_agrupado[apontamento_agrupado['peca'] == '494495'])
                    if filtro['modificado'] == '' or filtro['modificado'] is None:
                        check_peca_programado.at[i, 'qt_plan'] = filtro['qt_plan']
                        idx = apontamento_agrupado[apontamento_agrupado['peca'] == linha_atual['peca']].index[0]
                        apontamento_agrupado.at[idx, 'modificado'] = 'sim'
                        
                        if check_peca_programado.at[i, 'qt_plan'] <= 0 or (check_peca_programado.at[i, 'qt_plan'] < check_peca_programado.at[i,'quantidade_individual']):
                            check_peca_programado.at[i,'OPCIONAL 9'] = 'Aguardando Programação'
                            apontamento_agrupado.at[idx, 'qt_plan'] -= linha_atual['Quantidade']
                        else:
                            #reduzindo valor
                            apontamento_agrupado.at[idx, 'qt_plan'] -= linha_atual['Quantidade']
                            check_peca_programado.at[i,'OPCIONAL 9'] = 'Programada'
                    else:
                        check_peca_programado.at[i, 'qt_plan'] = filtro['qt_plan']

                        if check_peca_programado.at[i, 'qt_plan'] <= 0 or (check_peca_programado.at[i, 'qt_plan'] < check_peca_programado.at[i,'quantidade_individual']):
                            #reduzindo valor
                            check_peca_programado.at[i,'OPCIONAL 9'] = 'Aguardando Programação'
                        else:
                            idx = apontamento_agrupado[apontamento_agrupado['peca'] == linha_atual['peca']].index[0]
                            apontamento_agrupado.at[idx, 'qt_plan'] -= linha_atual['Quantidade']
                            check_peca_programado.at[i,'OPCIONAL 9'] = 'Programada'                
            elif pd.notna(linha_atual['OPCIONAL 8']) and str(linha_atual['OPCIONAL 8']).strip() != '' and str(linha_atual['OPCIONAL 8']).strip() == 'PEÇA':
                check_peca_programado.at[i, 'OPCIONAL 9'] = 'Aguardando Programação'

        colunas_desejadas = ['Data','Ordem de Produção','Produto','Cor','Tamanho','Descrição do Produto','Quantidade','Quantidade_Original','Cliente','Unidade Fabril','Local',
                         'Recurso','Grupo','SubGrupo','Status','Data de Emissão','Data de Liberação','Data de Entrega','Data de Encerramento','Valor Unitário',
                         'OPCIONAL 1','OPCIONAL 2','OPCIONAL 3','OPCIONAL 4','OPCIONAL 5','OPCIONAL 6','OPCIONAL 7','COR PRIORIDADE','OPCIONAL 8','OPCIONAL 9']
        
        check_peca_programado = check_peca_programado[colunas_desejadas]

        return check_peca_programado
    

    return base_com_pecas_dia

def apontamento_pecas(data_base_dt=None):
  
    time.sleep(0.5)

    #ID planilha Base de apontamentos
    sheet_id = '1XYe4PiUwnfjU3vhQjvr-ZmmAP3P1klXXpWfDtA3qG2I'

    print('base_apontamento 1...')

    #Abrindo a planilha lead time
    sh_leadtime = client.open_by_key(sheet_id)
    #worksheet_name - LEADTIME
    wks = sh_leadtime.worksheet('Criadas corte')
    list1 = wks.get_all_values()

    apontamento_pecas = pd.DataFrame(list1)
    # print(itens)
    apontamento_pecas.columns = apontamento_pecas.iloc[0]
    apontamento_pecas = apontamento_pecas.drop(index=[0])

    apontamento_pecas['data_criada'] = pd.to_datetime(apontamento_pecas['data_criada'].str.strip(), format="%d/%m/%Y %H:%M", errors='coerce')
    apontamento_pecas['data_criada'] = apontamento_pecas['data_criada'].dt.date

    apontamento_pecas['peca'] = apontamento_pecas['peca'].apply(regex_codigo)

    apontamento_pecas = apontamento_pecas[
        (apontamento_pecas['data_criada'] > data_base_dt) &
        (apontamento_pecas['status_atual'] != 'finalizada')
    ]


    apontamento_pecas['qt_plan'] = pd.to_numeric(apontamento_pecas['qt_plan'], errors='coerce').fillna(0).astype(int)

    # apontamento_agrupado = apontamento_pecas.groupby('peca')['qt_plan'].sum().reset_index()

    return apontamento_pecas


def split_codigo(peca):
    peca = str(peca).strip()

    if len(peca.split('-')) > 1:
        return split_codigo(peca.split('-')[0].strip())
    elif len(peca.split('.')) > 1:
        return split_codigo(peca.split('.')[0].strip())
    elif len(peca.strip().split(' ')) > 1:
        return split_codigo(peca.split(' ')[0].strip())
    else:
        return peca.strip()


def regex_codigo(peca):
    peca = str(peca).strip()

    match = re.search(r'\d+', peca)  # procura uma sequência de dígitos

    if match:
        codigo = match.group()
        # print(codigo)  # saída: 494653
        return codigo
    else:
        return peca
