import gspread
from google.oauth2 import service_account
import pandas as pd
from datetime import date
from pandas.tseries.offsets import BDay
import numpy as np

def busca_cargas(data_inicio,data_final):

    #Configuração inicial
    service_account_info = ["GOOGLE_SERVICE_ACCOUNT"]
    scope = ['https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive"]
    

    credentials = service_account.Credentials.from_service_account_file('credentials.json', scopes=scope)


    #ID planilha
    sheet_id = '1olnMhK7OI6W0eJ-dvsi3Lku5eCYqlpzTGJfh1Q7Pv9I'

    #Abrindo a planilha pelo ID
    client = gspread.authorize(credentials)

    sh = client.open_by_key(sheet_id)
    #worksheet_name
    wks = sh.worksheet('Importar Dados')
    list1 = wks.get_all_values()

    itens = pd.DataFrame(list1)
    itens.columns = itens.iloc[0]
    itens = itens.drop(index=0)

    #Pegando somente as colunas de interesse
    itens = itens[['PED_PREVISAOEMISSAODOC','PED_RECURSO.CODIGO','PED_QUANTIDADE','Carga']]
    itens['PED_PREVISAOEMISSAODOC'] = pd.to_datetime(itens['PED_PREVISAOEMISSAODOC'])
    itens['PED_QUANTIDADE'] = pd.to_numeric(itens['PED_QUANTIDADE'])
    
    # Agrupando pela Data e pelo Codigo Carreta
    itens = itens.groupby(['PED_PREVISAOEMISSAODOC','PED_RECURSO.CODIGO','Carga'],as_index=False).sum()

    # Filtrando o DataFrame para pegar as linhas dentro do intervalo de datas
    itens_filtrados = itens[(itens['PED_PREVISAOEMISSAODOC'] >= data_inicio) & (itens['PED_PREVISAOEMISSAODOC'] <= data_final)]

    #Desconsiderar os códigos de cores VJ, VM, AN, LC, LJ, AM
    codigos_desconsiderados = ['VJ', 'VM', 'AN', 'LC', 'LJ', 'AM']

    # Criando o padrão regex para corresponder a qualquer um desses códigos no final da string
    padrao = r'(' + '|'.join(codigos_desconsiderados) + r')$'

    # Remover os códigos indesejados (substituindo por uma string vazia)
    itens_filtrados.loc[:, 'PED_RECURSO.CODIGO'] = itens_filtrados['PED_RECURSO.CODIGO'].str.replace(padrao, '', regex=True)

    print(itens_filtrados)

    return itens_filtrados

def conectar_com_base(cargas_filtradas):

    #Configuração inicial
    service_account_info = ["GOOGLE_SERVICE_ACCOUNT"]
    scope = ['https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive"]
    

    credentials = service_account.Credentials.from_service_account_file('credentials.json', scopes=scope)


    #ID planilha
    sheet_id = '1n2J6n_VxOsVxY5ikjJeDGva7oHTUJOlzadFfUbJnaSE'

    #Abrindo a planilha pelo ID
    client = gspread.authorize(credentials)

    sh = client.open_by_key(sheet_id)
    #worksheet_name
    wks = sh.worksheet('BASE')
    list1 = wks.get_all_values()

    itens = pd.DataFrame(list1)
    itens.columns = itens.iloc[0]
    itens = itens.drop(index=0)
    
    #Pegar as colunas que interessam
    
    itens = pd.merge(cargas_filtradas,itens,left_on='PED_RECURSO.CODIGO',right_on='carreta',how='left')

    itens = itens[itens['PRIMEIRO PROCESSO'] == 'MONTAR']

    #Desconsiderar o que tiver COMPLETA E ACESSORIOS na descricao
    itens[','] = itens[','].str.upper()
    itens['DESCRICAO'] = itens[',']
    itens = itens[(~itens['DESCRICAO'].str.contains('ACESSORIO',regex=True)) & (~itens['DESCRICAO'].str.contains('COMPLETA',regex=True)) & (~itens['DESCRICAO'].str.contains('ACESSÓRIO',regex=True)) & (~itens['DESCRICAO'].str.contains('COMPLETO',regex=True))]

    itens['TOTAL'] = pd.to_numeric(itens['TOTAL'])
    itens['QTD'] = itens['PED_QUANTIDADE'] * itens['TOTAL']
    itens['QTD_ORIGINAL'] = itens['PED_QUANTIDADE'] * itens['TOTAL']

    colunas_desejadas = ['PED_PREVISAOEMISSAODOC','carreta','DESCRICAO','QTD','QTD_ORIGINAL','COD','Carga']


    conjuntos_filtrados = itens[colunas_desejadas]

    #Colunas Finais: Código, Descrição, quantidade de conjunto, data da carga
    return conjuntos_filtrados

def definir_leadtime(conjuntos):

    print(conjuntos)

    if conjuntos.empty:
        return None

    #Configuração inicial
    service_account_info = ["GOOGLE_SERVICE_ACCOUNT"]
    scope = ['https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive"]
    

    credentials = service_account.Credentials.from_service_account_file('credentials.json', scopes=scope)


    #ID planilha lead time
    sheet_id = '1yTQE0tUxiYHKXaACfay5iqGYzl-E01CMMio9Ou3uK2w'

    #ID planilha apontamento
    sheet_id_apontamento = '1x26yfwoF7peeb59yJuJuxCQNlqjCjh65NYS1RIrC0Zc'

    #ID planilha tempos montagem/solda - Pintura
    sheet_id_tempos_montagem = '12o38c0nYy4VEhtEu7ixuEyUOLT9ms13sUaPnlEniAGM'


    # Definindo o Cliente
    client = gspread.authorize(credentials)

    #Abrindo a planilha lead time
    sh_leadtime = client.open_by_key(sheet_id)

    # Abrindo a planilha de Apontamento Montagem
    sh_apontamento = client.open_by_key(sheet_id_apontamento)

    # Abrindo a planilha de tempos montagem/solda
    sh_tempos_montagem_solda = client.open_by_key(sheet_id_tempos_montagem)

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

    itens_tempos_montagem = itens_tempos_montagem[['codigo','data_inicio','data_fim_tratada','data_carga','qt_planejada','qt_apontada']]

    #Tratando o df de pintura para pegar as colunas de dados Pintura

    itens_pintura = pd.DataFrame(list_tempos_pintura)
    itens_pintura.columns = itens_pintura.iloc[0]
    itens_pintura = itens_pintura.drop(index=0)

    

    # Trocando os valores das colunas de pintura para fazer o concat
    itens_pintura['codigo'] = itens_pintura['CODIGO']
    itens_pintura['data_inicio'] = itens_pintura['DATA_INICIO']
    itens_pintura['data_fim_tratada'] = itens_pintura['DATA_FINALIZADA']
    itens_pintura['data_carga'] = itens_pintura['DATA_CARGA']
    itens_pintura['qt_planejada'] = itens_pintura['QT_PLAN']
    itens_pintura['qt_apontada'] = itens_pintura['QT_APONTADA']

    itens_pintura['data_carga'] = pd.to_datetime(itens_pintura['data_carga'],dayfirst=True)

    itens_pintura = itens_pintura[['codigo','data_inicio','data_fim_tratada','data_carga','qt_planejada','qt_apontada']]

    itens_tempos_montagem['etapa'] = 'montagem'
    itens_pintura['etapa'] = 'pintura'

    #Concatenando as duas planilhas de tempos
    itens_tempos = pd.concat([itens_tempos_montagem,itens_pintura])
    
    # 1. Obter a primeira ocorrência de cada carga → para data_liberacao
    primeira_aparicao = itens_tempos.groupby(['codigo','data_carga','etapa'], as_index=False).agg({
        'data_inicio': 'first',
        'qt_planejada': 'first',
    })

    # 2. Obter a última ocorrência de cada carga → para data_entrega
    ultima_aparicao = itens_tempos.groupby(['codigo','data_carga','etapa'], as_index=False).agg({
        'data_fim_tratada': 'last',
        'qt_apontada': 'last',
    })


    # PEGANDO OS CODIGOS QUE CONTEM ZERO E ESTABELECENDO OUTRA COLUNA
    conjuntos['CODIG'] = conjuntos['COD'].str.lstrip('0')

    # 3. Juntar com a tabela A
    df_resultado = pd.merge(conjuntos, primeira_aparicao, left_on=['CODIG','PED_PREVISAOEMISSAODOC'], right_on=['codigo','data_carga'], how='left')
    conjuntos_tempos = pd.merge(df_resultado, ultima_aparicao, left_on=['CODIG','PED_PREVISAOEMISSAODOC'], right_on=['codigo','data_carga'], how='left')


    # PLANILHA APONTAMENTO MONTAGEM
    wks_apontamento = sh_apontamento.worksheet('RQ PCP 002-000 (APONTAMENTO MONTAGEM)')
    list_montagem = wks_apontamento.get_all_values()

    #Tratando o df para pegar as colunas necessárias
    itens_montagem = pd.DataFrame(list_montagem)
    itens_montagem.columns = itens_montagem.iloc[4]
    itens_montagem = itens_montagem.drop(index=[0,1,2,3,4])

    itens_montagem = itens_montagem[['Código','Célula']]
    itens_montagem = itens_montagem.groupby(['Código','Célula'],as_index=False).first().reset_index()

    conjuntos_tempos_montagem = pd.merge(conjuntos_tempos,itens_montagem,left_on='COD',right_on='Código',how='inner')

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

    # Iterar sobre cada linha do DataFrame
    for _, row in itens.iterrows():
        if row['lead time montagem'] != '0' and row['lead time montagem'] != '' and row['lead time montagem'] != '?' and row['lead time montagem'] != '#VALUE!' and (row['etapa_x'] == 'montagem' and row['etapa_y'] == 'montagem'):
            linha = {coluna: row[coluna] for coluna in itens.columns}
            linha['ETAPA'] = 'MONTAGEM'
            novas_linhas.append(linha)

        # MANTER PARA QUANDO FOR ESTABELECER OS TEMPOS DE SOLDA
        # if row['lead time solda'] != '0' and row['lead time solda'] != '' and row['lead time solda'] != '?' and row['lead time solda'] != '#VALUE!':
        #     # colunas_excluidas = ['data_inicio','data_fim_tratada']
        #     linha = {coluna: row[coluna] for coluna in itens.columns}
        #     linha['ETAPA'] = 'SOLDA'
        #     linha['qt_planejada'] = ''
        #     linha['qt_apontada'] = ''
        #     linha['data_inicio'] = ''
        #     linha['data_fim_tratada'] = ''
        #     novas_linhas.append(linha)

        if row['lead time pintura'] != '0' and row['lead time pintura'] != '' and row['lead time pintura'] != '?' and row['lead time pintura'] != '#VALUE!' and (row['etapa_x'] == 'pintura' and row['etapa_y'] == 'pintura'):
            linha = {coluna: row[coluna] for coluna in itens.columns}
            linha['ETAPA'] = 'PINTURA'
            novas_linhas.append(linha)

    df_transformado = pd.DataFrame(novas_linhas)

    # Convertendo a data para número serial de dias desde 01/01/1900

    # Tratando colunas de quantidade que estejam vazias
    df_transformado['qt_planejada'] = pd.to_numeric(df_transformado['qt_planejada'],errors='coerce').fillna(0)
    df_transformado['qt_apontada'] = pd.to_numeric(df_transformado['qt_apontada'],errors='coerce').fillna(0)

    condicoes_status = [
        (df_transformado['data_inicio'].isna()) & (df_transformado['ETAPA'] != 'SOLDA'),
        (df_transformado['qt_planejada'] > df_transformado['qt_apontada']) & (df_transformado['qt_planejada'] > 0) & (df_transformado['ETAPA'] != 'SOLDA'),
        (df_transformado['qt_planejada'] <= df_transformado['qt_apontada']) & (df_transformado['ETAPA'] != 'SOLDA') & (df_transformado['qt_planejada'] > 0)
    ]

    valores_status = [
        'Aguardando Liberação',
        'Em Processo',
        'Finalizada'
    ]

    condicoes_data_entrega = [
        (df_transformado['ETAPA'] == 'PINTURA'),
        (df_transformado['ETAPA'] == 'MONTAGEM')
    ]

    
    valores_data_entrega = [
        ((df_transformado['PED_PREVISAOEMISSAODOC'] - BDay(1)).dt.strftime('%d/%m/%Y')),
        (((df_transformado['PED_PREVISAOEMISSAODOC'] - BDay(1)) - pd.to_timedelta(pd.to_numeric(df_transformado['pintura']),unit='D') - pd.to_timedelta(pd.to_numeric(df_transformado['solda']),unit='D')).dt.strftime('%d/%m/%Y'))
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
    df_transformado['Cor'] = ''
    df_transformado['Tamanho'] = ""
    df_transformado['Descrição do Produto'] = df_transformado['DESCRICAO']
    df_transformado['Quantidade'] = df_transformado['QTD']
    df_transformado['Quantidade_Original'] = df_transformado['QTD_ORIGINAL']
    df_transformado['Cliente'] = 'EXPEDIÇÃO'
    df_transformado['Unidade Fabril'] = 'CEMAG'
    df_transformado['Local'] = df_transformado['ETAPA']
    df_transformado['Recurso'] = df_transformado['Célula']
    df_transformado['Grupo'] = df_transformado['subgrupo']
    df_transformado['SubGrupo'] = df_transformado['subgrupo']
    df_transformado['Status'] = np.select(condicoes_status,valores_status,default='')
    df_transformado['Data de Emissão'] = df_transformado['PED_PREVISAOEMISSAODOC'].dt.strftime('%d/%m/%Y')
    df_transformado['Data de Liberação'] = df_transformado['data_inicio'].dt.strftime('%d/%m/%Y')
    df_transformado['Data de Entrega'] = np.select(condicoes_data_entrega,valores_data_entrega,default='')
    df_transformado['Data de Encerramento'] = np.where(
        df_transformado['Status'] != 'Em Processo',
        df_transformado['data_fim_tratada'],
        ''
    )
    df_transformado['Valor Unitário'] = ''
    df_transformado['OPCIONAL 1'] = (df_transformado['PED_PREVISAOEMISSAODOC'] - BDay(1)).dt.strftime('%d/%m/%Y')
    df_transformado['OPCIONAL 2'] = df_transformado['Data de Emissão']
    df_transformado['OPCIONAL 3'] = df_transformado['montagem']
    df_transformado['OPCIONAL 4'] = df_transformado['solda']
    df_transformado['OPCIONAL 5'] = df_transformado['pintura']
    df_transformado['OPCIONAL 6'] = ''
    df_transformado['OPCIONAL 7'] = df_transformado['carreta']
    df_transformado['COR PRIORIDADE'] = ''


    colunas_desejadas = ['Data','Ordem de Produção','Produto','Descrição do Produto','Quantidade','Quantidade_Original','Cliente','Unidade Fabril','Local',
                         'Recurso','Grupo','SubGrupo','Status','Data de Emissão','Data de Liberação','Data de Entrega','Data de Encerramento','Valor Unitário',
                         'OPCIONAL 1','OPCIONAL 2','OPCIONAL 3','OPCIONAL 4','OPCIONAL 5','OPCIONAL 6','OPCIONAL 7','COR PRIORIDADE']

    df_transformado = df_transformado[colunas_desejadas]


    df_transformado = df_transformado.where(pd.notnull(df_transformado),None)

    # plan = df_transformado.to_dict(orient='records')

    return df_transformado

