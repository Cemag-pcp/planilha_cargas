import os
import pandas as pd
from datetime import datetime

def unificar_planilhas(data_atual_arquivo,data_final,data_atual):
    # Caminho da pasta com os arquivos Excel
    pasta = "atualizacao-diaria/"  # <- Altere esse caminho
    # Gera nome único para o arquivo
    # nome_arquivo = f"planilha_unificada_{data_atual_arquivo}.xlsx"
    # os.makedirs('atualizacao-diaria/unificadas', exist_ok=True)
    # caminho = os.path.join("atualizacao-diaria/unificadas", nome_arquivo)
    # print(caminho)

    try:

        # Lista para armazenar os DataFrames
        todas_planilhas = []

        # Percorre os arquivos da pasta
        for arquivo in os.listdir(pasta):
            if arquivo.endswith('.xlsx') and not arquivo.startswith('~$') and verifica_data_arquivo(arquivo,data_atual):  # Ignora arquivos temporários do Excel
                caminho_arquivo = os.path.join(pasta, arquivo)
                df = pd.read_excel(caminho_arquivo)  # Lê a primeira aba
                df['OPCIONAL 2'] = pd.to_datetime(df['OPCIONAL 2'], dayfirst=True, errors='coerce')
                data_limite = pd.Timestamp(data_final)
                df = df[df['OPCIONAL 2'] <= data_limite]           
                # df['Arquivo_Origem'] = arquivo  # (opcional) adiciona coluna com nome do arquivo de origem
                todas_planilhas.append(df)

        # Pega a planilha de hoje, que é a última da lista e remove ela da lista
        planilha_de_hoje = todas_planilhas.pop()
        # Junta todos os DataFrames menos o de hoje
        planilha_unificada = pd.concat(todas_planilhas, ignore_index=True)
        
        # Remove duplicatas, mantendo apenas as linhas com status 'Finalizada'
        planilha_unificada_finalizadas = planilha_unificada[planilha_unificada['Status'] == 'Finalizada']
        planilha_unificada_finalizadas = planilha_unificada_finalizadas.drop_duplicates(subset=['Ordem de Produção', 'Produto', 'OPCIONAL 7'])
        #Modificando a coluna 'Data' para a data de hoje
        planilha_unificada_finalizadas['Data'] = datetime.today().strftime('%d/%m/%Y')




        # Juntar as planilhas unificadas com a planilha de hoje
        planilha_unificada_final = pd.concat([planilha_unificada,planilha_unificada_finalizadas,planilha_de_hoje], ignore_index=True)

        planilha_unificada_final = planilha_unificada_final.sort_values(by='OPCIONAL 2')
        planilha_unificada_final['OPCIONAL 2'] = planilha_unificada_final['OPCIONAL 2'].dt.strftime('%d/%m/%Y')
        # Salva em um novo arquivo Excel
        # planilha_unificada_final.to_excel(caminho, index=False)

        return planilha_unificada_final
    except Exception as e:
        print(f"❌ Erro ao unificar planilhas: {e}")
        return None


def verifica_data_arquivo(nome_arquivo, data_limite):
    # Extrai a data do nome do arquivo
    data_str = nome_arquivo.replace("cargas_", "").replace(".xlsx", "").strip()
    
    # Converte para datetime
    data_arquivo = datetime.strptime(data_str, "%Y-%m-%d")
    
    # Faz a comparação
    return data_arquivo >= data_limite

# Exemplo de uso
# nome_arquivo = "cargas_2025-04-30.xlsx"
# data_limite_str = "2025-06-05 00:00:00"

# if verifica_data_arquivo(nome_arquivo, data_limite_str):
#     print("✅ O arquivo tem data maior ou igual à data limite")
# else:
#     print("❌ O arquivo tem data menor que a data limite")
