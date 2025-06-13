# 1. Imports padrão do Python
import os
import time
import json
import uuid
import glob
import traceback
from datetime import date, timedelta, datetime
import requests
from zoneinfo import ZoneInfo

# 2. Bibliotecas externas
from flask import Flask, render_template, request, Response, send_file
import schedule
import threading
from functools import partial

# 3. Imports locais
from conexao_plan import busca_cargas, conectar_com_base, definir_leadtime
from unificar import unificar_planilhas


# Criando a instância da aplicação Flask
app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.html')


@app.route('/processar/', methods=['POST'])
def processar():
    limpar_tmp_antigos()
    
    data_inicio = request.form.get('data_inicio')
    data_final = request.form.get('data_final')

    print(data_inicio)
    print(data_final)
    
    cargas = busca_cargas(data_inicio, data_final)
    conjuntos_filtrados = conectar_com_base(cargas)
    planilha_final = definir_leadtime(conjuntos_filtrados)

    if planilha_final.empty:
        return Response(json.dumps({'dados': []}), content_type='application/json', status=400)

    # Gera nome único para o arquivo
    nome_arquivo = f"cargas_{uuid.uuid4().hex}.xlsx"
    os.makedirs('tmp', exist_ok=True)
    caminho = os.path.join("tmp", nome_arquivo)  # pasta "tmp" deve existir


    # Salva o DataFrame como Excel em disco
    planilha_final.to_excel(caminho, index=False)

    # Agrupando o df para gerar o gráfico

    # agrupado = planilha_final[['Recurso','Status','COR PRIORIDADE']].groupby(['Recurso', 'Status','COR PRIORIDADE']).size().reset_index(name='ocorrencias')

     # Gerar gráficos

    # Pegar todos os recursos únicos pro eixo X
    # recursos = planilha_final['Recurso'].unique().tolist()
    # print(recursos)

    # # Agrupar por Status + Cor
    # # agrupado = planilha_final.groupby(['Status', 'COR PRIORIDADE'])

    # agrupado = planilha_final.groupby(['Recurso', 'Status', 'COR PRIORIDADE']).size().reset_index(name='ocorrencias')
    # print(agrupado)

    # datasets = []

    # for (status, cor), grupo in agrupado.groupby(['Status', 'COR PRIORIDADE']):
    #     data = []
    #     print(grupo)
    #     for recurso in recursos:
    #         ocorrencias = grupo.loc[grupo['Recurso'] == recurso, 'ocorrencias']
    #         data.append(ocorrencias.iloc[0] if not ocorrencias.empty else 0)

    #     datasets.append({
    #         'label': f'{status}',
    #         'backgroundColor': cor.split('.')[-1].lower(),  # ex: 'AZUL'
    #         'data': data,
    #         'stack': 'stack1'
    #     })

    # print(datasets)

    # grafico_id = f"grafico_{uuid.uuid4().hex}.json"
    # caminho_grafico = os.path.join('tmp', grafico_id)
    # print(caminho_grafico)

    # # with open(caminho_grafico, 'w', encoding='utf-8') as f:
    # #     json.dump({'labels': recursos, 'datasets': datasets}, f, ensure_ascii=False)
    # with open(caminho_grafico, 'w', encoding='utf-8') as f:
    #     json.dump({'labels': recursos, 'datasets': datasets}, f, ensure_ascii=False, default=int)




    # Prepara JSON de resposta
    plan_json = planilha_final.to_dict(orient='records')
    json_data = json.dumps({'dados': plan_json, 
                            'arquivo': nome_arquivo,
                            }, ensure_ascii=False, indent=4)

    return Response(json_data, content_type='application/json', status=200)

@app.route('/exportar-excel/<nome_arquivo>')
def exportar_excel(nome_arquivo):
    caminho = os.path.join("tmp", nome_arquivo)

    if not os.path.exists(caminho):
        return "Arquivo não encontrado", 404

    return send_file(
        caminho,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='cargas_exportadas.xlsx'
    )

def atualizacao_diaria(tentativa_extra=False):
    try:
        print("Executando a atualização diária")
        
        data_atual = date.today()
        data_atual_arquivo = data_atual
        # caminho_arquivo = os.path.join("atualizacao-diaria", f"cargas_{data_atual_arquivo}.xlsx")  # pasta "tmp" deve existir

        # if os.path.exists(caminho_arquivo) and datetime.now().hour <= 14:
        #     print('Arquivo do dia já existe!')
        #     return "Arquivo já existe"
        # elif os.path.exists(caminho_arquivo) and datetime.now().hour > 15:
        #     os.remove(caminho_arquivo)
        #     print('Arquivo deletado')

        data_inicio = datetime(data_atual.year,data_atual.month,data_atual.day)
        data_final = data_inicio + timedelta(days=15)

        # data_inicio_formato_api = datetime(data_atual.year,data_atual.month,data_atual.day,tzinfo=ZoneInfo("America/Sao_Paulo"))
        data_final = datetime(data_final.year, data_final.month, data_final.day, tzinfo=ZoneInfo("America/Sao_Paulo"))
        data_final_formato_api = data_final.isoformat()
        data_inicio_busca = datetime(2025,6,1)

        try:
            url = f"https://apontamentousinagem.onrender.com/cargas/api/andamento-cargas?start=2025-06-01T00:00:00-03:00&end={data_final_formato_api}"

            response = requests.get(url)
            if response.status_code == 200:
                dados = response.json()
                tamanho_dados = len(dados)
                print(f'Tamanho dos dados recebidos: {tamanho_dados}')
                cont = 0
                data_inicio_escolhida = False
                for data in dados:
                    porcentagem_concluida = float(data['title'].split('-')[1].replace('%','').strip())
                    if porcentagem_concluida < 100.0 and not data_inicio_escolhida:
                        dia_com_carga_aberta = data['start'] + ' 00:00:00'
                        data_inicio = datetime.strptime(dia_com_carga_aberta,"%Y-%m-%d %H:%M:%S")
                        print(f'Ainda resta carga para o dia {dia_com_carga_aberta}, iniciando a partir deste dia.')
                        data_inicio_escolhida = True
                    if cont == (tamanho_dados/2) - 1:
                        ultimo_dia = data['start'] + ' 00:00:00'
                        data_final = datetime.strptime(ultimo_dia,"%Y-%m-%d %H:%M:%S")
                        print(f'Último dia de carga liberada: {ultimo_dia}')
                        break

                    cont+=1
                    
            else:
                print(f'Erro na requisição: {response.status_code}')
        except Exception as e:
            print(f'Erro na API:{e}')
            traceback.print_exc()

        print(data_inicio)
        print(data_final)
        
        cargas = busca_cargas(data_inicio, data_final)
        conjuntos_filtrados = conectar_com_base(cargas)
        planilha_final = definir_leadtime(conjuntos_filtrados)

        if planilha_final.empty:
            print('Planilha vazia!')
            return {"status": "vazio"}

        # Gera nome único para o arquivo
        nome_arquivo = f"cargas_{data_atual_arquivo}.xlsx"
        os.makedirs('atualizacao-diaria', exist_ok=True)
        caminho = os.path.join("atualizacao-diaria", nome_arquivo)  # pasta "tmp" deve existir
        print(caminho)

        # Salva o DataFrame como Excel em disco
        planilha_final.to_excel(caminho, index=False)

        time.sleep(1)  # Aguarda 1 segundo para garantir que o arquivo foi salvo
        # Junta as planilhas unificadas
        unificar_planilhas(data_atual, data_final, data_inicio_busca)

        # Prepara JSON de resposta
        plan_json = planilha_final.to_dict(orient='records')
        json_data = json.dumps({'dados': plan_json, 
                                'arquivo': nome_arquivo,
                                }, ensure_ascii=False, indent=4)
        return {"status": "ok", "dados": planilha_final.to_dict(orient='records'), "arquivo": nome_arquivo}
    
    except Exception as e:
        print(f"Ocorreu um erro! {e}")
        traceback.print_exc()
        if not tentativa_extra:
            print("Tentando reagendar a tarefa...")
            nova_tarefa = partial(atualizacao_diaria, tentativa_extra=True)
            schedule.every(10).minutes.do(nova_tarefa_com_cancelamento(nova_tarefa))
        return {"status": "erro", "mensagem": str(e)}

def nova_tarefa_com_cancelamento(func):
    def wrapper():
        func()
        return schedule.CancelJob
    return wrapper

def limpar_tmp_antigos(pasta='tmp', segundos=300):
    """
    Remove arquivos da pasta que são mais antigos que 'segundos' (default: 5 minutos).
    """
    agora = time.time()
    arquivos = glob.glob(os.path.join(pasta, '*'))

    for arquivo in arquivos:
        try:
            tempo_modificado = os.path.getmtime(arquivo)
            if agora - tempo_modificado > segundos:
                os.remove(arquivo)
                print(f"Arquivo removido: {arquivo}")
        except Exception as e:
            print(f"Erro ao tentar remover {arquivo}: {e}")

def agendar_atualizacao():
    print('agendar_atualizacao()')
    schedule.every().day.at("18:17").do(atualizacao_diaria)
    schedule.every().day.at("18:30").do(atualizacao_diaria)

    while True:
        jobs = schedule.get_jobs()  # Retorna a lista de jobs pendentes
        schedule.run_pending()
        time.sleep(3)
        if (datetime.now().hour == 9 and datetime.now().minute >= 40) or datetime.now().hour == 20 and datetime.now().minute >= 40:
            print(jobs)

# Inicia o agendamento em uma thread separada
def start_thread():
    print('start_thread()')
    agendamento_thread = threading.Thread(target=agendar_atualizacao)
    agendamento_thread.daemon = True # Isso permite que a thread seja finalizada quando o programa principal for finalizado
    agendamento_thread.start()

# Função para iniciar a thread de agendamento após o app estar configurado
def init_agendamento():
    print('init_agendamento()')
    with app.app_context():
        start_thread()  # Inicia a thread de agendamento

# Executa o agendamento quando o app for iniciado
init_agendamento()


# Função para depois
def exibir_grafico(grafico_id):
    caminho = os.path.join('tmp', grafico_id)
    if not os.path.exists(caminho):
        return "Gráfico não encontrado", 404

    with open(caminho, 'r', encoding='utf-8') as f:
        dados_json  = json.load(f)

    # ✅ Garante que labels e datasets existam
    labels = dados_json.get('grafico', {}).get('labels', [])
    datasets = dados_json.get('grafico', {}).get('datasets', [])

    print(dados_json)

    return render_template(
        'grafico.html',
        labelss=json.dumps(labels, ensure_ascii=False),
        datasets=json.dumps(datasets, ensure_ascii=False),
        dados=dados_json.get('dados', []),
        arquivo=dados_json.get('arquivo', '')
    )

# Executando a aplicação
if __name__ == '__main__':
    app.run(debug=True)

