from flask import Flask, render_template, request, Response, send_file
from conexao_plan import busca_cargas, conectar_com_base, definir_leadtime
import json
import uuid
import os
import glob
import time

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

    # Prepara JSON de resposta
    plan_json = planilha_final.to_dict(orient='records')
    json_data = json.dumps({'dados': plan_json, 'arquivo': nome_arquivo}, ensure_ascii=False, indent=4)

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

# Executando a aplicação
if __name__ == '__main__':
    app.run(debug=True)