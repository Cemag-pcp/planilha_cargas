# Sistema de Gestão de Cargas

Este projeto é uma aplicação **Flask** para processamento, unificação e exportação de planilhas de cargas, incluindo **atualizações automáticas diárias** e endpoints para exportação de Excel e visualização de dados.

---

## 🚀 Funcionalidades Principais

- Processamento de cargas a partir de uma API externa.
- Filtragem e definição de leadtime das cargas.
- Geração de planilhas Excel individuais e unificadas.
- Atualização automática diária em horários programados (06:30 e 18:30).
- Exportação de planilhas processadas via endpoint.
- Remoção automática de arquivos temporários.
- Preparação para visualização de gráficos agrupados (em desenvolvimento).

---

## 📁 Estrutura de Arquivos

- `app.py` → Arquivo principal da aplicação.
- `conexao_plan.py` → Contém funções para busca e filtragem de cargas (`busca_cargas`, `conectar_com_base`, `definir_leadtime`).
- `unificar.py` → Função `unificar_planilhas` para consolidar planilhas em uma única.
- `tmp/` → Pasta para arquivos temporários gerados durante o processamento.
- `atualizacao-diaria/` → Pasta para armazenar planilhas geradas automaticamente.
  - `arquivos-individuais/` → Planilhas individuais de cada execução diária.

---

## ⚙️ Regras de Negócio

1. **Processamento de Cargas**  
   - O usuário envia uma requisição POST para `/processar/` com `data_inicio` e `data_final`.
   - As cargas são buscadas via `busca_cargas` e filtradas pela função `conectar_com_base`.
   - Leadtime é calculado com `definir_leadtime`.
   - Se não houver dados, retorna JSON com status 400 e lista vazia.

2. **Geração de Planilhas**  
   - Planilhas geradas manualmente via rota GET `/` | Página inicial (`home.html`) são salvas com nomes únicos (`cargas_<uuid>.xlsx`) na pasta `tmp/` e removidas automaticamente na próxima geração manual, caso tenha mais de 5 minutos de criação.
   - Para atualizações diárias, as planilhas são salvas em `atualizacao-diaria/arquivos-individuais/` com nomes com as datas (`cargas_2025-11-14`) e depois unificadas e salvas em `atualizacao-diaria/` com o formato (`cargas_2025-11-11`) .

3. **Atualização Automática**  
   - Executada nos horários **06:30** e **18:30**, com checagem extra a cada minuto para garantir execução.
   - Caso a execução não ocorra no horário, há uma **tentativa de compensação** às 09:00 e 20:00.
   - Os arquivos unificados da atualização diária contêm todas as cargas desde 01/06/2025 até a data final calculada.
   - Flags `executou_630` e `executou_1830` controlam se a execução já ocorreu.

4. **Remoção de Arquivos Temporários**  
   - Arquivos na pasta `tmp/` com mais de 5 minutos são automaticamente deletados.

5. **Exportação e Visualização**  
   - Endpoint `/exportar-excel/<nome_arquivo>` permite download do Excel.
   - Função `exibir_grafico` prepara dados para visualização (em desenvolvimento).

---

## 🛠️ Endpoints

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET    | `/` | Página inicial (`home.html`) |
| POST   | `/processar/` | Processa cargas e retorna JSON com dados e nome do arquivo Excel |
| GET    | `/exportar-excel/<nome_arquivo>` | Baixa arquivo Excel processado |
| GET    | `/exibir-grafico/<grafico_id>` | Exibe gráfico a partir do JSON gerado (em desenvolvimento) |

