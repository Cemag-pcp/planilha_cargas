# 📄 Documentação — Script de Processamento de Cargas e Lead Time (xonexao_plan.py)

Este módulo reúne todas as funções responsáveis por:

- Buscar cargas entre intervalos de datas a partir de uma planilha Google Sheets  
- Conectar com a base de componentes das carretas  
- Processar tempos de montagem, solda e pintura  
- Calcular lead time de cada etapa  
- Calcular status, datas previstas, entrega e encerramento  
- Gerar um DataFrame final padronizado para integração com PCP

---

# 📁 Estrutura Geral do Script

O arquivo contém as seguintes funções principais:

1. **`busca_cargas()`**  
   Busca e consolida todas as carretas previstas entre duas datas.

2. **`conectar_com_base()`**  
   Retorna todos os componentes necessários para montar as carretas filtradas.

3. **`parse_data_condicional()`**  
   Converte strings de datas em formatos variados, tratando erros e normalizando.

4. **`dias_uteis()`**  
   Simula dias úteis (DIATRABALHO).

5. **`calcular_cor()`**  
   Reproduz a lógica de classificação visual de prioridades do Google Sheets.

6. **`definir_leadtime()`**  
   Monta toda a lógica de Lead Time para montagem, solda e pintura.

7. **`ajustar_para_dia_util()`**  
   Ajusta datas para o próximo dia útil quando necessário.

---

# 📌 1. Função `busca_cargas(data_inicio, data_final)`

### **Objetivo**
Buscar da planilha Google Sheets todas as carretas previstas no intervalo informado.

### **Processo de Negócio**
- Ler a planilha **Importar Dados**
- Padronizar colunas e converter datas
- Descartar linhas sem data
- Agrupar por:
  - Recurso  
  - Data prevista  
  - Tipo de carga  
- Somar quantidades planejas
- Remover códigos de cores (VJ, VM, AN, LC, LJ, AM, AV)

### **Retorno**
Um `DataFrame` com:

| Campo | Descrição |
|-------|-----------|
| PED_RECURSO.CODIGO | Código da carreta |
| PED_PREVISAOEMISSAODOC | Data prevista |
| Carga | Tipo da carga |
| PED_QUANTIDADE | Quantidade total agrupada |

---

# 📌 2. Função `conectar_com_base(cargas_filtradas)`

### **Objetivo**
Conectar com a planilha BASE e retornar todos os componentes necessários para montar as carretas filtradas.

### **Regras de Negócio**
- Merge por `PED_RECURSO.CODIGO → carreta`
- Manter somente peças cujo **primeiro processo = MONTAR**
- Remover descrições contendo:
  - COMPLETA  
  - COMPLETO  
  - ACESSORIO  
  - ACESSÓRIO  
- Calcular:
  - `QTD = PED_QUANTIDADE * TOTAL`
  - `QTD_ORIGINAL = QTD`

### **Retorno**
DataFrame contendo:

| Coluna | Descrição |
|--------|-----------|
| Data | Data da carga |
| carreta | Código |
| DESCRICAO | Nome do item |
| QTD | Quantidade total |
| COD | Código do item |
| Carga | Tipo da carga |

---

# 📌 3. `parse_data_condicional(data_str)`

Função robusta para lidar com datas em formatos variados:

- Datas com "T"  
- Strings inválidas  
- Datas com timezone  
- Valores vazios

Retorna sempre `datetime` ou `NaT`.

---

# 📌 4. `dias_uteis(data_inicial, dias)`

Implementação customizada de soma/subtração de dias úteis.

Usada em regras de prioridade das cores.

---

# 📌 5. `calcular_cor(row)`

Reproduz a lógica do Google Sheets para cálculo de cor (prioridade).

### **Regras**
- Se OP = 0 → **CINZA**
- Se data < emissão → **AZUL**
- 33% do lead time → **VERDE**
- 66% do lead time → **AMARELO**
- 100% até a entrega → **VERMELHO**
- Após entrega → **PRETO**

---

# 📌 6. Função **`definir_leadtime(conjuntos)`**

Esta é a função mais importante do script.  
Ela monta todo o fluxo de produção das carretas considerando:

### ✔️ Etapas:
- **Montagem**
- **Solda**
- **Pintura**

### ✔️ O que a função faz

1. Carrega várias planilhas:
   - Lead Time  
   - Apontamento montagem  
   - Apontamento solda  
   - Tempos de pintura  

2. Normaliza códigos, datas e formatos

3. Calcula:
   - Primeira ocorrência (liberação)  
   - Última ocorrência (entrega)  
   - Quantidade apontada  
   - Quantidade planejada  

4. Constrói um dataframe consolidado com:
   - ETAPA (montagem/solda/pintura)
   - datas de início/fim
   - status  
   - lead time  
   - previsão de entrega  
   - cálculo da cor (prioridade)

5. Aplica regras avançadas de status:
   - Aguardando Liberação  
   - Em Processo  
   - Finalizada  

6. Gera as colunas padronizadas PCP:
   - Ordem de Produção  
   - Produto  
   - Quantidade  
   - Status  
   - OPCIONAL 1 a OPCIONAL 7  
   - COR PRIORIDADE  

### **Retorno final**
Um DataFrame com **todas as carretas**, cada etapa, quantidades e tempos.

---

# 📌 7. `ajustar_para_dia_util(series)`

Ajusta datas em finais de semana para o próximo dia útil.

---

# 📊 Fluxo Geral de Negócio

```mermaid
flowchart TD
    A[busca_cargas] --> B[conectar_com_base]
    B --> C[definir_leadtime]
    C --> D[Tratamento de datas e quantidades]
    D --> E[Cálculo de status]
    E --> F[Classificação por cor]
    F --> G[DataFrame final padronizado PCP]
