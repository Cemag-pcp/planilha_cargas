# 📄 Documentação — unificar.py

Este módulo tem como objetivo unificar planilhas Excel diárias, aplicar filtros por data, corrigir dados inconsistentes e consolidar informações de produção. Ele faz parte de uma rotina de atualização diária onde diversas planilhas individuais são mescladas em uma única planilha consolidada.

---

# 📌 Regras de Negócio — Visão Geral

1. **Selecionar arquivos válidos** para unificação:
   - Extensão `.xlsx`
   - Não serem arquivos temporários (`~$`)
   - Terem data no nome **maior ou igual** à data de corte (`data_atual`)

2. **Tratar cada planilha individual**:
   - Converter a coluna `OPCIONAL 2` para datetime
   - Preencher automaticamente a coluna `Recurso` caso esteja vazia
   - Filtrar linhas onde `OPCIONAL 2 <= data_final`

3. **Identificar a planilha do dia atual**:
   - A última planilha encontrada
   - Será usada como base para a unificação

4. **Unificar as demais planilhas anteriores**:
   - Concatenar todas as planilhas exceto a de hoje

5. **Tratar registros finalizados**:
   - Manter somente linhas com `Status = "Finalizada"`
   - Remover duplicidades considerando:
     - Ordem de Produção  
     - Produto  
     - OPCIONAL 7  
     - Status  
   - Atualizar a coluna `Data` para a data do dia

6. **Construir a planilha final**:
   - Juntar a planilha de hoje + finalizadas únicas
   - Ordenar pela coluna `OPCIONAL 2`

---

# 📁 Estrutura esperada do projeto

atualizacao-diaria/  
├── arquivos-individuais/  
│   ├── cargas_2025-01-01.xlsx  
│   ├── cargas_2025-01-02.xlsx  
│   └── ...  
└── unificar.py  

---

# ⚙️ Descrição das Funções

## 🔧 unificar_planilhas(data_atual_arquivo, data_final, data_atual)

Função principal responsável pela unificação e tratamento das planilhas.

### **Parâmetros**
- `data_atual_arquivo`: string usada no nome do arquivo final (caso fosse salvo)
- `data_final`: data limite para filtrar linhas pela coluna `OPCIONAL 2`
- `data_atual`: data mínima permitida para os arquivos considerados

### **Fluxo interno (resumo claro)**
1. Normalização das datas recebidas  
2. Leitura dos arquivos da pasta  
3. Seleção apenas dos arquivos válidos  
4. Conversão da coluna `OPCIONAL 2`  
5. Preenchimento da coluna `Recurso`  
6. Filtragem por data limite  
7. Separação da planilha do dia atual  
8. Concatenação das demais planilhas  
9. Remoção de duplicidades em finalizadas  
10. Atualização da data  
11. Junção com a planilha de hoje  
12. Ordenação final  

### **Retorno**
Um `DataFrame` pandas contendo todos os dados consolidados.

---

## 🔍 verifica_data_arquivo(nome_arquivo, data_limite)

Função que valida se um arquivo deve ser processado.

### **Regra aplicada**
- O nome do arquivo deve seguir o padrão:  
  `cargas_YYYY-MM-DD.xlsx`
- A data extraída deve ser **>= data_limite**

---

## 🧩 preencher_recurso(row)

Função auxiliar que preenche automaticamente o campo `Recurso` quando ele estiver vazio.

### **Tabela de Regras**
Se o produto estiver nesta lista:

| Produto | Recurso        |
|---------|----------------|
| 030389  | CONJ INTERMED |
| 30389   | CONJ INTERMED |
| 450133  | CONJ INTERMED |

E o campo `Recurso` estiver vazio → preencher com **CONJ INTERMED**.

Caso contrário, manter o valor existente.

---

# 📎 Observações importantes

- O código assume que exista **uma planilha referente ao dia atual** na pasta.
- Arquivos temporários do Excel (`~$`) são ignorados automaticamente.
- Todas as planilhas devem conter as colunas:
  - OPCIONAL 2  
  - Produto  
  - Recurso  
  - Status  
  - Ordem de Produção  
  - OPCIONAL 7  
- A função retorna um DataFrame e não salva o Excel automaticamente (mas isso pode ser ativado ao descomentar o trecho de salvamento no código).

---
