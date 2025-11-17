# Documentação — unificar.py

Este módulo tem como objetivo unificar planilhas Excel diárias, aplicar filtros por data, corrigir dados inconsistentes e consolidar informações relacionadas à produção. Ele é utilizado em uma rotina de atualização diária, onde várias planilhas individuais precisam ser mescladas em uma única planilha final.

## Objetivo da Regra de Negócio

O script implementa a seguinte lógica funcional:

1. Ler todas as planilhas de um diretório específico e considerar somente as que:
   - Possuem extensão .xlsx
   - Não são arquivos temporários (~$)
   - Contêm uma data no nome do arquivo maior ou igual à data mínima definida

2. Processar cada planilha individual, realizando:
   - Conversão de datas da coluna OPCIONAL 2
   - Preenchimento automático do campo Recurso quando estiver vazio para produtos específicos
   - Filtro de linhas que possuam OPCIONAL 2 <= data_final

3. Separar a planilha do dia atual (última da lista) e preservar seus dados sem duplicações.

4. Unificar todas as demais planilhas anteriores ao dia atual.

5. Regra de negócio sobre finalizações:
   - Manter somente registros com Status = "Finalizada"
   - Remover duplicidades baseando-se nas colunas:
     - Ordem de Produção
     - Produto
     - OPCIONAL 7
     - Status
   - Atualizar a coluna Data para a data atual

6. Combinar a planilha do dia atual com os dados históricos finalizados.

7. Ordenar o resultado final pela coluna OPCIONAL 2.

## Estrutura esperada do projeto

atualizacao-diaria/
├── arquivos-individuais/
│   ├── cargas_2025-01-01.xlsx
│   ├── cargas_2025-01-02.xlsx
│   └── ...
└── unificar.py

## Descrição das Funções

### unificar_planilhas(data_atual_arquivo, data_final, data_atual)
Função principal do módulo. Responsável por carregar, filtrar e consolidar todas as planilhas Excel.

Parâmetros:
- data_atual_arquivo: string usada no nome do arquivo final (caso fosse salvo)
- data_final: limite máximo para filtrar a coluna OPCIONAL 2
- data_atual: data mínima permitida para o nome dos arquivos

Fluxo interno resumido:
1. Converte datas para remover timezone
2. Percorre a pasta com os arquivos
3. Seleciona arquivos válidos baseado no nome
4. Lê as planilhas
5. Converte OPCIONAL 2 para datetime
6. Preenche o campo Recurso com regras específicas
7. Filtra linhas por data
8. Separa a planilha mais recente
9. Concatena as demais
10. Remove duplicidades apenas em itens finalizados
11. Atualiza a data dos finalizados
12. Junta tudo e ordena

Retorno:
Um DataFrame pandas contendo a planilha unificada.

### verifica_data_arquivo(nome_arquivo, data_limite)
Valida se o arquivo deve ser processado.

Regras:
- Extrai a data do arquivo no formato cargas_YYYY-MM-DD.xlsx
- Converte para datetime
- Valida se: data_do_arquivo >= data_limite

### preencher_recurso(row)
Preenche automaticamente o campo Recurso para produtos cujo valor esteja vazio.

Regra aplicada:
Se Recurso estiver vazio e o produto for um dos listados:

030389 → CONJ INTERMED  
30389  → CONJ INTERMED  
450133 → CONJ INTERMED  

Então: Recurso = "CONJ INTERMED"

Caso contrário, mantém o valor original.

## Observações

- O código assume que existe uma planilha referente ao dia atual na pasta.
- Arquivos temporários do Excel (~$) são ignorados.
- Todas as planilhas devem conter as colunas:
  - OPCIONAL 2
  - Produto
  - Recurso
  - Status
  - Ordem de Produção
  - OPCIONAL 7
- A função retorna um DataFrame e não salva em Excel, mas isso pode ser ativado se você descomentar o trecho de salvamento.
