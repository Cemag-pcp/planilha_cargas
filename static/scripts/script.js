$(document).ready(function(){
    $('#formConsulta').submit(function(event){
        event.preventDefault();

        const formData = new FormData(this);

        const enviarButton = document.getElementById('enviarButton');

        const tabela = document.getElementById('minhaTabela');
        const cabecalho = document.getElementById('cabecalho-tabela');
        const corpo = tabela.querySelector('tbody');
        const divReturnEmpty = document.getElementById('returnEmpty');
        const csvButton = document.getElementById('csvButton');

        // Limpa texto de retorno de cargas vazias
        divReturnEmpty.innerHTML = '';
        // Limpa cabeçalho e corpo da tabela
        cabecalho.innerHTML = '';
        corpo.innerHTML = '';

        csvButton.style.display = 'none';

        inverterButton(enviarButton);

        fetch('/processar/',{
            method: 'POST',
            body: formData,
        })
        .then(response => {
            if (response.ok){
                return response.json();
            }
             
        })
        .then(data => {
            // Chama a função para preencher a tabela
            preencherTabela(data);
            inverterButton(enviarButton);
            $('#csvLink')
                .attr('href', '/exportar-excel/' + data.arquivo)
                .show();
            csvButton.style.display = 'block';
        })
        .catch(error => {
            console.error('Erro:', error);  // Trata erros
            inverterButton(enviarButton);
            divReturnEmpty.innerHTML = "<span>NÃO EXISTE CARGA PARA ESTE INTERVALO DE DATAS</span>";
        });
    })
})

// Função para preencher a tabela com o JSON
function preencherTabela(data) {
    const tabela = document.getElementById('minhaTabela');
    const cabecalho = document.getElementById('cabecalho-tabela');
    const corpo = tabela.querySelector('tbody');

    // Limpa cabeçalho e corpo da tabela
    cabecalho.innerHTML = '';
    corpo.innerHTML = '';

    // Adicionando os cabeçalhos dinamicamente com base nas chaves do primeiro item do JSON
    const chaves = Object.keys(data.dados[0]);

    chaves.forEach(chave => {
        const th = document.createElement('th');
        th.innerText = chave;
        cabecalho.appendChild(th);
    });

    // Adicionando as linhas dinamicamente com base nos valores do JSON
    data.dados.forEach(linha => {
        const tr = document.createElement('tr');
        
        // Para cada valor da linha, criar uma célula <td>
        Object.values(linha).forEach(valor => {
            const td = document.createElement('td');
            td.innerText = valor;
            tr.appendChild(td);
        });
        
        corpo.appendChild(tr);
    });
}

function inverterButton(button){
    if (button.disabled){
        button.disabled = false;
        button.textContent = 'Enviar';
    }else{
        button.disabled = true;
        button.textContent = 'Carregando...';
    }
    
}