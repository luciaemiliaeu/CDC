# Simulador de Eventos CDC

O ojetivo desse projeto é processar eventos de CDC (Change Data Capture) enquanto permite a consulta das informações processadas de maneira simutânea. 

O código é composto por três scripts: generate.py, log_change.py e read.py. O generate.py gera arquivos de cdc na pasta input_data. Enquanto isso, o log_change.py escuta eventos de criação na mesma pasta, disparando a função de processamento dos arquivos e atualizando dois arquivos na pasta output_data: update.csv e history.csv. O arquivo update.csv contém apenas os dados mais recentes de cada usuário, enquanto o arquivo history.py guarda todo o histório de dados do CDC. 
Por fim, o script read.py permite fazer consultas nos arquivos de output através de um menu iterativo. 

## 🛠️ Como executar o projeto
Para executar o projeto você pode clonar o repositório e executar os scripts nas seguinte ordem:
- Primeiro o log_change.py para que ele possa iniciar a escuta das pastas aguardando os evento. 
- Depois dispare os eventos executando o generate.py. Esse arquivo recebe um parâmetro que indica a quantidade de eventos que devem ser gerado. Você também pode executar em loop usando o valor 0 ou não passando nenhum parâmetro.
- Por fim, execute o read.py para fazer suas consultas. Você pode fazer mais de uma execução simutânea desse script. 

Para encerrar as execuções, o read.py tem uma opção de saída no menu e os demais arquivos podem ser interrompidos com Ctrl+c no prompt.

O código foi desenvolvido em python 3 e, além das bibliotecas nativas, utiliza duas biblioecas: pandas e watchdog. 
