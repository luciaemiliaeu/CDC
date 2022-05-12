
import pandas as pd
import os

PWD = os.getcwd()
WATCH_DIRECTORY = PWD + "\input_data"
SAVE_DIRECTORY = PWD + "\output_data"

##b. i. Cinco clientes com maior saldo
def get_top_balance():
    df = pd.read_csv(SAVE_DIRECTORY+"\\updated.csv")
    return df.sort_values(by='balance', ascending = False).head(5)

##b. ii. Cinco clientes atualizados por último
def get_last_update():
    df = pd.read_csv(SAVE_DIRECTORY+"\\updated.csv")
    return df.sort_values(by='update_timestamp', ascending = False).head(5)

## b. iii. Saldo dos clientes
def get_balance():
    return pd.read_csv(SAVE_DIRECTORY+"\\updated.csv")

## C. Histórico de atualizações dos clientes com mais atualizações
def get_history():

    history = pd.read_csv(SAVE_DIRECTORY+"\\history.csv")

    count_updates = history[history['op']=='u'].groupby('customer_id')['op'].count()
    max_updates = count_updates.max()
    customers = count_updates[count_updates==max_updates].index.tolist()
    print(f"Número máximo de atualizações: {max_updates}")
    return history[history['customer_id'].isin(customers)].sort_values(by=['customer_id', 'update_timestamp'])

def menu():
    print('''
            MENU:

            [A] - Cinco clientes com maior saldo
            [B] - Cinco clientes atualizados por último
            [C] - Saldo dos clientes
            [D] - Histórico dos clientes com mais atualizações
            [S] - Sair
        ''')
    return str(input('Escolha uma opção: '))

def inicial():
    while True:
        opcao = menu()
        if opcao == "S": break
        elif opcao == "A":
            print("[A] - Cinco clientes com maior saldo")
            print(get_top_balance())
        elif opcao == "B" :
            print("[B] - Cinco clientes atualizados por último")
            print(get_last_update())
        elif opcao == "C" :
            print("[C] - Saldo dos clientes")
            print(get_balance())
        elif opcao == "D" :
            print("[D] - Histórico dos clientes com mais atualizações")
            print(get_history())
        else: "Escolha uma opção válida. \n"

if __name__ == "__main__":
    inicial()









