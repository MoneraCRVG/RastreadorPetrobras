import dotenv.parser
import mysql.connector
import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error, errorcode
from dotenv import load_dotenv
import os
from datetime import datetime


SOURCE = '''https://precos.petrobras.com.br/sele%C3%A7%C3%A3o-de-estados-gasolina'''
def get_page() -> bytes:
    try:
        response = requests.get(SOURCE)
        if response.status_code == 200:
            return response.content
    except requests.exceptions as e:
        print(f"Falha ao conectar com a fonte: {e}")

def get_price(content: bytes) -> float:
    try:
        bs4 = BeautifulSoup(content, 'html.parser')
        price_element = bs4.find(id='telafinal-precofinal')
        price = price_element.get_text()
        return float(price.replace(',','.'))
    except FloatingPointError as e:
        print(f"Falha ao capturar o preço do combustível: {e}")

def push_price_to_database(price: float):
    global cnx, cursor
    try:
        load_dotenv()
        cnx = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=3306,
            user=os.getenv('DB_USERNAME'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
        )
        cursor = cnx.cursor()

        today_date = datetime.now().today().date().strftime('%Y-%m-%d')
        INSERT_SQL = '''INSERT INTO u287123255_gasolina.historico (data, preco, grupo) VALUES (%s, %s, %s)'''
        cursor.execute(INSERT_SQL, (today_date, str(price), 'nicolas'))
    except dotenv.parser.Error as e:
        print(f"Falha ao ler o arquivo .env: {e}")
    except Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Credenciais inválidas")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Banco de dados não existe")
        else:
            print(f"Erro no banco de dados: {err}")

    except Exception as e:
        print(f"Um erro inesperado ocorreu: {e}")

    finally:
        if cnx:
            if cnx.is_connected():
                cnx.commit()
                cursor.close()
                cnx.close()


def main():
    html_content = get_page()
    price = get_price(html_content)
    push_price_to_database(price)


if __name__ == '__main__':
    main()
