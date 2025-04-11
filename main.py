import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import re

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


registros_validos = []
registros_invalidos = []
cpf_cnpj_ja_vistos = []


df = pd.read_excel('data/base_dados.xlsx')

for i, row in df.iterrows():
    motivos_erro = []
    cpf_cnpj = re.sub(r"\D", "", str(row.get("CPF/CNPJ", "")))
    
    if len(cpf_cnpj) != 11 and len(cpf_cnpj) != 14:
        motivos_erro.append("Formato de CPF/CNPJ inválido")
        
    if cpf_cnpj in cpf_cnpj_ja_vistos:
            motivos_erro.append("Duplicado na base de dados")
    else:
        cpf_cnpj_ja_vistos.append(cpf_cnpj)

    if motivos_erro:
        registros_invalidos.append({
            "linha": row.to_dict(),
            "erros": motivos_erro
        })
    else:
        row["cpf_cnpj"] = cpf_cnpj
        registros_validos.append(row)


total_inseridos = 0
print(len(registros_validos))

with engine.begin() as conn:
    for row in registros_validos:
        cpf_cnpj = row["cpf_cnpj"]
        resultado = conn.execute(
            text("SELECT id FROM tbl_clientes WHERE cpf_cnpj = :cpf_cnpj"),
            {"cpf_cnpj": cpf_cnpj}
        ).fetchone()

        if resultado:
            continue

        insert_cliente = text("""
            INSERT INTO tbl_clientes (
                nome_razao_social,
                nome_fantasia,
                cpf_cnpj,
                data_nascimento,
                data_cadastro
            ) VALUES (
                :nome_razao_social,
                :nome_fantasia,
                :cpf_cnpj,
                :data_nascimento,
                :data_cadastro
            )
        """)
        
        if pd.isna(row.get("Nome Fantasia")):
            row["Nome Fantasia"] = "NÃO INFORMADO"

        row = row.where(pd.notnull(row), None)
        data_nascimento = row.get("Data Nasc.")
        data_cadastro = row.get("Data Cadastro cliente")

        if data_nascimento:
            data_nascimento = data_nascimento.date()

        if data_cadastro:
            data_cadastro = data_cadastro.date()
            
        conn.execute(insert_cliente, {
            "nome_razao_social": row.get("Nome/Razão Social"),
            "nome_fantasia": row.get("Nome Fantasia"),
            "cpf_cnpj": cpf_cnpj,
            "data_nascimento": row.get("Data Nasc."),
            "data_cadastro": row.get("Data Cadastro cliente")
        })
        
        
        conn.execute(text("""
            INSERT INTO tbl_tipos_contato (tipo_contato) VALUES 
                ('Celular'),
                ('Telefone'),
                ('E-mail')
            ON CONFLICT (tipo_contato) DO NOTHING
        """))
        
        cliente_id = conn.execute(
            text("SELECT id FROM tbl_clientes WHERE cpf_cnpj = :cpf_cnpj"),
            {"cpf_cnpj": cpf_cnpj}
        ).scalar()

        tipo_celular_id = conn.execute(text("SELECT id FROM tbl_tipos_contato WHERE tipo_contato = 'Celular'")).scalar()
        tipo_telefone_id = conn.execute(text("SELECT id FROM tbl_tipos_contato WHERE tipo_contato = 'Telefone'")).scalar()
        tipo_email_id = conn.execute(text("SELECT id FROM tbl_tipos_contato WHERE tipo_contato = 'E-mail'")).scalar()
        
        if row.get("Celulares"):
            conn.execute(text("""
                INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato)
                VALUES (:cliente_id, :tipo_contato_id, :contato)
            """), {
                "cliente_id": cliente_id,
                "tipo_contato_id": tipo_celular_id,
                "contato": str(row.get("Celulares"))
            })

        if row.get("Telefones"):
            conn.execute(text("""
                INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato)
                VALUES (:cliente_id, :tipo_contato_id, :contato)
            """), {
                "cliente_id": cliente_id,
                "tipo_contato_id": tipo_telefone_id,
                "contato": str(row.get("Telefones"))
            })

        if row.get("Emails"):
            conn.execute(text("""
                INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato)
                VALUES (:cliente_id, :tipo_contato_id, :contato)
            """), {
                "cliente_id": cliente_id,
                "tipo_contato_id": tipo_email_id,
                "contato": row.get("Emails")
            })
        
        total_inseridos += 1
    
    print(f"Total de clientes inseridos: {total_inseridos}")




        
        

