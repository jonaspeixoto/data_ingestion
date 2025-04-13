import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import re


# Implementaçao, configuração das variaveis de ambiente e drive de conexão com o banco de dados
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


# Variaveis Globais
total_inseridos = 0
clientes_inseridos = 0
clientes_existentes = 0
contatos_inseridos = 0
contratos_inseridos = 0
registros_validos = []
registros_invalidos = []
cpf_cnpj_ja_vistos = []


def inserir_cliente(conn, row):
    cpf_cnpj = row["cpf_cnpj"]
    cliente = conn.execute(text("SELECT id FROM tbl_clientes WHERE cpf_cnpj = :cpf_cnpj"),{"cpf_cnpj": cpf_cnpj}).scalar()
    if not cliente:  
        inserir_cliente = text("""
                              
        INSERT INTO tbl_clientes (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro) 
        VALUES (:nome_razao_social, :nome_fantasia, :cpf_cnpj, :data_nascimento, :data_cadastro)
                              
        """)

        # Se o nome fantasia for em branco colocar valor padrão como não informado
        if pd.isna(row.get("Nome Fantasia")):
            row["Nome Fantasia"] = "Não Informado"

        row = row.where(pd.notnull(row), None)
        data_nascimento = row.get("Data Nasc.")
        data_cadastro = row.get("Data Cadastro cliente")

        if data_nascimento:
            data_nascimento = data_nascimento.date()

        if data_cadastro:
            data_cadastro = data_cadastro.date()
            
        conn.execute(inserir_cliente, {
            "nome_razao_social": row.get("Nome/Razão Social"),
            "nome_fantasia": row.get("Nome Fantasia"),
            "cpf_cnpj": cpf_cnpj,
            "data_nascimento": row.get("Data Nasc."),
            "data_cadastro": row.get("Data Cadastro cliente")
        })

        global clientes_inseridos
        clientes_inseridos+=1
    else:
        global clientes_existentes
        clientes_existentes += 1
        print(f'Cliente {row.get("Nome/Razão Social")} de Cpf/cnpj:{cpf_cnpj} já foi cadastrado')


def inserir_tipos_contatos(conn):
    conn.execute(
        text("""
             
            INSERT INTO tbl_tipos_contato (tipo_contato) 
            VALUES ('Celular'), ('Telefone'), ('E-mail')
            ON CONFLICT (tipo_contato) DO NOTHING
             
        """))
    
def inserir_contatos(conn, row):
    if not pd.isna(row.get("Celulares")):
            conn.execute(text("""
                INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato)
                VALUES (:cliente_id, :tipo_contato_id, :contato)
                ON CONFLICT (cliente_id, tipo_contato_id, contato) DO NOTHING
            """), {
                "cliente_id": cliente_id,
                "tipo_contato_id": tipo_celular_id,
                "contato": str(row.get("Celulares")).rstrip('.0')
            })
    if not pd.isna(row.get("Telefones")):
        conn.execute(text("""
            INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato)
            VALUES (:cliente_id, :tipo_contato_id, :contato)
            ON CONFLICT (cliente_id, tipo_contato_id, contato) DO NOTHING
        """), {
            "cliente_id": cliente_id,
            "tipo_contato_id": tipo_telefone_id,
            "contato": str(row.get("Telefones")).rstrip('.0')
        })

    if not pd.isna(row.get("Emails")):
        conn.execute(text("""
            INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato)
            VALUES (:cliente_id, :tipo_contato_id, :contato)
            ON CONFLICT (cliente_id, tipo_contato_id, contato) DO NOTHING
        """), {
            "cliente_id": cliente_id,
            "tipo_contato_id": tipo_email_id,
            "contato": row.get("Emails")
        })
    
def inserir_plano(conn, row):
    insert_planos = text("""
        INSERT INTO tbl_planos (
            descricao,
            valor         
        ) VALUES (
            :descricao,
            :valor
        ) ON CONFLICT (descricao) DO NOTHING
    """)
    conn.execute(insert_planos, {
        "descricao": row.get("Plano"),
        "valor": row.get("Plano Valor"),
    })

def inserir_status(conn, row):
    insert_satus = text("""
        INSERT INTO tbl_status_contrato (
            status     
        ) VALUES (
            :status
        ) ON CONFLICT (status) DO NOTHING
    """)
    conn.execute(insert_satus, {
        "status": row.get("Status"),
    })

def inserir_contrato(conn, row):
    insert_cliente_contratos = text("""
            INSERT INTO tbl_cliente_contratos (
                cliente_id,
                plano_id,
                dia_vencimento,
                isento,
                endereco_logradouro,
                endereco_numero,
                endereco_bairro,
                endereco_cidade,
                endereco_complemento,
                endereco_cep,
                endereco_uf,
                status_id
            ) VALUES (
                :cliente_id,
                :plano_id,
                :dia_vencimento,
                :isento,
                :endereco_logradouro,
                :endereco_numero,
                :endereco_bairro,
                :endereco_cidade,
                :endereco_complemento,
                :endereco_cep,
                :endereco_uf,
                :status_id
            )
        """)

    if row.get('Isento') == "Sim":
        isento = True
    else:
        isento = False


    existe = conn.execute(text("""
        SELECT * FROM tbl_cliente_contratos
        WHERE cliente_id = :cliente_id
        AND plano_id = :plano_id
        AND endereco_cep = :endereco_cep
    """), {
    'cliente_id': cliente_id,
    'plano_id': plano_id,
    'endereco_cep': row.get("CEP"),
    }).fetchone()

    if not existe:
        conn.execute(insert_cliente_contratos, {
            "cliente_id": cliente_id,
            "plano_id": plano_id,
            "dia_vencimento": row.get("Vencimento"),
            "isento":isento,
            "endereco_logradouro": row.get("Endereço"),
            "endereco_numero":row.get("Número"),
            "endereco_bairro":row.get("Bairro"),
            "endereco_cidade":row.get("Cidade"),
            "endereco_complemento":row.get("Complemento"),
            "endereco_cep":row.get("CEP"),
            "endereco_uf":row.get("UF"),
            "status_id":status_id
        })


# Leitura da base de dados, remoção de incossistencias e tatamento de dados. 
df = pd.read_excel('data/base_dados.xlsx')
df = df.drop_duplicates()

mapeamento_uf = {
    'Paraná': 'PR',
    'Paraíba': 'PB',
    'Tocantins': 'TO',
    'São Paulo': 'SP',
    'Piauí': 'PI',
    'Roraima': 'RR',
    'Amapá': 'AP',
    'Rio Grande do Norte': 'RN',
    'Mato Grosso': 'MT',
    'Amazonas': 'AM',
    'Rondônia': 'RO',
    'Goiás': 'GO',
    'Mato Grosso do Sul': 'MS',
    'Espírito Santo': 'ES',
    'Santa Catarina': 'SC',
    'Acre': 'AC',
    'Alagoas': 'AL',
    'Bahia': 'BA',
    'Ceará': 'CE',
    'Rio Grande do Sul': 'RS',
    'Distrito Federal': 'DF',
    'Sergipe': 'SE',
    'Pará': 'PA',
    'Pernambuco': 'PE',
    'Minas Gerais': 'MG',
    'Rio de Janeiro': 'RJ',
    'Maranhão': 'MA'
}


df['UF'] = df['UF'].map(mapeamento_uf)
df['Endereço'] = df['Endereço'].fillna('')


for i, row in df.iterrows():
    motivos_erro = []
    cpf_cnpj = re.sub(r"\D", "", str(row.get("CPF/CNPJ", "")))
    cep = re.sub(r"\D", "", str(row.get("CEP", "")))
    
    if len(cpf_cnpj) != 11 and len(cpf_cnpj) != 14:
        motivos_erro.append("Formato de CPF/CNPJ inválido")

    if len(cep) != 8:
        motivos_erro.append("Formato de CEP inválido")

    # if pd.isna(row.get('Endereço')):
    #     motivos_erro.append("Endereço em branco")
        
    if motivos_erro:
        registros_invalidos.append({
            "linha": row.to_dict(),
            "erros": motivos_erro
        })

    else:
        row["cpf_cnpj"] = cpf_cnpj
        row["CEP"] = cep
        df['Endereço'] = df['Endereço'].fillna('')
        registros_validos.append(row)


# Conexão com banco de dados
with engine.begin() as conn:
    print("Inserindo registros...")
    for row in registros_validos:
        inserir_cliente(conn, row)
        cliente_id = conn.execute(text("SELECT id FROM tbl_clientes WHERE cpf_cnpj = :cpf_cnpj"),{"cpf_cnpj": row.get("cpf_cnpj")}).scalar()
        inserir_tipos_contatos(conn)
        
        tipo_celular_id = conn.execute(text("SELECT id FROM tbl_tipos_contato WHERE tipo_contato = 'Celular'")).scalar()
        tipo_telefone_id = conn.execute(text("SELECT id FROM tbl_tipos_contato WHERE tipo_contato = 'Telefone'")).scalar()
        tipo_email_id = conn.execute(text("SELECT id FROM tbl_tipos_contato WHERE tipo_contato = 'E-mail'")).scalar()
        
        inserir_contatos(conn, row)
        inserir_plano(conn, row)
        inserir_status(conn, row)

        plano_id = conn.execute(text("SELECT id FROM tbl_planos WHERE descricao = :descricao"),{"descricao": row.get("Plano")}).scalar()
        status_id = conn.execute(text("SELECT id FROM tbl_status_contrato WHERE status = :status"),{"status": row.get("Status")}).scalar()
        inserir_contrato(conn, row)


        

print("\nRegistros Inserido com sucesso!")    
print(f"\nTotal de registros processados: {len(df)}")
print(f"Registros válidos: {len(registros_validos)}")
print(f"Registros inválidos: {len(registros_invalidos)}")
print(f"\nClientes novos inseridos: {clientes_inseridos}")
print(f"Clientes já existentes: {clientes_existentes}")

df = pd.json_normalize(registros_invalidos)
df.columns = df.columns.str.replace('linha.', '')
df.to_excel('registros_nao_cadastrados.xlsx', index=False)




        
        

