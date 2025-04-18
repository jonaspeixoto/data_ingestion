# 📊 Sistema de Ingestão de Dados

Sistema ETL  para processamento de dados de clientes a partir de arquivos Excel e armazenamento em banco de dados PostgreSQL.


## ⚙️ Configuração do Ambiente

### 1. Criar Ambiente Virtual
```
python -m venv venv
```

### 2. Ative o ambiente Virtual
```
# Windows:
venv\Scripts\activate
```
```
# Linux/MacOS:
source venv/bin/activate
```
### 3. Crie uma pasta chamada 'data' na raiz do projeto
-   Adicione seu o Excel com os dados a serem inseridos

### 4. Instale as bibliotecas necessarias.
```
pip install -r requirements.txt
```

### 5. Configurar Banco de Dados
-   Crie um arquivo .env na raiz e configure:
```
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nome_do_banco
```
### 6. Estrutura final do projeto
```
data_ingestion/
├── data/                  # Arquivos de entrada Excel
├── venv/                  # Ambiente virtual
├── main.py                # Script principal
├── requirements.txt       # Dependências
└── .env                   # Configurações (gitignored)
```

### 7.Execução
```
python main.py
```
