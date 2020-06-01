import lxml.html as parser
import requests
from urllib.parse import urlsplit, urljoin
import pandas as pd
#import xlrd
import time
from datetime import timedelta
#from sqlalchemy import create_engine
import pyodbc
#import numpy

start_time = time.monotonic()
print('Executando...')


#Conectando ao site
start_url = "http://dados.prefeitura.sp.gov.br/pt_PT/dataset/remuneracao-servidores-prefeitura-de-sao-paulo" 
r = requests.get(start_url)
html = parser.fromstring(r.text)

#Buscando os links para download dos aquivos pag1
links = html.xpath("//a[@class='heading']/@href")
base_url = "http://dados.prefeitura.sp.gov.br/pt_PT/dataset/remuneracao-servidores-prefeitura-de-sao-paulo"
links = [urljoin(base_url, l) for l in links]


t_lnk = []
t_lnk = links

for i in t_lnk:
    print(i)

lista = []

#Buscando os links para download dos aquivos pag2
for i in t_lnk:
    start_url = i
    r = requests.get(start_url)
    html = parser.fromstring(r.text)
    links = html.xpath("//a[@class='btn btn-primary resource-url-analytics resource-type-None']/@href")
    base_url = i
    links = [urljoin(base_url, l) for l in links]
    #Tratamendo para seleção do tipo de arquivo
    for i in links:
        x = i[-4:]
        x = x.replace('.','')
        if x == 'xlsx': #seleção do tipo de arquivo
            lista.append(links[0])
        else:
            pass

lista = lista[0:1] #Seleção de quantos meses serão trabalhados

df = pd.DataFrame(lista, index = None)

#exportação de links usados (compor documentação)
df.to_csv(r'C:\Users\USUARIO1\Desktop\Dados_Prefeitura_de_Sao_Paulo.csv')

print('Links Exportados')


#DataFrame a ser usado no PBI
etl = pd.DataFrame()

#Inserindo coluna de mes referencia
for i in lista:
    x = i
    print(i)
    y = pd.read_excel(str(x))
    ref = str(x)
    ref = ref.split(sep= 'remuneracao')
    ref = str(ref[-1:])
    ref = str(ref[2:9])
    y.insert(0, 'Referencia', str(ref))
    etl = etl.append(y, ignore_index = True)
    
print(etl)

#Corrigindo Campos
etl = etl.rename(columns={'Nome completo': 'Nome_Completo','Cargo Base':'Cargo_Base','Cargo em Comissão':'Cargo_em_Comissao',"Remuneração do Mês":'Remuneracao_do_Mes',"Demais Elementos da Remuneração":'Demais_Elementos_da_Remuneracao',"Remuneração Bruta":'Remuneracao_Bruta',"Tp. Log":'Tp_Log'})

#etl.to_csv(r'C:\Users\USUARIO1\Desktop\Dados_Prefeitura_de_Sao_Paulo.csv')


#String Nomes das colunas do DataFrame 
columns_name = []

for col in etl.columns:
    
    columns_name.append(col)

columns_name = ', '.join(columns_name)

#Tratando dados NaN 
etl = etl.fillna(0)


#Lista Colunas
row = etl.columns
"""
Tenho que refatorar esse trecho, mas em resumo, aqui se pega a primeira linha do dataframe para identificar os tipos de dados, após
concatena esta informação com o nome da coluna,  criando assim uma lista de columns/type para o Create table no SQL
"""

teste = etl.head(5)
teste = teste.values
gg = []
for i in teste:
    gg.append(i)
    
row = etl.columns
row = list(row)
ddd = list(teste[0])
ddd
tipo = []
for i in ddd:
    if isinstance(i,str) == True:
        i = 'nvarchar(200)'
    elif isinstance(i,float) == True:
        i = 'float'
    elif isinstance(i,int) == True:
        i = 'int'
    else:
        pass
    tipo.append(i)
    


test_keys = row 
test_values = tipo

res = {} 
for key in test_keys: 
    for value in test_values: 
        res[key] = value 
        test_values.remove(value) 
        break  
create_table = []       
for key, value in res.items():
    k = key
    v = value
    conct = k + " " +v
    create_table.append(conct)

create_table = ', '.join(create_table)

# Drop da tabela no banco de dados
"""
Só uma medida paliativa enquanto não soluciono como faz update apenas dos dados novos
"""

query_table = "use master if exists (select * from sysobjects where name='PMSP' and xtype='U') drop table PMSP"

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=LAPTOP-RFQHJ2MP;'
                      'Database=master;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

cursor.execute(query_table)
               
conn.commit()
conn.close()

# Create da tabela no banco de dados
query_table = "use master if not exists (select * from sysobjects where name='PMSP' and xtype='U') create table PMSP ("

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=LAPTOP-RFQHJ2MP;'
                      'Database=master;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

cursor.execute(query_table+create_table+")")
               
conn.commit()
conn.close()


# Criação da tabela no banco de dados
query_table = "use master if not exists (select * from sysobjects where name='PMSP' and xtype='U') create table PMSP ("

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=LAPTOP-RFQHJ2MP;'
                      'Database=master;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

cursor.execute(query_table+create_table+")")
               
conn.commit()
conn.close()



# Insert os dados na tabela PMSP
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=LAPTOP-RFQHJ2MP;'
                      'Database=master;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in etl.iterrows():
    cursor.execute("insert into PMSP ("+columns_name+") values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row['Referencia'], row['Exceção'], row['Nome_Completo'], row['Cargo_Base'],row['Cargo_em_Comissao'], row['Remuneracao_do_Mes'], row['Demais_Elementos_da_Remuneracao'], row['Remuneracao_Bruta'], row['Unidade'],row['Tp_Log'], row['Logadrouro'], row['Número'], row['Complemento'], row['Jornada'])
    conn.commit()

cursor.close()
conn.close()


# Timer de execução
end_time = time.monotonic()
print(timedelta(seconds=end_time - start_time))



