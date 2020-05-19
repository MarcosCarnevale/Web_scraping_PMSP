import lxml.html as parser
import requests
from urllib.parse import urlsplit, urljoin
import pandas as pd
import xlrd
import time
from datetime import timedelta



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

lista = lista[0:12] #Seleção de quantos meses serão trabalhados

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

#Exportando dados
etl.to_excel(r'C:\Users\USUARIO1\Desktop\Dados_Prefeitura_de_Sao_Paulo.xlsx')

end_time = time.monotonic()
print(timedelta(seconds=end_time - start_time))



