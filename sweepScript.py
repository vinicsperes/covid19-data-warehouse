import sys
import uuid
import requests
import datetime
import mysql.connector

from requests.auth import HTTPBasicAuth
from ssl import ALERT_DESCRIPTION_UNRECOGNIZED_NAME

sys.setrecursionlimit(100000)

connection = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root",
  database="covid_vac_db"
)

dimPacienteInsertQuery = "INSERT INTO dim_paciente (paciente_id, paciente_idade, paciente_dataNascimento, paciente_sexo, paciente_racaCor_valor, paciente_nacionalidade, paciente_endereco_nmPais, paciente_endereco_uf, paciente_endereco_municipio, paciente_endereco_cep)"
dimPacienteInsertQuery += "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

dimTempoInsertQuery = "INSERT INTO dim_tempo (tempo_id, vacina_dataAplicacao, dia, mes, ano)"
dimTempoInsertQuery += "VALUES (%s, %s, %s, %s, %s);"

dimVacinaInsertQuery = "INSERT INTO dim_vacina(vacina_id, vacina_nome, vacina_grupo_atendimento, vacina_categoria, vacina_lote, vacina_fabricante, vacina_descricao_dose)"
dimVacinaInsertQuery += "VALUES (%s, %s, %s, %s, %s, %s, %s);"

dimEstabelecimentoInsertQuery = "INSERT INTO dim_estabelecimento (estabelecimento_id, estabelecimento_valor, estabelecimento_razaoSocial, estabelecimento_nomeFantasia, estabelecimento_municipio_nome, estabelecimento_uf)"
dimEstabelecimentoInsertQuery += "VALUES (%s, %s, %s, %s, %s, %s);"

fatoInsertQuery = "INSERT INTO fato (paciente_id, tempo_id, vacina_id, estabelecimento_id)"
fatoInsertQuery += "VALUES (%s, %s, %s, %s);"

cursor = connection.cursor()

auth = HTTPBasicAuth('imunizacao_public', 'qlto5t&7r_@+#Tlstigi')

def getPage(auth, scrollId):
  if (scrollId):
    pag_url = "https://imunizacao-es.saude.gov.br/_search/scroll"
    body = {
      "scroll_id": scrollId,
      "scroll": "1m"
    }
  else:
    pag_url = "https://imunizacao-es.saude.gov.br/_search?scroll=1m"  
    body = {"size": 2}

  response = requests.post(
    url=pag_url,
    json=body,
    auth=auth,
    headers={'Accept': 'application/xml; charset=utf-8','User-Agent':'foo'}
  )
  print(response.status_code, response.json().keys())
  print(response.status_code, len(response.json()['hits']))
  
  scrollId = response.json()['_scroll_id']

  for val in response.json()['hits']['hits']:
    print('--- PACIENTE ---')

    pacienteId = uuid.uuid4()

    pacienteData = (
      str(pacienteId),
      val['_source']['paciente_idade'],
      val['_source']['paciente_dataNascimento'],
      val['_source']['paciente_enumSexoBiologico'],
      val['_source']['paciente_racaCor_valor'],
      val['_source']['paciente_nacionalidade_enumNacionalidade'],
      val['_source']['paciente_endereco_nmPais'],
      val['_source']['paciente_endereco_uf'],
      val['_source']['paciente_endereco_nmMunicipio'],
      val['_source']['paciente_endereco_cep']
    )

    cursor.execute(dimPacienteInsertQuery, pacienteData)
    connection.commit()

    print('paciente_id: ',val['_source']['paciente_id'])
    print('paciente_idade: ',val['_source']['paciente_idade'])
    print('paciente_dataNascimento: ',val['_source']['paciente_dataNascimento'])
    print('paciente_sexo: ',val['_source']['paciente_enumSexoBiologico'])
    print('paciente_racaCor_valor: ',val['_source']['paciente_racaCor_valor'])
    print('paciente_nacionalidade: ',val['_source']['paciente_nacionalidade_enumNacionalidade'])
    print('paciente_endereco_nmPais: ',val['_source']['paciente_endereco_nmPais'])
    print('paciente_endereco_uf: ',val['_source']['paciente_endereco_uf'])
    print('paciente_endereco_nmMunicipio: ',val['_source']['paciente_endereco_nmMunicipio'])
    print('paciente_endereco_cep: ',val['_source']['paciente_endereco_cep'])
    
    print('--- TEMPO ---')

    dataHoraAplicacao = val['_source']['vacina_dataAplicacao']
    dataAplicacaoSplit = dataHoraAplicacao.split('T')
    dataAplicacao = dataAplicacaoSplit[0].split('-')

    ano = dataAplicacao[0]
    mes = dataAplicacao[1]
    dia = dataAplicacao[2]

    tempoId = uuid.uuid4()
    print('tempoId', tempoId)
    tempoData = (
      str(tempoId),
      dataAplicacaoSplit[0],
      dia,
      mes,
      ano
    )

    cursor.execute(dimTempoInsertQuery, tempoData)
    connection.commit()

    print('vacina_dataAplicacao: ',dataHoraAplicacao)
    print('vacina_dia: ',dia)
    print('vacina_mes: ',mes)
    print('vacina_ano: ',ano)

    print('--- VACINA ---')

    vacinaId = uuid.uuid4()

    vacinaData = (
      str(vacinaId),
      val['_source']['vacina_nome'],
      val['_source']['vacina_grupoAtendimento_nome'],
      val['_source']['vacina_categoria_nome'],
      val['_source']['vacina_lote'],
      val['_source']['vacina_fabricante_nome'],
      val['_source']['vacina_descricao_dose']
    )

    cursor.execute(dimVacinaInsertQuery, vacinaData)
    connection.commit()
    
    print('vacina_nome: ',val['_source']['vacina_nome'])
    print('vacina_grupo_atendimento: ',val['_source']['vacina_grupoAtendimento_nome'])
    print('vacina_categoria: ',val['_source']['vacina_categoria_nome'])
    print('vacina_lote: ',val['_source']['vacina_lote'])
    print('vacina_fabricante: ',val['_source']['vacina_fabricante_nome'])
    print('vacina_descricao_dose: ',val['_source']['vacina_descricao_dose'])

    print('--- ESTABELECIMENTO ---')

    estabelecimentoId = uuid.uuid4()

    estabelecimentoData = (
      str(estabelecimentoId),
      val['_source']['estabelecimento_valor'],
      val['_source']['estabelecimento_razaoSocial'],
      val['_source']['estalecimento_noFantasia'],
      val['_source']['estabelecimento_municipio_nome'],
      val['_source']['estabelecimento_uf']
    )

    cursor.execute(dimEstabelecimentoInsertQuery, estabelecimentoData)
    connection.commit()

    print('estabelecimento_valor: ',val['_source']['estabelecimento_valor'])
    print('estabelecimento_razaoSocial: ',val['_source']['estabelecimento_razaoSocial'])
    print('estabelecimento_noFantasia: ',val['_source']['estalecimento_noFantasia'])
    print('estabelecimento_municipio_nome: ',val['_source']['estabelecimento_municipio_nome'])
    print('estabelecimento_uf: ',val['_source']['estabelecimento_uf'])

    fatoData = (
      str(pacienteId),
      str(tempoId),
      str(vacinaId),
      str(estabelecimentoId)
    )

    cursor.execute(fatoInsertQuery, fatoData)
    connection.commit()

  if (len(response.json()['hits']) > 0):
    getPage(auth, scrollId)
  else:
    cursor.close()
    connection.close()

getPage(auth, 0)
