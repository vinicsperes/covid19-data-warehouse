CREATE DATABASE covid_vac_db;
USE covid_vac_db;

CREATE TABLE dim_paciente(
    paciente_id VARCHAR(180) PRIMARY KEY,
    paciente_idade VARCHAR(3),
    paciente_dataNascimento DATE,
    paciente_sexo VARCHAR(10),
    paciente_racaCor_valor VARCHAR(30),
    paciente_nacionalidade VARCHAR(30),
    paciente_endereco_nmPais VARCHAR(30),
    paciente_endereco_uf VARCHAR(30),
    paciente_endereco_municipio VARCHAR(120),
    paciente_endereco_cep VARCHAR(30)
);

CREATE TABLE dim_tempo(
    tempo_id VARCHAR(180) PRIMARY KEY,
    vacina_dataAplicacao DATE,
    dia INT(3),
    dia_semana VARCHAR(30),
    mes VARCHAR(30),
    semestre VARCHAR(30),
    ano INT(5)
);

CREATE TABLE dim_vacina(
    vacina_id VARCHAR(180) PRIMARY KEY,
    vacina_nome VARCHAR(180),
    vacina_grupo_atendimento VARCHAR(180),
    vacina_categoria VARCHAR(180),
    vacina_lote VARCHAR(30),
    vacina_fabricante VARCHAR(30),
    vacina_descricao_dose VARCHAR(180)
);

CREATE TABLE dim_estabelecimento(
    estabelecimento_id VARCHAR(180) PRIMARY KEY,
    estabelecimento_valor int(10),
    estabelecimento_razaoSocial VARCHAR(180),
    estabelecimento_nomeFantasia VARCHAR(180),
    estabelecimento_municipio_nome VARCHAR(180),
    estabelecimento_uf VARCHAR(180)
);

create table fato(
    fato_id INT PRIMARY KEY auto_increment,
    paciente_id VARCHAR(180),
    tempo_id VARCHAR(180),
    vacina_id VARCHAR(180),
    estabelecimento_id VARCHAR(180),
    FOREIGN KEY (paciente_id) REFERENCES dim_paciente(paciente_id),
    FOREIGN KEY (tempo_id) REFERENCES dim_tempo(tempo_id),
    FOREIGN KEY (vacina_id) REFERENCES dim_vacina(vacina_id),
    FOREIGN KEY (estabelecimento_id) REFERENCES dim_estabelecimento(estabelecimento_id)
)