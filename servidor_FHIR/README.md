# Parte 1: Servidor HAPI FHIR

Este componente cria um servidor HAPI FHI com um banco de dados PostgreSQL.

## Tecnologias
* HAPI FHIR
* PostgreSQL
* Docker Compose

## Configuração

O servidor HAPI foi configurado para usar o dialeto correto do PostgreSQL, evitando erros de sintaxe de SQL durante a persistência, através da variável de ambiente:
`- SPRING_JPA_DATABASE_PLATFORM=ca.uhn.fhir.jpa.model.dialect.HapiFhirPostgresDialect`

## Como Executar

1.  A partir desta pasta (`servidor_FHIR/`), inicie os contêineres:

    docker-compose up -d


2.  Aguarde 1-2 minutos para que o servidor HAPI FHIR (Java) inicie completamente.
