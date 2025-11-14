# Parte 1: Servidor HAPI FHIR

Este componente provisiona um servidor HAPI FHIR com um banco de dados PostgreSQL.

## Tecnologias
* HAPI FHIR
* PostgreSQL
* Docker Compose

## Como Executar

1.  A partir desta pasta (`servidor_FHIR/`), inicie os contêineres com o comando:
    
    docker-compose up -d
    

2.  Aguarde 1-2 minutos para que o servidor HAPI FHIR (Java) inicie completamente.

## Como Verificar

1.  Verifique se ambos os contêineres estão saudáveis (`(healthy)`) com o comando:

    docker-compose ps