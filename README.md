# Overview
___
Esse projeto é um exemplo de uso do **Kafka** utilizando kotlin para publicar e consumir mensagens.

As mensagens são produzidas com base em um schema avro que está na pasta `resources`. As mensagens são formadas pelo par:

  - Chave: nome de cidade
  - Valor: schema avro onde possui dois valores: temperatura e umidade

O produtor irá produzir mensagens com cidade, temperatura e umidade aleatória a cada 10 segundos.

# Observações
___
Para que esse projeto funcione é necessário que o **Kafka** e o **Schema Registry** já estejam instalados e configurados.

Talvez seja necessário adaptar as URLs do _bootstrap server_ e do **Schema Registry**.