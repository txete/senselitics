#!/bin/bash

# Nombre del archivo JSON de donde leeremos los tweets.
JSON_FILE="tweetsShort.json"

# Contar el número total de tweets en el archivo.
TOTAL_LINES=$(wc -l < "$JSON_FILE")

# Bucle infinito para enviar una línea aleatoria cada segundo
while true; do
  {
    while true; do
      # Generar un número aleatorio entre 1 y el número total de líneas.
      RANDOM_LINE=$(shuf -n 1 "$JSON_FILE")

      # Enviar la línea aleatoria al puerto 9999 usando nc.
      echo "$RANDOM_LINE"

      # Esperar un segundo antes de enviar la siguiente línea.
      sleep 1
    done
  } | nc -lk 9999
done
