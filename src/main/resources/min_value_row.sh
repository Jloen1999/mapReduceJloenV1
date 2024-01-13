#!/bin/bash

# Verifica si se proporcionó un nombre de archivo
if [ "$#" -ne 1 ]; then
    echo "Uso: $0 [archivo]"
    exit 1
fi

# Nombre del archivo
archivo=$1

# Elimina los símbolos '-' y '>' del archivo
sed -i 's/[-|>]//g' $archivo

# Elimina líneas en blanco
sed -i '/^$/d' $archivo

# Comando para encontrar la fila con el menor valor en la segunda columna
awk 'NR == 1 || $2 < min {min = $2; linea = $0} END {print linea}' $archivo