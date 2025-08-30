#!/bin/bash


set -e

TEST_MESSAGE="test_echo_message"

cleanup() {
    if [ ! -z "$TEMP_CONTAINER" ]; then
        docker rm -f "$TEMP_CONTAINER" >/dev/null 2>&1 || true
    fi
}

trap cleanup EXIT

if ! docker network ls | grep -q "tp0_testing_net"; then
    echo "action: test_echo_server | result: fail"
    exit 1
fi

# Crear un contenedor temporal con netcat para probar el servidor
TEMP_CONTAINER=$(docker run -d --rm --name "echo_test_$(date +%s)" --network tp0_testing_net alpine:latest sleep 30)

# Esperar un momento para que el contenedor esté listo
sleep 1

# Verificar que el contenedor del servidor esté ejecutándose
if ! docker ps | grep -q "server"; then
    echo "action: test_echo_server | result: fail"
    exit 1
fi

# Instalar netcat en el contenedor temporal y probar la comunicación
RESPONSE=$(docker exec "$TEMP_CONTAINER" sh -c "
    apk add --no-cache netcat-openbsd >/dev/null 2>&1
    echo '$TEST_MESSAGE' | nc server 12345
" 2>/dev/null)

# Verificar que la respuesta coincida con el mensaje enviado
if [ "$RESPONSE" = "$TEST_MESSAGE" ]; then
    echo "action: test_echo_server | result: success"
    exit 0
else
    echo "action: test_echo_server | result: fail"
    exit 1
fi
