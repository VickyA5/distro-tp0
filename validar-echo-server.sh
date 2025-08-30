#!/bin/sh

retrieve_config() {
    grep $1 server/config.ini | cut -d'=' -f2 | xargs
}

TEST_MSG="Test server"

HOST=$(retrieve_config "SERVER_IP")

PORT=$(retrieve_config "SERVER_PORT")

RESPONSE=$(docker run --rm --network=tp0_testing_net --entrypoint sh subfuzion/netcat -c "echo \"$TEST_MSG\" | nc -w 20 $HOST $PORT")

if [ "$RESPONSE" = "$TEST_MSG" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi
