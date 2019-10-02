#!/bin/bash
set -e
function cleanup {
    echo "* Cleaning local stack..."
    docker ps -aq | xargs docker rm -f
}

if [ ! -f /env/bin/activate ]; then
    python3 -m venv env
fi

# export AIO_APP_PATH="tests/mock/"
. env/bin/activate
trap cleanup EXIT
echo "* Running local stack..."
pip3 install -r ../requirements.txt

docker-compose -f docker-compose.yml up --build -d
export POSTSCHEMA_PORT=9999
export POSTGRES_PASSWORD=1234
export POSTGRES_DB=postschemadb
export POSTGRES_USER=postschema
export POSTGRES_HOST=0.0.0.0
export POSTGRES_PORT=5432
export REDIS_HOST=0.0.0.0
export REDIS_PORT=6379
export REDIS_DB=3

cd mock
export PYTHONPATH=$PYTHONPATH:$PWD/../..
adev runserver --host 0.0.0.0 --port 9999