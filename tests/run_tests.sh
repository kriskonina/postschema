#!/bin/bash
set -e
function cleanup {
    echo "* Cleaning local stack..."
    docker ps -aq | xargs docker rm -f
}

if [ ! -f /env/bin/activate ]; then
    python3 -m venv env
fi

export AIO_APP_PATH="mock/"
. env/bin/activate
trap cleanup EXIT
echo "* Running local stack..."
pip3 install -r requirements.txt

docker-compose -f docker-compose.yml up --build -d
export APP_MODE=test
export ADMIN_PASSWORD="aSAbSSnOMHkpx2gfQPo2TSdwyjQneos7QXEjQ19KQMw"
export FERNET_KEY="AszPcqphEfONbBEprJGo73fg0R-ApUsq77Rw10L5SWQ="
export EMAIL_HOSTNAME=localhost
export EMAIL_USERNAME="$USER@localhost"
export EMAIL_FROM="noreply@localhost"
export DEFAULT_SMS_SENDER=Postschema
export POSTSCHEMA_PORT=9999
export POSTGRES_PASSWORD=1234
export POSTGRES_DB=postschemadb
export POSTGRES_USER=postschema
export POSTGRES_HOST=0.0.0.0
export POSTGRES_PORT=5432
export REDIS_HOST=0.0.0.0
export REDIS_PORT=6379
export REDIS_DB=3
export PYTHONPATH=$PYTHONPATH:$PWD/../
# provision the db
python3 $PWD/mock/provision_db.py

# PYTHONPATH=$PYTHONPATH:$PWD/mock #adev runserver --host 0.0.0.0 --port 9999

# put the server in the background
python3 $PWD/mock/main.py &
PROC_ID=$!

# run the tests
while ! nc -z 0.0.0.0 9999 >/dev/null 2>&1; do
    sleep 0.5
done
newman run postman.json

# while kill -9 "$PROC_ID" >/dev/null 2>&1; do
#     sleep .1
# done