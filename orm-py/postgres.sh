#!/bin/bash
set -e
set -u

function create_user_and_database() {
  local database=$1
  echo "Creating user and database '$database'"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE $database;
    GRANT ALL PRIVILEGES ON DATABASE $database TO postgres;
EOSQL
}

POSTGRES_MULTIPLE_DATABASES="ormtest_m1,ormtest_m2,ormtest_m3"

if [ -n "${POSTGRES_MULTIPLE_DATABASES:-}" ]; then
  echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
  for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
    create_user_and_database $db
  done
  echo "Multiple databases created"
fi

