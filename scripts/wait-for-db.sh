#!/bin/sh

# Wait for the database to be ready
set -e

until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
  echo "Waiting for database at $DB_HOST:$DB_PORT..."
  sleep 2
done

echo "Database is ready!"