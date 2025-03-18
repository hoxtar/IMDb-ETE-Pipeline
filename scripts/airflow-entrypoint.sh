#!/usr/bin/env bash

airflow db reset -y  # force reset, no prompt
airflow db migrate   # replaces 'db init' and 'upgradedb'

airflow users create \
  -r Admin \
  -u admin \
  -e admin@admin.com \
  -f admin \
  -l admin \
  -p admin

airflow scheduler &

airflow webserver
