#!/usr/bin/env bash
# NB:install Apache Airflow first using install_airflow.sh script

# TODO: Change this to the path where airflow directory is located
# (default is ~/airflow)
export AIRFLOW_HOME=/mnt/c/Users/rezze/PycharmProjects/DE_HW/lesson_02/
# fixes issue on Mac:
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export no_proxy="*"

airflow standalone