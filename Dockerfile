FROM astrocrpublic.azurecr.io/runtime:3.1-9

USER root
# COPY ./dbt_project ./dbt_project
# COPY --chown=astro:0 . .

RUN mkdir -p /usr/local/airflow/.ssh
COPY .keys/rsa_snowflake_key.p8 /usr/local/airflow/.ssh/rsa_snowflake_key.p8
RUN chmod 600 /usr/local/airflow/.ssh/rsa_snowflake_key.p8

# install dbt into a virtual environment
# RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#     pip install --no-cache-dir -r dbt_project/dbt-requirements.txt  && \
#     cd dbt_project && dbt deps && cd .. && \
#     deactivate
