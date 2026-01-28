# analytics-engineer-bootcamp
DataExpert.io Analytics Engineer Bootcamp Work


tidy up + rebuild
bash```
deactivate || true && \
astro dev stop || true && \
rm -rf venv && \
readarray -t CONTAINERS < <(docker ps -a --filter "name=dataexpert-airflow-dbt" | awk 'NR>1') && \
for container in "${CONTAINER[@]}"; do
  CONTAINER_IDS[${#CONTAINER_IDS[@]}]=$(echo "$container" | awk '{print $1}')
  IMAGES[${#IMAGES[@]}]=$(echo "$container" | awk '{print $2}')
done && \
readarray -t IMAGES < <(printf '%s\n' "${IMAGES[@]}" | sort -u) && \
docker rm -f "${CONTAINER_IDS[@]}" && \
docker rmi -f "${IMAGES[@]}" && \
docker volume ls -q | grep "dataexpert-airflow-dbt" | xargs -r docker volume rm && \
python -m venv venv && \
source ./venv/bin/activate && \
pip cache purge && \
pip install --upgrade pip setuptools wheel && \
astro dev start --no-cache
```