FROM apache/airflow:2.9.2

# copy requirments file to the container
COPY requirements /requirements

# upgrade pip
RUN pip install --upgrade pip
# install packages
RUN pip install --no-cache-dir -r /requirements