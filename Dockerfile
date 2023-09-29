USER ${AIRFLOW_UID}

COPY requirements.txt .
RUN sudo apt-get install -y chromium-browser
RUN sudo apt-get install unzip
RUN pip install selenium
RUN pip install -no-cache-dir -r requirements.txt
