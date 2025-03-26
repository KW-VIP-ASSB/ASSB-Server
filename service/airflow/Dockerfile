
FROM apache/airflow:2.10.4-python3.12
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
ADD ./plugins /opt/airflow/plugins
USER root
RUN apt-get update && apt-get install -y \
    libnss3 libx11-xcb1 libxcomposite1 libxcursor1 libxdamage1 libxi6 \
    libxtst6 libappindicator1 libasound2 libpangocairo-1.0-0 libatk1.0-0 \
    libcups2 libxrandr2 xdg-utils libatk-bridge2.0-0 libxkbcommon0 libatspi2.0-0
USER airflow
RUN python3 -m playwright install chromium