FROM apache/airflow:2.10.4-python3.12
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
ADD ./plugins /opt/airflow/plugins
USER root
ENV AIRFLOW_PROJ_DIR=/home/jspark9703/project/kw_vip/service/airflow
RUN apt-get update && apt-get install -y \
    libnss3 libx11-xcb1 libxcomposite1 libxcursor1 libxdamage1 libxi6 \
    libxtst6 libappindicator1 libasound2 libpangocairo-1.0-0 libatk1.0-0 \
    libcups2 libxrandr2 xdg-utils libatk-bridge2.0-0 libxkbcommon0 libatspi2.0-0
# 로그 디렉토리 생성 및 권한 설정
RUN mkdir -p /opt/airflow/logs /opt/airflow/logs/scheduler && \
    chmod -R 777 /opt/airflow/logs
USER airflow
RUN python3 -m playwright install chromium
ENV AIRFLOW_UID=jspark9703
