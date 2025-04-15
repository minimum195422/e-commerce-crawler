FROM apache/airflow:2.7.3-python3.10

USER root

# Cài đặt Chrome và các phụ thuộc cần thiết
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    curl \
    chromium \
    chromium-driver \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Cài đặt các gói Python cần thiết
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Thay đổi thư mục làm việc
WORKDIR /opt/airflow