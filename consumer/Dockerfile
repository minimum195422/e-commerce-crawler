FROM python:3.10

WORKDIR /app

# Cài đặt các phụ thuộc cho Selenium
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    curl \
    chromium \
    chromium-driver \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường để ChromeDriver hoạt động đúng
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Cài đặt các gói Python cần thiết
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Tạo thư mục dữ liệu
RUN mkdir -p /app/data/tiki/images

# Thư mục làm việc
COPY ./tiki /app/tiki

CMD ["python", "-m", "tiki.__main__"]