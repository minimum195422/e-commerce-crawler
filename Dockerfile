FROM python:3.10-slim

# Cài đặt các gói phụ thuộc cần thiết
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    xvfb \
    libxi6 \
    libgconf-2-4 \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Kiểm tra phiên bản Chrome
RUN google-chrome --version

# Cài đặt ChromeDriver phù hợp với phiên bản Chrome
RUN CHROME_VERSION=$(google-chrome --version | awk -F '[ .]' '{print $3"."$4"."$5}') \
    && CHROMEDRIVER_VERSION=$(curl -sS "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION") \
    && wget -q "https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip" \
    && unzip chromedriver_linux64.zip -d /usr/local/bin/ \
    && rm chromedriver_linux64.zip \
    && chmod +x /usr/local/bin/chromedriver

# Tạo thư mục làm việc
WORKDIR /app

# Sao chép requirements và cài đặt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ dự án
COPY . .

# Tạo thư mục cần thiết
RUN mkdir -p cache data images logs

# Đặt quyền thực thi cho script chính
RUN chmod +x main.py

# Cài đặt wrapper để chạy Chrome trong Docker
ENV DISPLAY=:99
ENTRYPOINT ["./entrypoint.sh"]

# Command mặc định
CMD ["python", "main.py", "--headless"]