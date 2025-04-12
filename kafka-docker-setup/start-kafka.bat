@echo off
echo ===== Khởi động Kafka với Docker Compose =====

echo.
echo 1. Khởi động các containers...
docker-compose up -d

echo.
echo 2. Đợi Kafka khởi động hoàn tất (30 giây)...
timeout /t 30 /nobreak

echo.
echo 3. Tạo các topics từ file...
powershell -ExecutionPolicy Bypass -File .\create-topics.ps1

echo.
echo ===== Hoàn thành! =====
echo Kafka đã sẵn sàng tại localhost:9092
echo ZooKeeper đã sẵn sàng tại localhost:2181
echo.

echo Nhấn phím bất kỳ để kết thúc...
pause > nul