@echo off
echo ===== Dừng Kafka và các containers =====

echo.
echo Đang dừng và xóa containers...
docker-compose down

echo.
echo ===== Hoàn thành! =====
echo Nhấn phím bất kỳ để kết thúc...
pause > nul