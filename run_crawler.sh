#!/bin/bash

# Script để chạy crawler với cơ chế restart an toàn

# Tạo thư mục cache nếu chưa tồn tại
mkdir -p ./cache

# Biến đếm số lần khởi động lại
restart_count=0
max_restarts=5  # Số lần khởi động lại tối đa

# Thời gian chờ giữa các lần khởi động lại (giây)
restart_delay=60

# Thiết lập xử lý tín hiệu
trap 'echo "Đã nhận tín hiệu ngắt. Đang dừng..."; docker-compose down; exit 0' INT TERM

echo "=== Bắt đầu chạy Shopee Crawler với cơ chế theo dõi ==="

while [ $restart_count -lt $max_restarts ]; do
    # Hiển thị số lần khởi động lại
    if [ $restart_count -gt 0 ]; then
        echo "Đây là lần khởi động lại thứ $restart_count (tối đa $max_restarts)"
        echo "Đợi $restart_delay giây trước khi khởi động lại..."
        sleep $restart_delay
    fi
    
    # Đảm bảo dừng container cũ trước khi khởi động lại
    docker-compose down
    
    # Khởi động dịch vụ
    echo "Khởi động dịch vụ Shopee Crawler..."
    docker-compose up -d
    
    # Theo dõi log
    docker-compose logs -f shopee-crawler &
    DOCKER_PID=$!
    
    # Biến cờ để kiểm tra lỗi proxy
    proxy_error=0
    
    # Đếm lỗi proxy trong khoảng thời gian gần nhất
    counter=0
    while [ $counter -lt 30 ]; do
        # Kiểm tra nếu có lỗi proxy trong log
        proxy_error_count=$(docker-compose logs --tail=10 shopee-crawler | grep -c "Lỗi khi lấy proxy")
        
        if [ $proxy_error_count -gt 3 ]; then
            proxy_error=1
            echo "Phát hiện lỗi proxy lặp lại nhiều lần, chuẩn bị khởi động lại..."
            break
        fi
        
        # Kiểm tra nếu container đã dừng
        container_running=$(docker-compose ps | grep "shopee-crawler" | grep -c "Up")
        if [ $container_running -eq 0 ]; then
            echo "Container đã dừng, chuẩn bị khởi động lại..."
            break
        fi
        
        # Đợi và tăng counter
        sleep 10
        counter=$((counter + 1))
    done
    
    # Dừng theo dõi log
    kill $DOCKER_PID 2>/dev/null
    
    # Nếu không có lỗi, thoát khỏi vòng lặp
    if [ $proxy_error -eq 0 ] && [ $container_running -eq 1 ]; then
        echo "Dịch vụ đang chạy ổn định, thoát khỏi chế độ theo dõi"
        break
    fi
    
    # Tăng biến đếm khởi động lại
    restart_count=$((restart_count + 1))
    
    # Kiểm tra nếu đã đạt đến số lần khởi động lại tối đa
    if [ $restart_count -ge $max_restarts ]; then
        echo "Đã đạt đến số lần khởi động lại tối đa ($max_restarts). Dừng theo dõi."
    fi
done

echo "=== Kết thúc theo dõi ==="
echo "Crawler đang chạy. Xem logs với lệnh: docker-compose logs -f shopee-crawler"