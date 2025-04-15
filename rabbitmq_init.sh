#!/bin/bash

# Script này được thực thi khi container RabbitMQ khởi động
# Nó tạo queue và thiết lập các permissions nếu cần

# Đợi cho RabbitMQ khởi động hoàn toàn
sleep 15

# Tạo queue tiki_product_queue với tùy chọn durable=true
rabbitmqadmin declare queue name=tiki_product_queue durable=true

echo "RabbitMQ initialization completed"