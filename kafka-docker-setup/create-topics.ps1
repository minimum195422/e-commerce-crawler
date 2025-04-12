# Script để tạo Kafka topics từ file

# Cấu hình
$topicsFilePath = ".\topics.txt"  # Đường dẫn tới file chứa danh sách topics
$kafkaContainer = "kafka"        # Tên container Kafka

# Kiểm tra file topics có tồn tại không
if (-not (Test-Path $topicsFilePath)) {
    Write-Host "File topics không tồn tại: $topicsFilePath" -ForegroundColor Red
    exit 1
}

Write-Host "Đang đọc danh sách topics từ file: $topicsFilePath" -ForegroundColor Green

# Đọc file topics
$topics = Get-Content $topicsFilePath | Where-Object { $_ -and -not $_.StartsWith("#") }

if ($topics.Count -eq 0) {
    Write-Host "Không tìm thấy topics nào trong file" -ForegroundColor Yellow
    exit 0
}

Write-Host "Tìm thấy $($topics.Count) topics trong file" -ForegroundColor Green

# Lấy danh sách topics hiện có
Write-Host "Đang lấy danh sách topics hiện có từ Kafka..." -ForegroundColor Green
$existingTopics = docker exec $kafkaContainer kafka-topics --list --bootstrap-server localhost:9092 2>$null

if ($LASTEXITCODE -ne 0) {
    Write-Host "Không thể kết nối đến Kafka. Đảm bảo rằng container Kafka đang chạy." -ForegroundColor Red
    exit 1
}

# Tạo topics nếu chưa tồn tại
foreach ($topic in $topics) {
    $topic = $topic.Trim()
    
    if ($existingTopics -contains $topic) {
        Write-Host "Topic '$topic' đã tồn tại, bỏ qua" -ForegroundColor Yellow
    } else {
        Write-Host "Đang tạo topic: $topic" -ForegroundColor Green
        docker exec $kafkaContainer kafka-topics --create --topic $topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Topic '$topic' đã được tạo thành công" -ForegroundColor Green
        } else {
            Write-Host "Không thể tạo topic '$topic'" -ForegroundColor Red
        }
    }
}

Write-Host "Hoàn thành việc tạo topics!" -ForegroundColor Green