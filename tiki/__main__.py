import random
import time
import threading
from tiki_consumer import Tiki_Consumer


def consumer_worker(consumer_id, api_key):
    print(f"Consumer #{consumer_id} đang khởi động với API key: {api_key}")
    consumer = Tiki_Consumer(api_key)
    
    try:
        # Thêm thời gian ngẫu nhiên trước khi khởi động để tránh xung đột
        delay = random.uniform(1.0, 5.0)
        time.sleep(delay)
        
        # Chạy consumer
        consumer.run()
    except Exception as e:
        print(f"Consumer #{consumer_id} gặp lỗi: {str(e)}")
    finally:
        print(f"Consumer #{consumer_id} đã kết thúc")


def main():
    """
    Hàm main khởi tạo và quản lý 10 consumer chạy đa luồng
    """
    # Danh sách API key (thay thế bằng danh sách key thực của bạn)
    api_keys = [
        "BxHgfeqJKsNPAclVQnBfmD",  # Key mặc định từ mã nguồn gốc
        "hgvOiDXwraQZOjvKwRUehk"
    ]
    
    # Số lượng consumer cần chạy
    num_consumers = 2
    
    # Danh sách các luồng
    threads = []
    
    # Tạo và khởi động các luồng consumer
    for i in range(num_consumers):
        # Lấy API key từ danh sách, sử dụng modulo để lặp lại nếu số consumer > số key
        api_key = api_keys[i % len(api_keys)]
        
        # Tạo luồng cho consumer
        thread = threading.Thread(
            target=consumer_worker,
            args=(i + 1, api_key),
            name=f"Consumer-{i+1}"
        )
        
        # Thêm vào danh sách và khởi động
        threads.append(thread)
        thread.start()
        print(f"Đã khởi động Consumer #{i+1} với key: {api_key}")
        
        # Chờ một chút trước khi khởi động consumer tiếp theo
        time.sleep(1)
    
    # Chờ tất cả các consumer hoàn thành (tùy chọn)
    # Thông thường trong ứng dụng thực tế, bạn có thể muốn chương trình chạy mãi mãi
    # cho đến khi bị dừng bởi người dùng (Ctrl+C) hoặc một cơ chế dừng khác
    try:
        print("Tất cả consumer đã được khởi động. Nhấn Ctrl+C để dừng...")
        
        # Đợi tất cả các luồng hoàn thành 
        for thread in threads:
            thread.join()
            
    except KeyboardInterrupt:
        print("\nĐang dừng tất cả consumer...")
        # Lưu ý: Việc dừng các luồng không phải là trực tiếp trong Python
        # Các worker sẽ kết thúc khi consumer.run() kết thúc hoặc gặp lỗi
    
    print("Tất cả consumer đã kết thúc")

if __name__ == "__main__":
    main()