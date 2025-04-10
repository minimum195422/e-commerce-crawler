#!/usr/bin/env python3
# Script để thiết lập thời gian ban đầu cho proxy rotation

import os
import json
import time
import sys

def main():
    """Khởi tạo file cache ban đầu cho proxy rotation"""
    # Lấy API keys từ file cấu hình
    try:
        with open("config/config.json", "r") as f:
            config = json.load(f)
            api_keys = config.get("proxy", {}).get("api_keys", [])
    except Exception as e:
        print(f"Lỗi khi đọc file cấu hình: {e}")
        return
        
    if not api_keys:
        print("Không tìm thấy API key trong cấu hình")
        return
        
    # Tạo thư mục cache nếu chưa tồn tại
    cache_dir = "cache"
    os.makedirs(cache_dir, exist_ok=True)
    
    current_time = time.time()
    # Thiết lập thời gian ban đầu cho mỗi API key
    for api_key in api_keys:
        cache_file = os.path.join(cache_dir, f"proxy_rotation_{api_key[:5]}.json")
        
        # Chỉ tạo file nếu chưa tồn tại
        if not os.path.exists(cache_file):
            # Thời gian ban đầu là hiện tại - 70 giây (để có thể lấy proxy ngay)
            init_time = current_time - 70
            with open(cache_file, "w") as f:
                json.dump({"last_rotation_time": init_time}, f)
            print(f"Đã khởi tạo file cache cho API key {api_key[:5]}...")
        else:
            print(f"File cache cho API key {api_key[:5]} đã tồn tại")
            
    print("Hoàn thành khởi tạo cache!")

if __name__ == "__main__":
    main()