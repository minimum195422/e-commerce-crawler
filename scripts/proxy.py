import requests
import time
import json
from urllib.parse import quote


class RotatingProxy:
    def __init__(self, proxy_key):
        self.proxy_key = proxy_key
        self.current_proxy = None
        self.last_proxy_change = 0
        self.min_proxy_time = 60
        
    def get_new_proxy(self):
        try:
            # Tạo URL request đến API proxy xoay
            api_url = f"https://proxyxoay.shop/api/get.php?key={self.proxy_key}&nhamang=random&tinhthanh=0"
            
            # Gọi API để lấy proxy mới
            response = requests.get(api_url, timeout=10)
            
            # Kiểm tra xem response có thành công không
            if response.status_code == 200:
                proxy_data = json.loads(response.text)
                
                # In kết quả từ API
                # print(f"API Response: {proxy_data}")
                
                # Kiểm tra xem proxy_data có chứa proxysocks5 không
                if "proxysocks5" in proxy_data:
                    # Tách thông tin proxysocks5
                    proxy_parts = proxy_data["proxyhttp"].split(":")
                    
                    # Đảm bảo đủ 4 phần: ip, port, username, password
                    if len(proxy_parts) == 4:
                        proxy_info = {
                            "ip": proxy_parts[0],
                            "port": proxy_parts[1],
                            "username": proxy_parts[2],
                            "password": proxy_parts[3]
                        }
                        
                        # Cập nhật current_proxy và thời gian thay đổi
                        self.current_proxy = proxy_info
                        self.last_proxy_change = time.time()
                        
                        return proxy_info
                    else:
                        print("Định dạng proxysocks5 không hợp lệ")
                else:
                    print("Không tìm thấy thông tin proxysocks5 trong phản hồi API")
            else:
                print(f"Lỗi khi gọi API proxy: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"Lỗi khi lấy proxy mới: {str(e)}")
        
        return None