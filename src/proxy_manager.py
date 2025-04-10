import threading
import logging
import time
import random
import json
import requests
import os

logger = logging.getLogger("ProxyManager")

class ProxyManager:
    """
    Quản lý proxy xoay thông qua API
    """
    # Thêm biến class để lưu thời gian đổi proxy cuối cùng giữa các lần khởi tạo
    _last_rotation_time = 0
    _lock = threading.Lock()
    
    def __init__(self, api_key, proxy_rotation_interval=(60, 90)):
        """
        Khởi tạo proxy manager
        
        Args:
            api_key: Khóa API của dịch vụ proxy xoay
            proxy_rotation_interval: Khoảng thời gian (min, max) giữa các lần đổi proxy (giây)
        """
        self.api_key = api_key
        self.proxy_rotation_interval = proxy_rotation_interval
        self.proxy_url = f"https://proxyxoay.shop/api/get.php?key={api_key}&nhamang=random&tinhthanh=0"
        self.current_proxy = None
        self.proxy_expiry_time = None
        
        # Sử dụng biến static lưu thời gian đổi proxy lần cuối
        with ProxyManager._lock:
            self.last_rotation_time = ProxyManager._last_rotation_time
            
        self.stop_rotation = False
        self.rotation_thread = None
        self.proxy_lock = threading.Lock()
        self.force_rotation = False
        
        # Kiểm tra thời gian từ file nếu tồn tại
        self._load_last_rotation_time()
        
    def _load_last_rotation_time(self):
        """Tải thời gian đổi proxy cuối cùng từ file lưu trữ"""
        try:
            # Tạo thư mục cache nếu chưa tồn tại
            cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cache')
            os.makedirs(cache_dir, exist_ok=True)
            
            cache_file = os.path.join(cache_dir, f'proxy_rotation_{self.api_key[:5]}.json')
            
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                    with ProxyManager._lock:
                        self.last_rotation_time = data.get('last_rotation_time', 0)
                        ProxyManager._last_rotation_time = self.last_rotation_time
                        logger.info(f"Đã tải thời gian đổi proxy cuối: {self.last_rotation_time}")
        except Exception as e:
            logger.error(f"Lỗi khi tải thời gian đổi proxy từ cache: {e}")
    
    def _save_last_rotation_time(self):
        """Lưu thời gian đổi proxy cuối cùng vào file"""
        try:
            cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cache')
            os.makedirs(cache_dir, exist_ok=True)
            
            cache_file = os.path.join(cache_dir, f'proxy_rotation_{self.api_key[:5]}.json')
            
            with open(cache_file, 'w') as f:
                json.dump({'last_rotation_time': self.last_rotation_time}, f)
        except Exception as e:
            logger.error(f"Lỗi khi lưu thời gian đổi proxy vào cache: {e}")
        
    def get_new_proxy(self):
        """
        Lấy proxy mới từ API
        
        Returns:
            Dict chứa thông tin proxy hoặc None nếu có lỗi
        """
        # Kiểm tra thời gian đã trôi qua kể từ lần đổi proxy cuối
        current_time = time.time()
        time_since_last_rotation = current_time - self.last_rotation_time
        min_interval = self.proxy_rotation_interval[0]
        
        # Nếu chưa đủ thời gian tối thiểu, trả về None và ghi log
        if time_since_last_rotation < min_interval:
            remaining_time = min_interval - time_since_last_rotation
            logger.error(f"Lỗi khi lấy proxy: Con {int(remaining_time)}s moi co the doi proxy")
            return None
            
        try:
            response = requests.get(self.proxy_url, timeout=10)
            content = response.text
            
            # Trích xuất phần JSON từ response
            json_str = content.split('**', 2)[1] if '**' in content else content
            
            # Tìm phần JSON bắt đầu từ {
            json_start = json_str.find('{')
            if json_start != -1:
                json_str = json_str[json_start:]
                
                # Tìm dấu } cuối cùng
                json_end = json_str.rfind('}') + 1
                if json_end > 0:
                    json_str = json_str[:json_end]
            
            data = json.loads(json_str)
            
            if data.get('status') == 100:
                # Cập nhật thời gian đổi proxy cuối cùng
                with ProxyManager._lock:
                    self.last_rotation_time = current_time
                    ProxyManager._last_rotation_time = current_time
                    
                # Lưu thời gian đổi proxy vào file
                self._save_last_rotation_time()
                
                logger.info(f"Proxy mới nhận được: {data.get('proxyhttp')} (hết hạn sau {data.get('message', '').split()[-1] if 'message' in data else 'N/A'})")
                
                # Phân tích proxyhttp string: ip:port:username:password
                proxy_parts = data.get('proxyhttp', '').split(':')
                if len(proxy_parts) == 4:
                    proxy_info = {
                        'host': proxy_parts[0],
                        'port': proxy_parts[1],
                        'username': proxy_parts[2],
                        'password': proxy_parts[3],
                        'full_http': data.get('proxyhttp'),
                        'full_socks5': data.get('proxysocks5'),
                        'provider': data.get('Nha Mang'),
                        'location': data.get('Vi Tri'),
                        'expiry': data.get('Token expiration date')
                    }
                    return proxy_info
                else:
                    logger.error(f"Định dạng proxy không đúng: {data.get('proxyhttp')}")
            else:
                logger.error(f"Lỗi khi lấy proxy: {data.get('message', 'Không có thông báo lỗi')}")
                
            return None
        except Exception as e:
            logger.error(f"Lỗi khi kết nối với API proxy: {str(e)}")
            return None
    
    def format_proxy_for_selenium(self, proxy_info):
        """
        Định dạng thông tin proxy để sử dụng với Selenium
        
        Args:
            proxy_info: Dict chứa thông tin proxy
            
        Returns:
            Dict với cấu hình proxy cho Selenium
        """
        if not proxy_info:
            return None
        
        return {
            'proxy': {
                'http': f"http://{proxy_info['username']}:{proxy_info['password']}@{proxy_info['host']}:{proxy_info['port']}",
                'https': f"http://{proxy_info['username']}:{proxy_info['password']}@{proxy_info['host']}:{proxy_info['port']}",
                'no_proxy': 'localhost,127.0.0.1'
            }
        }
    
    def start_rotation(self):
        """
        Bắt đầu luồng tự động đổi proxy
        """
        self.stop_rotation = False
        self.rotation_thread = threading.Thread(target=self._rotation_worker)
        self.rotation_thread.daemon = True
        self.rotation_thread.start()
        logger.info("Đã bắt đầu luồng tự động đổi proxy")
    
    def stop_rotation_thread(self):
        """
        Dừng luồng tự động đổi proxy
        """
        self.stop_rotation = True
        if self.rotation_thread and self.rotation_thread.is_alive():
            self.rotation_thread.join(timeout=2)
        logger.info("Đã dừng luồng tự động đổi proxy")
    
    def _rotation_worker(self):
        """Worker thread để tự động đổi proxy theo chu kỳ hoặc khi gặp lỗi"""
        while not self.stop_rotation:
            now = time.time()
            
            # Chỉ đổi proxy khi vượt qua giới hạn thời gian hoặc khi bị ép buộc
            max_interval = self.proxy_rotation_interval[1]
            if (now - self.last_rotation_time >= max_interval) or self.force_rotation:
                with self.proxy_lock:
                    # Kiểm tra lại một lần nữa vì có thể đã có thread khác cập nhật
                    if (now - self.last_rotation_time >= max_interval) or self.force_rotation:
                        logger.info("Đang thực hiện đổi proxy tự động...")
                        self.current_proxy = self.get_new_proxy()
                        self.force_rotation = False
            
            # Ngủ ngắn hơn để phản ứng nhanh với force_rotation
            time.sleep(3)
    
    def can_rotate_proxy(self):
        """
        Kiểm tra xem có thể đổi proxy ngay lúc này không
        
        Returns:
            (bool, int): Tuple chứa (có thể đổi không, thời gian còn lại phải đợi)
        """
        current_time = time.time()
        time_since_last_rotation = current_time - self.last_rotation_time
        min_interval = self.proxy_rotation_interval[0]
        
        if time_since_last_rotation < min_interval:
            remaining_time = min_interval - time_since_last_rotation
            return False, int(remaining_time)
        return True, 0
    
    def get_current_proxy(self):
        """
        Lấy proxy hiện tại, nếu chưa có thì lấy mới
        
        Returns:
            Thông tin proxy hiện tại
        """
        with self.proxy_lock:
            if not self.current_proxy:
                can_rotate, remaining_time = self.can_rotate_proxy()
                if can_rotate:
                    self.current_proxy = self.get_new_proxy()
                else:
                    logger.warning(f"Không thể lấy proxy mới, cần đợi thêm {remaining_time} giây")
                
            return self.current_proxy
            
    def force_rotate_proxy_if_possible(self):
        """
        Yêu cầu đổi proxy nhưng chỉ thực hiện nếu đã đủ thời gian tối thiểu
        
        Returns:
            bool: True nếu đã đổi proxy thành công, False nếu chưa đủ thời gian
        """
        with self.proxy_lock:
            can_rotate, remaining_time = self.can_rotate_proxy()
            if can_rotate:
                logger.info("Thực hiện yêu cầu đổi proxy...")
                self.current_proxy = self.get_new_proxy()
                return self.current_proxy is not None
            else:
                logger.warning(f"Không thể đổi proxy ngay lúc này, cần đợi thêm {remaining_time} giây")
                return False