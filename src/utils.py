import os
import random
import time
import zipfile
import logging
import hashlib
import re
import requests
from PIL import Image
from io import BytesIO

logger = logging.getLogger("Utils")

def setup_logging(log_dir="logs"):
    """Thiết lập logging"""
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, "shopee_crawler.log")),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger("ShopeeCrawler")

def random_sleep(min_time=1, max_time=3):
    """Ngủ ngẫu nhiên để mô phỏng hành vi người dùng thật"""
    sleep_time = random.uniform(min_time, max_time)
    logger.debug(f"Ngủ {sleep_time:.2f} giây")
    time.sleep(sleep_time)

def create_proxy_auth_extension(host, port, username, password, scheme='http'):
    """
    Tạo plugin xác thực proxy cho Chrome
    
    Args:
        host: Địa chỉ proxy
        port: Cổng proxy
        username: Tên đăng nhập
        password: Mật khẩu
        scheme: Giao thức (http/https)
        
    Returns:
        Đường dẫn đến file plugin
    """
    try:
        plugin_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f'proxy_auth_plugin_{random.randint(100000, 999999)}.zip'
        )
        
        manifest_json = """
        {
            "version": "1.0.0",
            "manifest_version": 2,
            "name": "Chrome Proxy",
            "permissions": [
                "proxy",
                "tabs",
                "unlimitedStorage",
                "storage",
                "webRequest",
                "webRequestBlocking"
            ],
            "background": {
                "scripts": ["background.js"]
            },
            "minimum_chrome_version":"22.0.0"
        }
        """
        
        background_js = """
        var config = {
                mode: "fixed_servers",
                rules: {
                singleProxy: {
                    scheme: "%s",
                    host: "%s",
                    port: parseInt(%s)
                },
                bypassList: ["localhost"]
                }
            };

        chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

        function callbackFn(details) {
            return {
                authCredentials: {
                    username: "%s",
                    password: "%s"
                }
            };
        }

        chrome.webRequest.onAuthRequired.addListener(
                    callbackFn,
                    {urls: ["<all_urls>"]},
                    ['blocking']
        );
        """ % (scheme, host, port, username, password)
        
        with zipfile.ZipFile(plugin_path, 'w') as zp:
            zp.writestr("manifest.json", manifest_json)
            zp.writestr("background.js", background_js)
        
        return plugin_path
    except Exception as e:
        logger.error(f"Lỗi khi tạo plugin xác thực proxy: {str(e)}")
        return None

def extract_ids_from_url(url):
    """
    Trích xuất shop_id và product_id từ URL sản phẩm Shopee
    
    Args:
        url: URL sản phẩm Shopee
        
    Returns:
        Dictionary chứa shop_id và product_id
    """
    try:
        # Pattern URL Shopee: https://shopee.vn/[tên-sản-phẩm]-i.[shop_id].[product_id]
        pattern = r"i\.(\d+)\.(\d+)"
        match = re.search(pattern, url)
        
        if match:
            shop_id = match.group(1)
            product_id = match.group(2)
            return {
                "shop_id": shop_id,
                "product_id": product_id
            }
        else:
            logger.warning(f"Không thể trích xuất ID từ URL: {url}")
            return {"shop_id": "unknown", "product_id": "unknown"}
    except Exception as e:
        logger.error(f"Lỗi khi trích xuất ID từ URL: {e}")
        return {"shop_id": "unknown", "product_id": "unknown"}

def download_and_save_image(image_url, images_dir, variant_name=None):
    """
    Tải và lưu ảnh về thư mục local
    
    Args:
        image_url: URL của ảnh cần tải
        images_dir: Đường dẫn đến thư mục lưu ảnh
        variant_name: Tên biến thể (nếu có) để đặt tên file
        
    Returns:
        Đường dẫn local đến ảnh đã lưu
    """
    try:
        # Tạo thư mục nếu chưa tồn tại
        os.makedirs(images_dir, exist_ok=True)
        
        # Tạo tên file dựa trên URL ảnh (tạo mã hash từ URL để tên file không quá dài)
        image_hash = hashlib.md5(image_url.encode()).hexdigest()
        
        # Tạo tên file dựa trên tên biến thể và hash
        if variant_name:
            # Loại bỏ các ký tự không hợp lệ trong tên file
            safe_variant_name = "".join([c if c.isalnum() or c in ['-', '_'] else '_' for c in variant_name])
            image_filename = f"{safe_variant_name}_{image_hash[:8]}.jpg"
        else:
            image_filename = f"{image_hash}.jpg"
            
        image_path = os.path.join(images_dir, image_filename)
        
        # Kiểm tra nếu file đã tồn tại
        if os.path.exists(image_path):
            return image_path
            
        # Tải ảnh
        response = requests.get(image_url, timeout=10)
        if response.status_code == 200:
            # Mở và lưu ảnh
            img = Image.open(BytesIO(response.content))
            img.save(image_path)
            logger.info(f"Đã tải ảnh: {image_path}")
            return image_path
        else:
            logger.warning(f"Không thể tải ảnh từ {image_url}, status code: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Lỗi khi tải ảnh {image_url}: {e}")
        return None