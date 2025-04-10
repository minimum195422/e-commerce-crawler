from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
import json
import os
import re
from datetime import datetime
import random
import logging
import atexit

from .proxy_manager import ProxyManager
from .utils import random_sleep, create_proxy_auth_extension, extract_ids_from_url, download_and_save_image

logger = logging.getLogger("ShopeeCrawler")

class ShopeeCrawler:
    def __init__(self, proxy_api_key=None, headless=False, wait_time=10, max_retries=3, 
                 proxy_rotation_interval=(60, 90), use_proxy=True):
        """
        Khởi tạo crawler tổng hợp cho Shopee
        
        Args:
            proxy_api_key: Khóa API của dịch vụ proxy xoay
            headless: Chạy trình duyệt ở chế độ headless (không giao diện)
            wait_time: Thời gian chờ tối đa cho các phần tử (giây)
            max_retries: Số lần thử lại tối đa khi có lỗi
            proxy_rotation_interval: Khoảng thời gian giữa các lần đổi proxy (giây)
            use_proxy: Có sử dụng proxy hay không
        """
        self.homepage_url = "https://shopee.vn/"
        self.wait_time = wait_time
        self.headless = headless
        self.max_retries = max_retries
        self.product_links = []
        self.use_proxy = use_proxy
        
        # Khởi tạo proxy manager nếu có API key
        if proxy_api_key and use_proxy:
            self.proxy_manager = ProxyManager(proxy_api_key, proxy_rotation_interval)
            self.proxy_manager.start_rotation()
            logger.info("Đã khởi tạo proxy manager với API key")
        else:
            self.proxy_manager = None
            self.use_proxy = False
            logger.info("Không sử dụng proxy")
    
    def setup_driver(self):
        """Thiết lập trình duyệt Chrome với Selenium WebDriver và cấu hình proxy nếu có"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-infobars")
        
        # User-Agent để giả lập như đang sử dụng trình duyệt bình thường
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
        
        # Biến để theo dõi xem có thành công trong việc sử dụng proxy không
        proxy_configured = False
        
        # Thêm cấu hình proxy nếu được bật
        if self.use_proxy and self.proxy_manager:
            proxy_info = self.proxy_manager.get_current_proxy()
            if proxy_info:
                try:
                    # Cấu hình proxy cho Selenium
                    chrome_options.add_argument(f'--proxy-server={proxy_info["host"]}:{proxy_info["port"]}')
                    
                    # Thêm plugin để xác thực proxy (nếu cần username/password)
                    plugin_path = create_proxy_auth_extension(
                        proxy_info["host"],
                        proxy_info["port"],
                        proxy_info["username"],
                        proxy_info["password"]
                    )
                    if plugin_path:
                        chrome_options.add_extension(plugin_path)
                        # Xóa file plugin sau khi đã thêm
                        try:
                            os.remove(plugin_path)
                        except:
                            pass
                    
                    logger.info(f"Đã cấu hình proxy: {proxy_info['host']}:{proxy_info['port']}")
                    proxy_configured = True
                except Exception as e:
                    logger.error(f"Lỗi khi cấu hình proxy: {e}")
            else:
                # Kiểm tra xem có phải do giới hạn thời gian không
                can_rotate, remaining_time = self.proxy_manager.can_rotate_proxy()
                if not can_rotate:
                    logger.warning(f"Không thể lấy thông tin proxy do còn phải đợi {remaining_time}s, tiếp tục mà không có proxy")
                else:
                    logger.warning("Không thể lấy thông tin proxy, tiếp tục mà không có proxy")
        
        # Nếu không thể cấu hình proxy hoặc không sử dụng proxy, thêm tùy chọn để tránh bị chặn
        if not proxy_configured:
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Sử dụng ChromeDriverManager để tự động tải driver phù hợp
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Thêm đoạn mã JavaScript để ngụy trang webdriver
        if not proxy_configured:
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"})
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        wait = WebDriverWait(driver, self.wait_time)
        
        return driver, wait

    def handle_popups(self, driver):
        """Xử lý các popup có thể xuất hiện"""
        try:
            # Tìm và đóng các popup quảng cáo
            close_buttons = driver.find_elements(By.CSS_SELECTOR, "svg.shopee-svg-icon.icon-close-thin, div.shopee-popup__close-btn")
            for btn in close_buttons:
                try:
                    btn.click()
                    random_sleep(0.5, 1)
                    logger.info("Đã đóng một popup")
                except:
                    pass
        except Exception as e:
            logger.warning(f"Không thể xử lý popup: {e}")
    
    def collect_product_links(self):
        """Thu thập các đường link sản phẩm từ phần gợi ý trên trang chủ Shopee"""
        logger.info("Bắt đầu thu thập đường link sản phẩm từ trang chủ...")
        
        # Khởi tạo trình duyệt mới
        driver, wait = self.setup_driver()
        collected_links = []
        
        try:
            # Truy cập trang chủ
            driver.get(self.homepage_url)
            logger.info("Đã truy cập trang chủ Shopee")
            
            # Chờ trang tải xong
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            random_sleep(2, 3)
            
            # Xử lý popup nếu có
            self.handle_popups(driver)
            
            # Cuộn xuống để hiển thị phần gợi ý sản phẩm
            logger.info("Cuộn xuống phần GỢI Ý HÔM NAY...")
            for i in range(8):  # Cuộn 8 lần, mỗi lần 500px
                driver.execute_script("window.scrollBy(0, 500);")
                random_sleep(0.5, 1)
            
            # Chờ đợi để phần gợi ý hiện ra đầy đủ
            logger.info(f"Đang chờ {self.wait_time} giây để các sản phẩm gợi ý tải hoàn tất...")
            random_sleep(5, 8)
            
            # Tìm section chứa các sản phẩm gợi ý
            try:
                recommended_section = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".section-recommend-products-wrapper"))
                )
                
                # Tìm tất cả các sản phẩm
                product_elements = recommended_section.find_elements(By.CSS_SELECTOR, ".oMSmr0 div a.contents")
                logger.info(f"Tìm thấy {len(product_elements)} sản phẩm gợi ý")
                
                # Lấy các đường link
                for element in product_elements:
                    try:
                        # Lấy URL sản phẩm
                        product_url = element.get_attribute("href")
                        if product_url and "javascript:void(0)" not in product_url:
                            collected_links.append(product_url)
                    except Exception as e:
                        logger.error(f"Lỗi khi lấy link sản phẩm: {e}")
                        continue
                
                logger.info(f"Đã thu thập được {len(collected_links)} đường link sản phẩm")
                
            except Exception as e:
                logger.error(f"Lỗi khi tìm phần sản phẩm gợi ý: {e}")
            
        except Exception as e:
            logger.error(f"Lỗi khi thu thập đường link sản phẩm: {e}")
        finally:
            # Đóng trình duyệt sau khi thu thập xong
            driver.quit()
            logger.info("Đã đóng trình duyệt thu thập đường link")
            
        # Cập nhật danh sách đường link sản phẩm
        self.product_links = collected_links
        return collected_links

    def get_product_name(self, driver, wait):
        """Lấy tên sản phẩm"""
        try:
            name_element = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.vR6K3w, div.WBVL_7 h1"))
            )
            return name_element.text
        except Exception as e:
            logger.error(f"Không thể lấy tên sản phẩm: {e}")
            return "N/A"

    def get_product_category(self, driver):
        """Lấy phân loại hàng hóa"""
        try:
            category_elements = driver.find_elements(By.CSS_SELECTOR, "div.ybxj32:nth-child(1) a.EtYbJs, div.ybxj32:nth-child(1) a.R7vGdX, div.idLK2l a.EtYbJs")
            
            categories = []
            for element in category_elements:
                cat_text = element.text.strip()
                if cat_text and cat_text != "Shopee":
                    categories.append(cat_text)
                    
            return categories if categories else ["N/A"]
            
        except Exception as e:
            logger.error(f"Không thể lấy phân loại hàng hóa: {e}")
            return ["N/A"]

    def get_product_rating(self, driver):
        """Lấy điểm đánh giá sản phẩm"""
        try:
            rating_element = driver.find_element(By.CSS_SELECTOR, "div.F9RHbS, div.jMXp4d")
            return float(rating_element.text)
            
        except Exception as e:
            logger.error(f"Không thể lấy điểm đánh giá: {e}")
            return 0.0

    def get_total_rating(self, driver):
        """Lấy tổng số lượt đánh giá"""
        try:
            total_rating_element = driver.find_element(By.CSS_SELECTOR, "div.x1i_He, div.e2p50f:nth-child(2) div.F9RHbS")
            
            rating_text = total_rating_element.text.strip()
            if "đánh giá" in rating_text:
                rating_text = rating_text.replace("đánh giá", "").strip()
                
            if 'k' in rating_text.lower():
                return int(float(rating_text.lower().replace('k', '')) * 1000)
            else:
                return int(rating_text.replace(',', ''))
            
        except Exception as e:
            logger.error(f"Không thể lấy tổng số đánh giá: {e}")
            return 0

    def get_total_sold(self, driver):
        """Lấy tổng số lượng đã bán"""
        try:
            sold_element = driver.find_element(By.CSS_SELECTOR, "div.aleSBU span.AcmPRb, div.mnzVGI span")
            
            sold_text = sold_element.text.strip()
            sold_text = re.sub(r'Sold|Đã bán', '', sold_text).strip()
            
            if 'k' in sold_text.lower():
                return int(float(sold_text.lower().replace('k', '')) * 1000)
            else:
                return int(sold_text.replace(',', '').replace('.', ''))
            
        except Exception as e:
            logger.error(f"Không thể lấy số lượng đã bán: {e}")
            return 0
    
    def get_product_price(self, driver):
        """Lấy giá sản phẩm hiện tại"""
        try:
            price_element = driver.find_element(By.CSS_SELECTOR, "div.IZPeQz, div.jRlVo0 div.IZPeQz")
            
            price_text = price_element.text.strip()
            price_text = re.sub(r'[^\d]', '', price_text)
            
            return int(price_text) if price_text else 0
            
        except Exception as e:
            logger.error(f"Không thể lấy giá sản phẩm: {e}")
            return 0
    
    def get_product_variations(self, driver, images_dir):
        """
        Lấy thông tin các biến thể sản phẩm (màu sắc, kích thước, v.v.)
        
        Args:
            driver: Đối tượng WebDriver
            images_dir: Đường dẫn đến thư mục lưu ảnh
        """
        try:
            variations = {}
            
            # Lưu ảnh chính của sản phẩm
            main_image_elements = driver.find_elements(By.CSS_SELECTOR, "div.UdI7e2 img.uXN1L5")
            main_images = []
            for idx, img_elem in enumerate(main_image_elements[:5]):  # Giới hạn 5 ảnh chính
                img_url = img_elem.get_attribute("src")
                if img_url:
                    local_path = download_and_save_image(img_url, images_dir, f"main_{idx+1}")
                    main_images.append({
                        "original_url": img_url,
                        "local_path": local_path
                    })
            
            # Tìm các phần tử chứa biến thể
            variation_sections = driver.find_elements(By.CSS_SELECTOR, "section.flex.items-center, div.flex.KIoPj6 > div.flex.flex-column > section")
            
            for section in variation_sections:
                try:
                    # Lấy tên loại biến thể (vd: "Màu Sắc", "Kích Thước")
                    title_element = section.find_element(By.CSS_SELECTOR, "h2.Dagtcd")
                    title = title_element.text.strip()
                    
                    # Tìm các lựa chọn cho loại biến thể này
                    option_elements = section.find_elements(By.CSS_SELECTOR, "button.sApkZm")
                    options = []
                    
                    for option in option_elements:
                        option_data = {
                            "name": option.text.strip()
                        }

                        # Nếu là biến thể có ảnh (thường là màu sắc)
                        try:
                            img_element = option.find_element(By.TAG_NAME, "img")
                            img_url = img_element.get_attribute("src")
                            option_data["image_url"] = img_url
                            
                            # Tải và lưu ảnh
                            if img_url:
                                local_path = download_and_save_image(
                                    img_url, 
                                    images_dir, 
                                    f"{title}_{option_data['name']}"
                                )
                                option_data["image_local_path"] = local_path
                        except:
                            pass
                        
                        options.append(option_data)
                    
                    if title and options:
                        variations[title] = options
                        
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý một biến thể: {e}")
                    continue
            
            # Thêm ảnh chính vào kết quả
            variations["main_images"] = main_images
            
            return variations
            
        except Exception as e:
            logger.error(f"Không thể lấy thông tin biến thể sản phẩm: {e}")
            return {}
    
    def get_shop_info(self, driver):
        """Lấy thông tin về cửa hàng"""
        try:
            shop_info = {}
            
            # Phần thông tin cửa hàng thường nằm trong một section riêng
            shop_section = driver.find_element(By.CSS_SELECTOR, "section.page-product__shop, div#sll2-pdp-product-shop section")
            
            # Lấy tên cửa hàng
            try:
                shop_name_element = shop_section.find_element(By.CSS_SELECTOR, "div.fV3TIn")
                shop_info["name"] = shop_name_element.text.strip()
            except Exception as e:
                logger.error(f"Không thể lấy tên cửa hàng: {e}")
                shop_info["name"] = "N/A"
            
            # Lấy URL cửa hàng
            try:
                shop_url_element = shop_section.find_element(By.CSS_SELECTOR, "a.Z6yFUs, a.btn-light--link")
                shop_info["url"] = shop_url_element.get_attribute("href")
            except Exception as e:
                logger.error(f"Không thể lấy URL cửa hàng: {e}")
                shop_info["url"] = "N/A"
            
            # Lấy thông tin đánh giá cửa hàng
            try:
                shop_metrics = shop_section.find_elements(By.CSS_SELECTOR, "div.YnZi6x")
                for metric in shop_metrics:
                    try:
                        label = metric.find_element(By.CSS_SELECTOR, "label.ffHYws").text.strip().lower()
                        value = metric.find_element(By.CSS_SELECTOR, "span.Cs6w3G").text.strip()
                        
                        if "đánh giá" in label:
                            shop_info["rating_count"] = value
                        elif "tỉ lệ phản hồi" in label:
                            shop_info["response_rate"] = value
                        elif "tham gia" in label:
                            shop_info["joined"] = value
                        elif "sản phẩm" in label:
                            shop_info["product_count"] = value
                        elif "thời gian phản hồi" in label:
                            shop_info["response_time"] = value
                        elif "người theo dõi" in label:
                            shop_info["follower_count"] = value
                    except Exception as se:
                        logger.debug(f"Không thể lấy một metrics cửa hàng: {se}")
                        continue
            except Exception as e:
                logger.error(f"Không thể lấy thông tin metrics cửa hàng: {e}")
            
            return shop_info
            
        except Exception as e:
            logger.error(f"Không thể lấy thông tin cửa hàng: {e}")
            return {"name": "N/A", "url": "N/A"}
    
    def get_product_description(self, driver):
        """Lấy phần mô tả sản phẩm"""
        try:
            # Scroll đến phần mô tả sản phẩm để đảm bảo nó được tải
            driver.execute_script("window.scrollBy(0, 800);")
            random_sleep(1, 2)
            
            # Tìm phần tử chứa mô tả sản phẩm
            description_section = driver.find_element(By.CSS_SELECTOR, "div.e8lZp3, div.Gf4Ro0 > div")
            
            # Lấy text của tất cả các đoạn văn trong phần mô tả
            paragraphs = description_section.find_elements(By.CSS_SELECTOR, "p, span, div")
            description_texts = [p.text.strip() for p in paragraphs if p.text.strip()]
            
            # Nối các đoạn văn lại thành một chuỗi, phân cách bằng dấu xuống dòng
            return "\n".join(description_texts)
            
        except Exception as e:
            logger.error(f"Không thể lấy mô tả sản phẩm: {e}")
            return "N/A"
    
    def get_product_tags(self, driver):
        """Lấy các tag của sản phẩm thường ở cuối phần mô tả"""
        try:
            # Tìm phần tử chứa mô tả sản phẩm
            description_section = driver.find_element(By.CSS_SELECTOR, "div.e8lZp3, div.Gf4Ro0 > div")
            
            # Lấy text của đoạn văn cuối cùng - thường chứa các tag
            paragraphs = description_section.find_elements(By.CSS_SELECTOR, "p, span, div")
            
            for p in reversed(paragraphs):  # Duyệt từ dưới lên
                text = p.text.strip()
                if text and '#' in text:  # Tìm đoạn văn có chứa dấu #
                    # Trích xuất các tag bằng regex
                    tags = re.findall(r'#\w+', text)
                    if not tags:
                        # Nếu không tìm thấy theo định dạng #tag, thử tìm theo các từ ngăn cách bởi khoảng trắng
                        potential_tags = [word.strip() for word in text.split() if word.strip()]
                        return potential_tags
                    return [tag.replace('#', '') for tag in tags]  # Loại bỏ dấu # từ tag
            
            # Nếu không tìm thấy tag ở đoạn văn cuối, tìm trong toàn bộ mô tả
            full_text = description_section.text
            tags = re.findall(r'#\w+', full_text)
            
            return [tag.replace('#', '') for tag in tags] if tags else []
            
        except Exception as e:
            logger.error(f"Không thể lấy tag sản phẩm: {e}")
            return []
    
    # Cập nhật phương thức crawl_product_detail trong class ShopeeCrawler
    def crawl_product_detail(self, product_url, retry=0):
        """
        Trích xuất thông tin chi tiết của sản phẩm từ URL
        
        Args:
            product_url: URL của sản phẩm cần lấy thông tin
            retry: Số lần đã thử lại (dùng cho đệ quy)
            
        Returns:
            Dictionary chứa thông tin chi tiết của sản phẩm
        """
        if retry >= self.max_retries:
            logger.warning(f"Đã vượt quá số lần thử lại ({self.max_retries}) cho URL: {product_url}")
            return None
        
        # Khởi tạo trình duyệt mới cho mỗi sản phẩm
        driver, wait = self.setup_driver()
        
        try:
            # Truy cập trang sản phẩm
            driver.get(product_url)
            logger.info(f"Đang truy cập URL: {product_url}")
            
            # Chờ trang tải xong
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            random_sleep(3, 5)  # Đợi trang tải hoàn toàn
            
            # Xử lý popup nếu có
            self.handle_popups(driver)
            
            # Scroll xuống một chút để tải thêm nội dung
            driver.execute_script("window.scrollBy(0, 300);")
            random_sleep(1, 2)
            
            # Trích xuất ID sản phẩm và shop
            ids = extract_ids_from_url(product_url)
            
            # Tạo thư mục chứa dữ liệu sản phẩm này theo cấu trúc shopee.shopid.productid
            product_data_dir = os.path.join("data", f"shopee.{ids['shop_id']}.{ids['product_id']}")
            os.makedirs(product_data_dir, exist_ok=True)
            
            # Tạo thư mục images bên trong thư mục sản phẩm
            images_dir = os.path.join(product_data_dir, "images")
            os.makedirs(images_dir, exist_ok=True)
            
            # Thông tin proxy sử dụng (nếu có)
            proxy_info = "direct"
            if self.use_proxy and self.proxy_manager and self.proxy_manager.current_proxy:
                proxy_info = f"{self.proxy_manager.current_proxy['host']}:{self.proxy_manager.current_proxy['port']}"
            
            # Tạo dictionary để lưu dữ liệu sản phẩm
            product_data = {
                "url": product_url,
                "shop_id": ids["shop_id"],
                "product_id": ids["product_id"],
                "data_directory": product_data_dir,
                "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "product_name": self.get_product_name(driver, wait),
                "category": self.get_product_category(driver),
                "rating": self.get_product_rating(driver),
                "total_rating": self.get_total_rating(driver),
                "total_sold": self.get_total_sold(driver),
                "price": self.get_product_price(driver),
                "variations": self.get_product_variations(driver, images_dir),
                "shop_info": self.get_shop_info(driver),
                "description": self.get_product_description(driver),
                "tags": self.get_product_tags(driver),
                "proxy_used": proxy_info
            }
            
            # Lưu dữ liệu JSON vào thư mục sản phẩm
            json_path = os.path.join(product_data_dir, f"product_info.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(product_data, f, ensure_ascii=False, indent=4)
                
            logger.info(f"Đã lưu thông tin sản phẩm vào {json_path}")
            
            return product_data
        
        except Exception as e:
            logger.error(f"Lỗi khi lấy thông tin sản phẩm (lần thử {retry+1}): {str(e)}")
            
            # Thử lại nếu chưa vượt quá số lần cho phép
            if retry < self.max_retries:
                logger.info(f"Đang thử lại lần {retry+1}...")
                driver.quit()  # Đóng trình duyệt hiện tại
                
                # Nếu đang sử dụng proxy, kiểm tra xem có thể đổi proxy không
                if self.use_proxy and self.proxy_manager:
                    can_rotate, remaining_time = self.proxy_manager.can_rotate_proxy()
                    if can_rotate:
                        logger.info("Đổi proxy mới trước khi thử lại...")
                        rotated = self.proxy_manager.force_rotate_proxy_if_possible()
                        if rotated:
                            logger.info("Đã đổi proxy thành công")
                        else:
                            logger.warning("Không thể đổi proxy, tiếp tục với proxy hiện tại")
                    else:
                        logger.warning(f"Chưa thể đổi proxy, còn phải đợi {remaining_time} giây. Tiếp tục với proxy hiện tại")
                
                random_sleep(2, 5)  # Chờ một chút trước khi thử lại
                return self.crawl_product_detail(product_url, retry + 1)
                
            return None
        
        finally:
            # Đóng trình duyệt sau khi xong
            driver.quit()
            logger.info(f"Đã đóng trình duyệt sau khi lấy thông tin sản phẩm: {product_url}")


    def run(self, crawl_cycles=1, max_products_per_cycle=None, output_summary=True):
        """
        Chạy crawler theo quy trình yêu cầu: mở trang chủ -> thu thập link -> đóng trang chủ -> mở link -> thu thập dữ liệu -> lặp lại
        
        Args:
            crawl_cycles: Số chu kỳ crawl (mặc định: 1)
            max_products_per_cycle: Số lượng sản phẩm tối đa cho mỗi chu kỳ (None = tất cả)
            output_summary: Có tạo file tổng hợp hay không
        """
        logger.info(f"===== BẮT ĐẦU QUY TRÌNH CRAWL - {crawl_cycles} CHU KỲ =====")
        
        # Đảm bảo dừng proxy rotation thread khi kết thúc
        if self.use_proxy and self.proxy_manager:
            atexit.register(self.proxy_manager.stop_rotation_thread)
        
        # Tạo thư mục data nếu chưa tồn tại
        if not os.path.exists("data"):
            os.makedirs("data")
        
        # Danh sách để lưu tất cả sản phẩm đã crawl
        all_products = []
        
        # Thực hiện theo số chu kỳ đã chỉ định
        for cycle in range(1, crawl_cycles + 1):
            logger.info(f"\n===== BẮT ĐẦU CHU KỲ {cycle}/{crawl_cycles} =====")
            
            # 1. Mở trang chủ và thu thập đường link
            logger.info(f"Chu kỳ {cycle}: Đang thu thập đường link sản phẩm từ trang chủ...")
            self.collect_product_links()
            
            if not self.product_links:
                logger.warning(f"Chu kỳ {cycle}: Không thu thập được đường link nào, chuyển sang chu kỳ tiếp theo.")
                continue
                
            logger.info(f"Chu kỳ {cycle}: Đã thu thập được {len(self.product_links)} đường link.")
            
            # Giới hạn số lượng sản phẩm nếu cần
            links_to_crawl = self.product_links[:max_products_per_cycle] if max_products_per_cycle else self.product_links
            
            # 2. Mở từng đường link và thu thập dữ liệu
            logger.info(f"Chu kỳ {cycle}: Bắt đầu thu thập dữ liệu từ {len(links_to_crawl)} sản phẩm...")
            
            cycle_products = []
            for idx, link in enumerate(links_to_crawl):
                logger.info(f"Chu kỳ {cycle}: Đang crawl sản phẩm {idx+1}/{len(links_to_crawl)}: {link}")
                
                # Cập nhật proxy nếu cần
                if self.use_proxy and self.proxy_manager and not self.proxy_manager.rotation_thread.is_alive():
                    logger.warning("Luồng đổi proxy đã dừng, khởi động lại...")
                    self.proxy_manager.start_rotation()
                
                # Thu thập chi tiết sản phẩm
                product_detail = self.crawl_product_detail(link)
                
                if product_detail:
                    cycle_products.append(product_detail)
                    all_products.append(product_detail)
                
                # Đợi một chút trước khi crawl sản phẩm tiếp theo để tránh bị chặn
                wait_time = random.uniform(2, 5)
                logger.info(f"Đợi {wait_time:.2f} giây trước khi crawl sản phẩm tiếp theo...")
                time.sleep(wait_time)
            
            # Tạo báo cáo tổng hợp cho chu kỳ này
            if cycle_products:
                cycle_summary_file = os.path.join("data", f"cycle_{cycle}_products_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")
                with open(cycle_summary_file, 'w', encoding='utf-8') as f:
                    json.dump(cycle_products, f, ensure_ascii=False, indent=4)
                    
                logger.info(f"Chu kỳ {cycle}: Đã lưu thông tin của {len(cycle_products)} sản phẩm vào {cycle_summary_file}")
            
            logger.info(f"===== KẾT THÚC CHU KỲ {cycle}/{crawl_cycles} =====")
        
        # Tạo báo cáo tổng hợp cho toàn bộ quá trình crawl
        if output_summary and all_products:
            all_products_file = os.path.join("data", f"all_products_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")
            with open(all_products_file, 'w', encoding='utf-8') as f:
                json.dump(all_products, f, ensure_ascii=False, indent=4)
                
            logger.info(f"\n===== TỔNG KẾT =====")
            logger.info(f"Đã thu thập thông tin của {len(all_products)} sản phẩm qua {crawl_cycles} chu kỳ")
            logger.info(f"Tất cả dữ liệu đã được lưu vào {all_products_file}")
            
            # Tạo file CSV tổng hợp
            csv_summary_file = os.path.join("data", f"all_products_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
            
            # Chuẩn bị dữ liệu cho CSV
            csv_data = []
            for product in all_products:
                # Tạo bản ghi CSV đơn giản hơn JSON
                csv_record = {
                    "product_id": product.get("product_id", ""),
                    "shop_id": product.get("shop_id", ""),
                    "product_name": product.get("product_name", ""),
                    "price": product.get("price", 0),
                    "rating": product.get("rating", 0),
                    "total_rating": product.get("total_rating", 0),
                    "total_sold": product.get("total_sold", 0),
                    "url": product.get("url", ""),
                    "category": ", ".join(product.get("category", [])),
                    "shop_name": product.get("shop_info", {}).get("name", ""),
                    "crawl_time": product.get("crawl_time", "")
                }
                csv_data.append(csv_record)
            
            # Lưu vào CSV
            df = pd.DataFrame(csv_data)
            df.to_csv(csv_summary_file, index=False, encoding='utf-8-sig')
            logger.info(f"File CSV tổng hợp đã được lưu vào {csv_summary_file}")
        
        # Dừng proxy rotation nếu đang sử dụng
        if self.use_proxy and self.proxy_manager:
            self.proxy_manager.stop_rotation_thread()
            
        logger.info(f"\n===== KẾT THÚC QUY TRÌNH CRAWL =====")
        return all_products
        
    def __del__(self):
        """Hàm hủy đối tượng, đảm bảo dừng proxy rotation thread"""
        if hasattr(self, 'proxy_manager') and self.proxy_manager:
            self.proxy_manager.stop_rotation_thread()