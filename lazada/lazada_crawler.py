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
from .utils import random_sleep, create_proxy_auth_extension, download_and_save_image

logger = logging.getLogger("LazadaCrawler")

class LazadaCrawler:
    def __init__(self, proxy_api_key=None, headless=False, wait_time=10, max_retries=3, 
                 proxy_rotation_interval=(60, 90), use_proxy=True):
        """
        Khởi tạo crawler tổng hợp cho Lazada
        
        Args:
            proxy_api_key: Khóa API của dịch vụ proxy xoay
            headless: Chạy trình duyệt ở chế độ headless (không giao diện)
            wait_time: Thời gian chờ tối đa cho các phần tử (giây)
            max_retries: Số lần thử lại tối đa khi có lỗi
            proxy_rotation_interval: Khoảng thời gian giữa các lần đổi proxy (giây)
            use_proxy: Có sử dụng proxy hay không
        """
        self.homepage_url = "https://www.lazada.vn/"
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

    def crawl_product_detail(self, product_url):
        """
        Crawl thông tin chi tiết của một sản phẩm từ URL
        
        Args:
            product_url: URL của sản phẩm cần crawl
            
        Returns:
            Dict chứa thông tin chi tiết sản phẩm
        """
        logger.info(f"Bắt đầu crawl thông tin chi tiết của sản phẩm: {product_url}")
        retries = 0
        max_retries = self.max_retries
        
        while retries <= max_retries:
            try:
                # Khởi tạo trình duyệt
                driver, wait = self.setup_driver()
                
                try:
                    # Truy cập trang sản phẩm
                    driver.get(product_url)
                    logger.info(f"Đã truy cập URL sản phẩm, đợi trang tải xong...")
                    
                    # Chờ trang tải xong
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    random_sleep(3, 5)  # Chờ thêm để tránh bị phát hiện là bot
                    
                    # Xử lý popup nếu có
                    self.handle_popups(driver)
                    
                    # Tạo thư mục lưu ảnh cho sản phẩm này
                    # Trích xuất product_id từ URL
                    ids = self.extract_lazada_ids(product_url)
                    product_id = ids.get("product_id", "unknown")
                    
                    # Tạo thư mục images nếu chưa tồn tại
                    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    images_dir = os.path.join(base_dir, 'images', product_id)
                    os.makedirs(images_dir, exist_ok=True)
                    
                    # Bắt đầu lấy thông tin sản phẩm
                    product_info = {
                        "product_id": product_id,
                        "shop_id": ids.get("shop_id", "unknown"),
                        "url": product_url,
                        "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    # Lấy tên sản phẩm
                    product_info["product_name"] = self.get_product_name(driver, wait)
                    
                    # Lấy phân loại hàng hóa
                    product_info["category"] = self.get_product_category(driver)
                    
                    # Lấy giá sản phẩm
                    product_info["price"] = self.get_product_price(driver)
                    
                    # Lấy thông tin đánh giá
                    product_info["rating"] = self.get_product_rating(driver)
                    product_info["total_rating"] = self.get_total_rating(driver)
                    
                    # Lấy thông tin đã bán
                    product_info["total_sold"] = self.get_total_sold(driver)
                    
                    # Lấy thông tin shop
                    product_info["shop_info"] = self.get_shop_info(driver)
                    
                    # Lấy mô tả sản phẩm
                    product_info["description"] = self.get_product_description(driver)
                    
                    # Lấy thông số kỹ thuật
                    product_info["specifications"] = self.get_product_specifications(driver)
                    
                    # Lấy thông tin biến thể
                    product_info["variations"] = self.get_product_variations(driver, images_dir)
                    
                    # Lấy thông tin giao hàng
                    product_info["delivery_options"] = self.get_delivery_options(driver)
                    
                    logger.info(f"Đã crawl xong thông tin chi tiết của sản phẩm: {product_info['product_name']}")
                    
                    return product_info
                    
                except Exception as e:
                    logger.error(f"Lỗi khi crawl thông tin sản phẩm: {str(e)}")
                    retries += 1
                    
                    if retries <= max_retries:
                        # Nếu có proxy manager, thử đổi proxy
                        if self.proxy_manager:
                            self.proxy_manager.force_rotate_proxy_if_possible()
                            logger.info(f"Đổi proxy và thử lại lần {retries}/{max_retries}...")
                        else:
                            logger.info(f"Thử lại lần {retries}/{max_retries}...")
                            
                        # Đợi một lúc trước khi thử lại
                        time.sleep(random.uniform(5, 10))
                    else:
                        logger.error(f"Đã vượt quá số lần thử lại. Bỏ qua sản phẩm này.")
                        return None
                
                finally:
                    # Đảm bảo đóng trình duyệt
                    driver.quit()
                    logger.info("Đã đóng trình duyệt")
                    
            except Exception as outer_e:
                logger.error(f"Lỗi ngoài khi khởi tạo trình duyệt: {str(outer_e)}")
                retries += 1
                
                if retries <= max_retries:
                    # Đợi một lúc trước khi thử lại
                    time.sleep(random.uniform(5, 10))
                    logger.info(f"Thử lại lần {retries}/{max_retries}...")
                else:
                    logger.error(f"Đã vượt quá số lần thử lại. Bỏ qua sản phẩm này.")
                    return None
        
        return None  # Nếu tất cả các lần thử đều thất bại
    
    def setup_driver(self):
        """Thiết lập trình duyệt Chrome với Selenium WebDriver và cấu hình proxy nếu có"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--window-size=1366,768") 
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
        try:
            service = Service(ChromeDriverManager().install())
        except:
            # Trên Windows, đường dẫn thường có .exe
            service = Service("chromedriver.exe")
            
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Thêm đoạn mã JavaScript để ngụy trang webdriver
        if not proxy_configured:
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"})
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        wait = WebDriverWait(driver, self.wait_time)
        
        return driver, wait

    def handle_popups(self, driver):
        """Xử lý các popup có thể xuất hiện trên Lazada"""
        try:
            # Tìm và đóng các popup quảng cáo trên Lazada
            close_buttons = driver.find_elements(By.CSS_SELECTOR, ".lzd-popup__close-btn, .lazada-icon-close-thin, .next-dialog-close")
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
        """Thu thập các đường link sản phẩm từ phần gợi ý trên trang chủ Lazada"""
        logger.info("Bắt đầu thu thập đường link sản phẩm từ trang chủ...")
        
        # Khởi tạo trình duyệt mới
        driver, wait = self.setup_driver()
        collected_links = []
        
        try:
            # Truy cập trang chủ
            driver.get(self.homepage_url)
            logger.info("Đã truy cập trang chủ Lazada")
            
            # Chờ trang tải xong
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            random_sleep(2, 3)
            
            # Xử lý popup nếu có
            self.handle_popups(driver)
            
            # Cuộn xuống để hiển thị phần gợi ý sản phẩm "Dành riêng cho bạn"
            logger.info("Cuộn xuống phần Dành riêng cho bạn...")
            for i in range(5):  # Cuộn 5 lần, mỗi lần 500px
                driver.execute_script("window.scrollBy(0, 500);")
                random_sleep(0.5, 1)
            
            # Chờ đợi để phần gợi ý hiện ra đầy đủ
            logger.info(f"Đang chờ {self.wait_time} giây để các sản phẩm gợi ý tải hoàn tất...")
            random_sleep(5, 8)
            
            # Tìm section chứa các sản phẩm gợi ý
            try:
                # Tìm phần tử chứa các sản phẩm gợi ý dựa trên HTML mẫu
                recommended_section = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".hp-mod-card-content, .card-jfy-wrapper"))
                )
                
                # Tìm tất cả các sản phẩm - sử dụng CSS selector phù hợp với HTML mẫu
                product_elements = recommended_section.find_elements(By.CSS_SELECTOR, ".jfy-item a")
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
        time.sleep(60)
        return collected_links

    def get_product_name(self, driver, wait):
        """Lấy tên sản phẩm"""
        try:
            name_element = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.pdp-mod-product-badge-title"))
            )
            return name_element.text.strip()
        except Exception as e:
            logger.error(f"Không thể lấy tên sản phẩm: {e}")
            return "N/A"

    def get_product_category(self, driver):
        """Lấy phân loại hàng hóa"""
        try:
            category_elements = driver.find_elements(By.CSS_SELECTOR, "#J_breadcrumb .breadcrumb_item_anchor:not(.breadcrumb_item_anchor_last)")
            
            categories = []
            for element in category_elements:
                cat_text = element.text.strip()
                if cat_text:
                    categories.append(cat_text)
                    
            return categories if categories else ["N/A"]
            
        except Exception as e:
            logger.error(f"Không thể lấy phân loại hàng hóa: {e}")
            return ["N/A"]

    def get_product_rating(self, driver):
        """Lấy điểm đánh giá sản phẩm"""
        try:
            # Lazada không hiển thị điểm đánh giá trực tiếp bằng số, nên chúng ta cần đếm số sao hiển thị
            # Cách thực hiện: Đếm số lượng ngôi sao được làm nổi bật (top-layer checked)
            rating_container = driver.find_element(By.CSS_SELECTOR, ".pdp-review-summary__stars")
            # Kiểm tra phần trăm width của lớp sao được làm nổi bật
            checked_stars = rating_container.find_element(By.CSS_SELECTOR, ".card-jfy-rating-layer.top-layer.checked")
            width_style = checked_stars.get_attribute("style")
            # Trích xuất giá trị phần trăm
            match = re.search(r'width:\s*(\d+)%', width_style)
            if match:
                percentage = int(match.group(1))
                # Chuyển đổi phần trăm thành thang điểm 5 sao
                rating = (percentage / 100) * 5
                return rating
            # Cách sao lưu: nếu không tìm thấy width, đếm số lượng ngôi sao
            star_elements = rating_container.find_elements(By.CSS_SELECTOR, ".star")
            return len(star_elements)
            
        except Exception as e:
            logger.error(f"Không thể lấy điểm đánh giá: {e}")
            return 0.0

    def get_total_rating(self, driver):
        """Lấy tổng số lượt đánh giá"""
        try:
            rating_element = driver.find_element(By.CSS_SELECTOR, ".pdp-review-summary__link")
            # Trích xuất số từ chuỗi "đánh giá X"
            rating_text = rating_element.text.strip()
            # Sử dụng regex để trích xuất số
            match = re.search(r'đánh giá\s+(\d+(?:,\d+)*)', rating_text)
            if match:
                # Loại bỏ dấu phẩy và chuyển thành số nguyên
                return int(match.group(1).replace(',', ''))
            else:
                # Thử cách khác
                match = re.search(r'(\d+(?:,\d+)*)', rating_text)
                if match:
                    return int(match.group(1).replace(',', ''))
            return 0
            
        except Exception as e:
            logger.error(f"Không thể lấy tổng số đánh giá: {e}")
            return 0

    def get_total_sold(self, driver):
        """Lấy tổng số lượng đã bán"""
        try:
            # Trong mẫu HTML, không thấy thông tin số lượng đã bán được hiển thị rõ ràng
            # Có thể cần tìm các selector thích hợp hoặc sử dụng API
            sold_element = driver.find_element(By.CSS_SELECTOR, ".pdp-mod-product-info .sold")
            
            if not sold_element:
                return 0
                
            sold_text = sold_element.text.strip()
            # Xử lý text để trích xuất số
            sold_text = re.sub(r'Đã bán|Sold', '', sold_text).strip()
            
            if 'k' in sold_text.lower():
                return int(float(sold_text.lower().replace('k', '')) * 1000)
            else:
                return int(sold_text.replace(',', '').replace('.', ''))
            
        except Exception as e:
            # Có thể không có thông tin số lượng đã bán
            return 0
    
    def get_product_price(self, driver):
        """Lấy giá sản phẩm hiện tại"""
        try:
            price_element = driver.find_element(By.CSS_SELECTOR, ".pdp-price.pdp-price_type_normal.pdp-price_color_orange, .pdp-price")
            
            price_text = price_element.text.strip()
            # Xử lý chuỗi giá tiền để lấy chỉ số
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
            main_image_elements = driver.find_elements(By.CSS_SELECTOR, ".gallery-preview-panel__image, .next-slick-slide img")
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
            variation_sections = driver.find_elements(By.CSS_SELECTOR, ".sku-prop")
            
            for section in variation_sections:
                try:
                    # Lấy tên loại biến thể (vd: "Màu sắc", "Kích thước")
                    title_element = section.find_element(By.CSS_SELECTOR, ".section-title")
                    title = title_element.text.strip()
                    
                    # Tìm các lựa chọn cho loại biến thể này
                    if "Nhóm màu" in title or "Color" in title:
                        # Đối với biến thể màu sắc, thường có hình ảnh
                        option_elements = section.find_elements(By.CSS_SELECTOR, ".sku-variable-img-wrap, .sku-variable-img-wrap-selected")
                        options = []
                        
                        for option in option_elements:
                            try:
                                img_element = option.find_element(By.CSS_SELECTOR, "img")
                                name_element = section.find_element(By.CSS_SELECTOR, ".sku-name")
                                
                                option_data = {
                                    "name": name_element.text.strip(),
                                    "image_url": img_element.get_attribute("src")
                                }
                                
                                # Tải và lưu ảnh
                                if option_data["image_url"]:
                                    local_path = download_and_save_image(
                                        option_data["image_url"], 
                                        images_dir, 
                                        f"{title}_{option_data['name']}"
                                    )
                                    option_data["image_local_path"] = local_path
                                
                                options.append(option_data)
                            except Exception as inner_e:
                                logger.debug(f"Lỗi khi xử lý một tùy chọn màu: {inner_e}")
                                continue
                    else:
                        # Đối với các biến thể khác
                        option_elements = section.find_elements(By.CSS_SELECTOR, ".sku-variable-size, .sku-variable-size-selected")
                        options = [{"name": option.text.strip()} for option in option_elements if option.text.strip()]
                    
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
            
            # Lấy tên cửa hàng
            try:
                shop_name_element = driver.find_element(By.CSS_SELECTOR, ".seller-name__detail-name")
                shop_info["name"] = shop_name_element.text.strip()
            except Exception as e:
                logger.error(f"Không thể lấy tên cửa hàng: {e}")
                shop_info["name"] = "N/A"
            
            # Lấy URL cửa hàng
            try:
                shop_url_element = driver.find_element(By.CSS_SELECTOR, ".seller-name__detail-name")
                shop_info["url"] = shop_url_element.get_attribute("href")
            except Exception as e:
                logger.error(f"Không thể lấy URL cửa hàng: {e}")
                shop_info["url"] = "N/A"
            
            # Lấy thông tin đánh giá cửa hàng
            try:
                # Lấy đánh giá tích cực
                rating_positive_element = driver.find_element(By.CSS_SELECTOR, ".rating-positive")
                shop_info["positive_rating"] = rating_positive_element.text.strip()
                
                # Lấy tỷ lệ giao đúng hạn
                info_contents = driver.find_elements(By.CSS_SELECTOR, ".info-content")
                for info in info_contents:
                    try:
                        title_elem = info.find_element(By.CSS_SELECTOR, ".seller-info-title")
                        value_elem = info.find_element(By.CSS_SELECTOR, ".seller-info-value")
                        
                        title = title_elem.text.strip().lower()
                        value = value_elem.text.strip()
                        
                        if "giao đúng hạn" in title:
                            shop_info["on_time_delivery"] = value
                        elif "tỷ lệ phản hồi" in title:
                            shop_info["response_rate"] = value
                    except:
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
            try:
                description_section = driver.find_element(By.CSS_SELECTOR, ".html-content.detail-content")
                # Lấy text của tất cả các đoạn văn trong phần mô tả
                paragraphs = description_section.find_elements(By.CSS_SELECTOR, "p, span, div")
                description_texts = [p.text.strip() for p in paragraphs if p.text.strip()]
                
                # Nối các đoạn văn lại thành một chuỗi, phân cách bằng dấu xuống dòng
                return "\n".join(description_texts)
            except:
                # Thử cách khác nếu không tìm thấy phần tử
                try:
                    description_section = driver.find_element(By.CSS_SELECTOR, ".pdp-product-desc")
                    return description_section.text.strip()
                except:
                    return "N/A"
            
        except Exception as e:
            logger.error(f"Không thể lấy mô tả sản phẩm: {e}")
            return "N/A"
    
    def get_product_specifications(self, driver):
        """Lấy thông số kỹ thuật của sản phẩm"""
        try:
            specifications = {}
            
            # Tìm phần tử chứa thông số kỹ thuật
            spec_section = driver.find_element(By.CSS_SELECTOR, ".pdp-mod-specification")
            
            # Tìm tất cả các cặp key-value
            key_items = spec_section.find_elements(By.CSS_SELECTOR, ".key-li")
            
            for item in key_items:
                try:
                    key = item.find_element(By.CSS_SELECTOR, ".key-title").text.strip()
                    value = item.find_element(By.CSS_SELECTOR, ".key-value").text.strip()
                    
                    # Loại bỏ dấu hai chấm và khoảng trắng thừa
                    key = key.rstrip(':').strip()
                    
                    if key and value:
                        specifications[key] = value
                except:
                    continue
            
            return specifications
            
        except Exception as e:
            logger.error(f"Không thể lấy thông số kỹ thuật sản phẩm: {e}")
            return {}
    
    def get_delivery_options(self, driver):
        """Lấy thông tin các tùy chọn giao hàng"""
        try:
            delivery_options = []
            
            # Tìm phần tử chứa thông tin giao hàng
            delivery_section = driver.find_element(By.CSS_SELECTOR, ".delivery__content")
            
            # Tìm tất cả các tùy chọn giao hàng
            option_elements = delivery_section.find_elements(By.CSS_SELECTOR, ".delivery-option-item")
            
            for option in option_elements:
                try:
                    option_data = {}
                    
                    # Lấy tiêu đề
                    title_element = option.find_element(By.CSS_SELECTOR, ".delivery-option-item__title")
                    option_data["title"] = title_element.text.strip()
                    
                    # Lấy thời gian giao hàng nếu có
                    try:
                        time_element = option.find_element(By.CSS_SELECTOR, ".delivery-option-item__time")
                        option_data["time"] = time_element.text.strip()
                    except:
                        option_data["time"] = ""
                    
                    # Lấy phí giao hàng
                    try:
                        fee_element = option.find_element(By.CSS_SELECTOR, ".delivery-option-item__shipping-fee")
                        option_data["fee"] = fee_element.text.strip()
                    except:
                        option_data["fee"] = ""
                    
                    delivery_options.append(option_data)
                except:
                    continue
            
            return delivery_options
            
        except Exception as e:
            logger.error(f"Không thể lấy thông tin giao hàng: {e}")
            return []
    
    def extract_lazada_ids(self, product_url):
        """
        Trích xuất ID sản phẩm và shop từ URL Lazada
        
        Args:
            product_url: URL của sản phẩm Lazada
            
        Returns:
            Dictionary chứa shop_id và product_id
        """
        try:
            product_id = ""
            shop_id = ""
            
            # Trích xuất product_id
            product_id_match = re.search(r'i(\d+)', product_url)
            if product_id_match:
                product_id = product_id_match.group(1)
                
            # Trích xuất shop_id (có thể không có trong URL)
            shop_id_match = re.search(r'shop\/([^\/\?]+)', product_url)
            if shop_id_match:
                shop_id = shop_id_match.group(1)
            
            return {"product_id": product_id, "shop_id": shop_id}
        except Exception as e:
            logger.error(f"Lỗi khi trích xuất ID từ URL: {e}")
            return {"product_id": "", "shop_id": ""}