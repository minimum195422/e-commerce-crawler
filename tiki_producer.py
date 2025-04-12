# tiki_crawler.py
import time
import json
import random
from datetime import datetime
import os
from seleniumwire import webdriver  # Change: Use seleniumwire instead of selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from proxy import RotatingProxy

# Flag để quyết định có sử dụng Kafka hay không
USE_KAFKA = False

if USE_KAFKA:
    try:
        from kafka import KafkaProducer
        print("Kafka được kích hoạt")
    except ImportError:
        print("Không tìm thấy thư viện Kafka, chuyển sang lưu file")
        USE_KAFKA = False

def setup_proxy_option(proxy):
    # Tạo proxy URL
    proxy_url = f"http://{proxy['username']}:{proxy['password']}@{proxy['ip']}:{proxy['port']}"
    print(f"Đang sử dụng proxy: {proxy['ip']}:{proxy['port']}")
    
    # Tùy chọn cho seleniumwire
    return {
        'proxy': {
            'http': proxy_url,
            'https': proxy_url,
        },
        'verify_ssl': False,  # Bỏ qua lỗi SSL
        'connection_timeout': 60,  # Tăng timeout
        'suppress_connection_errors': False  # Hiển thị lỗi kết nối để debug
    }

def setup_driver(proxy=None):
    """Khởi tạo trình duyệt Chrome với options phù hợp và proxy sử dụng selenium-wire"""
    chrome_options = Options()
    
    # Thêm cấu hình cơ bản
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    
    # Thêm cấu hình để tránh màn hình trắng
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    
    # Tùy chọn để tránh phát hiện bot
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # User-Agent giả lập người dùng thực
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
    
    # Cấu hình seleniumwire options
    seleniumwire_options = {}
    
    # Thêm cấu hình proxy nếu được cung cấp
    if proxy:
        seleniumwire_options = setup_proxy_option(proxy)
    
    # Thêm timeout cho seleniumwire requests
    seleniumwire_options['connection_timeout'] = 60 
    
    # Tạo driver với selenium-wire
    driver = webdriver.Chrome(options=chrome_options, seleniumwire_options=seleniumwire_options)
    
    # Thiết lập page load timeout
    driver.set_page_load_timeout(60)
    
    # Thêm JavaScript để tránh phát hiện automation
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def initialize_kafka_producer():
    """Khởi tạo Kafka producer"""
    if not USE_KAFKA:
        return None
        
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        return producer
    except Exception as e:
        print(f"Lỗi kết nối Kafka: {str(e)}")
        return None

def extract_product_urls(driver):
    """Trích xuất URLs của sản phẩm từ phần gợi ý"""
    try:
        print("Bắt đầu trích xuất URL sản phẩm từ phần gợi ý...")
        
        # Đảm bảo đã tìm thấy phần gợi ý trước
        try:
            # Tìm container chứa phần gợi ý sản phẩm
            suggestion_container = driver.find_element(By.CSS_SELECTOR, ".sc-25579e0e-0.kzWQME")
            print("Đã tìm thấy phần gợi ý sản phẩm")
        except Exception as e:
            print(f"Không tìm thấy phần gợi ý sản phẩm: {str(e)}")
            return []
        
        # Tìm tất cả các thẻ link sản phẩm trong container gợi ý
        try:
            product_links = suggestion_container.find_elements(By.CSS_SELECTOR, "a.sc-8b415d9d-1.iRifC.product-item")
            print(f"Đã tìm thấy {len(product_links)} sản phẩm trong phần gợi ý")
        except Exception as e:
            print(f"Không tìm thấy link sản phẩm nào: {str(e)}")
            return []
        
        # Trích xuất URL từ các thẻ link
        product_urls = []
        for index, link in enumerate(product_links):
            try:
                url = link.get_attribute("href")
                if url:
                    # Làm sạch URL (nếu cần)
                    if "//tka.tiki.vn/pixel/pixel" in url:
                        # Đây là link theo dõi, thực hiện xử lý nếu cần
                        # Một số trường hợp có thể cần lọc lại URL hoặc chuyển đổi
                        # Ví dụ: Lấy thông tin sản phẩm từ tham số hoặc sử dụng JavaScript
                        # để lấy URL thực trong trường hợp URL bị ẩn hoặc được chuyển hướng
                        pass
                    
                    product_urls.append(url)
                    print(f"  - Sản phẩm {index+1}: {url[:100]}...")
            except Exception as e:
                print(f"Lỗi khi trích xuất URL cho sản phẩm thứ {index+1}: {str(e)}")
        
        print(f"Đã trích xuất thành công {len(product_urls)} URL sản phẩm")
        return product_urls
    
    except Exception as e:
        print(f"Lỗi tổng thể khi trích xuất URL sản phẩm: {str(e)}")
        return []

# Phiên bản nâng cao với JavaScript để xử lý trường hợp đặc biệt
def extract_product_urls_advanced(driver):
    """Trích xuất URLs của sản phẩm từ phần gợi ý sử dụng JavaScript"""
    try:
        print("Bắt đầu trích xuất URL sản phẩm với JavaScript...")
        
        # Sử dụng JavaScript để trích xuất tất cả URL sản phẩm
        product_links = driver.execute_script("""
            // Tìm container gợi ý sản phẩm
            const container = document.querySelector('.sc-25579e0e-0.kzWQME');
            if (!container) return [];
            
            // Tìm tất cả link sản phẩm
            const links = container.querySelectorAll('a.sc-8b415d9d-1.iRifC.product-item');
            
            // Trích xuất href attribute
            const urls = [];
            links.forEach(link => {
                if (link.href) {
                    urls.push(link.href);
                }
            });
            
            return urls;
        """)
        
        if product_links and len(product_links) > 0:
            print(f"Đã trích xuất thành công {len(product_links)} URL sản phẩm bằng JavaScript")
            
            # Hiển thị danh sách URLs (chỉ hiển thị 5 URLs đầu tiên để tránh quá dài)
            for index, url in enumerate(product_links[:5]):
                print(f"  - Sản phẩm {index+1}: {url[:100]}...")
                
            if len(product_links) > 5:
                print(f"  ... và {len(product_links) - 5} sản phẩm khác")
                
            return product_links
        else:
            print("Không tìm thấy URL sản phẩm nào bằng JavaScript")
            return []
            
    except Exception as e:
        print(f"Lỗi khi trích xuất URL sản phẩm bằng JavaScript: {str(e)}")
        return []

def send_to_kafka(producer, urls):
    """Gửi danh sách URLs vào Kafka topic hoặc lưu vào file"""
    if not urls:
        print("Không có URL nào để xử lý")
        return
        
    if producer:
        for url in urls:
            data = {
                "url": url,
                "timestamp": datetime.now().isoformat()
            }
            producer.send('product_urls', value=data)
        
        producer.flush()
        print(f"Đã gửi {len(urls)} URL sản phẩm vào Kafka")
    else:
        # Lưu vào file nếu không có Kafka
        with open('product_urls.json', 'w', encoding='utf-8') as f:
            data = [{
                "url": url,
                "timestamp": datetime.now().isoformat()
            } for url in urls]
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Đã lưu {len(urls)} URL sản phẩm vào file product_urls.json")

def close_popup_ads(driver):
    """Đóng popup quảng cáo nếu nó xuất hiện"""
    # Đợi một chút để popup hiện ra đầy đủ
    time.sleep(3)
    
    try:
        # Tìm phần tử div chứa popup
        popup_container = driver.find_element(By.ID, "VIP_BUNDLE")
        print("Đã tìm thấy popup VIP_BUNDLE")
        
        # Tìm nút đóng chính xác bằng nhiều cách
        try:
            # Cách 1: Tìm theo data-view-id attribute
            close_button = popup_container.find_element(By.CSS_SELECTOR, "[data-view-id='popup-manager.close']")
            driver.execute_script("arguments[0].click();", close_button)
            print("Đã đóng popup bằng selector data-view-id")
            return
        except Exception as e:
            print(f"Không thể đóng popup bằng data-view-id: {str(e)}")
            
        try:
            # Cách 2: Tìm theo ảnh nút đóng (32x32)
            close_button = popup_container.find_element(By.CSS_SELECTOR, "img[width='32'][height='32']")
            driver.execute_script("arguments[0].click();", close_button)
            print("Đã đóng popup bằng selector img 32x32")
            return
        except Exception as e:
            print(f"Không thể đóng popup bằng img 32x32: {str(e)}")
            
        try:
            # Cách 3: Sử dụng JavaScript để tìm và nhấn vào phần tử có class sc-900210d0-0 hFEtiz đầu tiên
            driver.execute_script("""
                const popup = document.getElementById('VIP_BUNDLE');
                if (popup) {
                    const closeButtons = popup.querySelectorAll('.sc-900210d0-0.hFEtiz');
                    if (closeButtons.length > 0) {
                        closeButtons[0].click();
                        return true;
                    }
                }
                return false;
            """)
            print("Đã đóng popup bằng JavaScript")
            return
        except Exception as e:
            print(f"Không thể đóng popup bằng JavaScript: {str(e)}")
            
        # Cách 4: Thử ẩn popup nếu không đóng được
        try:
            driver.execute_script("""
                const popup = document.getElementById('VIP_BUNDLE');
                if (popup) {
                    popup.style.display = 'none';
                    return true;
                }
                return false;
            """)
            print("Đã ẩn popup bằng JavaScript")
            return
        except Exception as e:
            print(f"Không thể ẩn popup: {str(e)}")
            
    except Exception as e:
        print(f"Không tìm thấy popup VIP_BUNDLE: {str(e)}")
        
    # Phương án dự phòng: tìm và nhấn vào bất kỳ phần tử nào có thể là nút đóng
    try:
        close_selectors = [
            ".sc-48d606f0-3.xWOUT",  # Class của div nút đóng
            ".sc-900210d0-0.hFEtiz[width='32'][height='32']",  # Class của img đóng với kích thước cụ thể
            "[data-view-id='popup-manager.close']",  # Selector theo data attribute
            "div[class*='popup'] img[width='32'][height='32']"  # Bất kỳ popup nào có ảnh 32x32
        ]
        
        for selector in close_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    # Thử nhấn vào phần tử đầu tiên
                    driver.execute_script("arguments[0].click();", elements[0])
                    print(f"Đã đóng popup với selector: {selector}")
                    return
            except Exception:
                continue
                
        print("Đã thử tất cả các cách nhưng không thể đóng popup")
    except Exception as e:
        print(f"Lỗi khi xử lý phương án dự phòng: {str(e)}")

def click_view_more_button(driver, num_clicks=3):
    """
    Thực hiện thao tác tìm và nhấn nút "Xem thêm" nhiều lần
    
    Tham số:
        driver: Đối tượng WebDriver
        num_clicks: Số lần thực hiện nhấn nút "Xem thêm", mặc định là 3
        
    Trả về:
        bool: True nếu đã nhấn Xem thêm ít nhất 1 lần, False nếu không tìm thấy nút từ đầu
    """
    print(f"Bắt đầu thực hiện thao tác Xem thêm {num_clicks} lần...")
    at_least_one_click = False
    
    for i in range(num_clicks):
        try:
            # Cuộn xuống để tìm nút "Xem thêm"
            view_more_found = False
            view_more_scroll_attempts = 0
            max_view_more_scroll_attempts = 5
            
            while not view_more_found and view_more_scroll_attempts < max_view_more_scroll_attempts:
                # Cuộn xuống để tìm nút "Xem thêm"
                distance = random.randint(300, 500)
                driver.execute_script(f"window.scrollBy(0, {distance});")
                view_more_scroll_attempts += 1
                time.sleep(1)
                
                # Tìm nút "Xem thêm" bằng nhiều cách
                try:
                    # Cách 1: Sử dụng CSS Selector với class
                    view_more_button = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".sc-29fc655e-0.eogKxs.view-more"))
                    )
                    view_more_found = True
                except Exception:
                    try:
                        # Cách 2: Sử dụng data-view-id attribute
                        view_more_button = WebDriverWait(driver, 3).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-view-id='home_infinity_view.more']"))
                        )
                        view_more_found = True
                    except Exception:
                        try:
                            # Cách 3: Sử dụng XPath với text
                            view_more_button = WebDriverWait(driver, 3).until(
                                EC.presence_of_element_located((By.XPATH, "//a[contains(text(), 'Xem Thêm')]"))
                            )
                            view_more_found = True
                        except Exception:
                            try:
                                # Cách 4: Kết hợp class và data-view-id
                                view_more_button = WebDriverWait(driver, 3).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, ".sc-29fc655e-0.eogKxs.view-more[data-view-id='home_infinity_view.more']"))
                                )
                                view_more_found = True
                            except Exception:
                                print(f"Chưa tìm thấy nút Xem thêm, tiếp tục cuộn... ({view_more_scroll_attempts}/{max_view_more_scroll_attempts})")
            
            if view_more_found:
                # Cuộn đến nút để đảm bảo nó hiển thị
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", view_more_button)
                time.sleep(1)
                
                print(f"Nhấn nút Xem thêm lần {i+1}/{num_clicks}")
                
                # Thử nhấn bằng JavaScript
                try:
                    driver.execute_script("arguments[0].click();", view_more_button)
                    print(f"Đã nhấn nút Xem thêm lần {i+1} bằng JavaScript")
                except Exception as e:
                    print(f"Không thể nhấn bằng JavaScript: {e}")
                    try:
                        # Thử phương pháp nhấn thông thường
                        view_more_button.click()
                        print(f"Đã nhấn nút Xem thêm lần {i+1} bằng phương pháp thông thường")
                    except Exception as e:
                        print(f"Không thể nhấn nút Xem thêm lần {i+1}: {e}")
                        continue
                
                # Đánh dấu đã nhấn thành công ít nhất 1 lần
                at_least_one_click = True
                
                # Đợi để trang tải thêm sản phẩm
                time.sleep(5)
                
                # Có thể thêm xử lý popup ở đây nếu cần
                # close_popup_ads(driver)
                
            else:
                print(f"Không tìm thấy nút Xem thêm sau lần nhấn thứ {i}")
                break
                
        except Exception as e:
            print(f"Lỗi khi thực hiện thao tác Xem thêm lần {i+1}: {str(e)}")
    
    print(f"Hoàn thành thao tác Xem thêm, nhấn được {i+1 if at_least_one_click else 0}/{num_clicks} lần")

def crawl_tiki_product_list(proxy=None):
    """Hàm chính để crawl danh sách sản phẩm"""
    max_retries = 3
    for attempt in range(max_retries):
        driver = setup_driver(proxy)
        all_product_urls = set()
        
        try:
            
            # Mở trang Tiki
            print("Đang truy cập trang Tiki...")
            driver.get("https://tiki.vn/")
            time.sleep(20)
            print("Truy cập thành công, bắt đầu xác định dữ liệu")

            close_popup_ads(driver)

            print("Bắt đầu cuộn trang để tìm phần sản phẩm gợi ý")
            max_scroll_attempts = 10
            scroll_attempts = 0
            target_found = False
            
            while scroll_attempts < max_scroll_attempts and not target_found:
                # Cuộn xuống thêm một đoạn
                distance = random.randint(300, 500)
                driver.execute_script(f"window.scrollBy(0, {distance});")
                scroll_attempts += 1
                print(f"Cuộn lần {scroll_attempts}/{max_scroll_attempts}")
                
                # Đợi một chút để trang tải các phần tử mới
                time.sleep(3)
                close_popup_ads(driver)
                
                # Kiểm tra xem phần tử mục tiêu đã xuất hiện chưa
                try:
                    target_element = driver.find_element(By.CSS_SELECTOR, ".sc-25579e0e-0.kzWQME")
                    target_found = True
                    print("Đã tìm thấy phần sản phẩm gợi ý!")
                    time.sleep(3)  # Đợi cuộn hoàn tất
                except Exception:
                    print("Chưa tìm thấy phần tử mục tiêu, tiếp tục cuộn...")

            click_view_more_button(driver, num_clicks=3)
            product_urls = extract_product_urls(driver)
            if product_urls:
                print(f"Đã lấy được {product_urls.__len__} đường dẫn sản phẩm")
            else:
                print("Không lấy được đường link sản phẩm nào!!!")
            break
                            
        except TimeoutException:
            print("Trang Tiki không tải được")
            
            print("Thử làm mới trang...")
            try:
                driver.refresh()
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                print("Làm mới trang thành công")
            except:
                print("Làm mới trang thất bại, thoát crawler")
                return
            
        finally:
            # Đảm bảo đóng driver
            if driver:
                try:
                    driver.quit()
                    print("Đã đóng trình duyệt")
                except Exception as e:
                    print(f"Lỗi khi đóng trình duyệt: {str(e)}")
    
if __name__ == "__main__":
    # Định dạng proxy: http://username:password@host:port
    # Ví dụ: HTTP_PROXY = "username:password@1.2.3.4:8080"
    api_key = "hgvOiDXwraQZOjvKwRUehk"
    proxy_manager = RotatingProxy(api_key)

    HTTP_PROXY = proxy_manager.get_new_proxy()
    
    # Tùy chọn chạy với hoặc không có proxy
    USE_PROXY = True
    
    try:
        if USE_PROXY:
            print(f"Đang chạy với proxy: {HTTP_PROXY['ip']}:{HTTP_PROXY['port']}")
            crawl_tiki_product_list(HTTP_PROXY)
        else:
            print("Đang chạy không dùng proxy")
            crawl_tiki_product_list(None)
    except Exception as e:
        print(f"Lỗi khi chạy crawler: {str(e)}")
        import traceback
        traceback.print_exc()