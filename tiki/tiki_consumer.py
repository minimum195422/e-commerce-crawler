# tiki_consumer.py
import time
import json
import random
import threading
import re
import os
import requests
from datetime import datetime
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from urllib.parse import urlparse, parse_qs

from proxy import RotatingProxy
from rabbitmq_connector import RabbitMQConnector

class Tiki_Consumer:
    def __init__(self, API_PROXY_KEY=None):
        self.api_proxy_key = API_PROXY_KEY
        self.proxy_manager = RotatingProxy(self.api_proxy_key)

    
    def setup_proxy_option(self, proxy):
        # Tạo proxy URL
        proxy_url = f"http://{proxy['username']}:{proxy['password']}@{proxy['ip']}:{proxy['port']}"
        # print(f"Đang sử dụng proxy: {proxy['ip']}:{proxy['port']}")
        
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

    
    def setup_driver(self, proxy=None):
        """Khởi tạo trình duyệt Chrome với options phù hợp và proxy sử dụng selenium-wire"""
        chrome_options = Options()
        
        # Thêm cấu hình cơ bản
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument(f"--lang=vi-VN")
        
        # Thêm cấu hình để tránh màn hình trắng
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument('--ignore-ssl-errors=yes')
        chrome_options.add_argument('--ignore-certificate-errors')
        
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
            seleniumwire_options = self.setup_proxy_option(proxy)
        
        # Thêm timeout cho seleniumwire requests
        seleniumwire_options['connection_timeout'] = 60 
        
        # Tạo driver với selenium-wire
        driver = webdriver.Chrome(options=chrome_options, seleniumwire_options=seleniumwire_options)
        
        # Thiết lập page load timeout
        driver.set_page_load_timeout(60)
        
        # Thêm JavaScript để tránh phát hiện automation
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver

    
    def process_url(self, message):
        try:
            # Kiểm tra cấu trúc message
            if 'url' not in message:
                # print(f"Lỗi: Message không chứa URL: {message}")
                return False
            
            url = message['url']
            # print(f"Đang xử lý URL: {url}")
            
            # TODO: Thực hiện các thao tác với URL ở đây
            # Ví dụ: crawl thông tin chi tiết sản phẩm từ URL
            
            # Giả lập xử lý mất 2 giây
            time.sleep(2)
            
            # Giả sử quá trình xử lý thành công
            # print(f"Đã xử lý xong URL: {url}")
            return True
            
        except Exception as e:
            # print(f"Lỗi khi xử lý URL: {str(e)}")
            return False

    
    def extract_product_id_from_tiki_url(self, url):
        if not url or not isinstance(url, str):
            return None
        
        try:
            # Phân tích URL
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            
            # Kiểm tra URL loại 1: Tracking URL (tka.tiki.vn)
            if 'tka.tiki.vn' in parsed_url.netloc:
                pos = query_params.get('pos', [None])[0]
                reqid = query_params.get('reqid', [None])[0]
                return pos or reqid or None
            
            # Kiểm tra URL loại 2: URL sản phẩm trực tiếp (tiki.vn)
            elif 'tiki.vn' in parsed_url.netloc:
                # Phương pháp 1: Trích xuất từ path URL - định dạng p112289302.html
                product_id_match = re.search(r'p(\d+)\.html', parsed_url.path)
                if product_id_match:
                    return product_id_match.group(1)
                
                # Phương pháp 2: Trích xuất từ tham số spid trong query
                spid = query_params.get('spid', [None])[0]
                if spid:
                    return spid
                
                # Phương pháp 3: Trích xuất từ tham số PID trong query
                for key, value in query_params.items():
                    pid_match = re.search(r'PID\.(\d+)', value[0] if value else '')
                    if pid_match:
                        return pid_match.group(1)
            
            return None
        
        except Exception as e:
            # print(f'Lỗi khi phân tích URL: {e}')
            return None

    
    def create_tiki_images_folder(self, product_id):
        # Tạo tên thư mục
        folder_name = f"tiki_{product_id}"
        
        # Đường dẫn đầy đủ đến thư mục mới
        folder_path = os.path.join("../data/tiki/images", folder_name)
        
        # Kiểm tra và tạo thư mục nếu chưa tồn tại
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)
            print(f"Đã tạo thư mục: {folder_path}")
        else:
            print(f"Thư mục đã tồn tại: {folder_path}")
            
        return folder_path

   
    def extract_thumbnail_url_from_driver(self, driver):
        try:
            # Tìm thẻ img trong div.image-frame
            img_element = driver.find_element(By.CSS_SELECTOR, "div.image-frame img[srcset]")
            
            if img_element:
                # Lấy giá trị thuộc tính srcset
                srcset = img_element.get_attribute("srcset")
                
                if srcset:
                    # Trích xuất URL đầu tiên từ srcset
                    import re
                    match = re.search(r'(https://[^\s]+)', srcset)
                    if match:
                        # Trả về URL không có phần "1x," ở cuối
                        return match.group(1).split(' ')[0]
            
            # Nếu không tìm thấy img có srcset, thử tìm source trong picture
            source_element = driver.find_element(By.CSS_SELECTOR, "div.image-frame picture source[srcset]")
            if source_element:
                srcset = source_element.get_attribute("srcset")
                if srcset:
                    import re
                    match = re.search(r'(https://[^\s]+)', srcset)
                    if match:
                        url = match.group(1).split(' ')[0]
                        # Chuyển đổi từ webp về định dạng gốc nếu cần
                        if url.endswith('.webp'):
                            url = url[:-5]  # Loại bỏ phần .webp
                        return url
            
            return None
        except Exception as e:
            # print(f'Lỗi khi trích xuất thumbnail: {e}')
            return None

    
    def extract_product_name(self, driver):
        h1_element = driver.find_element(By.CSS_SELECTOR, "h1.sc-c0f8c612-0.dEurho")
        h1_text = h1_element.text
        product_name = h1_text.split(',')[0].strip()
        return product_name

    
    def extract_rating(self, driver):
        try:
            rating_elements = driver.find_elements(By.CSS_SELECTOR, ".sc-1a46a934-0 div:first-child div:first-child")
            
            if rating_elements and rating_elements[0].text.strip():
                rating_text = rating_elements[0].text.strip()
                
                # Check if this is a number (rating)
                try:
                    rating = float(rating_text)
                    return rating_text
                except ValueError:
                    # Not a number, so it's not a rating
                    return None
            
            return None
        except Exception as e:
            # print(f"Error extracting rating: {e}")
            return None
        
    
    def extract_sales_quantity(self, driver):
        try:
            # Extract sales quantity (Đã bán 6k in your example)
            sales_element = driver.find_element(By.CSS_SELECTOR, ".sc-1a46a934-3.geGARt")
            sales_text = sales_element.text
            # Extract just the number portion from "Đã bán 6k"
            sales_quantity = sales_text.replace("Đã bán ", "")
            
            return sales_quantity
        except Exception as e:
            # print(f"Error extracting product information: {e}")
            return None
        
    
    def extract_current_price(self, driver):
        try:
            # Extract current price
            current_price_element = driver.find_element(By.CSS_SELECTOR, ".product-price__current-price")
            current_price_text = current_price_element.text.replace("₫", "").strip()
            # Remove thousands separator (dots)
            current_price = float(current_price_text.replace(".", ""))
            return current_price
        except Exception as e:
            # print(f"Error extracting current price: {e}")
            return None
        
    
    def extract_original_price(self, driver):
        try:
            # Try to find the original price element
            # If it doesn't exist (no discount), return None
            try:
                original_price_element = driver.find_element(By.CSS_SELECTOR, ".product-price__original-price del")
                original_price_text = original_price_element.text.replace("₫", "").strip()
                original_price = float(original_price_text.replace(".", ""))
                return original_price
            except:
                # If no original price element found, return None or same as current price
                return None
        except Exception as e:
            # print(f"Error extracting original price: {e}")
            return None
    
    
    def extract_discount(self, driver):
        try:
            # Try to find the discount element
            # If it doesn't exist (no discount), return 0
            try:
                discount_element = driver.find_element(By.CSS_SELECTOR, ".product-price__discount-rate")
                discount_percentage = discount_element.text.replace("-", "").replace("%", "").strip()
                discount_percentage = int(discount_percentage) if discount_percentage else 0
                return discount_percentage
            except:
                # If no discount element found, return 0
                return 0
        except Exception as e:
            # print(f"Error extracting discount: {e}")
            return 0

    
    def extract_seller_info(self, driver):
        """
        Trích xuất thông tin của người bán (seller) với khả năng xử lý nhiều trường hợp
        cấu trúc HTML và tránh lấy nhầm dữ liệu từ các phần khuyến mãi.
        
        Args:
            driver: Selenium WebDriver đã được khởi tạo và điều hướng đến trang sản phẩm
        
        Returns:
            dict: Thông tin seller bao gồm tên, link, điểm đánh giá và số lượng đánh giá
        """
        # Khởi tạo biến để lưu kết quả
        seller_name = None
        seller_link = None
        rating_score = None
        review_count = None
        
        # PHƯƠNG PHÁP 1: JavaScript - Cách đáng tin cậy nhất để xử lý DOM phức tạp
        try:
            result = driver.execute_script("""
                try {
                    // Tìm tất cả container có thể chứa thông tin seller
                    let sellerName = null;
                    let sellerLink = null;
                    let ratingScore = null;
                    let reviewCount = null;
                    
                    // Loại bỏ các phần khuyến mãi, vùng Freeship
                    function isPromotionSection(element) {
                        if (!element) return false;
                        
                        // Kiểm tra màu nền xanh của phần khuyến mãi
                        const bgColor = element.style.backgroundColor;
                        if (bgColor && (bgColor.includes('EFFFF4') || bgColor.includes('rgb(239, 255, 244)'))) {
                            return true;
                        }
                        
                        // Kiểm tra nội dung có chứa từ khóa liên quan đến khuyến mãi
                        const text = element.textContent.toLowerCase();
                        return text.includes('freeship') || 
                            text.includes('giảm') || 
                            text.includes('khuyến mãi') ||
                            text.includes('mã giảm');
                    }
                    
                    // Tìm seller container - thường là div chứa logo và tên seller
                    // Đặc điểm nhận dạng: Có thẻ a chứa picture và img, không phải vùng khuyến mãi
                    const potentialContainers = Array.from(document.querySelectorAll('div[style*="display:flex"]'))
                        .filter(div => {
                            // Bỏ qua các phần khuyến mãi
                            if (isPromotionSection(div)) return false;
                            
                            // Tìm container có thẻ a với picture/img bên trong (logo người bán)
                            const hasSellerLogo = div.querySelector('a picture img') !== null;
                            
                            // Cấu trúc thường có "seller-name" hoặc review rating
                            const hasSellerName = div.querySelector('span.seller-name') !== null || 
                                                div.querySelector('div.seller-name') !== null ||
                                                div.querySelector('div[class*="kjrAcw"]') !== null;
                                                
                            const hasReview = div.querySelector('.item.review') !== null ||
                                            div.querySelector('div[class*="gjnreP"]') !== null;
                                            
                            return hasSellerLogo || hasSellerName || hasReview;
                        });
                    
                    if (potentialContainers.length > 0) {
                        // Kiểm tra từng container tiềm năng
                        for (const container of potentialContainers) {
                            // Kiểm tra xem container có phải phần khuyến mãi không
                            if (isPromotionSection(container)) continue;
                            
                            // Tìm link seller - thường là thẻ a đầu tiên trong container chứa hình ảnh
                            const sellerLinkElem = Array.from(container.querySelectorAll('a'))
                                .find(a => a.querySelector('picture img') !== null);
                                
                            if (sellerLinkElem) {
                                const href = sellerLinkElem.getAttribute('href');
                                // Kiểm tra xem link có phải link của seller không (thường có '/cua-hang/' hoặc 'seller')
                                if (href && (href.includes('/cua-hang/') || href.includes('seller'))) {
                                    sellerLink = href;
                                    
                                    // Tìm tên seller trong các phần tử gần link
                                    const sellerNameContainer = 
                                        container.querySelector('span.seller-name') || 
                                        container.querySelector('div.seller-name') ||
                                        container.querySelector('div[class*="kjrAcw"]');
                                    
                                    if (sellerNameContainer) {
                                        const nameSpan = sellerNameContainer.querySelector('span') || sellerNameContainer;
                                        sellerName = nameSpan.textContent.trim();
                                    }
                                    
                                    // Nếu chưa tìm thấy, tìm trong các thẻ span gần đó
                                    if (!sellerName) {
                                        // Tìm div chứa thông tin seller (thường nằm sau thẻ a chứa logo)
                                        const infoContainer = container.querySelector('a + div');
                                        if (infoContainer) {
                                            const nameSpan = infoContainer.querySelector('span') || 
                                                        infoContainer.querySelector('div[class*="kjrAcw"] span');
                                            if (nameSpan) {
                                                sellerName = nameSpan.textContent.trim();
                                            }
                                        }
                                    }
                                    
                                    // Tìm thông tin rating và review trong cùng container
                                    const reviewContainer = container.querySelector('.item.review') || 
                                                        container.querySelector('div[class*="gjnreP"] .item.review');
                                                        
                                    if (reviewContainer) {
                                        const titleElement = reviewContainer.querySelector('.title');
                                        if (titleElement) {
                                            const ratingSpan = titleElement.querySelector('span');
                                            if (ratingSpan) {
                                                ratingScore = ratingSpan.textContent.trim();
                                            }
                                        }
                                        
                                        const subTitleElement = reviewContainer.querySelector('.sub-title');
                                        if (subTitleElement) {
                                            reviewCount = subTitleElement.textContent.trim().replace(/[()]/g, '');
                                        }
                                    }
                                    
                                    // Đã tìm thấy đủ thông tin, thoát khỏi vòng lặp
                                    if (sellerName && sellerLink) break;
                                }
                            }
                        }
                    }
                    
                    return {
                        name: sellerName,
                        link: sellerLink,
                        rating_score: ratingScore,
                        review_count: reviewCount
                    };
                } catch (error) {
                    console.error('JavaScript error:', error);
                    return null;
                }
            """)
            
            if result and (result.get('name') or result.get('link')):
                # print("Trích xuất seller thành công bằng JavaScript")
                return result
        except Exception as js_error:
            print(f"JavaScript thất bại: {js_error}")
        
        # PHƯƠNG PHÁP 2: Sử dụng Selenium để tìm kiếm các phần tử
        try:
            # Loại bỏ các phần khuyến mãi
            def is_promotion_section(element):
                try:
                    # Kiểm tra theo màu nền
                    bg_color = element.get_attribute("style")
                    if bg_color and ("background-color:#EFFFF4" in bg_color or "background-color: rgb(239, 255, 244)" in bg_color):
                        return True
                    
                    # Kiểm tra theo nội dung
                    text = element.text.lower()
                    if "freeship" in text or "giảm" in text or "khuyến mãi" in text:
                        return True
                    
                    return False
                except:
                    return False
            
            # Tìm tất cả container có thể chứa thông tin seller
            potential_containers = []
            
            # Tìm theo các selector có khả năng cao chứa thông tin seller
            selectors = [
                "div[style*='display:flex'][style*='align-items:center']",
                "div[class*='sc-524d1555-0']",
                "div[class*='vIHcS']"
            ]
            
            for selector in selectors:
                containers = driver.find_elements(By.CSS_SELECTOR, selector)
                for container in containers:
                    # Bỏ qua nếu là phần khuyến mãi
                    if is_promotion_section(container):
                        continue
                    
                    # Tìm thẻ a có chứa picture/img (logo shop)
                    links = container.find_elements(By.CSS_SELECTOR, "a")
                    has_seller_logo = False
                    
                    for link in links:
                        if link.find_elements(By.CSS_SELECTOR, "picture img") or link.find_elements(By.CSS_SELECTOR, "img"):
                            # Kiểm tra xem link có phải link seller không
                            href = link.get_attribute("href")
                            if href and ("/cua-hang/" in href or "seller" in href):
                                has_seller_logo = True
                                break
                    
                    if has_seller_logo:
                        potential_containers.append(container)
                        break
            
            if potential_containers:
                container = potential_containers[0]
                
                # Tìm link seller (link có chứa logo và dẫn đến trang bán hàng)
                seller_link_elements = container.find_elements(By.CSS_SELECTOR, "a")
                for link in seller_link_elements:
                    if link.find_elements(By.CSS_SELECTOR, "picture img") or link.find_elements(By.CSS_SELECTOR, "img"):
                        href = link.get_attribute("href")
                        if href and ("/cua-hang/" in href or "seller" in href):
                            seller_element = link
                            seller_link = href
                            break
                
                if seller_link:
                    # Tìm tên seller
                    try:
                        # Tìm div chứa thông tin (thường nằm sau thẻ a logo)
                        seller_info_divs = driver.execute_script("""
                            const link = arguments[0];
                            // Tìm div chứa thông tin sau thẻ a
                            return Array.from(link.parentElement.querySelectorAll('div')).filter(div => {
                                return !div.querySelector('picture') && div.textContent.trim().length > 0;
                            });
                        """, seller_element)
                        
                        if seller_info_divs:
                            for div in seller_info_divs:
                                # Tìm các thẻ span chứa tên seller
                                spans = div.find_elements(By.TAG_NAME, "span")
                                for span in spans:
                                    text = span.text.strip()
                                    if text and len(text) < 50 and "freeship" not in text.lower():
                                        seller_name = text
                                        break
                                
                                if seller_name:
                                    break
                        
                        # Nếu chưa tìm được, thử tìm span.seller-name
                        if not seller_name:
                            seller_name_elements = container.find_elements(By.CSS_SELECTOR, "span.seller-name span, div.seller-name span, div[class*='kjrAcw'] span")
                            if seller_name_elements:
                                seller_name = seller_name_elements[0].text.strip()
                    except Exception as name_error:
                        print(f"Lỗi khi tìm tên seller: {name_error}")
                
                # Tìm thông tin đánh giá
                try:
                    for rating_selector in ["div.item.review .title span", "div[class*='gjnreP'] .item.review .title span"]:
                        rating_elements = container.find_elements(By.CSS_SELECTOR, rating_selector)
                        if rating_elements:
                            rating_score = rating_elements[0].text.strip()
                            break
                    
                    for review_selector in ["div.item.review .sub-title", "div[class*='gjnreP'] .item.review .sub-title"]:
                        review_elements = container.find_elements(By.CSS_SELECTOR, review_selector)
                        if review_elements:
                            review_count = review_elements[0].text.strip()
                            review_count = review_count.replace("(", "").replace(")", "")
                            break
                except Exception as rating_error:
                    print(f"Lỗi khi tìm thông tin đánh giá: {rating_error}")
        
        except Exception as e:
            print(f"Lỗi khi trích xuất thông tin seller với Selenium: {e}")
        
        # Kiểm tra và trả về kết quả
        if seller_name or seller_link:
            result = {
                "name": seller_name,
                "link": seller_link,
                "rating_score": rating_score,
                "review_count": review_count
            }
            return result
        
        # Nếu tất cả các phương pháp đều thất bại
        return None

    
    def wait_for_option_active(self, option_element):
        try:
            # Kiểm tra xem element có class 'active' không
            classes = option_element.get_attribute("class")
            if classes and "active" in classes:
                return True
                
            return False
        except Exception as e:
            # print(f"Lỗi khi kiểm tra trạng thái active: {e}")
            return False

    
    def extract_product_variants(self, driver):
        result = {
            "variant_categories": {},  # Dictionary các loại biến thể và giá trị 
            "variants_with_images": {}  # Các biến thể có ảnh
        }
        
        try:
            # Tìm tất cả các danh mục tùy chọn
            variant_categories = driver.find_elements(By.CSS_SELECTOR, "div[data-view-id='pdp_main_select_configuration']")
            
            # Kiểm tra và trả về số lượng danh mục tìm được
            # print(f"Tìm thấy {len(variant_categories)} danh mục tùy chọn")
            for category in variant_categories:
                # Lấy tên danh mục
                try:
                    category_name = category.find_element(By.CSS_SELECTOR, "p.option-name").text
                except:
                    category_name = "Unnamed Category"
                
                # Tìm các tùy chọn trong danh mục
                options = category.find_elements(By.CSS_SELECTOR, "div[data-view-id='pdp_main_select_configuration_item']")
                
                # Danh sách lưu các tùy chọn
                option_list = []
                # Danh sách lưu text của các tùy chọn cho variant_categories
                option_text_list = []
                
                # Kiểm tra xem danh mục có ảnh không
                has_images = False

                for option in options:
                    option.click()
                    try:
                        WebDriverWait(driver, 10).until(lambda d: self.wait_for_option_active(option))
                    except Exception as e:
                        # print(f"Timeout đợi option active: {e}")
                        continue
                    
                    # Chờ để ảnh chính được cập nhật
                    time.sleep(5)
                    
                    # Lấy text của option
                    try:
                        # Kiểm tra cấu trúc option có phức tạp (có hình ảnh) hay không
                        if option.find_elements(By.CSS_SELECTOR, "span span"):
                            option_text = option.find_element(By.CSS_SELECTOR, "span span").text
                        else:
                            option_text = option.find_element(By.CSS_SELECTOR, "span").text
                    except:
                        option_text = "Unknown Option"
                    
                    # Thêm text vào danh sách cho variant_categories
                    option_text_list.append(option_text)
                    
                    # Kiểm tra xem danh mục này có ảnh không (phát hiện các option có dạng hình ảnh)
                    try:
                        # Chỉ kiểm tra xem option có picture.webpimg-container không để biết đây có phải danh mục có ảnh
                        if option.find_elements(By.CSS_SELECTOR, "picture.webpimg-container"):
                            has_images = True
                            
                            # Sử dụng hàm extract_thumbnail_url_from_driver đã có để lấy URL hình ảnh
                            img_src = self.extract_thumbnail_url_from_driver(driver)
                            
                            if img_src:
                                # Nếu tìm được URL ảnh từ hàm đã có
                                option_info = {
                                    "text": option_text,
                                    "image_url": img_src
                                }
                                option_list.append(option_info)
                            else:
                                # Nếu không tìm được ảnh từ hàm đã có, thử phương pháp hiện tại
                                try:
                                    thumbnail = driver.find_element(By.CSS_SELECTOR, ".image-frame .sc-7bce5df0-0.fvWcVx")
                                    # Kiểm tra thuộc tính srcset trước
                                    if thumbnail.get_attribute("srcset"):
                                        img_srcset = thumbnail.get_attribute("srcset")
                                        srcset_urls = [url.strip().split(' ')[0] for url in img_srcset.split(',')]
                                        img_src = srcset_urls[-1] if srcset_urls else thumbnail.get_attribute("src")
                                    else:
                                        img_src = thumbnail.get_attribute("src")
                                    
                                    option_info = {
                                        "text": option_text,
                                        "image_url": img_src
                                    }
                                    option_list.append(option_info)
                                except Exception as e:
                                    # print(f"Lỗi khi lấy ảnh lớn: {e}")
                                    # Nếu không tìm được ảnh lớn, thử lấy ảnh nhỏ từ option
                                    try:
                                        img_element = option.find_element(By.CSS_SELECTOR, "picture.webpimg-container img")
                                        img_src = img_element.get_attribute("src")
                                        option_info = {
                                            "text": option_text,
                                            "image_url": img_src
                                        }
                                        option_list.append(option_info)
                                    except Exception as e2:
                                        # print(f"Lỗi khi lấy ảnh nhỏ từ option: {e2}")
                                        option_list.append(option_text)
                        else:
                            # Nếu không có ảnh, chỉ thêm text
                            option_list.append(option_text)
                    except Exception as e:
                        # print(f"Lỗi khi trích xuất ảnh: {e}")
                        option_list.append(option_text)
                
                # Thêm danh sách text của các tùy chọn vào variant_categories
                result["variant_categories"][category_name] = option_text_list
                    
                if has_images:
                    result["variants_with_images"][category_name] = option_list
                
            return result
        
        except Exception as e:
            # print(f"Lỗi khi trích xuất danh mục tùy chọn: {e}")
            return result
    
    
    def extract_product_details(self, driver):
        result_text = "Thông tin chi tiết:\n"
        
        try:
                
            # Số lần thử cuộn
            max_attempts = 10
            all_processed_containers = set()  # Lưu lại các container đã xử lý
            
            for attempt in range(max_attempts):
                # Cuộn trang để tìm thông tin chi tiết
                distance = random.randint(400, 600)
                driver.execute_script(f"window.scrollBy(0, {distance});")
                # print(f"Scroll down lần {attempt+1}")
                time.sleep(1.5)
                
                # Tìm tất cả các container có thể chứa thông tin chi tiết
                containers = driver.find_elements(By.CSS_SELECTOR, "div.sc-34e0efdc-0")
                # print(f"Tìm thấy {len(containers)} containers với class sc-34e0efdc-0")
                
                found_data_in_any_container = False
                
                for container_index, container in enumerate(containers):
                    # Tạo id duy nhất cho container để tránh xử lý lại
                    try:
                        container_id = container.id
                    except:
                        container_id = f"container_{container_index}_{attempt}"
                    
                    # Bỏ qua nếu đã xử lý container này rồi
                    if container_id in all_processed_containers:
                        # print(f"Bỏ qua container #{container_index+1} vì đã xử lý trước đó")
                        continue
                    
                    # print(f"Kiểm tra container #{container_index+1}")
                    all_processed_containers.add(container_id)
                    
                    # Kiểm tra tiêu đề
                    title_found = False
                    title_text = ""
                    
                    try:
                        # Phương pháp 1: Tìm theo XPath
                        title_elements = container.find_elements(By.XPATH, "./div[contains(text(), 'Thông tin chi tiết')]")
                        if len(title_elements) > 0:
                            title_found = True
                            title_text = title_elements[0].text
                            # print(f"Tìm thấy tiêu đề '{title_text}' bằng XPath")
                    except Exception as e:
                        print(f"Lỗi khi tìm tiêu đề bằng XPath: {e}")
                    
                    try:
                        # Phương pháp 2: Tìm theo class
                        if not title_found:
                            title_elements = container.find_elements(By.CSS_SELECTOR, "div.sc-34e0efdc-1")
                            if len(title_elements) > 0:
                                for title_el in title_elements:
                                    title_el_text = title_el.text.strip()
                                    if "Thông tin chi tiết" in title_el_text:
                                        title_found = True
                                        title_text = title_el_text
                                        # print(f"Tìm thấy tiêu đề '{title_text}' bằng class")
                                        break
                    except Exception as e:
                        print(f"Lỗi khi tìm tiêu đề bằng class: {e}")
                    
                    # Tiếp tục nếu tìm thấy tiêu đề chứa "Thông tin chi tiết"
                    if title_found and "Thông tin chi tiết" in title_text:
                        # print(f"Đã xác nhận container #{container_index+1} chứa tiêu đề 'Thông tin chi tiết'")
                        
                        # PHƯƠNG PHÁP 1: Tìm trong grid-template-columns
                        try:
                            # Lấy ra div chứa nội dung (thường là div con thứ 2)
                            content_div = None
                            content_divs = container.find_elements(By.XPATH, "./div")
                            if len(content_divs) > 1:
                                content_div = content_divs[1]
                            
                            if content_div:
                                # Tìm tất cả grid div chứa thông tin
                                grid_divs = content_div.find_elements(By.XPATH, ".//div[contains(@style, 'grid-template-columns')]")
                                # print(f"Tìm thấy {len(grid_divs)} grid div trong content")
                                
                                for grid_div in grid_divs:
                                    spans = grid_div.find_elements(By.TAG_NAME, "span")
                                    if len(spans) >= 2:
                                        label = spans[0].text.strip()
                                        value = spans[1].text.strip()
                                        
                                        if label and value:
                                            # print(f"Tìm thấy cặp từ grid: '{label}': '{value}'")
                                            result_text += f"{label}: {value}\n"
                                            found_data_in_any_container = True
                                
                                if found_data_in_any_container:
                                    # print("Đã trích xuất dữ liệu từ grid div!")
                                    continue
                        except Exception as e:
                            print(f"Lỗi khi tìm trong grid: {e}")
                        
                        # PHƯƠNG PHÁP 2: Tìm trong div.kAFhAU
                        try:
                            detail_rows = []
                            if content_div:
                                detail_rows = content_div.find_elements(By.CSS_SELECTOR, "div.kAFhAU")
                            else:
                                detail_rows = container.find_elements(By.CSS_SELECTOR, "div.kAFhAU")
                                
                            # print(f"Tìm thấy {len(detail_rows)} dòng chi tiết bằng class kAFhAU")
                            
                            for row in detail_rows:
                                # Bỏ qua nếu là benefit-item
                                if "benefit-item" in row.get_attribute("class"):
                                    continue
                                    
                                try:
                                    grid_div = row.find_element(By.CSS_SELECTOR, "div.jcYGog, div.sc-34e0efdc-3")
                                    spans = grid_div.find_elements(By.TAG_NAME, "span")
                                    
                                    if len(spans) >= 2:
                                        label = spans[0].text.strip()
                                        value = spans[1].text.strip()
                                        
                                        if label and value:
                                            # print(f"Tìm thấy cặp từ kAFhAU: '{label}': '{value}'")
                                            result_text += f"{label}: {value}\n"
                                            found_data_in_any_container = True
                                except Exception as e:
                                    print(f"Lỗi khi tìm span trong kAFhAU: {e}")
                                    
                            if found_data_in_any_container:
                                # print("Đã trích xuất dữ liệu từ kAFhAU!")
                                continue
                        except Exception as e:
                            print(f"Lỗi khi tìm trong kAFhAU: {e}")
                        
                        # PHƯƠNG PHÁP 3: Tìm span màu xám trực tiếp
                        try:
                            gray_spans = container.find_elements(By.XPATH, ".//span[contains(@style, 'color: rgb(128, 128, 137)')]")
                            # print(f"Tìm thấy {len(gray_spans)} span màu xám trong container")
                            
                            for gray_span in gray_spans:
                                label = gray_span.text.strip()
                                
                                # Tìm span giá trị liên quan
                                try:
                                    # Tìm span cha
                                    parent_div = gray_span.find_element(By.XPATH, "./parent::div")
                                    if parent_div:
                                        # Tìm span giá trị trong cùng div cha
                                        value_spans = parent_div.find_elements(By.CSS_SELECTOR, "span.kdahle, span.sc-2a4bd363-0")
                                        if value_spans:
                                            value = value_spans[0].text.strip()
                                            if label and value:
                                                # print(f"Tìm thấy cặp từ span màu xám: '{label}': '{value}'")
                                                result_text += f"{label}: {value}\n"
                                                found_data_in_any_container = True
                                except Exception as e:
                                    print(f"Lỗi khi tìm giá trị cho span màu xám: {e}")
                                    
                            if found_data_in_any_container:
                                # print("Đã trích xuất dữ liệu từ span màu xám!")
                                continue
                        except Exception as e:
                            print(f"Lỗi khi tìm span màu xám: {e}")
                
                # Nếu đã tìm thấy dữ liệu trong bất kỳ container nào, ngừng cuộn và trả về kết quả
                if found_data_in_any_container:
                    break
            
            # Kiểm tra và trả về kết quả
            if result_text != "Thông tin chi tiết:\n":
                # print("Đã tìm thấy thông tin chi tiết!")
                return result_text
            
            # PHƯƠNG PHÁP CUỐI CÙNG: Thử sử dụng JavaScript trực tiếp tìm tất cả span màu xám
            # print("Thử phương pháp JavaScript cuối cùng...")
            
            try:
                js_result = driver.execute_script("""
                    const detailsArray = [];
                    
                    // Tìm tất cả span màu xám và span giá trị kế tiếp
                    const graySpans = document.querySelectorAll('span[style*="color: rgb(128, 128, 137)"]');
                    
                    for (const graySpan of graySpans) {
                        const label = graySpan.textContent.trim();
                        if (!label) continue;
                        
                        // Tìm parent div chứa grid
                        let parentGrid = graySpan.closest('div[style*="grid-template-columns"]');
                        if (parentGrid) {
                            const spans = parentGrid.querySelectorAll('span');
                            // Nếu có ít nhất 2 span và span đầu tiên là gray span hiện tại
                            if (spans.length >= 2 && spans[0] === graySpan) {
                                const value = spans[1].textContent.trim();
                                if (value) {
                                    detailsArray.push(`${label}: ${value}`);
                                }
                            }
                        }
                    }
                    
                    return detailsArray.join('\\n');
                """)
                
                if js_result:
                    # print("Tìm thấy dữ liệu bằng JavaScript cuối cùng!")
                    result_text += js_result
                    return result_text
            except Exception as e:
                print(f"Lỗi khi thực hiện JavaScript cuối cùng: {e}")
            
            return "Không tìm thấy thông tin chi tiết sản phẩm."
            
        except Exception as e:
            # print(f"Lỗi tổng thể khi trích xuất thông tin chi tiết: {e}")
            return "Không tìm thấy thông tin chi tiết sản phẩm."

    
    def extract_product_description(self, driver):
        result_text = "Mô tả sản phẩm:\n"
        
        try:
            # Số lần thử cuộn
            max_attempts = 10
            
            # Tìm container chính chứa tiêu đề "Mô tả sản phẩm"
            description_section = None
            
            for attempt in range(max_attempts):
                try:
                    # Tìm kiếm tiêu đề "Mô tả sản phẩm"
                    description_title = driver.find_element(By.XPATH, "//div[normalize-space(text())='Mô tả sản phẩm']")
                    
                    # Tìm container cha
                    description_section = description_title.find_element(By.XPATH, "./..")
                    
                    if description_section:
                        # print(f"Đã tìm thấy phần Mô tả sản phẩm sau {attempt+1} lần thử")
                        # Cuộn đến phần tử để đảm bảo nó được tải đầy đủ
                        driver.execute_script("arguments[0].scrollIntoView({block: 'start', behavior: 'smooth'});", description_section)
                        time.sleep(2)
                        
                        # Click vào nút "Xem thêm" nếu có
                        try:
                            view_more_button = description_section.find_element(By.CSS_SELECTOR, "a.btn-more")
                            driver.execute_script("arguments[0].click();", view_more_button)
                            # print("Đã click nút Xem thêm")
                            time.sleep(2)  # Chờ nội dung hiển thị đầy đủ
                        except:
                            print("Không tìm thấy nút Xem thêm hoặc không cần click")
                        
                        break
                        
                except NoSuchElementException:
                    # Không tìm thấy, tiếp tục cuộn trang
                    distance = random.randint(400, 600)
                    driver.execute_script(f"window.scrollBy(0, {distance});")
                    # print(f"Scroll down lần {attempt+1}")
                    time.sleep(1.5)
            
            if not description_section:
                # print("Không tìm thấy phần Mô tả sản phẩm")
                return "Không tìm thấy mô tả sản phẩm."
            
            # Tìm container nội dung mô tả
            try:
                content_div = description_section.find_element(By.CSS_SELECTOR, "div.content")
            except:
                try:
                    # Tìm div con thứ hai nếu không tìm thấy class content
                    content_div = description_section.find_element(By.XPATH, "./div[2]") 
                except:
                    content_div = description_section  # Sử dụng chính description_section nếu không tìm thấy
            
            # Tìm tất cả các thẻ văn bản (h1, h2, h3, p, li, span) trong content_div
            text_elements = content_div.find_elements(By.XPATH, ".//h1 | .//h2 | .//h3 | .//h4 | .//h5 | .//p | .//li | .//span")
            
            # Nếu không tìm thấy các thẻ văn bản, thử tìm trong div sâu hơn 
            if len(text_elements) == 0:
                inner_div = content_div.find_element(By.CSS_SELECTOR, "div.sc-f5219d7f-0")
                text_elements = inner_div.find_elements(By.XPATH, ".//h1 | .//h2 | .//h3 | .//h4 | .//h5 | .//p | .//li | .//span")
            
            # print(f"Tìm thấy {len(text_elements)} phần tử văn bản")
            
            # Nối tất cả phần tử văn bản lại với nhau
            for element in text_elements:
                text = element.text.strip()
                if text and text != "Xem thêm" and not text.startswith("Giá sản phẩm trên Tiki"):
                    tag_name = element.tag_name
                    
                    # Định dạng dựa trên loại thẻ
                    if tag_name in ['h1', 'h2', 'h3', 'h4', 'h5']:
                        result_text += f"\n{text}\n"
                    elif tag_name == 'li':
                        result_text += f"• {text}\n"
                    elif tag_name == 'p':
                        result_text += f"{text}\n\n"
                    else:
                        result_text += f"{text}\n"
            
            # Nếu không tìm thấy phần tử văn bản nào hoặc kết quả trống
            if result_text == "Mô tả sản phẩm:\n" or len(text_elements) == 0:
                # Lấy toàn bộ văn bản từ content_div
                full_text = content_div.text
                # Loại bỏ dòng "Giá sản phẩm trên Tiki..." và "Xem thêm"
                lines = full_text.split('\n')
                filtered_lines = [line for line in lines if not line.startswith("Giá sản phẩm trên Tiki") and line != "Xem thêm"]
                result_text += '\n'.join(filtered_lines)
            
            return result_text
            
        except Exception as e:
            # print(f"Lỗi khi trích xuất mô tả sản phẩm: {e}")
            return f"Lỗi khi trích xuất mô tả sản phẩm: {e}"

    
    def is_product_out_of_stock(self, driver):
        try:
            # Tìm thông báo "Sản phẩm đã hết hàng" dựa vào text
            out_of_stock_element = driver.find_element(
                By.XPATH, 
                "//*[contains(text(), 'Sản phẩm đã hết hàng')]"
            )
            
            # Nếu tìm thấy phần tử, sản phẩm đã hết hàng
            if out_of_stock_element:
                # print("Sản phẩm đã hết hàng!")
                return True
                
        except Exception:
            # Không tìm thấy thông báo hết hàng
            pass
            
        return False

    def crawl_product_infomation(self, product_url):
        max_retries = 3
        for attempt in range(max_retries):
            proxy = self.proxy_manager.get_new_proxy()
            driver = self.setup_driver(proxy)
            try:
                # Mở trang sản phẩm
                # print("Đang truy cập trang sản phẩm...")
                driver.get(product_url)
                WebDriverWait(driver, 61).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # print("Truy cập thành công, thu thập dữ liệu")
                
                # Kiểm tra xem sản phẩm có hết hàng không
                if self.is_product_out_of_stock(driver):
                    print("Bỏ qua sản phẩm này vì đã hết hàng")
                    break
                
                # print("Trích xuất id sản phẩm")
                product_id = self.extract_product_id_from_tiki_url(product_url)
                # print("Tạo vùng lưu trữ ảnh sản phẩm")
                # create_tiki_images_folder(product_id, "C:/Users/DELL/Desktop/e-commerce-crawler/data/tiki/images")

                # Tiếp tục thu thập dữ liệu nếu sản phẩm còn hàng
                # print(extract_product_id_from_tiki_url(product_url))
                product_json = {
                    'platform': 'tiki',
                    'url': product_url,
                    'product_id': product_id,
                    'default_thumbnail': self.extract_thumbnail_url_from_driver(driver),
                    'product_name': self.extract_product_name(driver),
                    'sale_quantity': self.extract_sales_quantity(driver),
                    'rating': self.extract_rating(driver),
                    'current_price': self.extract_current_price(driver),
                    'discount': self.extract_discount(driver),
                    'original_price': self.extract_original_price(driver),
                    'seller': self.extract_seller_info(driver),
                    'variants': self.extract_product_variants(driver),
                    'product_details': self.extract_product_details(driver),
                    'product_description': self.extract_product_description(driver)
                }
                
                return product_json

            except TimeoutException:
                # print("Không tải được trang sản phẩm")
                # print("Thử lại...")
                continue
            finally:
                # Đảm bảo đóng driver
                if driver:
                    try:
                        driver.quit()
                        # print("Đã đóng trình duyệt")
                    except Exception as e:
                        print(f"Lỗi khi đóng trình duyệt: {str(e)}")

    def run(self):
        """
        Chạy consumer để lấy và xử lý một message một lần
        """
        # Khởi tạo kết nối RabbitMQ
        rabbitmq = RabbitMQConnector(
            host='localhost',
            port=5672,
            username='admin',
            password='admin',
            queue_name='tiki_product_queue'
        )
        
        # Kết nối đến RabbitMQ
        if not rabbitmq.connect():
            # print("Không thể kết nối tới RabbitMQ")
            return
        
        try:
            # print("Consumer đã khởi động. Đang đợi messages...")
            
            # Vòng lặp vô hạn để tiếp tục lấy và xử lý messages
            while True:
                # Kiểm tra số lượng messages trong queue
                message_count = rabbitmq.count_messages()
                
                if message_count > 0:
                    # print(f"Có {message_count} messages trong queue")
                    
                    # Lấy và xử lý một message
                    message = rabbitmq.get_one_message(auto_ack=True)
                    
                    if message and 'message' in message and 'url' in message['message']:
                        url = message['message']['url']
                        # print(url)
                        # print("Bắt đầu thu thập dữ liệu sản phẩm")
                        product_infomation = self.crawl_product_infomation(url)
                        product_infomation = json.dumps(product_infomation, indent=4, ensure_ascii=False)
                        print(product_infomation)
                    else:
                        # print("Không thể xử lý message, sẽ thử lại sau")
                        time.sleep(5)

                    break # Nhớ xoá đi để đảm bảo có thể lấy dữ liệu liên tục
                else:
                    # print("Không có message nào trong queue. Đợi dữ liệu...")
                    time.sleep(30)
                
        except KeyboardInterrupt:
            print("\nNgười dùng dừng chương trình")
        finally:
            rabbitmq.close()
