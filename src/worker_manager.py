import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor
import random
import os
import json
import logging
from datetime import datetime
import pandas as pd

from .shopee_crawler import ShopeeCrawler

logger = logging.getLogger("WorkerManager")

class ShopeeWorkerManager:
    def __init__(self, proxy_api_keys=None, headless=True, num_workers=None):
        """
        Khởi tạo worker manager với nhiều API key cho proxy xoay
        
        Args:
            proxy_api_keys: Danh sách các API key cho proxy xoay, mỗi worker sẽ dùng một key
            headless: Chế độ headless cho Chrome
            num_workers: Số lượng worker (mặc định bằng số lượng API key)
        """
        self.proxy_api_keys = proxy_api_keys or []
        self.headless = headless
        
        # Số worker tối đa bằng số API key có sẵn
        self.num_workers = num_workers or len(self.proxy_api_keys)
        
        # Giới hạn số worker không vượt quá số API key
        if self.num_workers > len(self.proxy_api_keys):
            logger.warning(f"Số lượng worker ({self.num_workers}) vượt quá số lượng API key ({len(self.proxy_api_keys)}). Giảm xuống {len(self.proxy_api_keys)}")
            self.num_workers = len(self.proxy_api_keys)
        
        self.product_queue = queue.Queue()
        self.results = []
        self.results_lock = threading.Lock()
        
    def worker_task(self, worker_id):
        """Công việc của mỗi worker với API key proxy xoay riêng"""
        # Lấy API key tương ứng cho worker này
        api_key = self.proxy_api_keys[worker_id - 1]  # worker_id bắt đầu từ 1
        
        logger.info(f"Worker {worker_id} sử dụng API key proxy: {api_key[:5]}...{api_key[-3:]}")
        
        # Khởi tạo crawler riêng với API key riêng
        crawler = ShopeeCrawler(
            proxy_api_key=api_key,
            headless=self.headless,
            wait_time=10,
            max_retries=3,
            use_proxy=True,
            proxy_rotation_interval=(60, 90)  # Có thể điều chỉnh chu kỳ đổi proxy theo nhu cầu
        )
        
        logger.info(f"Worker {worker_id} đã được khởi tạo với proxy xoay riêng")
        
        # Xử lý các URL trong queue
        while not self.product_queue.empty():
            try:
                url = self.product_queue.get(block=False)
                logger.info(f"Worker {worker_id} đang xử lý URL: {url}")
                
                # Crawl chi tiết sản phẩm
                product_detail = crawler.crawl_product_detail(url)
                
                # Thêm thông tin worker và API key để theo dõi
                if product_detail:
                    product_detail["worker_id"] = worker_id
                    product_detail["api_key_used"] = f"{api_key[:5]}...{api_key[-3:]}"  # Chỉ lưu một phần API key cho an toàn
                    
                    # Lưu kết quả với lock để tránh race condition
                    with self.results_lock:
                        self.results.append(product_detail)
                
                # Báo hiệu task hoàn thành
                self.product_queue.task_done()
                
                # Đợi một chút trước khi xử lý URL tiếp theo để tránh quá tải
                time.sleep(random.uniform(2, 4))
                
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} gặp lỗi: {str(e)}")
                self.product_queue.task_done()
        
        # Đảm bảo dừng luồng đổi proxy khi worker hoàn thành
        if crawler.proxy_manager:
            crawler.proxy_manager.stop_rotation_thread()
            
        logger.info(f"Worker {worker_id} đã hoàn thành công việc")
    
    def run(self, cycles=1, max_products_per_cycle=None, collect_url_api_key=None):
        """
        Chạy crawl với nhiều worker song song, mỗi worker có API key proxy riêng
        
        Args:
            cycles: Số chu kỳ crawl
            max_products_per_cycle: Số lượng sản phẩm tối đa mỗi chu kỳ
            collect_url_api_key: API key riêng để thu thập URL (nếu None sẽ dùng API key đầu tiên)
        """
        if not self.proxy_api_keys:
            logger.error("Không có API key nào cho proxy xoay. Không thể tiếp tục.")
            return []
        
        # Tạo thư mục cache nếu chưa tồn tại
        cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cache')
        os.makedirs(cache_dir, exist_ok=True)
                
        all_products = []
        max_retries_per_cycle = 3  # Số lần thử lại tối đa cho mỗi chu kỳ
        
        for cycle in range(1, cycles + 1):
            logger.info(f"\n===== BẮT ĐẦU CHU KỲ {cycle}/{cycles} =====")
            
            # Sử dụng API key được chỉ định hoặc API key đầu tiên để thu thập URL
            url_api_key = collect_url_api_key or self.proxy_api_keys[0]
            
            # Khởi tạo crawler để thu thập URL từ trang chủ
            for retry in range(max_retries_per_cycle):
                try:
                    main_crawler = ShopeeCrawler(
                        proxy_api_key=url_api_key,
                        headless=self.headless,
                        use_proxy=True
                    )
                    
                    logger.info(f"Đang thu thập URL sản phẩm với API key: {url_api_key[:5]}...{url_api_key[-3:]}")
                    
                    # Thu thập URL từ trang chủ
                    main_crawler.collect_product_links()
                    
                    # Đảm bảo dừng luồng đổi proxy sau khi thu thập URL
                    if main_crawler.proxy_manager:
                        main_crawler.proxy_manager.stop_rotation_thread()
                    
                    if not main_crawler.product_links:
                        logger.warning(f"Chu kỳ {cycle} (Lần thử {retry+1}): Không thu thập được đường link nào, thử lại.")
                        time.sleep(5)  # Chờ một chút trước khi thử lại
                        continue
                    
                    # Nếu thành công, thoát khỏi vòng lặp retry
                    break
                    
                except Exception as e:
                    logger.error(f"Lỗi khi thu thập URL (Chu kỳ {cycle}, Lần thử {retry+1}): {str(e)}")
                    if retry < max_retries_per_cycle - 1:
                        logger.info(f"Đang thử lại lần {retry+2}...")
                        time.sleep(10)  # Chờ lâu hơn trước khi thử lại
                    else:
                        logger.error(f"Đã vượt quá số lần thử lại cho chu kỳ {cycle}. Chuyển sang chu kỳ tiếp theo.")
                        main_crawler = None
            
            # Kiểm tra nếu không thu thập được link sau tất cả các lần thử
            if not main_crawler or not main_crawler.product_links:
                logger.warning(f"Chu kỳ {cycle}: Không thu thập được đường link nào sau nhiều lần thử, chuyển sang chu kỳ tiếp theo.")
                continue
            
            # Giới hạn số lượng sản phẩm nếu cần
            links_to_crawl = main_crawler.product_links[:max_products_per_cycle] if max_products_per_cycle else main_crawler.product_links
            
            # Đưa URL vào queue
            for link in links_to_crawl:
                self.product_queue.put(link)
            
            # Reset kết quả cho chu kỳ mới
            self.results = []
            
            logger.info(f"Chu kỳ {cycle}: Đã đưa {len(links_to_crawl)} URL vào queue để xử lý")
            
            # Tạo và bắt đầu các worker
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                worker_futures = []
                for i in range(self.num_workers):
                    worker_futures.append(executor.submit(self.worker_task, i+1))
                
                # Theo dõi và đợi các worker hoàn thành
                for future in worker_futures:
                    try:
                        future.result()  # Đợi worker hoàn thành và kiểm tra ngoại lệ
                    except Exception as e:
                        logger.error(f"Một worker đã gặp lỗi không xử lý được: {str(e)}")
            
            # Đợi tất cả task hoàn thành
            self.product_queue.join()
            
            # Thêm kết quả vào danh sách tổng
            all_products.extend(self.results)
            
            # Lưu kết quả của chu kỳ hiện tại
            if self.results:
                cycle_summary_file = os.path.join("data", f"cycle_{cycle}_products_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")
                with open(cycle_summary_file, 'w', encoding='utf-8') as f:
                    json.dump(self.results, f, ensure_ascii=False, indent=4)
                
                logger.info(f"Chu kỳ {cycle}: Đã lưu thông tin của {len(self.results)} sản phẩm vào {cycle_summary_file}")
            
            # Cho phép một số thời gian nghỉ giữa các chu kỳ
            if cycle < cycles:
                logger.info(f"Đợi 30 giây trước khi bắt đầu chu kỳ tiếp theo...")
                time.sleep(30)
            
            logger.info(f"===== KẾT THÚC CHU KỲ {cycle}/{cycles} =====")
        
        # Lưu tổng kết
        if all_products:
            all_products_file = os.path.join("data", f"all_products_multi_proxy_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")
            with open(all_products_file, 'w', encoding='utf-8') as f:
                json.dump(all_products, f, ensure_ascii=False, indent=4)
            
            # Tạo CSV tổng hợp
            csv_summary_file = os.path.join("data", f"all_products_multi_proxy_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
            csv_data = []
            for product in all_products:
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
                    "crawl_time": product.get("crawl_time", ""),
                    "worker_id": product.get("worker_id", ""),
                    "api_key_used": product.get("api_key_used", "")
                }
                csv_data.append(csv_record)
            
            df = pd.DataFrame(csv_data)
            df.to_csv(csv_summary_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"\n===== TỔNG KẾT =====")
            logger.info(f"Đã thu thập thông tin của {len(all_products)} sản phẩm qua {cycles} chu kỳ")
            logger.info(f"Tất cả dữ liệu đã được lưu vào {all_products_file} và {csv_summary_file}")
        
        return all_products