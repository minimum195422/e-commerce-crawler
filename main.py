import json
import os
import sys
import argparse
from src import ShopeeCrawler, ShopeeWorkerManager, setup_logging

def load_config(config_path="config/config.json"):
    """Đọc file cấu hình"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"Lỗi khi đọc file cấu hình: {e}")
        return None

def parse_arguments():
    """Phân tích tham số dòng lệnh"""
    parser = argparse.ArgumentParser(description='Shopee Crawler với hỗ trợ proxy xoay')
    parser.add_argument('--config', type=str, default='config/config.json',
                        help='Đường dẫn đến file cấu hình (mặc định: config/config.json)')
    parser.add_argument('--mode', type=str, choices=['single', 'multi'], default='single',
                        help='Chế độ chạy: single (một crawler) hoặc multi (nhiều worker) (mặc định: single)')
    parser.add_argument('--cycles', type=int, default=None,
                        help='Số chu kỳ crawl (ghi đè lên cấu hình)')
    parser.add_argument('--max-products', type=int, default=None,
                        help='Số lượng sản phẩm tối đa cho mỗi chu kỳ (ghi đè lên cấu hình)')
    parser.add_argument('--headless', action='store_true',
                        help='Chạy trình duyệt ở chế độ headless (không giao diện)')
    parser.add_argument('--no-proxy', action='store_true',
                        help='Không sử dụng proxy xoay')
    
    return parser.parse_args()

def main():
    """Hàm chính"""
    # Khởi tạo logging
    logger = setup_logging()
    
    # Phân tích tham số dòng lệnh
    args = parse_arguments()
    
    # Đọc cấu hình
    config = load_config(args.config)
    if not config:
        logger.error("Không thể đọc file cấu hình. Kết thúc.")
        sys.exit(1)
    
    # Áp dụng các tham số ghi đè từ dòng lệnh
    if args.cycles is not None:
        config['worker']['cycles'] = args.cycles
    
    if args.max_products is not None:
        config['worker']['max_products_per_cycle'] = args.max_products
    
    if args.headless:
        config['crawler']['headless'] = True
    
    if args.no_proxy:
        config['proxy']['use_proxy'] = False
    
    # Tạo thư mục data nếu chưa tồn tại
    os.makedirs("data", exist_ok=True)
    
    if args.mode == 'single':
        # Chế độ single crawler
        logger.info("Chạy ở chế độ single crawler")
        
        # Sử dụng API key đầu tiên nếu có sử dụng proxy
        proxy_api_key = config['proxy']['api_keys'][0] if config['proxy']['use_proxy'] and config['proxy']['api_keys'] else None
        
        crawler = ShopeeCrawler(
            proxy_api_key=proxy_api_key,
            headless=config['crawler']['headless'],
            wait_time=config['crawler']['wait_time'],
            max_retries=config['crawler']['max_retries'],
            proxy_rotation_interval=tuple(config['proxy']['rotation_interval']),
            use_proxy=config['proxy']['use_proxy']
        )
        
        # Chạy crawler
        crawler.run(
            crawl_cycles=config['worker']['cycles'],
            max_products_per_cycle=config['worker']['max_products_per_cycle']
        )
    else:
        # Chế độ multi worker
        logger.info("Chạy ở chế độ multi worker")
        
        # Kiểm tra có API key cho proxy xoay không
        if config['proxy']['use_proxy'] and not config['proxy']['api_keys']:
            logger.error("Cần ít nhất một API key cho proxy xoay trong chế độ multi worker")
            sys.exit(1)
        
        # Khởi tạo worker manager
        manager = ShopeeWorkerManager(
            proxy_api_keys=config['proxy']['api_keys'] if config['proxy']['use_proxy'] else None,
            headless=config['crawler']['headless'],
            num_workers=config['worker']['num_workers']
        )
        
        # Chạy các worker
        manager.run(
            cycles=config['worker']['cycles'],
            max_products_per_cycle=config['worker']['max_products_per_cycle']
        )
    
    logger.info("Hoàn thành quá trình crawl dữ liệu")

if __name__ == "__main__":
    main()