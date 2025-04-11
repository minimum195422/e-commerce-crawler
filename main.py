#!/usr/bin/env python3
import os
import sys
import argparse
from lazada import LazadaCrawler, LazadaWorkerManager, setup_logging

def main():
    # Thiết lập parser cho tham số dòng lệnh
    parser = argparse.ArgumentParser(description='Lazada Crawler')
    parser.add_argument('--proxy-keys', nargs='+', help='API key của dịch vụ proxy xoay', required=False)
    parser.add_argument('--headless', action='store_true', help='Chạy trình duyệt ở chế độ headless')
    parser.add_argument('--cycles', type=int, default=1, help='Số chu kỳ crawl')
    parser.add_argument('--max-products', type=int, help='Số sản phẩm tối đa mỗi chu kỳ')
    parser.add_argument('--single-url', help='URL sản phẩm cụ thể cần crawl (chạy chỉ crawler đơn)')
    parser.add_argument('--workers', type=int, help='Số lượng worker (mặc định bằng số lượng API key)')
    
    args = parser.parse_args()
    
    # Thiết lập logging
    logger = setup_logging()
    
    # Tạo thư mục cần thiết
    base_dir = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(os.path.join(base_dir, 'cache'), exist_ok=True)
    os.makedirs(os.path.join(base_dir, 'data'), exist_ok=True)
    os.makedirs(os.path.join(base_dir, 'images'), exist_ok=True)
    os.makedirs(os.path.join(base_dir, 'logs'), exist_ok=True)
    
    if args.single_url:
        # Chạy chỉ crawler đơn để lấy một sản phẩm cụ thể
        logger.info(f"Chạy crawler đơn cho URL: {args.single_url}")
        
        use_proxy = args.proxy_keys is not None and len(args.proxy_keys) > 0
        proxy_key = args.proxy_keys[0] if use_proxy else None
        
        crawler = LazadaCrawler(
            proxy_api_key=proxy_key,
            headless=args.headless,
            use_proxy=use_proxy
        )
        
        product_detail = crawler.crawl_product_detail(args.single_url)
        
        if product_detail:
            # Lưu kết quả
            import json
            output_file = os.path.join(base_dir, 'data', f"product_{product_detail['product_id']}.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(product_detail, f, ensure_ascii=False, indent=4)
                
            logger.info(f"Đã lưu thông tin sản phẩm vào {output_file}")
        else:
            logger.error("Không thể lấy thông tin sản phẩm")
    else:
        # Chạy crawler đa luồng với worker manager
        if not args.proxy_keys:
            logger.error("Cần ít nhất một API key cho dịch vụ proxy xoay.")
            sys.exit(1)
            
        logger.info(f"Chạy crawler đa luồng với {len(args.proxy_keys)} API key proxy")
        
        worker_manager = LazadaWorkerManager(
            proxy_api_keys=args.proxy_keys,
            headless=args.headless,
            num_workers=args.workers
        )
        
        worker_manager.run(
            cycles=args.cycles,
            max_products_per_cycle=args.max_products
        )
        
        logger.info("Crawl hoàn tất!")

if __name__ == "__main__":
    main()