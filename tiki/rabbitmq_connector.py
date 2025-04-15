# rabbitmq_connector.py
import pika
import json
import time
import threading

class RabbitMQConnector:
    """
    Class quản lý kết nối và tương tác với RabbitMQ
    """
    def __init__(self, host='localhost', port=5672, username='admin', password='admin', queue_name='tiki_product_queue'):
        """
        Khởi tạo kết nối RabbitMQ
        
        Tham số:
            host: RabbitMQ host, mặc định là localhost
            port: RabbitMQ port, mặc định là 5672
            username: Tên người dùng RabbitMQ
            password: Mật khẩu RabbitMQ
            queue_name: Tên queue để gửi message
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.max_retries = 3
        self.consumer_thread = None
        self.is_consuming = False
    
    def connect(self):
        """
        Thiết lập kết nối tới RabbitMQ server với cơ chế retry
        
        Trả về:
            bool: True nếu kết nối thành công, False nếu không
        """
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                # Tạo thông tin xác thực
                credentials = pika.PlainCredentials(self.username, self.password)
                
                # Thiết lập các tham số kết nối
                parameters = pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=credentials,
                    heartbeat=600,  # Tăng thời gian heartbeat để tránh ngắt kết nối
                    blocked_connection_timeout=300  # Thời gian chờ khi kết nối bị block
                )
                
                # Thiết lập kết nối
                self.connection = pika.BlockingConnection(parameters)
                
                # Tạo kênh để giao tiếp
                self.channel = self.connection.channel()
                
                # Khai báo queue để đảm bảo queue tồn tại
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                
                print(f"Đã kết nối thành công đến RabbitMQ tại {self.host}:{self.port}")
                return True
                
            except Exception as e:
                retry_count += 1
                print(f"Lỗi kết nối RabbitMQ (lần {retry_count}/{self.max_retries}): {str(e)}")
                if retry_count < self.max_retries:
                    # Đợi một khoảng thời gian trước khi thử lại
                    time.sleep(2)
                else:
                    print("Đã vượt quá số lần thử kết nối RabbitMQ tối đa")
                    return False
    
    def send_message(self, message, routing_key=None):
        """
        Gửi message vào queue
        
        Tham số:
            message: Message cần gửi (dict hoặc list), sẽ được chuyển đổi thành JSON
            routing_key: Routing key để gửi message, mặc định là tên queue
            
        Trả về:
            bool: True nếu gửi thành công, False nếu không
        """
        if routing_key is None:
            routing_key = self.queue_name
            
        # Kiểm tra kết nối
        if not self.connection or self.connection.is_closed:
            print("Kết nối RabbitMQ đã đóng, thử kết nối lại...")
            if not self.connect():
                return False
        
        try:
            # Chuyển đổi message thành chuỗi JSON
            if isinstance(message, (dict, list)):
                message_body = json.dumps(message)
            else:
                message_body = str(message)
            
            # Gửi message lên RabbitMQ
            self.channel.basic_publish(
                exchange='',  # Sử dụng exchange mặc định
                routing_key=routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Đảm bảo tin nhắn không bị mất khi RabbitMQ khởi động lại
                    content_type='application/json'
                )
            )
            return True
            
        except Exception as e:
            print(f"Lỗi khi gửi message lên RabbitMQ: {str(e)}")
            return False
    
    def send_product_urls(self, urls):
        """
        Gửi danh sách URL sản phẩm lên RabbitMQ, mỗi URL là một message riêng biệt
        
        Tham số:
            urls: Danh sách các URL sản phẩm
            
        Trả về:
            int: Số lượng URL đã gửi thành công
        """
        if not urls:
            print("Không có URL nào để gửi")
            return 0
            
        # Đảm bảo kết nối được thiết lập
        if not self.connection or self.connection.is_closed:
            if not self.connect():
                return 0
        
        # Tạo thời gian stamp
        timestamp = int(time.time())
        sent_count = 0
        
        # Gửi từng URL như một message riêng biệt
        for index, url in enumerate(urls):
            message = {
                "timestamp": timestamp,
                "index": index + 1,
                "total": len(urls),
                "url": url,
                "source": "tiki_crawler"
            }
            
            if self.send_message(message):
                sent_count += 1
                if sent_count % 10 == 0 or sent_count == len(urls):
                    print(f"Đã gửi {sent_count}/{len(urls)} URL")
            else:
                print(f"Lỗi khi gửi URL #{index+1}")
        
        print(f"Tổng cộng đã gửi {sent_count}/{len(urls)} URL lên RabbitMQ")
        return sent_count
    
    # PHẦN THÊM MỚI - CÁC PHƯƠNG THỨC CONSUMER
    
    def setup_consumer(self, prefetch_count=1):
        """
        Thiết lập consumer để chỉ nhận 1 message mỗi lần
        
        Tham số:
            prefetch_count: Số lượng message được gửi tới consumer cùng lúc, 
                          mặc định là 1 (chỉ gửi 1 message đến khi message trước đó được xác nhận)
        
        Trả về:
            bool: True nếu thiết lập thành công, False nếu không
        """
        if not self.connection or self.connection.is_closed:
            if not self.connect():
                return False
        
        try:
            # Thiết lập prefetch_count để kiểm soát số lượng message được gửi tới consumer
            self.channel.basic_qos(prefetch_count=prefetch_count)
            print(f"Đã thiết lập consumer với prefetch_count={prefetch_count}")
            return True
        except Exception as e:
            print(f"Lỗi khi thiết lập consumer: {str(e)}")
            return False
    
    def count_messages(self):
        """
        Đếm số lượng message trong queue
        
        Trả về:
            int: Số lượng message trong queue, -1 nếu có lỗi
        """
        try:
            if not self.connection or self.connection.is_closed:
                if not self.connect():
                    return -1
            
            # Khai báo queue với passive=True để kiểm tra thông tin mà không tạo mới nếu không tồn tại
            queue_info = self.channel.queue_declare(queue=self.queue_name, passive=True)
            return queue_info.method.message_count
        except Exception as e:
            print(f"Lỗi khi đếm số lượng message: {str(e)}")
            return -1
    
    def get_one_message(self, auto_ack=False):
        """
        Lấy một message từ queue
        
        Tham số:
            auto_ack: Tự động xác nhận đã nhận message, mặc định là False
            
        Trả về:
            dict hoặc None: Dictionary chứa message và delivery_tag, hoặc None nếu không có message
        """
        try:
            if not self.connection or self.connection.is_closed:
                if not self.connect():
                    return None
            
            # Phương thức basic_get lấy một message từ queue
            method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_name, auto_ack=auto_ack)
            
            if method_frame:
                # Có message
                try:
                    message = json.loads(body)
                    return {
                        'message': message,
                        'delivery_tag': method_frame.delivery_tag
                    }
                except json.JSONDecodeError:
                    # Nếu không phải JSON, trả về dạng chuỗi
                    return {
                        'message': body.decode('utf-8'),
                        'delivery_tag': method_frame.delivery_tag
                    }
            else:
                # Không có message
                return None
        except Exception as e:
            print(f"Lỗi khi lấy message từ queue: {str(e)}")
            return None
    
    def consume_one_message(self, callback_function, auto_ack=False):
        """
        Lấy và xử lý một message duy nhất từ queue
        
        Tham số:
            callback_function: Hàm xử lý message, nhận vào dữ liệu message
            auto_ack: Tự động xác nhận đã nhận message, mặc định là False
            
        Trả về:
            bool: True nếu xử lý thành công, False nếu không
        """
        # Lấy một message từ queue
        message_data = self.get_one_message(auto_ack=auto_ack)
        
        if not message_data:
            print("Không có message nào trong queue")
            return False
        
        try:
            # Gọi callback function để xử lý message
            message = message_data['message']
            delivery_tag = message_data['delivery_tag']
            
            # Xử lý message bằng callback function
            result = callback_function(message)
            
            # Xác nhận message nếu callback thành công và không tự động xác nhận
            if not auto_ack:
                if result:
                    # Xác nhận đã xử lý xong message
                    self.ack_message(delivery_tag)
                else:
                    # Từ chối message và đưa vào lại queue
                    self.reject_message(delivery_tag, requeue=True)
            
            return True
        except Exception as e:
            print(f"Lỗi khi xử lý message: {str(e)}")
            
            # Nếu không tự động xác nhận, từ chối message và đưa vào lại queue
            if not auto_ack:
                self.reject_message(message_data['delivery_tag'], requeue=True)
            
            return False
    
    def ack_message(self, delivery_tag):
        """
        Xác nhận đã xử lý xong message
        
        Tham số:
            delivery_tag: Tag của message cần xác nhận
            
        Trả về:
            bool: True nếu xác nhận thành công, False nếu không
        """
        try:
            if not self.connection or self.connection.is_closed:
                print("Kết nối đã đóng, không thể xác nhận message")
                return False
            
            self.channel.basic_ack(delivery_tag=delivery_tag)
            return True
        except Exception as e:
            print(f"Lỗi khi xác nhận message: {str(e)}")
            return False
    
    def reject_message(self, delivery_tag, requeue=True):
        """
        Từ chối xử lý message
        
        Tham số:
            delivery_tag: Tag của message cần từ chối
            requeue: Đưa message vào lại queue nếu True, mặc định là True
            
        Trả về:
            bool: True nếu từ chối thành công, False nếu không
        """
        try:
            if not self.connection or self.connection.is_closed:
                print("Kết nối đã đóng, không thể từ chối message")
                return False
            
            self.channel.basic_reject(delivery_tag=delivery_tag, requeue=requeue)
            return True
        except Exception as e:
            print(f"Lỗi khi từ chối message: {str(e)}")
            return False
    
    def consume_messages(self, callback_function, auto_ack=False):
        """
        Bắt đầu tiêu thụ message từ queue liên tục (blocking)
        
        Tham số:
            callback_function: Hàm callback nhận vào body của message
            auto_ack: Tự động xác nhận đã nhận message, mặc định là False
            
        Trả về:
            None
        """
        if not self.connection or self.connection.is_closed:
            if not self.connect():
                return False
        
        def wrapped_callback(ch, method, properties, body):
            """Wrapper cho callback function để xử lý body"""
            try:
                # Parse body thành json nếu có thể
                try:
                    message = json.loads(body)
                except json.JSONDecodeError:
                    message = body.decode('utf-8')
                
                # Gọi callback function với message
                result = callback_function(message)
                
                # Nếu callback trả về False và auto_ack=False, từ chối message
                if result is False and not auto_ack:
                    self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
                # Nếu callback trả về True và auto_ack=False, xác nhận message
                elif result is True and not auto_ack:
                    self.channel.basic_ack(delivery_tag=method.delivery_tag)
                # Các trường hợp khác được xử lý theo auto_ack
            except Exception as e:
                print(f"Lỗi khi xử lý message: {str(e)}")
                if not auto_ack:
                    # Nếu có lỗi và không tự động xác nhận, từ chối message và đưa vào lại queue
                    self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
        
        try:
            # Đăng ký consumer
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=wrapped_callback,
                auto_ack=auto_ack
            )
            
            print(f"Bắt đầu tiêu thụ message từ queue '{self.queue_name}'. Nhấn CTRL+C để dừng.")
            self.is_consuming = True
            
            # Bắt đầu tiêu thụ message (blocking)
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("Người dùng dừng tiêu thụ message")
            self.stop_consuming()
        except Exception as e:
            print(f"Lỗi khi bắt đầu tiêu thụ message: {str(e)}")
            self.is_consuming = False
    
    def start_consuming_in_thread(self, callback_function, auto_ack=False):
        """
        Bắt đầu tiêu thụ message trong một thread riêng biệt (non-blocking)
        
        Tham số:
            callback_function: Hàm callback nhận vào body của message
            auto_ack: Tự động xác nhận đã nhận message, mặc định là False
            
        Trả về:
            bool: True nếu thread được khởi động thành công, False nếu không
        """
        def consumer_thread():
            self.consume_messages(callback_function, auto_ack)
        
        # Nếu đã có thread đang chạy, dừng lại
        if self.consumer_thread and self.consumer_thread.is_alive():
            print("Đã có consumer thread đang chạy, dừng và khởi động lại...")
            self.stop_consuming()
        
        # Tạo và khởi động thread mới
        try:
            self.consumer_thread = threading.Thread(target=consumer_thread)
            self.consumer_thread.daemon = True  # Thread sẽ tự động kết thúc khi chương trình chính kết thúc
            self.consumer_thread.start()
            print(f"Đã khởi động consumer thread cho queue '{self.queue_name}'")
            return True
        except Exception as e:
            print(f"Lỗi khi khởi động consumer thread: {str(e)}")
            return False
    
    def stop_consuming(self):
        """
        Dừng tiêu thụ message
        
        Trả về:
            bool: True nếu dừng thành công, False nếu không
        """
        try:
            if self.is_consuming and self.channel and self.channel.is_open:
                self.channel.stop_consuming()
                self.is_consuming = False
                print("Đã dừng tiêu thụ message")
                return True
            return False
        except Exception as e:
            print(f"Lỗi khi dừng tiêu thụ message: {str(e)}")
            return False
    
    def purge_queue(self):
        """
        Xóa tất cả message trong queue
        
        Trả về:
            int: Số lượng message đã xóa, -1 nếu có lỗi
        """
        try:
            if not self.connection or self.connection.is_closed:
                if not self.connect():
                    return -1
            
            # Phương thức queue_purge xóa tất cả message trong queue
            frame = self.channel.queue_purge(queue=self.queue_name)
            print(f"Đã xóa {frame.method.message_count} message từ queue '{self.queue_name}'")
            return frame.method.message_count
        except Exception as e:
            print(f"Lỗi khi xóa message từ queue: {str(e)}")
            return -1
    
    def close(self):
        """Đóng kết nối RabbitMQ"""
        try:
            # Dừng tiêu thụ message nếu đang tiêu thụ
            if hasattr(self, 'is_consuming') and self.is_consuming:
                self.stop_consuming()
            
            # Đóng kết nối
            if self.connection and self.connection.is_open:
                self.connection.close()
                print("Đã đóng kết nối RabbitMQ")
        except Exception as e:
            print(f"Lỗi khi đóng kết nối RabbitMQ: {str(e)}")