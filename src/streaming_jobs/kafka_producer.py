import os
import sys
import csv
import json
import time
import argparse
from datetime import datetime
from kafka import KafkaProducer

# Hàm mã hóa dữ liệu dict sang định dạng JSON (UTF-8) gửi vào Kafka
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# Hàm làm sạch giá trị số tiền (xóa dấu $ và dấu phẩy)
def clean_amount(val):
    if not val:
        return 0.0
    cleaned = val.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def main():
    # KHỞI TẠO CÁC THAM SỐ LỆNH 
    parser = argparse.ArgumentParser(description="KAFKA PRODUCER - PHÁT GIAO DỊCH CHUẨN THỜI GIAN THỰC")
    parser.add_argument("--bootstrap-servers", type=str, default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
                        help="Địa chỉ Kafka Broker")
    parser.add_argument("--topic", type=str, default="transactions_topic", help="Tên Kafka topic")
    parser.add_argument("--limit", type=int, default=0, help="Giới hạn số bản ghi phát (0 = phát toàn bộ)")
    parser.add_argument("--batch-delay", type=float, default=0.01, help="Độ trễ giữa các bản ghi (giây)")
    parser.add_argument("--log-every", type=int, default=100, help="In log tiến trình sau mỗi N tin nhắn")
    parser.add_argument("--continuous", action="store_true", help="Bật chế độ phát vòng lặp 24/7 vô hạn")
    args = parser.parse_args()

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    csv_path = os.path.join(base_dir, "data", "bronze", "credit_card_transactions-ibm_v2.csv")

    print(f"-> Kafka Server : {args.bootstrap_servers}")
    print(f"-> Topic Name   : {args.topic}")
    print(f"-> Nguồn CSV    : {csv_path}")

    #KẾT NỐI VỚI KAFKA BROKER (CÓ TỰ ĐỘNG RETRY 10 LẦN)
    producer = None
    for attempt in range(1, 11):
        try:
            print(f"-> Đang kết nối Kafka (Lần {attempt}/10)...")
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers,
                value_serializer=json_serializer,
                acks=1
            )
            print("-> KẾT NỐI KAFKA THÀNH CÔNG!")
            break
        except Exception as e:
            print(f"Chờ Kafka sẵn sàng ({e}), thử lại sau 3 giây...")
            time.sleep(3)

    if not producer:
        print("Lỗi: Không thể kết nối tới Kafka sau 10 lần thử. Dừng chương trình.")
        sys.exit(1)

    sent_count = 0
    start_time = time.time()

    #ĐỌC DỮ LIỆU CSV VÀ BẮN TIN NHẮN VÀO KAFKA 24/7
    while True:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                now_iso = datetime.now().isoformat()
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Tạo gói dữ liệu Payload (Gắn mốc thời gian thực tại)
                payload = {
                    "user_id": int(row.get("User", 0)),
                    "card_index": int(row.get("Card", 0)),
                    "year": datetime.now().year,
                    "month": datetime.now().month,
                    "day": datetime.now().day,
                    "time": datetime.now().strftime('%H:%M'),
                    "amount": clean_amount(row.get("Amount", "$0")),
                    "use_chip": row.get("Use Chip", "").strip(),
                    "merchant_name": row.get("Merchant Name", "").strip(),
                    "merchant_city": row.get("Merchant City", "").strip(),
                    "merchant_state": row.get("Merchant State", "").strip(),
                    "zip": row.get("Zip", "").strip(),
                    "mcc": row.get("MCC", "").strip(),
                    "errors": row.get("Errors?", "").strip(),
                    "is_fraud": row.get("Is Fraud?", "No").strip(),
                    "current_timestamp": now_iso,
                    "ingestion_timestamp": now_iso
                }

                # Gửi tin nhắn vào Kafka Topic
                producer.send(args.topic, value=payload)
                sent_count += 1

                # In log giám sát tiến trình gửi
                if sent_count % args.log_every == 0 or args.batch_delay >= 0.1:
                    elapsed = time.time() - start_time
                    rate = sent_count / elapsed if elapsed > 0 else 0
                    fraud_flag = "🚨 FRAUD" if payload["is_fraud"].lower() == "yes" else "OK"
                    print(f"[{now_str}] Giao dịch #{sent_count:06d} | User: {payload['user_id']:<4} | ${payload['amount']:<7.2f} | {payload['merchant_city']:<12} | {fraud_flag:<7} | Tốc độ: {rate:.1f} msg/s")

                # Kiểm tra giới hạn bản ghi nếu có
                if args.limit > 0 and sent_count >= args.limit:
                    print(f"-> Đã đạt giới hạn {args.limit} bản ghi.")
                    break

                if args.batch_delay > 0:
                    time.sleep(args.batch_delay)

        # Nếu không bật --continuous hoặc đã chạm limit thì thoát vòng lặp
        if not args.continuous or (args.limit > 0 and sent_count >= args.limit):
            break

    producer.flush()
    producer.close()
    elapsed = time.time() - start_time
    print(f"=== [KAFKA PRODUCER] HOÀN THÀNH: ĐÃ PHÁT {sent_count} GIAO DỊCH TRONG {elapsed:.2f}S ===")

if __name__ == "__main__":
    main()
