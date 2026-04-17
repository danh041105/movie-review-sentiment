import os
import requests
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[!] Thiếu Bot Token hoặc Chat ID trong file .env")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"[!] Gửi Telegram thất bại: {e}")

def task_fail_alert(context):
    task_instance = context.get("task_instance")
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    exec_date = context.get('logical_date').strftime("%Y-%m-%d %H:%M:%S")
    log_url = task_instance.log_url
     # Trình bày tin nhắn
    alert_msg = f"""
    <b>PIPELINE ALERT: TASK FAILED!</b>
    <b>DAG:</b> <code>{dag_id}</code>
    <b>Task:</b> <code>{task_id}</code>
    <b>Time:</b> {exec_date}
    Task đã lỗi và đạt giới hạn Retry (không cứu được nữa).
    <a href="{log_url}">Xem chi tiết Log lỗi tại đây</a>
        """
    send_telegram_message(alert_msg)
    
def dag_success_alert(context):
    """Callback kích hoạt khi CẢ LUỒNG PIPELINE màu xanh là cây"""
    dag_run = context.get('dag_run')
    dag_id = dag_run.dag_id
    exec_date = context.get('logical_date').strftime("%Y-%m-%d %H:%M:%S")
    # Tính thời gian chạy tổng
    duration_minutes = (dag_run.end_date - dag_run.start_date).total_seconds() / 60.0
    success_msg = f"""
    <b>PIPELINE SUCCESS!</b>
    <b>DAG:</b> <code>{dag_id}</code>
    <b>Time:</b> {exec_date}
    <b>Duration:</b> {duration_minutes:.2f} phút
    Tất cả dữ liệu phim, reviews, NLP Sentiment và Gold Layer đã cập nhật xong!
    """
    send_telegram_message(success_msg)