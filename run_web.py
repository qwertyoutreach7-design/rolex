import os
import sys
import threading
import time
import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

# ====================== HEALTH SERVER (для Render) ======================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK - SERP Bot + Scheduler is running")
    
    def log_message(self, format, *args):
        return  # Не засмічувати логи

def run_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f"🌐 Health Server запущено на порту {port}")
    server.serve_forever()

# ====================== ЗАПУСК SCHEDULER (автопарсинг) ======================
def run_scheduler():
    print(f"[{datetime.datetime.now()}] ⏰ Автопарсинг scheduler.py запущено")
    try:
        sys.path.insert(0, os.getcwd())
        import scheduler
        if hasattr(scheduler, "main"):
            scheduler.main()
        else:
            exec(open("scheduler.py").read())
    except Exception as e:
        print(f"❌ Критична помилка в scheduler: {e}")
        import traceback
        traceback.print_exc()

# ====================== ЗАПУСК TELEGRAM БОТА ======================
def run_bot():
    print(f"[{datetime.datetime.now()}] 🤖 Telegram Bot запущено")
    try:
        sys.path.insert(0, os.getcwd())
        import tg_bot
        if hasattr(tg_bot, "main"):
            tg_bot.main()
        else:
            exec(open("tg_bot.py").read())
    except Exception as e:
        print(f"❌ Критична помилка в Telegram Bot: {e}")
        import traceback
        traceback.print_exc()

# ====================== MAIN ======================
if __name__ == "__main__":
    print("="*60)
    print("🚀 SERP Parser + Bot запущено на Render Web Service")
    print("="*60)
    
    # Створюємо необхідні папки
    os.makedirs("data", exist_ok=True)
    
    # Запускаємо автопарсинг кожні 3 години
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Запускаємо Telegram бота
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    
    print(f"[{datetime.datetime.now()}] ✅ Всі сервіси запущені успішно!")
    print("   • Telegram Bot")
    print("   • Автопарсинг (scheduler)")
    print("   • Health Server (для Render)")
    
    # Запускаємо сервер, щоб Render не засипав
    run_health_server()
