import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# Запуск твого основного бота
def run_bot():
    print("🚀 Запускаємо tg_bot.py ...")
    try:
        exec(open("tg_bot.py").read())
    except Exception as e:
        print(f"Помилка запуску бота: {e}")

# Фейковий сервер для Render
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK - Bot is running on Render")
    
    def log_message(self, format, *args):
        return

# Головна функція
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 Запуск на порту {port}")
    
    # Запускаємо бота в окремому потоці
    threading.Thread(target=run_bot, daemon=True).start()
    
    # Запускаємо сервер
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f"✅ Health server запущено на порту {port}")
    server.serve_forever()
