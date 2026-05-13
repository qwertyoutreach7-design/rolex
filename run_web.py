import os
import threading
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

# ====================== HEALTH SERVER ======================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK - SERP Bot is running")
    def log_message(self, format, *args):
        return

# ====================== ЗАПУСК БОТА ======================
def run_bot():
    print("🚀 Запускаємо Telegram бота...")
    try:
        # Додаємо поточну папку в PATH (важливо!)
        sys.path.insert(0, os.getcwd())
        
        # Імпортуємо і запускаємо головний файл
        import tg_bot
        
        # Якщо в tg_bot.py є if __name__ == "__main__": main()
        if hasattr(tg_bot, "main"):
            print("Запускаємо функцію main() з tg_bot.py")
            tg_bot.main()
        else:
            print("Функція main() не знайдена в tg_bot.py")
            
    except Exception as e:
        print(f"❌ Помилка при запуску бота: {e}")
        import traceback
        traceback.print_exc()

# ====================== MAIN ======================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 Render Web Service запущено на порту {port}")
    
    # Запускаємо бота в окремому потоці
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    
    # Запускаємо health server (обов'язково для Render)
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f"✅ Health server працює на порту {port}")
    server.serve_forever()
