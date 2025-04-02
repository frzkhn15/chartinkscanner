- name: Create browser_manager.py if missing
  run: |
    if [ ! -f browser_manager.py ]; then
      cat << EOF > browser_manager.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class BrowserManager:
    def __init__(self):
        self.drivers = {}
    
    def initialize_browser(self, name, headless=True):
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        self.drivers[name] = driver
        return driver
    
    def kill_browser(self, name):
        if name in self.drivers:
            try:
                self.drivers[name].quit()
            except:
                pass
            del self.drivers[name]

def create_browser_manager():
    return BrowserManager()
EOF
    fi
