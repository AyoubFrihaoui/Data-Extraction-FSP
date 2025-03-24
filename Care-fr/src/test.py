from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Set up headless Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

# Initialize the driver (make sure you have the correct chromedriver installed)
driver = webdriver.Chrome(options=chrome_options)

try:
    # Navigate to the login page
    driver.get("https://www.care.com/fr-fr/login")
    
    # Wait until the email input is present (adjust the timeout as needed)
    wait = WebDriverWait(driver, 10)
    email_input = wait.until(EC.presence_of_element_located((By.ID, "j_username")))
    password_input = driver.find_element(By.ID, "j_password")
    
    # Enter your credentials (replace with your actual credentials or fetch from secure storage)
    email_input.send_keys("your_email@example.com")
    password_input.send_keys("your_password")
    
    # Submit the form (you might need to click the login button)
    login_button = driver.find_element(By.ID, "32641-login-btn")
    login_button.click()
    
    # Wait for some element on the post-login page or check URL change to ensure login success
    wait.until(EC.url_contains("/dashboard"))  # adjust as per the expected URL
    
    # Extract cookies
    cookies = driver.get_cookies()
    print("Cookies:", cookies)
    
    # (Optional) Save cookies to a config file or other storage
    # with open('config.py', 'w') as f:
    #     f.write("COOKIES = " + repr(cookies))
    
finally:
    driver.quit()
