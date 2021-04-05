from selenium import webdriver
import time
import pandas as pd

path_to_chromeDriver = '/Users/drahcir1/Documents/chromedriver'
driver = webdriver.Chrome(executable_path=path_to_chromeDriver)
driver.get('https://genrandom.com/cats/')
time.sleep(3)
driver.close()
