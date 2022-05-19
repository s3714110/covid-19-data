from selenium.webdriver.chrome.webdriver import WebDriver
import time

from cowidev.utils.web import get_driver

source_url = "https://app.powerbi.com/view?r=eyJrIjoiN2ExNWI3ZGQtZDk3My00YzE2LWFjYmQtNGMwZjk0OWQ1MjFhIiwidCI6ImY2MTBjMGI3LWJkMjQtNGIzOS04MTBiLTNkYzI4MGFmYjU5MCIsImMiOjh9"

with get_driver() as driver:
    driver.get(source_url)
    time.sleep(6)
    driver.find_elements_by_class_name("fill.ui-role-button-fill")[3].click()
    table_string = driver.find_elements_by_class_name("vcBody.themableBackgroundColor.themableBorderColorSolid")[
        32
    ].text

table_string.split("\n")[4:40]

countries = table_string.split("\n")[4:40][0:22]
figures = table_string.split("\n")[4:40][22:42]
# need to expand view to get all figures
