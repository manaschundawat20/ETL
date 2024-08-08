import sys
import os
from os import listdir
from os.path import isfile, join, splitext
import platform
import shutil
import re
import json

# uncomment on linux machine
if __file__.find("") > 0:
    sys.path.insert(0, 'path')
else:
    sys.path.append(". .")

from mip2.util.mip2main import mip2main
from mip2.util.mip2main import *

from datetime import datetime, timedelta
import time
import glob
import filecmp

from urllib.parse import urlparse, parse_qs
import requests
from urllib import parse
import pandas as pd

#selenium

from selenium import webdriver
from selenium import common
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import keys
from selenium.webdriver.support.ui import WebDriverWait
from select.webdriver.support import expected_conditioins as EC
from selenium.webdriver.common.by import By
from selenium.common.excptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.select import Select


class FileParsingError(Exception):
    pass


class FileWritingError(Exception):
    pass


class FileNotFoundError(Exception):
    pass


class FileMovingError(Exception):
    pass


class CaptureData(mip2main):

    PARAM_DIC = None
    ROOT_PATH = 'etlBaseDir'
    ETL_DIR = 'etlDir'
    CAPTURE = 'etlCaptureDir'
    TRANSFROM = 'etlTransformDir'
    PROCESSED = 'PROCESSED'

    FILEDS_JSON_FILE = None


    CORE_DSV_DELIMITER = "CORE_DSV_DELIMITER"
    CORE_STALE_FILE_CHECK = "CORE_STALE_FILE_CHECK"
    DATE_FORMAT_OUTFILE = "DATE_FORMAT_OUTFILE "
    FILE_EXTENSION = "FILE_EXTENSION"
    CORE_ETL_CLASS = "CORE_ETL_CLASS"
    DATE_FORMAT_FILENAME = "DATE_FORMAT_FILENAME"
    FILENAME = "FILENAME"
    PAGE_DELAY = "PAGE_DELAY"
    DELAY_SHORT = "DELAY_SHORT"
    DELAY_MEDIUM = "DELAY_MEDIUM"
    url = None
    capture_path = None

    # non configurable properties

    last_row_ind = -1
    rows = []
    driver = None

    ENCODING = "ENCODING"
    LOOKBACK_DAYS = "LOOKBACK_DAYS"
    LOOP_DELAY = "LOOP_DELAY"
    TABS_MAPPING = "TABS_MAPPING"
    CORE_HTTP_BASIC_URL = "CORE_HTTP_BASIC_URL"
    ALL_MAPPING = "ALL_MAPPING"
    IN_STATE_MAPPING = "IN_STATE_MAPPING"
    OUT_OF_STATE_MAPPING = "OUT_OF_STATE_MAPPING"
    CORE_DESTINATION_FILE_EXTENSION = "CORE_DESTINATION_FILE_EXTENSION"
    CORE_WEB_DRIVER_DIRECTORY = "CORE_WEB_DRIVER_DIRECTORY"

    CORE_EXT_STALE_EXIT_CODE = 254

    def init_parameters(self):

        self.ROOT_PATH = self.get_param_value_r(self.CORE_ETL_CLASS, True)
        self.CAPTURE = self.get_param_value_r(self.CAPTURE, 'CAPTURE')
        self.PROCESSED = self.get_param_value_r(self.PROCESSED, 'PROCESSED')
        self.ETL_DIR = self.get_param_value_r(self.ETL_DIR, True)
        self.TRANSFROM = self.get_param_value_r(self.TRANSFROM, 'TRANSFROM')
        self.caputure_path = join(
            self.ROOT_PATH, self.ETL_DIR, self.CAPTURE)
        self.transform_path = join(
            self.ROOT_PATH, self.ETL_DIR, self.TRANSFROM)

        
        self.CORE_DSV_DELIMITER = self.get_param_value_r(self.CORE_DSV_DELIMITER, True)
        self.CORE_STALE_FILE_CHECK = self.get_param_value_r(self.CORE_STALE_FILE_CHECK, True)
        self.PROCESSED = self.get_param_value_r(self.PROCESSED, True)
        self.FILENAME = self.get_param_value_r(self.FILENAME, True)
        self.CORE_DESTINATION_FILE_EXTENSION = self.get_param_value_r(self.CORE_DESTINATION_FILE_EXTENSION, True)
        self.ENCODING = self.get_param_value_r(self.ENCODING, True)
        self.DATE_FORMAT_FILENAME = self.get_param_value_r(self.DATE_FORMAT_FILENAME, True)
        self.LOOKBACK_DAYS = int(self.get_param_value_r(self.LOOKBACK_DAYS, True))
        self.LOOP_DELAY = self.get_param_value_r(self.LOOP_DELAY, True)
        self.TABS_MAPPING = self.get_param_value_r(self.TABS_MAPPING, True)
        self.CORE_HTTP_BASIC_URL = self.get_param_value_r(self.CORE_HTTP_BASIC_URL, True)
        self.IN_STATE_MAPPING = self.get_param_value_r(self.IN_STATE_MAPPING, True)
        self.ALL_MAPPING = self.get_param_value_r(self.ALL_MAPPING, True)
        self.OUT_OF_STATE_MAPPING = self.get_param_value_r(self.OUT_OF_STATE_MAPPING, True)
        self.PAGE_DELAY = int(self.get_param_value_r(self.PAGE_DELAY, True))
        self.DELAY_SHORT = int(self.get_param_value_r(self.DELAY_SHORT, True))
        self.DELAY_MEDIUM = int(self.get_param_value_r(self.DELAY_MEDIUM, True))

        def setup_driver(self):

            self.driver = webdriver.Chrome(executable_path=self.CORE_WEB_DRIVER_DIRECTORY, options=options)

        def process_page(self):
            self.setup_driver()

            self.status_log = {}

            tab_ids_list = {
            "ALL_MAPPING":json.loads(self.ALL_MAPPING.replace('[DQUOTE]', '"')),
            "IN_STATE_MAPPING":json.loads(self.IN_STATE_MAPPING.replace('[DQUOTE]', '"')),
            "OUT_OF_STATE_MAPPING":json.loads(self.OUT_OF_STATE_MAPPING.replace('[DQUOTE]', '"')),
            
            }

            for key,tab_ids  in tab_ids_list.items():
                try:
                    self.process_url(tab_ids)
                    time.sleep(self.DELAY_SHORT)
                except Exception as e:
                    eprint(f"error processing {key}: {e}")
                    raise Exception(e)

            total_files = len(self.status_log.keys())
            if total_files <1:
                raise Exception("no file found on the source")
            self.check_all_downloaded_were_stale()

        def process_url(self, tab_ids):
            if not self.driver:
                self.setup_driver()

            try:
                self.driver.get(self.CORE_HTTP_BASIC_URL)

                if len(tab_ids)<1:
                    raise Exception("no specific tab to fetch data")
                for tab_id, item in tab_ids.items():
                    # looping through all the tab_ids
                    self.driver.find_element(By.ID,tab_id).click()
                    body = self.driver.find_element(By.TAG_NAME,"BODY")
                    body_html = body.get_attribute("innerHTML")

                    time.sleep(self.DELAY_SHORT)
                    if not "ddl_id" in item:
                        raise Exception("ddl id not found")
                    if not "label" in item:
                        raise Exception("label not found")
                    if not "submit_btn" in item:
                        raise Exception("submit btn not found")

                    ddl_id = item["ddl_id"]
                    label =  item["label"]
                    submit_btn = item["submit_btn"]
                    table_id = item["id"]
                    state_element_id = item["STATE_ELEMENT_DROPDOWN"]

                    tab_id_elem = WebDriverWait(self.driver, self.PAGE_DELAY).until(EC.visibility_of_element_located((By.ID, tab_id)))
                    ddl_id_elem = WebDriverWait(self.driver, self.PAGE_DELAY).until(EC.visibility_of_element_located((By.ID, ddl_id)))
                    state_elem_dropdown = WebDriverWait(self.driver, self.PAGE_DELAY).until(EC.visibility_of_element_located((By.ID, state_element_id)))
                    submit_btn_elem = WebDriverWait(self.driver, self.PAGE_DELAY).until(EC.visibility_of_element_located((By.ID, submit_btn)))

                    if tab_id_elem is None:
                        raise Exception("{}: Tab not found".format(tab_id))
                    if ddl_id_elem is None:
                        raise Exception("{}: ddl id not found".format(ddl_id))
                    if state_elem_dropdown is None:
                        raise Exception("{}: state elem dropdown not found".format(state_element_id))
                    if submit_btn_elem is None:
                        raise Exception("{}: submit btn not found".format(submit_btn))

                    time.sleep(self.DELAY_SHORT)

                    self.iterate_through_all_years(label, table_id, ddl_id, state_element_id, submit_btn)
                time.sleep(self.DELAY_SHORT)

            except common.NoSuchElementException as e:
                eprint("element not accissible")
                raise Exception(e)
            except Exception as e:
                eprint("Error occured while accessing URL: {}".format(self.CORE_HTTP_BASIC_URL), e)

        def navigate_to_page(self):
            if not self.driver:
                self.setup_driver()
            
            try:
                self.driver.get(self.CORE_HTTP_BASIC_URL)
            except Exception as e:
                eprint("error occured while accessing URL")

        def select_state(self, state, state_elem_dropdown):
            state_dropdown = WebDriverWait(self.driver, self.PAGE_DELAY).until(EC.visibility_of_element_located((By.ID, state_elem_dropdown)))

            state_dropdown.send_keys(keys.CONTROL + "a")
            
            state_dropdown.send_keys(state)
            state_dropdown.send_keys(keys.TAB)

        def set_date_range(self, date_range_str, ddl_id):
            date_range_input = self.driver.find_element(By.ID, ddl_id)
            date_range_input.send_keys(Keys.CONTRO + "a")
            time.sleep(self.DELAY_MEDIUM)
            date_range_input.send_keys(f"{date_range_str}")
            date_range_input.send_keys(keys.TAB)

        def click_submit_button(self, submit_btn):
            submit_button = self.driver.find_element(By.ID, submit_btn)
            submit_button.click()

        def get_state_date_lsit(self,state):
            data = []
            api_url = self.CORE_CUSTOM_URL.format(state)
            res = requests.get(api_url)
            if res.status_code == 200:
                decoded_str = res.content.decode()
                if decoded_str !="":
                    date_list = json.loads(decoded_str)
                    for item in date_list:
                        date_range = item[self.STATE_DATE_LIST_ID]
                        date_range_values = [datetime.strptime(val.strip(),self.DATE_FORMAT_DDL) for val in date_range.split("-")]
                        date_item = {"ddl_value":item[self.DATE_ITEM_ID],"range_values":date_range_values}
                        data.append(date_item)

            else:
                raise Exception("data values for state could not be retrieved")

            return data

        def get_table_html(self):
            return self.driver.find_element(By.ID, table_id).get_attribute("outerHTML")

        def wirte_data_to_file(self, capture_path, data, filename, extension):
            csvFile = filename+datetime.now().strftime(self.DATE_FORMAT_FILENAME)+extension
            output_data_file = os.path.join(capture_path, csvFile)
            print("Writing file: {0}".format(output_data_file))
            start_write_ts = datetime.now()
            try:
                with open(output_data_file,"w") as fp:
                    fp.write(data)

                print("writing to CSV Complete: started @ {} and finished @ {1} - Total {2} seconds".fomat)
            except Exception as e:
                eprint(output_data_file)
                raise FileWritingError(output_data_file)


        def iterate_through_all_years(self, label, table_id, ddl_id, state_element_id, submit_btn):
            lookback_date = self.calulate_start_year()
            for state in self.STATES:

                try:
                    self.select_state(state,state_element_id)
                    state_dropdown_enabled = self.driver.find_element(By.ID, state_element_id).is_enabled()
                    if not state_dropdown_enabled:
                        eprint("no more data available for the state {state}")
                        continue
                    date_range_list = self.get_state_date_lsit(state)

                    for item in date_range_list:
                        start_date = item["range_values"][0]
                        to_date = item["range_values"][1]
                        ddl_value = item["ddl_value"]

                        if lookback_date > to_date:
                            continue
                        self.set_date_range(date_range_str=ddl_value,ddl_id=ddl_id)
                        self.click_submit_button()
                        table_html = self.get_table_html(table_id)
                        table_html = table_html.encode(self.ENCODING).decode("ascii",errors="ignore")
                        filename = self.FILENAME+"_"+label+"_"+state.replace(" ","")+"_"

                        output_file = self.wirte_data_to_file(capture_path=self.capture_path,data=table_html,filename=filename,extension=self.CORE_DESTINATION_FILE_EXTENSION)

                        stale_status = False
                        if(self.CORE_STALE_FILE_CHECK.lower() == 'y'):
                            stale_status = self.compare_single_stale_file(filename=output_file)
                        self.status_log[output_file] = stale_status 
                        time.sleep(self.DELAY_SHORT)
                except common.exceptions.NoSuchElementException as e:
                    eprint("element not loaded")
                    raise Exception(e)
                except Exception as e:
                    eprint(f"an error occured for state {state}: {str(e)}")
                    continue

        def check_all_downloaded_were_stale(self):
            try:
                total_files = len(self.status_log.keys())
                print("Total FIles: {}".format(total_files))
                stale_files = 0
                for filename, stale_status in self.status_log.items():
                    if stale_status:
                        stale_files += 1
                print("Total files {} Stale Files: {}".format(total_files, stale_files))
                if stale_files == total_files:
                    raise Exception ("all downloaded files were stale")

            except Exception as e:
                eprint(e)
                exit(254)

        def compare_single_stale_file(self, filename):

            capture_process_path = join(self.capture_path,self.PROCESSED)
            status = False
            try:
                list_of_files = glob.glob(capture_process_path + '//')
                if len(list_of_files) == 0:
                    pass
                else:
                    for file in list_of_files:
                        comp = filecmp.cmp(file,filename)
                        if comp:
                            os.remove(filename) 
                            print("Removed FIle {0}".format(filename))
                            status = True
                            break
            except Exception as e:
                eprint(e)
                raise Exception(e)
            return status

    def calculate_start_date(self):
        return datetime.now() - timedelta(days=self.LOOKBACK_DAYS)

    def capture_data(self):
        self.navigate_to_page()
        self.process_page()

    def free_resources(self):
        if not self.driver is None:
            self.driver.close()
            self.driver.quit()
            self.driver = None

    def capture(self):
        try:
            self.init_parameters()
            self.capture_data()

        except Exception as e:
            eprint("exception while running capture "+str(e))
            traceback.print_exc()
            self.free_resources()
            sys.exit(-100)
        finally:
            self.free_resources()



def main(args):
    print('calling')
    capture = CaptureData(args)
    capture.capture()

    print("all done exit")
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(0)

def eprint(*args, **kwargs):
    sys.stderr.write(' '.join(map(str,args)) + '\n', **kwargs)



if __name__ == "__main__":

    params = """{
        "etlDir": "",
        "CORE_ETL_CLASS": "",
        "descr": "",
        "etlBaseDir": "",
        "etlTransformDir": "TRANFORM",
        "etlCaptureDir": "CAPTURE",
        "CORE_STALE_FILE_CHECK": "Y",
        "PROCESSED": "PROCESSED",
        "FILENAME": "",
        "FILE_EXTENSION": ".dsv"
        "INPUT_DATETIME_FORMAT": "%m/%d/%Y %H:%M:%S",
        "OUTPUT_DATETIME_FORMAT": "%Y-%m-%d %H:%M:%S",

        "INPUT_DATE_FORMAT": "%m/%d/%Y",
        "OUTPUT_DATE_FORMAT": "%Y-%m-%d",
        "DATE_FORMAT_FILENAME": "%Y%m%d%H%M%I%S",
        "CORE_DSV_DELIMITER": "|",
        "LOOKBACK_DAYS": "50",
        "DATE_FORMAT_DDL": "%m%Y",
        "DATE_ITEM_ID":"ReportingYearValue",
        "STATE_DATE_LIST_ID":"ReportingYearDisplay",
        "STATES":"",
        "ALL_MAPPING": "[DEQUOTE]pageControl_T3[DEQUOTE]:{[DEQUOTE]id[DEQUOTE]:[DEQUOTE]GridView3MainTable[DEQUOTE], [DEQUOTE]label[DEQUOTE]:[DEQUOTE]by Location[DEQUOTE], [DEQUOTE]ddl_id[DEQUOTE]:[DEQUOTE]SelectedTimeD3_I[DEQUOTE], [DEQUOTE]submit_btn[DEQUOTE]:[DEQUOTE]SubmitButtom3_CD[DEQUOTE], [DEQUOTE]table_id[DEQUOTE]:[DEQUOTE]GridView3[DEQUOTE], [DEQUOTE]ddl_option_table_id[DEQUOTE]:[DEQUOTE]SelectedTimeID0_DDD_L_LBT[DEQUOTE]}}",
        "IN_STATE_MAPPING": "{[DEQUOTE]pageControl_T0S[DEQUOTE]:{[DEQUOTE]id[DEQUOTE]:[DEQUOTE]GridView0MainTable[DEQUOTE], [DEQUOTE]label[DEQUOTE]:[DEQUOTE]by Fuel[DEQUOTE], [DEQUOTE]ddl_id[DEQUOTE]:[DEQUOTE]SelectedTimeD0_I[DEQUOTE], [DEQUOTE]submit_btn[DEQUOTE]:[DEQUOTE]SubmitButtom0_CD[DEQUOTE], [DEQUOTE]table_id[DEQUOTE]:[DEQUOTE]GridView0[DEQUOTE], [DEQUOTE]ddl_option_table_id[DEQUOTE]:[DEQUOTE]SelectedTimeID0_DDD_L_LBT[DEQUOTE]}}",
        "OUT_OF_STATE   _MAPPING": "{[DEQUOTE]pageControl_T1[DEQUOTE]:{[DEQUOTE]id[DEQUOTE]:[DEQUOTE]GridView1MainTable[DEQUOTE], [DEQUOTE]label[DEQUOTE]:[DEQUOTE]by Renewable[DEQUOTE], [DEQUOTE]ddl_id[DEQUOTE]:[DEQUOTE]SelectedTimeD1_I[DEQUOTE], [DEQUOTE]submit_btn[DEQUOTE]:[DEQUOTE]SubmitButtom1_CD[DEQUOTE], [DEQUOTE]table_id[DEQUOTE]:[DEQUOTE]GridView1[DEQUOTE], [DEQUOTE]ddl_option_table_id[DEQUOTE]:[DEQUOTE]SelectedTimeID0_DDD_L_LBT[DEQUOTE]}}"
    }"""

    if __file__.find("mip_proj") >=0:
        main(params)
    else:
        main(sys.argv[1:])