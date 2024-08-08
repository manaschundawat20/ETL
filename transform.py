import sys
import os
from os import listdir
from os.path import isfile, join, splitext
import platform
import shutil
import re
import json
import time
import pandas as pd
import numpy as np
# uncomment on Linux machine jobs_posts
# if __file__.find("") >= 0:
#     sys.path.insert(0,'')
# else:
sys.path.append("..")
# sys.path.insert(0,'')
from mip2_util.mip2main import mip2main
from mip2_util.mip2main import *
 
import glob
import filecmp
from datetime import datetime,timezone
import pandas as pd
import traceback
 
class FileParsingError(Exception):
    pass
 
 
class FileWritingError(Exception):
    pass
 
 
class FileNotFoundError(Exception):
    pass
 
 
class FileMovingError(Exception):
    pass
 
 
class TransformData(mip2main):
 
    PARAM_DIC = None
    ROOT_PATH = 'etlBaseDir'
    ETL_DIR = 'etlDir'
    CAPTURE = 'etlCaptureDir'
    TRANSFORM = 'etlTransformDir'
    PROCESSED = 'PROCESSED'
 
    WRITE_HEADER = True
 
    CORE_DSV_DELIMITER="CORE_DSV_DELIMITER"
    CORE_STALE_FILE_CHECK="CORE_STALE_FILE_CHECK"
    FILE_EXTENSION="FILE_EXTENSION"
 
    CORE_ETL_CLASS="CORE_ETL_CLASS"
   
 
 
    FILENAME="FILENAME"
    DATE_FORMAT_FILENAME="DATE_FORMAT_FILENAME"
    ENCODING="ENCODING"
    ORDERED_COLS = "ORDERED_COLS"
   
 
   
    DATE_FORMAT_POSTED_DATETIME_IN = "DATE_FORMAT_POSTED_DATETIME_IN"
    DATE_FORMAT_POSTED_DATETIME_OUT = "DATE_FORMAT_POSTED_DATETIME_OUT"
    OUTPUT_DATE_FORMAT = "OUTPUT_DATE_FORMAT"
    CONSTANT_UOM = "CONSTANT_UOM"
    STATUS_COL_INDEX = "STATUS_COL_INDEX"
    NAME_COL_INDEX = "NAME_COL_INDEX"
    SOURCE_COL_INDEX = "SOURCE_COL_INDEX"
    # DATE_FORMAT_INPUT_FILENAME = "DATE_FORMAT_INPUT_FILENAME"
 
    CORE_EXT_STALE_EXIT_CODE = "CORE_EXT_STALE_EXIT_CODE"
 
    status_log = {}
    file_ctr=0
    ept_value_datetime_format = ""
    utc_value_datetime_format = ""
 
#init props
 
    def init_parameters(self):
 
        #basic
        self.ROOT_PATH = self.get_param_value_r(self.ROOT_PATH, True)
        self.CAPTURE = self.get_param_value_d(self.CAPTURE, 'CAPTURE')
        self.PROCESSED = self.get_param_value_d(self.PROCESSED, 'PROCESSED')
        self.ETL_DIR = self.get_param_value_r(self.ETL_DIR, True)
        self.TRANSFORM = self.get_param_value_d(self.TRANSFORM, 'TRANSFORM')
        self.capture_path = join(
                self.ROOT_PATH, self.ETL_DIR, self.CAPTURE)
        self.transform_path = join(
                self.ROOT_PATH, self.ETL_DIR, self.TRANSFORM)
 
        #custom
 
        self.CORE_ETL_CLASS = self.get_param_value_r(self.CORE_ETL_CLASS, True)
        self.CORE_EXT_STALE_EXIT_CODE = int(self.get_param_value_r(self.CORE_EXT_STALE_EXIT_CODE, True))
        self.CORE_STALE_FILE_CHECK = self.get_param_value_r(self.CORE_STALE_FILE_CHECK, True)
        self.PROCESSED = self.get_param_value_r(self.PROCESSED, True)
        self.FILENAME = self.get_param_value_r(self.FILENAME, True)
        self.FILE_EXTENSION = self.get_param_value_r(self.FILE_EXTENSION, True)
 
        self.DATE_FORMAT_FILENAME = self.get_param_value_r(self.DATE_FORMAT_FILENAME, True)
        self.ENCODING = self.get_param_value_r(self.ENCODING, True)
 
 
        self.CORE_DSV_DELIMITER = self.get_param_value_r(self.CORE_DSV_DELIMITER, True)
        ordered_cols = self.get_param_value_r(self.ORDERED_COLS, True)
        self.ORDERED_COLS = ordered_cols.replace(" ","").split(",")
 
        self.DATE_FORMAT_POSTED_DATETIME_IN = self.get_param_value_r(self.DATE_FORMAT_POSTED_DATETIME_IN, True)
        self.DATE_FORMAT_POSTED_DATETIME_OUT = self.get_param_value_r(self.DATE_FORMAT_POSTED_DATETIME_OUT, True)
       
        self.OUTPUT_DATE_FORMAT = self.get_param_value_r(self.OUTPUT_DATE_FORMAT, True)
        self.CCI_UOM = self.get_param_value_r(self.CCI_UOM, True)
        self.STATUS_COL_INDEX = int(self.get_param_value_r(self.STATUS_COL_INDEX, True))
        self.NAME_COL_INDEX = int(self.get_param_value_r(self.NAME_COL_INDEX, True))
 
        self.SOURCE_COL_INDEX = int(self.get_param_value_r(self.SOURCE_COL_INDEX, True))
   
 
    def check_all_downloaded_were_stale(self ):
        try:
            total_files = len(self.status_log.keys())
            print("Total files: {}".format(total_files))
            statle_files = 0
            for filename, stale_status in self.status_log.items():
                if stale_status:
                    statle_files+=1
            print("Total files: {} Stale File: {}".format(total_files, statle_files))
            if statle_files == total_files:
                raise Exception("All downloaded files were STALE")
 
        except Exception as e:
            eprint(e)
            exit(self.CORE_EXT_STALE_EXIT_CODE)
 
    def read_excel_file(self, file_path):
        try:
        # Determine the file extension
            file_extension = os.path.splitext(file_path)[1]
            # Read the Excel file with the appropriate engine
            if file_extension == '.xlsx':
                df = pd.read_excel(file_path, header=None, engine='openpyxl')
            else:
                raise ValueError("Unsupported file format: " + file_extension)
           
            return df
        except Exception as e:
 
            raise Exception(f"Error reading file {file_path}: {e}")
 
    def clean_dataframe(self, df):
        try:
        # Remove all rows and columns with NaN values
            df_cleaned = df.dropna(how='all').dropna(axis=1, how='all')
 
            df_cleaned.replace(r'^\s*-\s*$','', regex=True, inplace=True)
 
            if df_cleaned.shape[0] < 2:
                raise Exception("Not enough rows in the DataFrame to process.")
   
            # Store the first value in the first column as TIME and remove comma
            post_date = df_cleaned.iat[1, 0]
            post_date = str(post_date).replace(',', '')
            formatted_time = self.format_date(post_date)
            # Remove the first and second rows
            df_cleaned = df_cleaned.drop(df_cleaned.index[[0, 1]])
   
            # Remove the last two columns
            df_cleaned = df_cleaned.iloc[:, :-2]
   
            # Reset the index for easier manipulation
            df_cleaned = df_cleaned.reset_index(drop=True)
   
            return df_cleaned, formatted_time
        except Exception as e:
            raise Exception(f"Error cleaning dataframe: {e}")
 
 
    def extract_metadata(self, df_cleaned):
       
        try:
        # Extract metadata rows
            if self.STATUS_COL_INDEX >= df_cleaned.shape[0]:
                raise Exception("STATUS_COL_INDEX is out of bounds.")
            if self.NAME_COL_INDEX >= df_cleaned.shape[0]:
                raise Exception("NAME_COL_INDEX is out of bounds.")
            if self.SOURCE_COL_INDEX >= df_cleaned.shape[0]:
                raise Exception("SOURCE_COL_INDEX is out of bounds.")
           
            df_status = df_cleaned.iloc[self.STATUS_COL_INDEX].ffill()
            df_observation_names = df_cleaned.iloc[self.NAME_COL_INDEX]
            df_source_uom = df_cleaned.iloc[self.SOURCE_COL_INDEX]
   
            # Drop the first three rows (they have been extracted)
            df_values = df_cleaned.drop(index=[0, 1, 2])
   
            return df_values, df_status, df_observation_names, df_source_uom
        except Exception as e:
            raise Exception(f"Error extracting metadata: {e}")
 
    def prepare_dataframe(self, df_values):
        try:
 
        # Ensure the 'COMPANIES' column is present
            if 'COMPANIES' not in df_values.columns:
                # Set the first column as 'COMPANIES' if it's not present
                df_values.insert(0, 'COMPANIES', df_values.iloc[:, 0])
                df_values = df_values.drop(df_values.columns[1], axis=1)
   
            df_values = df_values.rename(columns={'COMPANIES': 'COMPANY'})
   
            # Identify and remove blank rows between tables
            blank_row_indices = df_values[df_values['COMPANY'].isna()].index
            if len(blank_row_indices) > 0:
                first_blank_row_index = blank_row_indices[0]
                df_values = df_values.iloc[:first_blank_row_index]
   
            return df_values
        except Exception as e:
            raise Exception(f"Error preparing dataframe: {e}")
       
    def format_date(self, raw_date):
        # Parse the raw date string into a datetime object
        date_obj = datetime.strptime(str(raw_date),self.DATE_FORMAT_POSTED_DATETIME_IN)
        # Format the datetime object into 'YYYY-MM-DD' format
        formatted_date = date_obj.strftime(self.DATE_FORMAT_POSTED_DATETIME_OUT)
        return formatted_date
 
    def unpivot_dataframe(self, df_values, df_status, df_observation_names, df_source_uom, formatted_time):
        try:
            df_values = df_values.melt(id_vars=['COMPANY'], var_name='Column', value_name='OBSERVATION_VALUE')
   
            # Create a mapping dictionary for STATUS, OBSERVATION_NAME, and SOURCE_UOM
            status_dict = df_status.to_dict()
            observation_names_dict = df_observation_names.to_dict()
            source_uom_dict = df_source_uom.to_dict()
            # Map the STATUS, OBSERVATION_NAME, and SOURCE_UOM
            df_values['STATUS'] = df_values['Column'].map(status_dict)
 
            df_values['OBSERVATION_NAME'] = df_values['Column'].map(observation_names_dict).str.strip()
            df_values['OBSERVATION_NAME'] = df_values['OBSERVATION_NAME'].replace(r'\s+',' ', regex=True)
 
            df_values['SOURCE_UOM'] = df_values['Column'].map(source_uom_dict).str.replace('(','').str.replace(')','')
 
            df_values["CONSTANT_UOM"] = self.CONSTANT_UOM
            # Drop the 'Column' column as it's no longer needed
            df_values = df_values.drop(columns='Column')  
            # Remove rows with 'N/A = Not Available' or NaN in the 'COMPANY' column
            df_values = df_values[~df_values['COMPANY'].isna() & (df_values['COMPANY'] != 'N/A = Not Available')]
            df_values.insert(1, 'RECORD_TIME', formatted_time)
            df_values = df_values[self.ORDERED_COLS]
            return df_values
        except Exception as e:
            raise Exception(f"Error unpivoting dataframe: {e}")
   
    def process_excel_file(self, file_path):
        try:
            df = self.read_excel_file(file_path)
            df_cleaned, post_date = self.clean_dataframe(df)
            # Extract metadata and values
            df_values, df_status, df_observation_names, df_source_uom = self.extract_metadata(df_cleaned)
            # Prepare the DataFrame and unpivot
            df_values = self.prepare_dataframe(df_values)
            df_values = self.unpivot_dataframe(df_values, df_status, df_observation_names, df_source_uom, post_date)
            print(df_values.head())
 
            return df_values
 
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
 
    def process_all_files(self, filepath):
 
        if isfile(filepath):
            df_transformed = self.process_excel_file(filepath)
            output_file = self.write_df_to_file(transform_path=self.transform_path, data=df_transformed, filename=self.FILENAME, extension=self.FILE_EXTENSION, output_file_separator="|")
            if (self.CORE_STALE_FILE_CHECK.lower() == 'y'):
                stale_status = self.compare_single_stale_file(filename=output_file)
                self.status_log[output_file] = stale_status
            else:
                raise Exception("json response not valid")
 
    def write_df_to_file(self, transform_path, data, filename, extension, output_file_separator="|"):
        """Write DF of Transformed input data into DSV
        in TRANSFORM folder"""
        self.file_ctr +=1
        csvFile = filename+"_"+str(self.file_ctr)+"_"+datetime.now().strftime(self.DATE_FORMAT_FILENAME)+extension
        output_data_file = join(transform_path, csvFile)
        # print("Writing DSV: {0}".format(output_data_file))      
        try:
            with open(output_data_file,"w",encoding=self.ENCODING) as fw:
                if float(pd.__version__[0:3])>=1.5:
                    data.to_csv(fw, sep=output_file_separator,
                        index=False, header=self.WRITE_HEADER,lineterminator="\n")
                else:
                    data.to_csv(fw, sep=output_file_separator,
                        index=False, header=self.WRITE_HEADER,line_terminator="\n")
        except Exception as e:
            print(e)
            eprint(output_data_file)
            raise FileWritingError(output_data_file)
 
        return output_data_file
 
    def compare_single_stale_file(self, filename):
        """Compare generated file with processed file and remove if file have equivalent in PROCESSED folder"""
        capture_process_path = join(self.transform_path, self.PROCESSED)
        status = False
        try:
            list_of_files = glob.glob(capture_process_path + '//*')
            if len(list_of_files) == 0:
                pass
            else:
                for file in list_of_files:
                    comp = filecmp.cmp(file, filename)
                    if comp:
                        # eprint("Duplicate file {0}".format(filename))
                        os.remove(filename)
                        print("Removed file {0}".format(filename))
                        status = True
                        break
 
        except Exception as e:
            eprint(e)
            raise Exception(e)
 
        return status
 
    def move_file_to_processed(self, capture_path, filename):
        """Move file from Capture path to PROCESSED Folder of Capture """
        src = os.path.join(capture_path, filename)
        target = os.path.join(capture_path, self.PROCESSED, filename)
        shutil.move(src, target)
 
    def move_file_to_failed(self, capture_path, filename):
        """Move file from Capture path to PROCESSED Folder of Capture """
        src = os.path.join(capture_path, filename)
        target = os.path.join(capture_path, "FAILED", filename)
 
    def transform(self):
        try:
            self.init_parameters()
            print("Transform Directory {0}".format(self.transform_path))
            capture_path = self.capture_path
            fileslist = [f for f in listdir(
                capture_path) if isfile(join(capture_path, f))]
            print("List of files to be processed {0} ".format(fileslist))
 
            capture_files_to_move = []
            is_fail = False
            failed_files = []
 
            if len(fileslist)<1:
                raise Exception("No files to process")
            self.file_ctr = 0
            for filename in fileslist:
                is_fail = True
 
                try:
                    filepath = join(capture_path, filename)
                    self.process_all_files(
                        filepath=filepath)
                    capture_files_to_move.append(filename)
 
                    print("Finished Processing file {0}".format(
                        filename))
                    is_fail = False
 
                   
                except FileNotFoundError as fileError:
                    eprint(fileError)
                    traceback.print_exc()
                    eprint('Could not open the file: {0}'.format(filename))
                    failed_files.append(filename)
                    is_fail = True
 
                except FileParsingError as fileParsing:
                    eprint(fileParsing)
                    traceback.print_exc()
                    eprint('Could not parse the file: {0}'.format(filename))
                    failed_files.append(filename)
                    is_fail = True
 
                except FileMovingError as fileMoving:
                    eprint(fileMoving)
                    traceback.print_exc()
                    eprint('Exception while moving the file : {0}'.format(filename))
                    failed_files.append(filename)
                    is_fail = True
 
                except FileWritingError as fileWriting:
                    eprint(fileWriting)
                    traceback.print_exc()
                    eprint('Exception while Writing the Transformed file : {0}'.format(filename))
                    failed_files.append(filename)
                    is_fail = True
                except Exception as e:
                    traceback.print_exc()
                    eprint(str(e))
                    eprint('Exception while Processing the Transformed file : {0}'.format(filename))
                    failed_files.append(filename)
                    is_fail = True
 
                finally:
                    if is_fail:
                        self.move_file_to_failed(capture_path=self.capture_path,filename=filename)
                    else:
 
                        mip2main.moveFileToProcessed(self,self.capture_path,filename)
                        # print("REMVEOCOMMENT TODO file moave")
 
            if is_fail:
                eprint('Atleast one file is failed to Transform : {0}'.format(failed_files))
                exit(-100)
            else:
                total_files = len(self.status_log.keys())                
                if total_files<1:
                    raise Exception("No file found on the source")
                #stale check for each
                self.check_all_downloaded_were_stale()
                print("Transform Process is complete.. Exiting ")
 
        except Exception as e:
            # todo modify into eprint
 
            eprint('Exception while running Transform')
            eprint(e)
            sys.exit(-100)
 
def main(args):
    print("calling")
    transformer = TransformData(args)
    transformer.transform()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)
 
def eprint(*args, **kwargs):
    sys.stderr.write(' '.join(map(str, args)) + '\n', **kwargs)
 
if __name__ == "__main__":
    # main(sys.argv[1:])
    params = """{
  "descr":"",
  "etlDir":"",
  "etlCaputreDir":"CAPTURE",
  "etlBaseDir":"C:/mnt/",
  "CORE_ETL_CLASS":"cci.foa.mip2.core.process.ETLExternalProcess",
  "PROCESSED":"PROCESSED",
  "FILENAME":"",
  "FILE_EXTENSION":".dsv",
  "CCI_UOM":"MMSCF",
  "CORE_DSV_DELIMITER":"|",
  "CORE_STALE_FILE_CHECK":"Y",
  "CORE_EXT_STALE_EXIT_CODE":"254",
  "ENCODING":"utf-8",
  "ORDERED_COLS":"COMPANY,RECORD_TIME,STATUS,OBSERVATION_NAME,OBSERVATION_VALUE,SOURCE_UOM,CONSTANT_UOM",
  "OUTPUT_DATE_FORMAT":"%Y-%m-%d",
  "DATE_FORMAT_POSTED_DATETIME_IN":"%B %Y",
  "DATE_FORMAT_POSTED_DATETIME_OUT":"%Y-%m-%d",
  "DATE_FORMAT_FILENAME":"_%Y%m%d%H%M%S",
  "STATUS_COL_INDEX":"0",
  "NAME_COL_INDEX":"1",
  "SOURCE_COL_INDEX":"2"
 
}"""

    # params = params.replace('"', '').replace('\\n', ' ')
    if __file__.find("") >= 0:
        main(params)
    else:
        main(sys.argv[1:])
