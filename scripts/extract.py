import os
import chardet
import pandas as pd
import logging


class SalarySurveyExtractor:
    """
    Handles data extraction from CSV files with encoding detection.
    """

    def __init__(self, input_file_path):
        """
        Initialize extractor with file path.
        
        Args:
            input_file_path (str): Path to the input CSV file
        """
        self.input_file_path = input_file_path
        self.logger = logging.getLogger("salary-survey-extract")

    def detect_encoding(self):
        """
        Detect file encoding using chardet library.
        
        Returns:
            str: Detected encoding or default to 'utf-8'
        """
        try:
            if not self.input_file_path:
                self.logger.warning("Input file path is empty - detect_encoding()")
                return 'utf-8'
            
            with open(self.input_file_path, 'rb') as input_file:
                encoding = chardet.detect(input_file.read(10000))
                self.logger.info(f"File Encoding : {encoding['encoding']}")
                if encoding['encoding'] != 'utf-8':
                    return 'utf-8'
                return encoding['encoding']

        except UnicodeDecodeError:
            self.logger.error("Encoding error occurred - detect_encoding()")
            return 'utf-8'
        except FileNotFoundError:
            self.logger.warning("File not found - detect_encoding()")
            return 'utf-8'
        except Exception as e:
            self.logger.critical(f"Exception occurred in - detect_encoding() : {e}")
            return 'utf-8'

    def extract_csv_data(self, encoding=None):
        """
        Extract data from CSV file with specified encoding.
        
        Args:
            encoding (str): File encoding to use for reading
            
        Returns:
            pd.DataFrame: DataFrame with extracted data
        """
        try:
            self.logger.info("Extracting data from CSV...")
            
            if encoding is None:
                encoding = self.detect_encoding()
            
            with open(self.input_file_path, 'r', encoding=encoding, errors='strict') as f:
                df = pd.read_csv(f)
            
            self.logger.info(f"Successfully extracted {len(df)} rows from CSV")
            return df
        
        except Exception as e:
            self.logger.critical(f"Exception occurred in extract_csv_data : {e}")
            return pd.DataFrame()


def extract_data(input_file_path):
    """
    Convenience function to extract data from CSV file.
    
    Args:
        input_file_path (str): Path to the input CSV file
        
    Returns:
        pd.DataFrame: Extracted data
    """
    extractor = SalarySurveyExtractor(input_file_path)
    return extractor.extract_csv_data()