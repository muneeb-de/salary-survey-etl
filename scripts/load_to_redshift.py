import boto3
import psycopg2
import pandas as pd
import logging
from io import StringIO


class SalarySurveyLoader:
    """
    Handles loading transformed data to Amazon Redshift via S3.
    """

    def __init__(self, s3_bucket, iam_role_arn, redshift_config, redshift_table):
        """
        Initialize loader with AWS and Redshift configuration.
        
        Args:
            s3_bucket (str): S3 bucket name for staging data
            iam_role_arn (str): IAM Role ARN for Redshift COPY command
            redshift_config (dict): Redshift connection parameters
            redshift_table (str): Target table in Redshift
        """
        self.s3_bucket = s3_bucket
        self.iam_role_arn = iam_role_arn
        self.redshift_config = redshift_config
        self.redshift_table = redshift_table
        self.logger = logging.getLogger("salary-survey-load")

    def upload_to_s3(self, df: pd.DataFrame, file_name: str) -> str:
        """
        Upload DataFrame as CSV to S3 bucket.
        
        Args:
            df: DataFrame to upload
            file_name: Name for the CSV file in S3
            
        Returns:
            str: S3 path where file was uploaded
        """
        try:
            s3_client = boto3.client('s3')

            # Convert DataFrame to CSV in-memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            s3_key = file_name

            self.logger.info(f"Uploading {file_name} to s3://{self.s3_bucket}/{s3_key}")
            s3_client.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

            self.logger.info("✅ Upload to S3 successful!")
            return f"s3://{self.s3_bucket}/{s3_key}"

        except Exception as e:
            self.logger.critical(f"❌ Error uploading to S3: {e}")
            raise

    def load_to_redshift(self, s3_path: str):
        """
        Load data from S3 to Redshift using COPY command.
        
        Args:
            s3_path: S3 path to the CSV file
        """
        try:
            copy_command = f"""
                COPY {self.redshift_table}
                FROM '{s3_path}'
                IAM_ROLE '{self.iam_role_arn}'
                FORMAT AS CSV
                IGNOREHEADER 1
                NULL AS ''
                BLANKSASNULL
                EMPTYASNULL
                TIMEFORMAT 'auto'
                DATEFORMAT 'auto';
            """

            self.logger.info("Loading data into Redshift...")

            # Connect to Redshift and execute COPY command
            conn = psycopg2.connect(
                host=self.redshift_config['host'],
                dbname=self.redshift_config['dbname'],
                user=self.redshift_config['user'],
                password=self.redshift_config['password'],
                port=self.redshift_config['port']
            )
            cursor = conn.cursor()
            cursor.execute(copy_command)
            conn.commit()
            
            # Get number of rows loaded
            cursor.execute(f"SELECT COUNT(*) FROM {self.redshift_table}")
            row_count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()

            self.logger.info(f"✅ Data successfully loaded into Redshift! Total rows: {row_count}")

        except Exception as e:
            self.logger.critical(f"❌ Error while loading into Redshift: {e}")
            raise

    def execute_load(self, df: pd.DataFrame, file_name: str = 'transformed_salary_data.csv'):
        """
        Execute complete load process: S3 upload + Redshift COPY.
        
        Args:
            df: Transformed DataFrame to load
            file_name: Name for the CSV file in S3
        """
        try:
            self.logger.info("Starting data load process...")
            
            # Upload to S3
            s3_path = self.upload_to_s3(df, file_name)
            
            # Load to Redshift
            self.load_to_redshift(s3_path)
            
            self.logger.info("✅ Data load process completed successfully!")
            
        except Exception as e:
            self.logger.critical(f"❌ Error in load process: {e}")
            raise


def load_data(df, s3_bucket, iam_role_arn, redshift_config, redshift_table, file_name='transformed_salary_data.csv'):
    """
    Convenience function to load data to Redshift.
    
    Args:
        df (pd.DataFrame): Transformed DataFrame to load
        s3_bucket (str): S3 bucket name
        iam_role_arn (str): IAM Role ARN
        redshift_config (dict): Redshift connection parameters
        redshift_table (str): Target table name
        file_name (str): S3 file name
    """
    loader = SalarySurveyLoader(s3_bucket, iam_role_arn, redshift_config, redshift_table)
    loader.execute_load(df, file_name)