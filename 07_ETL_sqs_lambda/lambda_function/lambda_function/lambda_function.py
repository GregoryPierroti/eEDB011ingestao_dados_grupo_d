import json
import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
import os
from typing import Dict, Any, List
from urllib.parse import unquote_plus
import io

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# Environment variables
RAW_BUCKET = os.environ['RAW_BUCKET']
TRUSTED_BUCKET = os.environ['TRUSTED_BUCKET']
DELIVERY_BUCKET = os.environ['DELIVERY_BUCKET']
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']

class ETLProcessor:
    """ETL Processor for RAW → Trusted → Delivery pipeline with folder structure preservation"""
    
    def __init__(self):
        self.timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        
    def extract_folder_structure(self, object_key: str) -> Dict[str, str]:
        """Extract folder structure information from S3 object key"""
        try:
            # Remove 'raw/' prefix if present and get the path components
            path_without_raw = object_key.replace('raw/', '', 1) if object_key.startswith('raw/') else object_key
            path_parts = path_without_raw.split('/')
            
            # Extract folder and filename
            if len(path_parts) >= 2:
                folder = path_parts[0]  # e.g., 'Reclamacoes', 'Bancos', 'Empregados'
                filename = path_parts[-1]  # actual filename
                subfolder_path = '/'.join(path_parts[:-1])  # full folder path
            else:
                folder = 'General'  # fallback for files in root
                filename = path_parts[0]
                subfolder_path = 'General'
            
            # Determine data category based on folder
            category = self.categorize_data(folder)
            
            return {
                'folder': folder,
                'subfolder_path': subfolder_path,
                'filename': filename,
                'category': category,
                'base_name': os.path.splitext(filename)[0],
                'extension': os.path.splitext(filename)[1]
            }
            
        except Exception as e:
            logger.error(f"Error extracting folder structure from {object_key}: {str(e)}")
            return {
                'folder': 'Unknown',
                'subfolder_path': 'Unknown',
                'filename': os.path.basename(object_key),
                'category': 'General',
                'base_name': os.path.splitext(os.path.basename(object_key))[0],
                'extension': os.path.splitext(object_key)[1]
            }
    
    def categorize_data(self, folder: str) -> str:
        """Categorize data based on folder name"""
        folder_lower = folder.lower()
        
        if 'reclamacoes' in folder_lower or 'reclamacao' in folder_lower:
            return 'complaints'
        elif 'bancos' in folder_lower or 'banco' in folder_lower:
            return 'banks'
        elif 'empregados' in folder_lower or 'employee' in folder_lower:
            return 'employees'
        else:
            return 'general'
    
    def process_file(self, bucket_name: str, object_key: str) -> None:
        """Process file through RAW → Trusted → Delivery pipeline"""
        try:
            # Extract folder structure information
            folder_info = self.extract_folder_structure(object_key)
            logger.info(f"Folder structure: {folder_info}")
            
            # Step 1: Read from RAW layer
            raw_data = self.read_raw_data(bucket_name, object_key)
            
            # Step 2: Process to Trusted layer
            trusted_data = self.process_to_trusted(raw_data, object_key, folder_info)
            trusted_key = self.save_to_trusted(trusted_data, object_key, folder_info)

            # Step 3: (Optional) Further processing to Delivery layer can be added here
            
            logger.info(f"Successfully processed {object_key} to Trusted layer: {trusted_key}")
            
        except Exception as e:
            logger.error(f"Error processing file {object_key}: {str(e)}")
            raise
    
    def read_raw_data(self, bucket_name: str, object_key: str) -> pd.DataFrame:
        """Read data from RAW layer with enhanced file type support"""
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            content = response['Body'].read()
            
            # Determine file type and read accordingly
            if object_key.lower().endswith('.csv'):
                # Try different separators
                try:
                    df = pd.read_csv(io.StringIO(content.decode('utf-8')))
                except:
                    # Try pipe separator (common in your data)
                    df = pd.read_csv(io.StringIO(content.decode('utf-8')), sep='|')
            elif object_key.lower().endswith('.tsv'):
                df = pd.read_csv(io.StringIO(content.decode('utf-8')), sep='\t')
            elif object_key.lower().endswith('.json'):
                data = json.loads(content.decode('utf-8'))
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                else:
                    df = pd.DataFrame([data])
            elif object_key.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(content))
            else:
                # Default to CSV with multiple separator attempts
                try:
                    df = pd.read_csv(io.StringIO(content.decode('utf-8')))
                except:
                    try:
                        df = pd.read_csv(io.StringIO(content.decode('utf-8')), sep='|')
                    except:
                        df = pd.read_csv(io.StringIO(content.decode('utf-8')), sep=';')
            
            logger.info(f"Successfully read {len(df)} rows from RAW layer")
            return df
            
        except Exception as e:
            logger.error(f"Error reading raw data: {str(e)}")
            raise
    
    def process_to_trusted(self, raw_data: pd.DataFrame, object_key: str, folder_info: Dict[str, str]) -> pd.DataFrame:
        """Process RAW data to Trusted layer with data quality and validation"""
        try:
            df = raw_data.copy()
            
            # Data Quality Checks and Cleaning
            initial_rows = len(df)
            
            # 1. Remove duplicates
            df = df.drop_duplicates()
            duplicates_removed = initial_rows - len(df)
            
            # 2. Handle missing values
            # Fill numeric columns with median, categorical with mode
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            categorical_columns = df.select_dtypes(include=['object']).columns
            
            for col in numeric_columns:
                if df[col].isnull().any():
                    median_value = df[col].median()
                    df[col].fillna(median_value, inplace=True)
            
            for col in categorical_columns:
                if df[col].isnull().any():
                    mode_value = df[col].mode().iloc[0] if not df[col].mode().empty else 'Unknown'
                    df[col].fillna(mode_value, inplace=True)
            
            # 3. Data type validation and conversion
            # Convert date columns if detected
            for col in df.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    except:
                        pass
            
            # 4. Category-specific processing
            df = self.apply_category_specific_rules(df, folder_info['category'])
            
            # 5. Add metadata columns
            df['processed_timestamp'] = datetime.now(timezone.utc)
            df['source_file'] = object_key
            df['processing_layer'] = 'TRUSTED'
            df['data_category'] = folder_info['category']
            df['source_folder'] = folder_info['folder']
            df['data_quality_score'] = self.calculate_quality_score(df)
            
            final_rows = len(df)
            logger.info(f"Trusted processing: {initial_rows} → {final_rows} rows, removed {duplicates_removed} duplicates")
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing to trusted layer: {str(e)}")
            raise
    
    def apply_category_specific_rules(self, df: pd.DataFrame, category: str) -> pd.DataFrame:
        """Apply category-specific business rules"""
        try:
            if category == 'complaints':
                # Rules for Reclamacoes data
                logger.info("Applying complaints-specific rules")
                # Add complaint-specific validations here
                
            elif category == 'banks':
                # Rules for Bancos data
                logger.info("Applying banks-specific rules")
                # Add bank-specific validations here
                
            elif category == 'employees':
                # Rules for Empregados data
                logger.info("Applying employees-specific rules")
                # Convert rating columns to numeric if they exist
                rating_columns = ['Geral', 'Cultura e valores', 'Diversidade e inclusão', 
                                'Qualidade de vida', 'Alta liderança', 'Remuneração e benefícios', 
                                'Oportunidades de carreira']
                
                for col in rating_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Convert percentage columns
                percentage_columns = ['Recomendam para outras pessoas(%)', 'Perspectiva positiva da empresa(%)']
                for col in percentage_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
            
        except Exception as e:
            logger.warning(f"Error applying category-specific rules: {str(e)}")
            return df
    
    def save_to_trusted(self, data: pd.DataFrame, original_key: str, folder_info: Dict[str, str]) -> str:
        """Save processed data to Trusted bucket maintaining folder structure"""
        try:
            # Create partitioned path maintaining folder structure
            timestamp = datetime.now(timezone.utc)
            
            # Maintain folder structure: folder/year=YYYY/month=MM/day=DD/
            partition_path = f"{folder_info['subfolder_path']}/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}"
            
            filename = f"trusted_{folder_info['base_name']}_{self.timestamp}.parquet"
            trusted_key = f"{partition_path}/{filename}"
            
            # Convert to parquet for better performance
            parquet_buffer = io.BytesIO()
            data.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            s3_client.put_object(
                Bucket=TRUSTED_BUCKET,
                Key=trusted_key,
                Body=parquet_buffer.getvalue(),
                Metadata={
                    'source_file': original_key,
                    'processing_timestamp': self.timestamp,
                    'record_count': str(len(data)),
                    'layer': 'TRUSTED',
                    'data_category': folder_info['category'],
                    'source_folder': folder_info['folder']
                }
            )
            
            logger.info(f"Saved to Trusted: s3://{TRUSTED_BUCKET}/{trusted_key}")
            return trusted_key
            
        except Exception as e:
            logger.error(f"Error saving to trusted bucket: {str(e)}")
            raise
    
    def calculate_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate data quality score for each record"""
        try:
            # Initialize quality score
            quality_score = pd.Series(100.0, index=df.index)
            
            # Deduct points for missing values
            missing_penalty = df.isnull().sum(axis=1) * 5
            quality_score -= missing_penalty
            
            # Deduct points for potential outliers in numeric columns
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if col in df.columns and len(df[col].dropna()) > 0:
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    if IQR > 0:
                        outliers = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR)))
                        quality_score.loc[outliers] -= 10
            
            # Ensure score is between 0 and 100
            quality_score = quality_score.clip(0, 100)
            
            return quality_score
            
        except Exception as e:
            logger.error(f"Error calculating quality score: {str(e)}")
            return pd.Series(100.0, index=df.index)


def lambda_handler(event, context):
    """AWS Lambda entry point - MUST be at module level"""
    try:
        logger.info(f"Lambda started - Processing event: {json.dumps(event)}")
        
        # Create processor instance
        processor = ETLProcessor()
        
        processed_records = 0
        errors = []
        
        # Process SQS records
        for record in event.get('Records', []):
            try:
                # Parse SQS message body (S3 event notification)
                message_body = json.loads(record['body'])
                
                # Handle S3 event notification
                if 'Records' in message_body:
                    for s3_record in message_body['Records']:
                        if s3_record['eventName'].startswith('ObjectCreated'):
                            bucket_name = s3_record['s3']['bucket']['name']
                            object_key = unquote_plus(s3_record['s3']['object']['key'])
                            
                            logger.info(f"Processing file: s3://{bucket_name}/{object_key}")
                            
                            # Process the file through ETL pipeline
                            processor.process_file(bucket_name, object_key)
                            processed_records += 1
                            
            except Exception as e:
                error_msg = f"Error processing record: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        # Return response
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'processed_records': processed_records,
                'errors': errors,
                'timestamp': processor.timestamp
            })
        }
        
        logger.info(f"Lambda execution completed: {response}")
        return response
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            })
        }