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
    """ETL Processor for RAW → Trusted → Delivery pipeline"""
    
    def __init__(self):
        self.timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        
    def lambda_handler(self, event: Dict[Any, Any], context: Any) -> Dict[str, Any]:
        """Main Lambda handler function"""
        try:
            logger.info(f"Processing event: {json.dumps(event)}")
            
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
                                self.process_file(bucket_name, object_key)
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
                    'timestamp': self.timestamp
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
                    'timestamp': self.timestamp
                })
            }
    
    def process_file(self, bucket_name: str, object_key: str) -> None:
        """Process file through RAW → Trusted → Delivery pipeline"""
        try:
            # Step 1: Read from RAW layer
            raw_data = self.read_raw_data(bucket_name, object_key)
            
            # Step 2: Process to Trusted layer
            trusted_data = self.process_to_trusted(raw_data, object_key)
            trusted_key = self.save_to_trusted(trusted_data, object_key)
            
            # Step 3: Process to Delivery layer
            delivery_data = self.process_to_delivery(trusted_data, object_key)
            delivery_key = self.save_to_delivery(delivery_data, object_key)
            
            logger.info(f"Successfully processed {object_key} through all layers")
            
        except Exception as e:
            logger.error(f"Error processing file {object_key}: {str(e)}")
            raise
    
    def read_raw_data(self, bucket_name: str, object_key: str) -> pd.DataFrame:
        """Read data from RAW layer"""
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            content = response['Body'].read()
            
            # Determine file type and read accordingly
            if object_key.lower().endswith('.csv'):
                df = pd.read_csv(io.StringIO(content.decode('utf-8')))
            elif object_key.lower().endswith('.json'):
                data = json.loads(content.decode('utf-8'))
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                else:
                    df = pd.DataFrame([data])
            elif object_key.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(content))
            else:
                # Default to CSV
                df = pd.read_csv(io.StringIO(content.decode('utf-8')))
            
            logger.info(f"Successfully read {len(df)} rows from RAW layer")
            return df
            
        except Exception as e:
            logger.error(f"Error reading raw data: {str(e)}")
            raise
    
    def process_to_trusted(self, raw_data: pd.DataFrame, object_key: str) -> pd.DataFrame:
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
            
            # 4. Add metadata columns
            df['processed_timestamp'] = datetime.now(timezone.utc)
            df['source_file'] = object_key
            df['processing_layer'] = 'TRUSTED'
            df['data_quality_score'] = self.calculate_quality_score(df)
            
            # 5. Validate data ranges (example business rules)
            self.apply_business_rules(df)
            
            final_rows = len(df)
            logger.info(f"Trusted processing: {initial_rows} → {final_rows} rows, removed {duplicates_removed} duplicates")
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing to trusted layer: {str(e)}")
            raise
    
    def process_to_delivery(self, trusted_data: pd.DataFrame, object_key: str) -> pd.DataFrame:
        """Process Trusted data to Delivery layer with enrichment and aggregations"""
        try:
            df = trusted_data.copy()
            
            # SQL-like enrichment and transformations
            
            # 1. Add calculated columns (enrichment)
            if 'amount' in df.columns:
                df['amount_category'] = pd.cut(df['amount'], 
                                             bins=[0, 100, 500, 1000, float('inf')], 
                                             labels=['Low', 'Medium', 'High', 'Premium'])
            
            # 2. Create aggregations
            delivery_data = []
            
            # Main detailed data
            df_main = df.copy()
            df_main['record_type'] = 'DETAIL'
            delivery_data.append(df_main)
            
            # Daily aggregations
            if 'processed_timestamp' in df.columns:
                df['date'] = df['processed_timestamp'].dt.date
                daily_agg = df.groupby('date').agg({
                    col: ['count', 'sum', 'mean'] if df[col].dtype in ['int64', 'float64'] 
                    else 'count' for col in df.select_dtypes(include=[np.number]).columns
                }).round(2)
                
                daily_agg.columns = ['_'.join(col) for col in daily_agg.columns]
                daily_agg = daily_agg.reset_index()
                daily_agg['record_type'] = 'DAILY_SUMMARY'
                daily_agg['processed_timestamp'] = datetime.now(timezone.utc)
                delivery_data.append(daily_agg)
            
            # 3. Create final delivery dataset
            final_df = pd.concat(delivery_data, ignore_index=True, sort=False)
            
            # 4. Add delivery layer metadata
            final_df['delivery_timestamp'] = datetime.now(timezone.utc)
            final_df['processing_layer'] = 'DELIVERY'
            final_df['pipeline_version'] = '1.0'
            
            logger.info(f"Delivery processing: Created {len(final_df)} delivery records")
            
            return final_df
            
        except Exception as e:
            logger.error(f"Error processing to delivery layer: {str(e)}")
            raise
    
    def save_to_trusted(self, data: pd.DataFrame, original_key: str) -> str:
        """Save processed data to Trusted bucket"""
        try:
            # Create partitioned path: year=YYYY/month=MM/day=DD/
            timestamp = datetime.now(timezone.utc)
            partition_path = f"year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}"
            
            filename = f"trusted_{os.path.splitext(os.path.basename(original_key))[0]}_{self.timestamp}.parquet"
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
                    'layer': 'TRUSTED'
                }
            )
            
            logger.info(f"Saved to Trusted: s3://{TRUSTED_BUCKET}/{trusted_key}")
            return trusted_key
            
        except Exception as e:
            logger.error(f"Error saving to trusted bucket: {str(e)}")
            raise
    
    def save_to_delivery(self, data: pd.DataFrame, original_key: str) -> str:
        """Save delivery data to Delivery bucket"""
        try:
            # Create partitioned path with record type
            timestamp = datetime.now(timezone.utc)
            
            # Save different record types separately
            for record_type in data['record_type'].unique():
                type_data = data[data['record_type'] == record_type].copy()
                
                partition_path = f"record_type={record_type}/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}"
                filename = f"delivery_{record_type.lower()}_{os.path.splitext(os.path.basename(original_key))[0]}_{self.timestamp}.parquet"
                delivery_key = f"{partition_path}/{filename}"
                
                # Convert to parquet
                parquet_buffer = io.BytesIO()
                type_data.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                
                s3_client.put_object(
                    Bucket=DELIVERY_BUCKET,
                    Key=delivery_key,
                    Body=parquet_buffer.getvalue(),
                    Metadata={
                        'source_file': original_key,
                        'processing_timestamp': self.timestamp,
                        'record_count': str(len(type_data)),
                        'record_type': record_type,
                        'layer': 'DELIVERY'
                    }
                )
                
                logger.info(f"Saved to Delivery: s3://{DELIVERY_BUCKET}/{delivery_key}")
            
            return delivery_key
            
        except Exception as e:
            logger.error(f"Error saving to delivery bucket: {str(e)}")
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
                if col in df.columns:
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    outliers = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR)))
                    quality_score.loc[outliers] -= 10
            
            # Ensure score is between 0 and 100
            quality_score = quality_score.clip(0, 100)
            
            return quality_score
            
        except Exception as e:
            logger.error(f"Error calculating quality score: {str(e)}")
            return pd.Series(100.0, index=df.index)
    
    def apply_business_rules(self, df: pd.DataFrame) -> None:
        """Apply business validation rules"""
        try:
            # Example business rules
            validation_errors = []
            
            # Rule 1: Check for negative amounts if amount column exists
            if 'amount' in df.columns:
                negative_amounts = df['amount'] < 0
                if negative_amounts.any():
                    validation_errors.append(f"Found {negative_amounts.sum()} records with negative amounts")
            
            # Rule 2: Check for future dates
            date_columns = df.select_dtypes(include=['datetime64']).columns
            for col in date_columns:
                future_dates = df[col] > datetime.now(timezone.utc)
                if future_dates.any():
                    validation_errors.append(f"Found {future_dates.sum()} records with future dates in {col}")
            
            # Log validation results
            if validation_errors:
                logger.warning(f"Business rule violations: {'; '.join(validation_errors)}")
            else:
                logger.info("All business rules passed successfully")
                
        except Exception as e:
            logger.error(f"Error applying business rules: {str(e)}")

# Create processor instance
processor = ETLProcessor()

# Lambda handler function
def lambda_handler(event, context):
    """AWS Lambda entry point"""
    return processor.lambda_handler(event, context)