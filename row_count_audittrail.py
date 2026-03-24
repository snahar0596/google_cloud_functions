import smtplib 
import email.utils
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText 
from datetime import timezone,datetime
import os
import logging
from google.cloud import bigquery
import mysql.connector
import os 
import csv
import tempfile
from google.cloud import storage
from email import encoders 

from datetime import datetime , timedelta
from zoneinfo import ZoneInfo 


INTEGRATION_TABLE_NAME = 'ctrl_dataset.integration_layer_tables'
CORESYSTEM_TABLE_NAME  = 'ctrl_dataset.coresystem_tables'
  

from datetime import datetime, timedelta 
from zoneinfo import ZoneInfo 
# Get Riyadh timezone
riyadh_tz = ZoneInfo("Asia/Riyadh")

end_date = datetime.now(riyadh_tz).date()
start_date = end_date - timedelta(days=2)
  


def get_condition_col(table_name):
    

    if  'tree_integration' in table_name :
        ctrl_table = INTEGRATION_TABLE_NAME
    elif   'tree_db' in table_name: 
        ctrl_table = CORESYSTEM_TABLE_NAME
         
    condition_query = f" select condition_col from  {ctrl_table} where table_name = '{table_name}' limit 1"

    client = bigquery.Client()
    query_job = client.query(condition_query)
    row = next(iter(query_job.result()), None)  # get the first row, or None if empty

    # Extract the column value
    condition_value = row.condition_col if row else None
    
    return condition_value

 

def get_pk_col(table_name):
    

    if  'tree_integration' in table_name :
        ctrl_table = INTEGRATION_TABLE_NAME
    elif   'tree_db' in table_name: 
        ctrl_table = CORESYSTEM_TABLE_NAME
         
    pk_query = f" select pk_col from  {ctrl_table} where table_name = '{table_name}' limit 1"

    client = bigquery.Client()
    query_job = client.query(pk_query)
    row = next(iter(query_job.result()), None)  # get the first row, or None if empty

    # Extract the column value
    pk_value = row.pk_col if row else None
    
    return pk_value
     
def get_db_config(db):
    db_config = {
        'host':   os.getenv(f'{db}_DB_HOST') ,
        'user':   os.getenv('DB_USER') ,
        'password':   os.getenv('DB_PASSWORD').strip('"') ,
        'database':   os.getenv(f'{db}_DB_NAME') ,
        'port':   int(os.getenv('DB_PORT'))
    } 
    return db_config
     
def get_table_counts_oci(query,db_config):
      
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return result
    
    except Exception as e:
        return None
 
def get_table_counts_bq(query):
    client = bigquery.Client()
    query_job = client.query(query)
    rows = query_job.result()
    results = []
    for row in rows:
        results.append({
            "row_count": row.row_count,
            "table_name": row.table_name
        })
    return results

def get_count_query(db_bq,db_oci, table):
    client = bigquery.Client()

    query = f"""
        SELECT table_name 
        FROM {table}
        where table_name != 'tree_db_policy'
    """
    query_job = client.query(query)
    rows = query_job.result()
 
    
    query_parts = []

    for row in rows:
             
            pk = get_pk_col(row.table_name)
            condition_col = get_condition_col(row.table_name)
            where_condition =''
            if condition_col: 
                where_condition =f"""
                    WHERE {condition_col}>= timestamp'{start_date}'  
                    """
            
            query = f"""
                    SELECT '{row.table_name}' AS table_name, COUNT( distinct payload.{pk}) AS row_count 
                       FROM {db_bq}.{row.table_name}  
             """ 
            query_parts.append(query.strip())

    query_bq = "\nUNION ALL\n".join(query_parts) 
    
    
    query_oci = query_bq.replace(db_bq,db_oci)
    query_oci = query_oci.replace("tree_db_","")
    query_oci = query_oci.replace("distinct payload.","")
    query_oci = query_oci.replace("tree_integration_","")
 
    return query_bq,query_oci

def compare_number(bq_list,oci_list,db,comparsion_list):
    
    oci_dict = {item["table_name"]: item["row_count"] for item in oci_list}
    bq_dict = {item["table_name"].replace("tree_db_","").replace("tree_integration_",""): item["row_count"] for item in bq_list}

    # Combine all table names
    all_tables = set(oci_dict) | set(bq_dict)

    # Build comparison result
    comparison = comparsion_list
    for table in sorted(all_tables):
        count_oci = oci_dict.get(table, 0)
        count_bq = bq_dict.get(table, 0)
        
        if count_oci - count_bq > 100 :  
            comparison.append({
                "db":db,
                "table_name": table,
                "count_oci": count_oci,
                "count_bq": count_bq,
                "diff_oci_bq": count_oci - count_bq
            })
    return comparison

from concurrent.futures import ThreadPoolExecutor



def validate_fn(request):
    
    integration_db_bq ='IngestionL_AuditTrailZ_IntegrationLayer'    
    integration_db_oci ='tree_integration' 
    integration_table_name = 'ctrl_dataset.integration_layer_tables'
    
    integration_bq_query,integration_oci_query = get_count_query(integration_db_bq,integration_db_oci,integration_table_name)
    integration_db_config = get_db_config('INTEGRATION_LAYER')
    logging.error(integration_bq_query)
    logging.error(integration_oci_query)
    
    coresystem_db_bq ='IngestionL_AuditTrailZ_Coresystem'
    coresystem_db_oci ='tree_db' 
    coresystem_table_name = 'ctrl_dataset.coresystem_tables'
    coresystem_bq_query,coresystem_oci_query = get_count_query(coresystem_db_bq,coresystem_db_oci,coresystem_table_name)
    coresystem_db_config = get_db_config('CORE_SYSTEM')
    logging.error(coresystem_bq_query)
    logging.error(coresystem_oci_query)
    # integration_bq_counts = get_table_counts_bq(integration_bq_query)  
    
    # 1. Initialize the executor
    with ThreadPoolExecutor() as executor:
        # 2. Start both tasks immediately (non-blocking)
        future_bq = executor.submit(get_table_counts_bq, coresystem_bq_query)
        future_oci = executor.submit(get_table_counts_oci, coresystem_oci_query, coresystem_db_config)
        
        future_bq_int = executor.submit(get_table_counts_bq, integration_bq_query)
        future_oci_int = executor.submit(get_table_counts_oci,integration_oci_query, integration_db_config)

        # 3. Wait for both to finish and collect results
        # .result() blocks until the specific function is done
        coresystem_bq_counts = future_bq.result()
        coresystem_oci_counts = future_oci.result()
        integration_bq_counts = future_bq_int.result()
        integration_oci_counts = future_oci_int.result()
        
        
    # Now both are finished, and you can proceed
    logging.error(coresystem_bq_counts)
    logging.error(coresystem_oci_counts)
    
    # coresystem_bq_counts = get_table_counts_bq(coresystem_bq_query)  
    # # logging.error(integration_bq_counts)
    # logging.error(coresystem_bq_counts)
    # # integration_oci_counts = get_table_counts_oci(integration_oci_query,integration_db_config)  
    # coresystem_oci_counts = get_table_counts_oci(coresystem_oci_query,coresystem_db_config)  
    # # logging.error(integration_oci_counts)
    # logging.error(coresystem_oci_counts)
    comparsion_list = []
    comparsion_list = compare_number(integration_bq_counts,integration_oci_counts,integration_db_oci,comparsion_list)
    comparsion_list = compare_number(coresystem_bq_counts,coresystem_oci_counts,coresystem_db_bq,comparsion_list)
    write_and_notify(comparsion_list)
    return comparsion_list
 
 
def write_and_notify(data):
    recipients =['mabokhashba@tree.com.sa','nalhamidi@tree.com.sa','hmadhi@tree.com.sa','spatel@tree.com.sa']

    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H:%M:%S')
    run_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    smtp_username = "ocid1.user.oc1..aaaaaaaaglb3fh2uqpcldlc27n5bo6sfn6mwf4lhqqxpkrwt5gqstpfoghmq@ocid1.tenancy.oc1..aaaaaaaap4ltts5q2cebopk32bddzfifukf5zeaq3xlmth26wjzndrkl47eq.jl.com"
    smtp_password = "iueH;8ZFroyKsx;Z1D04"
    smtp_host = "smtp.email.me-jeddah-1.oci.oraclecloud.com"
    smtp_port = 587   
    sender_email = 'treedata@tree.com.sa'
    sender_name = 'Tree Data'
    

    subject = 'Data Quality ROW COUNT AuditTrail Zone Mismatch'
    
    if len(data) == 0: 
        body = f"""
        There are no tables with data issues.
        ⏱️ Time: {timestamp}
        """ 
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = email.utils.formataddr((sender_name, sender_email))
        msg['To'] = ", ".join(recipients)
        msg.attach(MIMEText(body, 'plain'))
        try:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender_email, recipients, msg.as_string())
            server.close()
        except Exception as ex:
            print("ERROR")
        else:
            print("INFO: Email successfully sent!", flush=True)
        
        return 
    else: 

        bucket_name = os.getenv("GCS_BUCKET")  # set in your env vars
        destination_blob_name = f'Data_quality/Audit_Trail/Row_count_{run_date}.csv'

        # Create temp CSV file
        with tempfile.NamedTemporaryFile(mode="w", newline="", delete=False) as tmp_file:
            writer = csv.DictWriter(tmp_file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            tmp_file_path = tmp_file.name

        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(tmp_file_path)
        
        
        mismatch_list =[]
        for item in  data: 
            mismatch_list.append(item)
                
        
        
        body = f"""
            There is a mismatch in the daily validation in AuditTrail ZONE for the ROW COUNT Quality check.
            Kindly find the attached csv file with the values.
            For more info you can see all the output here: {bucket_name}/{destination_blob_name}
            ⏱️ Time: {timestamp}"""

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = email.utils.formataddr((sender_name, sender_email))
        msg['To'] = ", ".join(recipients)
        msg.attach(MIMEText(body, 'plain'))
        
        
        # Create temp CSV file
        with tempfile.NamedTemporaryFile(mode="w", newline="", delete=False) as tmp_file:
            writer = csv.DictWriter(tmp_file, fieldnames=mismatch_list[0].keys())
            writer.writeheader()
            writer.writerows(mismatch_list)
            tmp_file_path_mismatch = tmp_file.name
            
        part = MIMEBase('application', "octet-stream")
        with open(tmp_file_path_mismatch, 'rb') as file:
            part.set_payload(file.read())

        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename=dq-row-count-AuditTrail-{run_date}.csv'
        )
        msg.attach(part)

        try:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender_email, recipients, msg.as_string())
            server.close()
        except Exception as ex:
            print("ERROR")
        else:
            print("INFO: Email successfully sent!", flush=True)
