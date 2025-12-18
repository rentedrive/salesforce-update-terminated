import pandas as pd
import logging
import sys, os
from dateutil.relativedelta import relativedelta
from requests.models import Response
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
import awswrangler as wr
import json
import io
import zipfile
from zoneinfo import ZoneInfo
from typing import List
import traceback
import boto3
import ast
import re

from concurrent.futures import ThreadPoolExecutor

import lib

col_mapping = {
    'LEASE_START': 'None',
    'LEASE_END_DATE': 'None',
    'RETURN_DATE': 'Data_Fine_Contratto__c',   # Aggiorniamo Data Fine Contratto con la data di restituzione
    'RETURN_ODO': 'Return_Odo__c',
    'REGISTRATION': 'Targa_Veicolo__c',
    'COLLECTION_REASON_DESC': 'causale__c',
    'RINNOVO': 'Rinnovato__c',
    'EOC_TOTALE': 'Costi_Extra_Contratto__c'
}

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.addHandler(logging.StreamHandler(stream=sys.stdout)) if __name__ == '__main__' else None

aws_session = boto3.Session()
sf_session = lib.get_salesforce_session(
    aws_session=aws_session,
    secret_name=os.environ['SECRET_NAME'],
    region_name=os.environ['REGION_NAME']
)

def update_record(record):
    record_id = record.pop('Id')
    try:
        response = sf_session.Ordine__c.update(record_id=record_id, data=record, raw_response=True)
        if isinstance(response, Response) and response.status_code == 204:
            return {
                'status_code': 200,
                'Id': record_id,
                'status_description': 'Success'
            }
        else:
            return {
                'status_code': 400,
                'Id': record_id,
                'status_description': str(response)
            }
    except Exception as e:
        return {
            'status_code': 400,
            'Id': record_id,
            'status_description': f"{e.__class__}: {e}"
        }


def handler(event, context):

    if isinstance(event, str):
        event = json.loads(event)

    event['start_time'] = datetime.now(tz=ZoneInfo("Europe/Rome"))
    event['bucket_name'] = os.environ.get('BUCKET_NAME')
    event['download_availability_days'] = int(os.environ.get('DOWNLOAD_AVAILABILITY_DAYS'))
    event['job_owner'] = os.environ.get('JOB_OWNER')
    event['job_owner_email'] = os.environ.get('JOB_OWNER_EMAIL')
    event['order_chunk_size'] = int(os.environ.get('ORDER_CHUNCK_SIZE'))
    event['secret_name'] = os.environ.get('SECRET_NAME')
    event['upload_path'] = os.environ.get('UPLOAD_PATH')
    event['email_sender'] = os.environ.get('EMAIL_SENDER')
    event['email_subject'] = os.environ.get('EMAIL_SUBJECT')
    event['email_sender_name'] = os.environ.get('EMAIL_SENDER_NAME')
    event['email_receipts'] = ast.literal_eval(os.environ.get('EMAIL_RECEIPTS').strip())
    event['input_columns'] = ast.literal_eval(os.environ.get('TABLE_COLUMNS').strip())

    try:
        tags = aws_session.client('lambda').get_function(FunctionName=os.environ.get('AWS_LAMBDA_FUNCTION_NAME')).get('Tags', {})
        event['is_prod'] = tags['amplify:branch-name'] == 'master' or tags['amplify:branch-name'] == 'main'
    except Exception as e:
        print(f'Eccezione riscontrata durante recupero tag {e.__class__}: {e}')
        event['is_prod'] = False

    logger.info(f'Event: {event}')

    try:

        return update_records(event)

    except Exception as e:

        end_time = datetime.now(tz=ZoneInfo("Europe/Rome"))

        error_traceback = traceback.format_exc()

        if event['is_prod']:
            url_copertina = "https://rentedrive-static-elements-s3-bucket-prod.s3.eu-west-1.amazonaws.com/copertina/Copertina+Linkedin.png"
        else:
            url_copertina = "https://rentedrive-static-elements-s3-bucket-dev.s3.eu-west-1.amazonaws.com/copertina/Copertina+Linkedin.png"

        failed_mail_context = {
            "copertina_rentedrive_url": url_copertina,
            "job_owner": event['job_owner'],
            "send_ts": datetime.now(tz=ZoneInfo("Europe/Rome")).strftime("%Y-%m-%d %H:%M:%S"),
            "data_inizio": event["start_time"].strftime("%d/%m/%Y"),
            "ora_inizio": event["start_time"].strftime("%H:%M:%S"),
            "data_fine": end_time.strftime("%d/%m/%Y"),
            "ora_fine": end_time.strftime("%H:%M:%S"),
            "mail_owner": event['job_owner_email'],
            "error_code": error_traceback
        }

        html_body = Environment(loader=FileSystemLoader('./', encoding='utf-8'), autoescape=True) \
            .get_template('failed.html') \
            .render(**failed_mail_context)

        text_body = f"""
        Aggiornamento Ordini Terminati – Failed
        Inizio elaborazione: {event['start_time'].strftime("%d/%m/%Y %H:%M:%S")},
        Fine elaborazione: {end_time.strftime("%d/%m/%Y %H:%M:%S")}
        Errore elaborazione: {error_traceback}
        """

        if event['job_owner_email'] not in event['email_receipts']:
            event['email_receipts'].append(event['job_owner_email'])

        mail_response = send_email(
            sender_email=event['email_sender'],
            subject=f"{event['email_subject']} - Failed" if event['is_prod'] else f"TEST:{event['email_subject']} - Failed",
            sender_name=event['email_sender_name'],
            receipts=event['email_receipts'],
            html_body=html_body,
            text_body=text_body
        )

        return {
            'statusCode': 400,
            'statuDescription': f'ERROR: {e.__class__.__name__}: {e}',
            'startTS': event['start_time'].strftime("%d/%m/%Y %H:%M:%S"),
            'endTS': datetime.now(tz=ZoneInfo("Europe/Rome")).strftime("%d/%m/%Y %H:%M:%S"),
            'logs_response': {
                'zip_write': {},
                'remove_input_excel': {},
                'send_mail': mail_response
            },
            'presigned_url': None
        }


def cleanup_keep_latest_by_filename(
    aws_session: boto3.Session,
    bucket_name: str,
    folder_path: str
):
    """
        Mantiene solo il file .xlsx con il più alto timestamp Unix nel nome.
        Elimina tutti gli altri file nel percorso specificato.
    """
    s3_client = aws_session.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_path)

    latest_ts = -1
    latest_key = None
    to_delete = []

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.lower().endswith('.xlsx'):
                # Assumiamo che il key sia tipo "1234567890-suffix.xlsx"
                try:
                    ts_str = os.path.basename(key).split('-', 1)[0]
                    ts = int(ts_str)
                except (ValueError, IndexError):
                    continue

                if ts > latest_ts:
                    latest_ts = ts
                    latest_key = key

                to_delete.append({'Key': key})

    if latest_key is None:
        return None, None

    to_delete = [obj for obj in to_delete if obj['Key'] != latest_key]

    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i:i+1000]
        s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': batch})

    return latest_key, latest_ts


def update_records(event):

    latest_file_key, latest_file_ts = cleanup_keep_latest_by_filename(
        aws_session=aws_session,
        bucket_name=event['bucket_name'],
        folder_path=f"{event['upload_path'].rstrip('/')}/"
    )

    if latest_file_key is None:
        logger.info(f"No file found at s3://{event['bucket_name']}/{event['upload_path']}")
        return {
            'statusCode': 202,
            'statuDescription': 'No file found',
            'startTS': event['start_time'].strftime("%d/%m/%Y %H:%M:%S"),
            'endTS': datetime.now(tz=ZoneInfo("Europe/Rome")).strftime("%d/%m/%Y %H:%M:%S"),
            'logs_response': {
                'zip_write': {},
                'remove_input_excel': {},
                'send_mail': {}
            },
            'presigned_url': None
        }

    file_path = f"s3://{event['bucket_name']}/{latest_file_key}"

    logger.info(f"Working with {file_path}")

    df_input = wr.s3.read_excel(
        path=file_path,
        boto3_session=aws_session,
        dtype=str,
        na_values=['NaN', 'ND', 'None']
    )
    df_input.columns = [x.upper() for x in df_input.columns]
    df_input = df_input[event['input_columns']].copy()

    input_buffer = io.BytesIO()
    with pd.ExcelWriter(input_buffer, engine='xlsxwriter') as writer:
        df_input.to_excel(writer, sheet_name=os.path.basename(latest_file_key).split('-')[0], index=False)

    numero_ordini_acquisiti = len(df_input['REGISTRATION'].unique())

    df_input['LEASE_START'] = pd.to_datetime(df_input['LEASE_START'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')
    df_input['LEASE_END_DATE'] = pd.to_datetime(df_input['LEASE_END_DATE'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')
    df_input['RETURN_DATE'] = pd.to_datetime(df_input['RETURN_DATE'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')
    df_input['RETURN_ODO'] = df_input['RETURN_ODO'].astype('Int32')
    df_input['RINNOVO'] = df_input['RINNOVO'].str.upper().map({'RINNOVATO': True}).fillna(False)
    df_input['EOC_TOTALE'] = pd.to_numeric(df_input['EOC_TOTALE'],errors='coerce')
    df_input['EOC_TOTALE'] = df_input['EOC_TOTALE'].round(2)

    df_input['COLLECTION_REASON_DESC'] = df_input['COLLECTION_REASON_DESC'].str.title()

    # Cancelliamo LEASE_END_DATE perchè al momnento non viene utilizzata
    df_input.drop(columns=['LEASE_END_DATE', 'LEASE_START'], inplace=True)

    input_acquisition_ids = list(set(df_input['REGISTRATION'].values))
    dfs_ordini = list()

    for chuck in [input_acquisition_ids[i : i + event['order_chunk_size']] for i in range(0, len(input_acquisition_ids), event['order_chunk_size'])]:
        soql = lib.build_soql(sf_session=sf_session, acquisition_ids=[f"'{x}'" for x in chuck])
        dfs_ordini.append(lib.fetch_ordini_all(sf=sf_session, soql=soql['output'], chunk_size=event['order_chunk_size']*2))
    df_ordini = pd.concat(dfs_ordini).reset_index(drop=True)
    del dfs_ordini

    # TODO: pensare se è il caso di inserire un controllo

    # Controllo picklist causale termine
    record_types_ordine = list()
    for rt in sf_session.query_all("SELECT Id, Name, DeveloperName FROM RecordType WHERE SobjectType = 'Ordine__c'").get('records', []):
        record_types_ordine.append(rt['Id'])

    avaible_values_causale_termine = lib.get_unique_available_values_picklist(
        sf=sf_session,
        obj_name='Ordine__c',
        record_types_obj=record_types_ordine,
        field='causale__c'
    )
    df_causale_termine_no_salesfoce = df_input[~df_input['COLLECTION_REASON_DESC'].isin(avaible_values_causale_termine) & df_input['COLLECTION_REASON_DESC'].notnull()]
    del avaible_values_causale_termine

    # Ordini presenti nel file di Arval ma non presenti in Salesforce
    left = df_input.merge(
        df_ordini[['Targa_Veicolo__c']],
        how='left',
        left_on='REGISTRATION',
        right_on='Targa_Veicolo__c',
        indicator=True
    )
    right = df_input.merge(
        df_ordini[['Targa_Veicolo__c']],
        how='right',
        left_on='REGISTRATION',
        right_on='Targa_Veicolo__c',
        indicator=True
    )
    df_ordini_no_salesforce = pd.concat([left, right])
    df_ordini_no_salesforce = df_ordini_no_salesforce[df_ordini_no_salesforce['_merge'] == 'left_only'].drop(columns=['_merge', 'Targa_Veicolo__c'])
    del left, right

    scol_to_keep = ['Targa_Veicolo__c', 'Id', 'Stato__c']
    df_input = df_input.merge(df_ordini[scol_to_keep].set_index('Targa_Veicolo__c'), left_on='REGISTRATION', right_index=True, how='inner')

    # Ordini per cui il valore di Arval differisce da quello di Salesforce
    df_rinnovati_difference = df_input[['REGISTRATION', 'RINNOVO', 'Id']].merge(
        df_ordini[df_ordini['Stato__c'] == 'Live'][['Id', 'Rinnovato__c', 'Stato__c']].set_index('Id'),
        left_on='Id',
        right_index=True,
        how='inner'
    )
    df_rinnovati_difference = df_rinnovati_difference[
        df_rinnovati_difference['RINNOVO'] != df_rinnovati_difference['Rinnovato__c']
    ]
    df_rinnovati_difference.rename(columns={'RINNOVO': 'RINNOVO ARVAL', 'Rinnovato__c': 'RINNOVO SALESFORCE'}, inplace=True)
    df_rinnovati_difference = df_rinnovati_difference[['REGISTRATION', 'Id', 'RINNOVO SALESFORCE', 'RINNOVO ARVAL']]

    df_info_mail = df_input.copy()

    # Al momento non utilizziamo la colonna RINNOVO perchè non la aggiornamo secondo il file Arval, ma facciamo solo un check
    df_input.drop(columns=['RINNOVO'], inplace=True)
    df_input = df_input.rename(columns=col_mapping).drop(columns=['Targa_Veicolo__c'])

    df_ordini.set_index('Id', inplace=True)

    # Facciamo inizialmente un retry brute-force, potremmo poi sviluppare una strategia migliore!
    for _ in range(3):

        to_modify = list()
        to_skip = list()
        still_closed = list()
        to_close = list()

        for input_row in df_input.to_dict(orient='records'):

            rec = dict()

            # Se è chiuso non modifichiamo alcun campo nell'ordine
            if input_row['Stato__c'] == 'Chiuso':
                still_closed.append(input_row)
            else:
                rec['Stato__c'] = 'Chiuso'
                to_close.append(input_row)

                for k, v in input_row.items():
                    if k == 'Id' or (pd.notnull(v) and df_ordini.loc[input_row['Id']][k] != v):
                        rec[k] = v
                if set(rec.keys()) != {'Id'}:
                    to_modify.append(rec)
                else:
                    rec['status_code'] = 200
                    rec['status_description'] = 'Skipped'
                    to_skip.append(rec)

        logger.info(to_modify)
        logger.info(to_skip)

        if len(to_modify) == 0:
            df_modify = pd.DataFrame(columns=list(df_input.columns)+['status_code', 'status_description'])
            break

        with ThreadPoolExecutor(max_workers=10) as executor:
            df_modify = pd.DataFrame.from_dict(list(executor.map(update_record, to_modify)))
        if len(df_modify[df_modify['status_description'] != 'Success']) == 0:
            break
        else:
            logger.info(df_modify)

    df_modify = df_info_mail.merge(df_modify, on='Id', how='right')
    df_success = df_modify[df_modify['status_code'] == 200].drop(columns=['status_code']).rename(columns={'status_description': 'Stato Esecuzione'})
    df_failed = df_modify[df_modify['status_code'] != 200].drop(columns=['status_code']).rename(columns={'status_description': 'Stato Esecuzione'})

    if len(still_closed) > 0:
        df_still_closed = pd.DataFrame.from_dict(still_closed)
        df_still_closed = df_info_mail.merge(df_still_closed, on='Id', how='right')
    else:
        df_still_closed = pd.DataFrame(columns=df_input.columns)

    if len(to_close) > 0:
        df_closed = pd.DataFrame.from_dict(to_close)
        df_closed = df_info_mail.merge(df_closed, on='Id', how='right')
    else:
        df_closed = pd.DataFrame(columns=df_input.columns)

    if len(to_skip) > 0:
        df_skipped = pd.DataFrame.from_dict(to_skip)
        df_skipped = df_info_mail.merge(df_skipped, on='Id',how='right')
    else:
        df_skipped = pd.DataFrame(columns=df_input.columns)

    # Generazione zip di log

    # Crea il file Excel con tre fogli in memoria
    report_buffer = io.BytesIO()
    with pd.ExcelWriter(report_buffer, engine='xlsxwriter') as writer:
        df_success.to_excel(writer, sheet_name='Success', index=False)
        df_skipped.to_excel(writer, sheet_name='Skipped', index=False)
        df_closed.to_excel(writer, sheet_name='Contratti Chiusi', index=False)
        df_still_closed.to_excel(writer, sheet_name='Contratti Già Chiusi', index=False)
        df_rinnovati_difference.to_excel(writer, sheet_name='Differenza Rinnovati', index=False)
        df_failed.to_excel(writer, sheet_name='Failed', index=False)
        df_ordini_no_salesforce.to_excel(writer, sheet_name='Ordini No Salesforce', index=False)
        if df_causale_termine_no_salesfoce.shape[0] > 0:
            df_causale_termine_no_salesfoce.to_excel(writer, sheet_name='Casuali No Salesforce', index=False)
    report_buffer.seek(0)

    # Comprimi in ZIP
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('report.xlsx', report_buffer.getvalue())
        zf.writestr('input.xlsx', input_buffer.getvalue())
    zip_buffer.seek(0)

    zip_log_key = f'{event["upload_path"].replace("uploads","logs")}report_order_update_{event["start_time"].strftime("%Y%m%d_%H%M%S")}.zip'
    log_write_response = aws_session.client('s3').put_object(
        Bucket=event['bucket_name'],
        Key=zip_log_key,
        Body=zip_buffer.getvalue(),
        ContentType='application/zip'
    )

    # Crea un presigned_url per il file zip appena creato
    presigned_url_response = aws_session.client('s3').generate_presigned_url(
        'get_object',
        Params={'Bucket': event['bucket_name'], 'Key': zip_log_key},
        ExpiresIn=event['download_availability_days']*24*60*60
    )
    try:
        expiration_time = lib.unix_to_rome(int(re.search(r"[?&]Expires=([^&]+)", presigned_url_response).group(1)))
    except:
        expiration_time = datetime.now(tz=ZoneInfo("Europe/Rome")) + relativedelta(days=event['download_availability_days'])

    # Cancelliamo il file di input
    del_excel_response = aws_session.client('s3').delete_object(Bucket=event['bucket_name'], Key=latest_file_key)

    end_time = datetime.now(tz=ZoneInfo("Europe/Rome"))

    if event['is_prod']:
        url_copertina = "https://rentedrive-static-elements-s3-bucket-prod.s3.eu-west-1.amazonaws.com/copertina/Copertina+Linkedin.png"
    else:
        url_copertina = "https://rentedrive-static-elements-s3-bucket-dev.s3.eu-west-1.amazonaws.com/copertina/Copertina+Linkedin.png"

    success_mail_context = {
        "copertina_rentedrive_url": url_copertina,
        "job_owner": event['job_owner'],
        "send_ts": datetime.now(tz=ZoneInfo("Europe/Rome")).strftime("%Y-%m-%d %H:%M:%S"),
        "data_inizio": event["start_time"].strftime("%d/%m/%Y"),
        "ora_inizio": event["start_time"].strftime("%H:%M:%S"),
        "data_fine": end_time.strftime("%d/%m/%Y"),
        "ora_fine": end_time.strftime("%H:%M:%S"),
        "ordini_acquisiti": numero_ordini_acquisiti,
        "ordini_non_trovati": df_ordini_no_salesforce.shape[0],
        "update_success": df_success.shape[0],
        "update_skipped": df_skipped.shape[0],
        "update_failed": df_failed.shape[0],
        "rinnovati_difference": df_rinnovati_difference.shape[0],
        "missing_casuale_termine": len(df_causale_termine_no_salesfoce['COLLECTION_REASON_DESC'].unique()),
        "contratti_closed": df_closed.shape[0],
        "contratti_still_closed": df_still_closed.shape[0],
        "expiration_day": expiration_time.strftime("%d/%m/%Y"),
        "expiration_hour": expiration_time.strftime("%H:%M"),
        "zip_presigned_url": presigned_url_response
    }

    html_body = Environment(loader=FileSystemLoader('./', encoding='utf-8'), autoescape=True) \
        .get_template('success.html') \
        .render(**success_mail_context)

    text_body = f"""
    Aggiornamento Ordini – Successo
    Inizio elaborazione: {event['start_time'].strftime("%d/%m/%Y %H:%M:%S")},
    Fine elaborazione: {end_time.strftime("%d/%m/%Y %H:%M:%S")},
    Ordini Acquisiti: {numero_ordini_acquisiti},
    Ordini Non Trovati: {df_ordini_no_salesforce.shape[0]},
    Aggiornamenti Salesforce con Successo: {df_success.shape[0]},
    Aggiornamenti Salesforce con Errori: {df_failed.shape[0]}
    Report completo scaricabile al seguente link disponibile per {event['download_availability_days']} giorni:
        {presigned_url_response}
    """

    mail_response = send_email(
        sender_email=event['email_sender'],
        subject=f"{event['email_subject']} - Success" if event['is_prod'] else f"TEST:{event['email_subject']} - Success",
        sender_name=event['email_sender_name'],
        receipts=event['email_receipts'],
        html_body=html_body,
        text_body=text_body
    )

    return {
        'statusCode': 200,
        'statuDescription': 'Success',
        'startTS': event['start_time'].strftime("%d/%m/%Y %H:%M:%S"),
        'endTS': end_time.strftime("%d/%m/%Y %H:%M:%S"),
        'logs_response': {
            'zip_write': log_write_response,
            'remove_input_excel': del_excel_response,
            'send_mail': mail_response
        },
        'presigned_url': presigned_url_response
    }


def send_email(
    sender_email: str,
    subject: str,
    sender_name: str,
    receipts: List[str],
    html_body: str,
    text_body: str
):

    mail_response = aws_session.client('ses').send_email(
        Source=f"{sender_name} <{sender_email}>",
        Destination={
            'BccAddresses': receipts,
        },
        Message={
            'Subject': {
                'Data': subject,
                'Charset': 'UTF-8'
            },
            'Body': {
                'Html': {
                    'Data': html_body,
                    'Charset': 'UTF-8'
                },
                'Text': {
                    'Data': text_body,
                    'Charset': 'UTF-8'
                }
            }
        }
    )
    return mail_response

