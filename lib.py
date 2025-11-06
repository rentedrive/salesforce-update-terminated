import boto3
import json
from simple_salesforce import Salesforce
from simple_salesforce import format_soql
from typing import Dict
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    import pytz
import pandas as pd


def unix_to_rome(ts: int) -> datetime:
    dt_utc = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC") if "ZoneInfo" in globals() else pytz.UTC)
    if "ZoneInfo" in globals():
        dt_rome = dt_utc.astimezone(ZoneInfo("Europe/Rome"))
    else:
        dt_rome = dt_utc.astimezone(pytz.timezone("Europe/Rome"))
    return dt_rome


def get_secret(
    session: boto3.session.Session,
    secret_name: str,
    region_name: str
) -> str:

    client = session.client(service_name="secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    if "SecretString" in response:
        return response["SecretString"]
    else:
        return response["SecretBinary"].decode("utf-8")


def get_salesforce_session(
    aws_session: boto3.session.Session,
    secret_name: str,
    region_name: str
) -> Salesforce:
    credentials = get_secret(
        session=aws_session,
        secret_name=secret_name,
        region_name=region_name
    )
    credentials = json.loads(credentials)
    return Salesforce(**credentials)


def fetch_ordini_all(sf: Salesforce, soql: str, chunk_size: int = 2000) -> pd.DataFrame:
    results = sf.query_all(soql)
    records = results.get('records', [])
    cleaned = [{k: v for k, v in rec.items() if k != 'attributes'} for rec in records]
    return pd.json_normalize(cleaned)


def build_soql(
    sf_session: Salesforce,
    acquisition_ids: list[str]
) -> Dict[str, str]:

    if not acquisition_ids:
        return {
            "statusCode": 400,
            'description': 'No IDS in input',
            'output': None
        }

    obj_desc = sf_session.Ordine__c.describe()
    field_names = [f['name'] for f in obj_desc['fields']]

    soql = format_soql(
        f"SELECT {', '.join(field_names)} FROM Ordine__c WHERE Targa_Veicolo__c IN {{ids}} AND (Stato__c = 'Live' OR Stato__c = 'Chiuso')",
        ids=acquisition_ids
    )

    return {
            "statusCode": 200,
            'description': 'Success',
            'output': soql.replace("\\'", "")
        }
