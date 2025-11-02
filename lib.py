import boto3
import pandas as pd
import json
import awswrangler as wr
from urllib.parse import urlparse
from simple_salesforce import Salesforce
from simple_salesforce import format_soql
from typing import Dict
import re
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    import pytz


def unix_to_rome(ts: int) -> datetime:
    dt_utc = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC") if "ZoneInfo" in globals() else pytz.UTC)
    if "ZoneInfo" in globals():
        dt_rome = dt_utc.astimezone(ZoneInfo("Europe/Rome"))
    else:
        dt_rome = dt_utc.astimezone(pytz.timezone("Europe/Rome"))
    return dt_rome


def assume_role(name: str):
    sts = boto3.client('sts')
    resp = sts.assume_role(
        RoleArn=name,
        RoleSessionName='local-session'
    )
    return resp['Credentials']


def get_session(
    profile_name: str = None,
    creds: dict = None,
    **kwargs
) -> boto3.Session:

    if profile_name is not None:
        return boto3.Session(
            profile_name=profile_name,
            **kwargs
        )
    elif creds is not None:
        return boto3.Session(
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken'],
            **kwargs
        )
    else:
        return boto3.Session(**kwargs)


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
        f"SELECT {', '.join(field_names)} FROM Ordine__c WHERE Numero_Ordine__c IN {{ids}} AND Stato__c = 'Attesa Consegna'",
        ids=acquisition_ids
    )

    return {
            "statusCode": 200,
            'description': 'Success',
            'output': soql.replace("\\'", "")
        }


def aggiorna_data_installazione(args: pd.Series):

    testo_nota = args['Note__c']
    data_installazione_bb = args['DATA_INSTALLAZIONE_BB']

    pattern_con_data = r'(DATA INSTALLAZIONE BB: )\d{2}/\d{2}/\d{4}'
    parola_chiave = 'DATA INSTALLAZIONE BB:'

    if data_installazione_bb:
        if not isinstance(testo_nota, str):
            testo_nota = ""
        if re.search(pattern_con_data, testo_nota):
            return re.sub(pattern_con_data, rf'\g<1>{data_installazione_bb}', testo_nota)
        elif parola_chiave not in testo_nota:
            nuova_stringa = f"{parola_chiave} {data_installazione_bb}"
            return f"{testo_nota} {nuova_stringa}".strip()
        else:
            return testo_nota
    else:
        return testo_nota


def split_s3_path(s3_path: str) -> tuple[str, str]:
    """
    Divide un percorso S3 in (bucket, key).
    Rimuove eventuale slash iniziale nel key.
    """
    o = urlparse(s3_path, allow_fragments=False)
    bucket = o.netloc
    key = o.path.lstrip('/')
    return bucket, key


template_email_failed = """

"""