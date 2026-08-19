#!/usr/bin/env python3
# Permet d'exécuter directement ce script sous Linux/macOS avec la commande ./extract.py

from datetime import datetime
from time import time
import argparse
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
import json
import os
import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')
REGION = os.getenv('REGION')
# URL de l'API Vélib' Métropole pour récupérer les informations statiques (noms des stations, coordonnées, capacité...)
STATION_INFO_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"

# URL de l'API Vélib' Métropole pour récupérer le statut dynamique en temps réel (nombre de vélos dispos, bornes libres...)
STATION_STATUS_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"

# URL de l'API Open-Meteo pour récupérer les prévisions météo horaires de Paris.
# Explications de la requête :
# - latitude/longitude : 48.8566 et 2.3522 correspondent aux coordonnées géographiques de Paris.
# - hourly=temperature_2m,weather_code : Demande la température à 2m et le code météo pour chaque heure.
# - timezone=Europe/Paris : Aligne les données sur le fuseau horaire de Paris.
# - forecast_days=1 : Limite la prévision uniquement à la journée en cours (aujourd'hui).
WEATHER_URL = "https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&hourly=temperature_2m,weather_code&timezone=Europe/Paris&forecast_days=1"


def fetch_data(url, api_name):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data

    except requests.Timeout as e:
        raise TimeoutError(f"Timeout error {e}") from e

    except requests.RequestException as e:
        raise RuntimeError(
            f"Erreur HTTP/réseau lors de l'appel à l'API {api_name} : {e}"
        ) from e

    except ValueError as e:
        raise RuntimeError(
            f"La réponse de l'API {api_name} n'est pas un JSON valide"
        ) from e


def check_data(payload, endpoint):
    if payload is None:
        raise RuntimeError(f"{endpoint} is empty")

    if not isinstance(payload, dict):
        raise RuntimeError(f"{endpoint} is not a dict")

    if "data" not in payload:
        raise RuntimeError(f"{endpoint} does not contain 'data'")

    if not isinstance(payload["data"], dict):
        raise RuntimeError(f"{endpoint}['data'] is not a dict")

    if "stations" not in payload["data"]:
        raise RuntimeError(f"{endpoint} does not contain 'data.stations'")

    stations = payload["data"]["stations"]

    if not isinstance(stations, list):
        raise RuntimeError(f"{endpoint}['data']['stations'] is not a list")

    if len(stations) == 0:
        raise RuntimeError(f"{endpoint} contains zero stations")


def check_weather_data(payload):
    if payload is None:
        raise RuntimeError("WEATHER is empty")

    if not isinstance(payload, dict):
        raise RuntimeError("WEATHER is not a dict")

    if "hourly" not in payload:
        raise RuntimeError("WEATHER does not contain 'hourly'")

    hourly = payload["hourly"]
    if not isinstance(hourly, dict):
        raise RuntimeError("WEATHER['hourly'] is not a dict")

    if "time" not in hourly:
        raise RuntimeError("WEATHER does not contain 'hourly.time'")

    if not isinstance(hourly["time"], list):
        raise RuntimeError("WEATHER['hourly']['time'] is not a list")

    if len(hourly["time"]) == 0:
        raise RuntimeError("WEATHER contains zero hourly forecasts")

def json_to_s3(data, s3_key, region=REGION, bucket_name=BUCKET_NAME):
    try:
        s3_client = boto3.client('s3', region_name=region)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
    except ClientError as e:
        raise RuntimeError(f"Erreur AWS S3 lors de l'écriture : {e}") from e

    except BotoCoreError as e:
        raise RuntimeError(f"Erreur boto3 lors de l'écriture S3 : {e}") from e
    
    logging.info(f"{s3_key} created in AWS S3 BUCKET")

def create_snapshot(payload, collected_at=None):
    record_count = 0

    if "data" in payload and "stations" in payload["data"]:
        record_count = len(payload["data"]["stations"])

    if record_count == 0:
        raise ValueError(
            "Le nombre d'enregistrements (record_count) est de 0, ce qui est anormal."
        )

    if collected_at is None:
        collected_at = datetime.now().isoformat()

    return {
        "metadata": {
            "collected_at": collected_at,
            "record_count": record_count,
        },
        "data": payload,
    }


def build_s3_key(data_type, collected_at):
    dt = datetime.fromisoformat(collected_at)
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    hour = dt.strftime("%H")
    timestamp = dt.strftime("%Y%m%dT%H%M%S")
    return f"raw/{data_type}/year={year}/month={month}/day={day}/hour={hour}/snapshot_{timestamp}Z.json"


def run_extraction(data_type, date_str=None):
    if date_str is None:
        collected_at = datetime.now().isoformat()
    else:
        # Standardisation de la date reçue au format ISO (ex: support timezone +00:00)
        dt = datetime.fromisoformat(date_str)
        collected_at = dt.isoformat()

    if data_type == "status":
        stations_status = fetch_data(STATION_STATUS_URL, "Vélib status")
        check_data(stations_status, "STATUS")
        snapshot_station_status = create_snapshot(stations_status, collected_at)
        s3_key = build_s3_key("status", collected_at)
        json_to_s3(snapshot_station_status, s3_key)
        return f"s3a://{BUCKET_NAME}/{s3_key}"

    elif data_type == "info":
        stations_information = fetch_data(STATION_INFO_URL, "Vélib info")
        check_data(stations_information, "INFO")
        snapshot_station_information = create_snapshot(stations_information, collected_at)
        s3_key = build_s3_key("info", collected_at)
        json_to_s3(snapshot_station_information, s3_key)
        return f"s3a://{BUCKET_NAME}/{s3_key}"

    elif data_type == "weather":
        weather_forecast = fetch_data(WEATHER_URL, "Open-Meteo")
        check_weather_data(weather_forecast)
        s3_key = build_s3_key("weather", collected_at)
        json_to_s3(weather_forecast, s3_key)
        return f"s3a://{BUCKET_NAME}/{s3_key}"
    
    else:
        raise ValueError(f"Type d'extraction inconnu : {data_type}")


def main():
    parser = argparse.ArgumentParser(
        description="Execution de station status, info, weather ou all"
    )

    parser.add_argument(
        "type",
        choices=["status", "info", "weather", "both", "all"],
        help="On recupere info, status, weather, les deux velib (both) ou tous (all) ?",
    )
    
    parser.add_argument(
        "--date",
        help="Date logique de l'extraction au format ISO 8601 (ex: 2026-08-19T12:00:00)"
    )

    args = parser.parse_args()

    # Déterminer la liste des extractions à effectuer
    types_to_extract = []
    if args.type == "status":
        types_to_extract = ["status"]
    elif args.type == "info":
        types_to_extract = ["info"]
    elif args.type == "weather":
        types_to_extract = ["weather"]
    elif args.type == "both":
        types_to_extract = ["status", "info"]
    elif args.type == "all":
        types_to_extract = ["status", "info", "weather"]

    # Exécuter les extractions et imprimer l'URI S3 finale sur stdout (pour Airflow XCom)
    for t in types_to_extract:
        s3_uri = run_extraction(t, args.date)
        print(s3_uri)


if __name__ == "__main__":
    main()


