import os
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Global Variables
SERVICE_KEY = Variable.get("KOPIS_SERVICE_KEY", default_var="b334b3a40adc4d40bc3211b94b1c7d11")
LOCAL_JSON_PATH = "/opt/airflow/data/kopis_boxoffice.json"
region_dict = {
    11: "서울", 28: "인천", 30: "대전", 27: "대구", 29: "광주", 26: "부산", 
    31: "울산", 36: "세종", 41: "경기", 43: "충청", 44: "충청", 47: "경상", 
    48: "경상", 45: "전라", 46: "전라", 51: "강원", 50: "제주", "UNI": "대학로"
}

def fetch_boxoffice_data(**kwargs):
    """
    Kopis API에서 박스오피스 데이터를 가져와 로컬 JSON 파일로 저장
    """
    aggregated_data = []
    base_url = "http://www.kopis.or.kr/openApi/restful/boxoffice"
    now = datetime.now()
    stdate_str = (now.date() - timedelta(days=1)).strftime("%Y%m%d")
    eddate_str = stdate_str

    for region in region_dict.keys():
        params = {
            "service": SERVICE_KEY,
            "stdate": stdate_str,
            "eddate": eddate_str,
            "area": region
        }
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()  # HTTP 에러 체크
            root_resp = ET.fromstring(response.content)
            boxof_elements = root_resp.findall("boxof")
            if not boxof_elements:
                print(f"[박스오피스] 데이터 없음: 지역 {region}")
                continue
            for boxof in boxof_elements:
                data = {
                    "prfplcnm": boxof.findtext("prfplcnm"),
                    "seatcnt": int(boxof.findtext("seatcnt") or 0),
                    "rnum": int(boxof.findtext("rnum") or 0),
                    "poster": boxof.findtext("poster"),
                    "prfpd": boxof.findtext("prfpd"),
                    "mt20id": boxof.findtext("mt20id"),
                    "prfnm": boxof.findtext("prfnm"),
                    "cate": boxof.findtext("cate"),
                    "prfdtcnt": int(boxof.findtext("prfdtcnt") or 0),
                    "area": boxof.findtext("area"),
                }
                aggregated_data.append(data)
        except requests.exceptions.RequestException as e:
            print(f"[박스오피스] HTTP 요청 실패: 지역 {region} / {e}")
        except ET.ParseError as e:
            print(f"[박스오피스] XML 파싱 오류: 지역 {region} / {e}")

    if not aggregated_data:
        print("[박스오피스] 적재할 데이터가 없습니다.")
        return

    # JSON 파일로 저장
    with open(LOCAL_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)

    print(f"[박스오피스] JSON 저장 완료: {LOCAL_JSON_PATH}")

def load_boxoffice_to_redshift(**kwargs):
    """
    로컬 JSON 데이터를 Redshift에 적재
    """
    if not os.path.exists(LOCAL_JSON_PATH):
        print(f"[박스오피스] JSON 파일이 존재하지 않습니다: {LOCAL_JSON_PATH}")
        return

    with open(LOCAL_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        print("[박스오피스] JSON 파일이 비어 있습니다.")
        return

    rows = [
        (
            item["prfplcnm"],
            item["seatcnt"],
            item["rnum"],
            item["poster"],
            item["prfpd"],
            item["mt20id"],
            item["prfnm"],
            item["cate"],
            item["prfdtcnt"],
            item["area"],
        )
        for item in data
    ]

    redshift_hook = PostgresHook(postgres_conn_id="myredshift")

    try:
        redshift_hook.run("TRUNCATE TABLE kopis_boxoffice;")
        print("[박스오피스] 기존 데이터 삭제 완료.")
    except Exception as e:
        print(f"[박스오피스] Redshift 테이블 TRUNCATE 실패: {e}")

    try:
        redshift_hook.insert_rows(
            table="kopis_boxoffice",
            rows=rows,
            target_fields=["prfplcnm", "seatcnt", "rnum", "poster", "prfpd", "mt20id", "prfnm", "cate", "prfdtcnt", "area"],
            commit_every=100
        )
        print(f"[박스오피스] {len(rows)} 건의 데이터가 Redshift에 적재되었습니다.")
    except Exception as e:
        print(f"[박스오피스] Redshift 데이터 적재 실패: {e}")


# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kopis_boxoffice_to_redshift',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description="예매상황판 박스오피스 데이터를 JSON 저장 후 Redshift에 적재"
) as dag:

    task_fetch_boxoffice = PythonOperator(
        task_id='fetch_boxoffice_data',
        python_callable=fetch_boxoffice_data
    )

    task_load_redshift = PythonOperator(
        task_id='load_boxoffice_to_redshift',
        python_callable=load_boxoffice_to_redshift
    )

    task_fetch_boxoffice >> task_load_redshift  # 순차 실행
