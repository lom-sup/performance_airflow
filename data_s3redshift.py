# 인증키
#b334b3a40adc4d40bc3211b94b1c7d11
import os
import calendar
import requests
import xml.etree.ElementTree as ET
import json
import xmltodict
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from tenacity import retry, stop_after_attempt, wait_fixed

# Global Variables
SERVICE_KEY = Variable.get("KOPIS_SERVICE_KEY", default_var="b334b3a40adc4d40bc3211b94b1c7d11")
S3_BUCKET = "performance-airflow-e2e"  # S3 버킷명
S3_FOLDER = "kopis_data"              # S3 내 저장 폴더
AWS_CONN_ID = "myaws"                 # AWS 인증 Airflow Connection 이름

# 공통 함수

def convert_xml_to_json(xml_file, json_file):
    """XML 파일을 읽어 JSON으로 변환 후 저장."""
    with open(xml_file, 'r', encoding='utf-8') as f:
        xml_data = f.read()
    json_data = xmltodict.parse(xml_data)
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)

def get_date_range(year, month):
    """해당 연도/월의 시작일과 마지막일을 'YYYYMMDD' 포맷으로 반환."""
    start_date = datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = datetime(year, month, last_day)
    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

def save_and_upload(xml_root, xml_path, json_path, s3_key):
    """
    XML ElementTree를 파일로 저장하고, JSON으로 변환한 후 S3에 업로드.
    """
    tree = ET.ElementTree(xml_root)
    tree.write(xml_path, encoding="utf-8", xml_declaration=True)
    convert_xml_to_json(xml_path, json_path)
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_file(filename=json_path, key=s3_key, bucket_name=S3_BUCKET, replace=True)
    logging.info(f"업로드 완료: s3://{S3_BUCKET}/{s3_key}")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
def fetch_xml(url, params, timeout=30):
    """
    지정 URL과 파라미터로 API 호출 후 XML Element를 반환.
    오류 발생 시 None 반환.
    """
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()  # HTTP 오류 발생 시 예외 발생
        return ET.fromstring(response.content)
    except Exception as e:
        logging.error(f"API 호출 오류: {url} / params: {params} / {e}")
        return None

# --- 태스크별 함수 ---

def fetch_and_upload_kopis_data(**kwargs):
    """
    Task 1: 공연 목록 데이터를 분기별, 월별, 상태코드(02, 03)로 조회한 후 XML 생성,
            JSON 변환 후 S3 업로드.
    """
    base_url = "http://kopis.or.kr/openApi/restful/pblprfr"
    quarters = [("Q1", 1, 3), ("Q2", 4, 6), ("Q3", 7, 9), ("Q4", 10, 12)]
    
    for quarter, start_month, end_month in quarters:
        quarter_elem = ET.Element("quarter", attrib={"period": f"2024_{quarter}"})
        for month in range(start_month, end_month + 1):
            month_elem = ET.Element("month", attrib={"period": f"2024{month:02d}"})
            stdate, eddate = get_date_range(2024, month)
            
            # 두 상태코드(02, 03)에 대해 페이지별 조회
            for state in ["02", "03"]:
                cpage = 1
                while True:
                    params = {
                        "service": SERVICE_KEY,
                        "stdate": stdate,
                        "eddate": eddate,
                        "cpage": str(cpage),
                        "rows": "100",
                        "prfstate": state
                    }
                    root_resp = fetch_xml(base_url, params)
                    if root_resp is None:
                        break
                    db_elements = root_resp.findall("db")
                    if not db_elements:
                        logging.info(f"[공연목록] 데이터 없음: {stdate} ~ {eddate}, state {state}, page {cpage}")
                        break
                    for db in db_elements:
                        month_elem.append(db)
                    # 한 페이지에 100건 미만이면 더 이상 페이지가 없음
                    if len(db_elements) < 100:
                        break
                    cpage += 1
            quarter_elem.append(month_elem)
        
        # 파일 저장 및 S3 업로드
        xml_output_path = f"/opt/airflow/data/kopis_2024_{quarter}_stats.xml"
        json_output_path = f"/opt/airflow/data/kopis_2024_{quarter}_stats.json"
        s3_key = f"{S3_FOLDER}/kopis_2024_{quarter}_stats.json"
        save_and_upload(quarter_elem, xml_output_path, json_output_path, s3_key)

def fetch_performance_details(**kwargs):
    """
    Task 2: 기존 분기별 XML 파일을 결합하여 공연 목록을 만들고,
            중복 제거 후 각 공연 상세정보 조회 → XML 생성, JSON 변환, S3 업로드.
    """
    base_url = "http://kopis.or.kr/openApi/restful/pblprfr"
    combined_root = ET.Element("kopis_data")
    
    # 분기별 XML 파일에서 <db> 엘리먼트 결합
    for quarter in ["Q1", "Q2", "Q3", "Q4"]:
        file_path = f"/opt/airflow/data/kopis_2024_{quarter}_stats.xml"
        if os.path.exists(file_path):
            tree = ET.parse(file_path)
            root = tree.getroot()
            for month in root.findall("month"):
                for db in month.findall("db"):
                    combined_root.append(db)
    
    # 중복 제거 (mt20id 기준)
    seen = set()
    unique_ids = []
    for db in combined_root.findall("db"):
        mt20id_elem = db.find('mt20id')
        if mt20id_elem is not None:
            id_val = mt20id_elem.text
            if id_val not in seen:
                seen.add(id_val)
                unique_ids.append(id_val)
    logging.info(f"[공연상세] 추출된 고유 공연 ID 수: {len(unique_ids)}")
    
    details_root = ET.Element("kopis_details")
    for mt20id in unique_ids:
        detail_url = f"{base_url}/{mt20id}"
        params = {"service": SERVICE_KEY}
        detail_xml = fetch_xml(detail_url, params)
        if detail_xml is None:
            continue
        db_elements = detail_xml.findall("db")
        if not db_elements:
            logging.info(f"[공연상세] {mt20id} 상세정보 없음")
            continue
        for db in db_elements:
            details_root.append(db)
        logging.info(f"[공연상세] {mt20id} 조회 성공")
    
    xml_output_file = "/opt/airflow/data/kopis_2024_details.xml"
    json_output_file = "/opt/airflow/data/kopis_2024_details.json"
    s3_key = f"{S3_FOLDER}/kopis_2024_details.json"
    save_and_upload(details_root, xml_output_file, json_output_file, s3_key)

def fetch_generic_stats(task_label, base_url, year, month_range, extra_params, xml_path, s3_key):
    """
    Task 3, 4 (지역별, 장르별 통계)에서 공통으로 사용할 통계 데이터 조회 함수.
    task_label: 로깅에 사용할 태스크 이름
    base_url: API 엔드포인트
    year, month_range: 대상 연도 및 월 리스트
    extra_params: 추가 파라미터 (없으면 None)
    xml_path: 최종 XML 파일 경로
    s3_key: S3 업로드 대상 키
    """
    aggregated_root = ET.Element("prfsts_aggregated")
    for month in month_range:
        month_elem = ET.Element("month", attrib={"period": f"{year}{month:02d}"})
        stdate, eddate = get_date_range(year, month)
        params = {"service": SERVICE_KEY, "stdate": stdate, "eddate": eddate}
        if extra_params:
            params.update(extra_params)
        logging.info(f"[{task_label}] 요청: {stdate} ~ {eddate}")
        root_resp = fetch_xml(base_url, params)
        if root_resp is not None:
            prfst_elements = root_resp.findall("prfst")
            if prfst_elements:
                for prfst in prfst_elements:
                    month_elem.append(prfst)
                logging.info(f"[{task_label}] 데이터 집계됨: {stdate} ~ {eddate}")
            else:
                logging.info(f"[{task_label}] 조회된 데이터 없음: {stdate} ~ {eddate}")
        aggregated_root.append(month_elem)
    json_output_file = xml_path.replace(".xml", ".json")
    save_and_upload(aggregated_root, xml_path, json_output_file, s3_key)

def fetch_region_stats(**kwargs):
    """
    Task 3: 지역별 통계 (2024년, 월별)
    """
    base_url = "http://kopis.or.kr/openApi/restful/prfstsArea"
    fetch_generic_stats(
        task_label="지역통계",
        base_url=base_url,
        year=2024,
        month_range=range(1, 13),
        extra_params=None,
        xml_path="/opt/airflow/data/kopis_2024_region_stats.xml",
        s3_key=f"{S3_FOLDER}/kopis_2024_region_stats.json"
    )

def fetch_genre_stats(**kwargs):
    """
    Task 4: 장르별 통계 (2024년, 월별)
    """
    base_url = "http://kopis.or.kr/openApi/restful/prfstsCate"
    fetch_generic_stats(
        task_label="장르통계",
        base_url=base_url,
        year=2024,
        month_range=range(1, 13),
        extra_params=None,
        xml_path="/opt/airflow/data/kopis_2024_genre_stats.xml",
        s3_key=f"{S3_FOLDER}/kopis_2024_genre_stats.json"
    )

def fetch_period_stats(**kwargs):
    """
    Task 5: 기간별 통계 (2018~2024, 연도별/월별)
    """
    base_url = "http://kopis.or.kr/openApi/restful/prfstsTotal"
    for year in range(2018, 2025):
        aggregated_root = ET.Element("prfsts_aggregated")
        for month in range(1, 13):
            month_elem = ET.Element("month", attrib={"period": f"{year}{month:02d}"})
            stdate, eddate = get_date_range(year, month)
            params = {"service": SERVICE_KEY, "ststype": "day", "stdate": stdate, "eddate": eddate}
            logging.info(f"[기간통계] {year}-{month:02d} 요청: {stdate} ~ {eddate}")
            root_resp = fetch_xml(base_url, params)
            if root_resp is not None:
                prfst_elements = root_resp.findall("prfst")
                if prfst_elements:
                    for prfst in prfst_elements:
                        month_elem.append(prfst)
                    logging.info(f"[기간통계] {year}-{month:02d} 데이터 집계됨")
                else:
                    logging.info(f"[기간통계] {year}-{month:02d} 조회된 데이터 없음")
            aggregated_root.append(month_elem)
        xml_output_file = f"/opt/airflow/data/kopis_{year}_period_stats.xml"
        json_output_file = f"/opt/airflow/data/kopis_{year}_period_stats.json"
        s3_key = f"{S3_FOLDER}/kopis_{year}_period_stats.json"
        save_and_upload(aggregated_root, xml_output_file, json_output_file, s3_key)

# --- DAG 정의 및 Task 순서 지정 ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kopis_full_pipeline_2024',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    description="Kopis 데이터 파이프라인: XML → JSON, 공연 목록, 상세정보, 지역별, 장르별, 기간별 통계(연도별)"
) as dag:
    
    task1 = PythonOperator(
        task_id='fetch_performance_list',
        python_callable=fetch_and_upload_kopis_data,
    )
    
    task2 = PythonOperator(
        task_id='fetch_performance_details',
        python_callable=fetch_performance_details,
    )
    
    task3 = PythonOperator(
        task_id='fetch_region_stats',
        python_callable=fetch_region_stats,
    )
    
    task4 = PythonOperator(
        task_id='fetch_genre_stats',
        python_callable=fetch_genre_stats,
    )
    
    task5 = PythonOperator(
        task_id='fetch_period_stats',
        python_callable=fetch_period_stats,
    )
    
    # task1 >> task2 >> 
    task3 >> task4 >> task5
