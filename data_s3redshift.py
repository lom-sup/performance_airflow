# 인증키
#b334b3a40adc4d40bc3211b94b1c7d11
import os
import calendar
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable


SERVICE_KEY = Variable.get("KOPIS_SERVICE_KEY", default_var="b334b3a40adc4d40bc3211b94b1c7d11")
S3_BUCKET = "performance-airflow-e2e"
S3_FOLDER = "kopis_data"               # 모든 결과 파일을 저장할 S3 폴더
AWS_CONN_ID = "myaws"                  # AWS 인증을 위한 Airflow Connection 이름


# Task 1: 공연 목록(2024년, 공연상태코드 02) 조회 및 월별 구분하여 S3 업로드


def fetch_and_upload_kopis_data(**kwargs):
    """
    2024년 한 해 동안 월별로 Kopis 공연 목록 데이터를(공연상태코드 02)
    조회하여 각 달마다 별도의 XML 요소로 집계한 후, 하나의 XML 파일로 저장하고 S3에 업로드합니다.
    결과 파일: kopis_2024.xml
    """
    aggregated_root = ET.Element("kopis_data")
    base_url = "http://www.kopis.or.kr/openApi/restful/pblprfr"
    
    # 2024년 1월부터 12월까지 각 달에 대해 데이터를 조회
    for month in range(1, 13):
        # 각 달을 나타내는 XML 요소 생성 (예: period="202401")
        month_elem = ET.Element("month", attrib={"period": f"2024{month:02d}"})
        start_date = datetime(2024, month, 1)
        last_day = calendar.monthrange(2024, month)[1]
        end_date = datetime(2024, month, last_day)
        
        stdate_str = start_date.strftime("%Y%m%d")
        eddate_str = end_date.strftime("%Y%m%d")
        cpage = 1
        
        while True:
            params = {
                "service": SERVICE_KEY,
                "stdate": stdate_str,
                "eddate": eddate_str,
                "cpage": str(cpage),
                "rows": "100",
                "prfstate": "02"  # 공연상태코드 02
            }
            
            response = requests.get(base_url, params=params, timeout= 30)
            if response.status_code != 200:
                print(f"[공연목록] HTTP {response.status_code} 에러: {stdate_str} ~ {eddate_str}, 페이지 {cpage}")
                break
            
            try:
                root_resp = ET.fromstring(response.content)
            except ET.ParseError as e:
                print(f"[공연목록] XML 파싱 오류: {e} / {stdate_str} ~ {eddate_str}, 페이지 {cpage}")
                break
            
            db_elements = root_resp.findall("db")
            if not db_elements:
                print(f"[공연목록] 데이터 없음: {stdate_str} ~ {eddate_str}, 페이지 {cpage}")
                break
            
            for db in db_elements:
                month_elem.append(db)
            
            if len(db_elements) < 100:
                break
            cpage += 1
        
        aggregated_root.append(month_elem)
        
    
    # 집계된 XML 데이터를 로컬 파일에 저장
    output_path = "/opt/airflow/data/kopis_2024.xml"
    tree = ET.ElementTree(aggregated_root)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    print(f"[공연목록] 집계 파일 저장: {output_path}")
    
    # S3 업로드
    s3_key = f"{S3_FOLDER}/kopis_2024.xml"
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_file(filename=output_path, key=s3_key, bucket_name=S3_BUCKET, replace=True)
    print(f"[공연목록] S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")


# Task 2: 공연 상세정보 조회 및 S3 업로드


def fetch_performance_details(**kwargs):
    """
    Task 1에서 생성된 공연 목록 파일(kopis_2024.xml)에서 공연 ID를 추출하여,
    각 공연의 상세정보를 조회한 후 하나의 XML 파일로 집계하고,
    로컬 파일 및 S3에 업로드합니다.
    결과 파일: kopis_2024_details.xml
    """
    base_url = "http://www.kopis.or.kr/openApi/restful/pblprfr"
    input_file = "/opt/airflow/data/kopis_2024.xml"
    
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"[공연상세] 입력 파일이 없습니다: {input_file}")
    
    tree = ET.parse(input_file)
    root = tree.getroot()
    
    performance_ids = [db.find('mt20id').text for db in root.findall(".//db") if db.find('mt20id') is not None]
    print(f"[공연상세] 추출된 공연 ID 수: {len(performance_ids)}")
    
    details_root = ET.Element("kopis_details")
    for mt20id in performance_ids:
        detail_url = f"{base_url}/{mt20id}"
        params = {"service": SERVICE_KEY}
        try:
            response = requests.get(detail_url, params=params, timeout=30)
            if response.status_code != 200:
                print(f"[공연상세] {mt20id} HTTP {response.status_code} 에러")
                continue
            detail_xml = ET.fromstring(response.content)
            db_elements = detail_xml.findall("db")
            if not db_elements:
                print(f"[공연상세] {mt20id} 상세정보 없음")
                continue
            for db in db_elements:
                details_root.append(db)
            print(f"[공연상세] {mt20id} 조회 성공")
        except Exception as e:
            print(f"[공연상세] {mt20id} 조회 중 예외 발생: {e}")
            continue
    
    output_file = "/opt/airflow/data/kopis_2024_details.xml"
    details_tree = ET.ElementTree(details_root)
    details_tree.write(output_file, encoding="utf-8", xml_declaration=True)
    print(f"[공연상세] 집계 파일 저장: {output_file}")
    
    s3_key = f"{S3_FOLDER}/kopis_2024_details.xml"
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_file(filename=output_file, key=s3_key, bucket_name=S3_BUCKET, replace=True)
    print(f"[공연상세] S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")



# Task 3: 지역별 통계 조회 및 월별 구분하여 S3 업로드


def fetch_region_stats(**kwargs):
    """
    2024년 1월 1일부터 12월 31일까지의 지역별 통계 데이터를
    Kopis API (prfstsArea)를 각 달 단위로 조회하여, 월별 구분한 XML 요소로 집계한 후,
    로컬 파일 및 S3에 업로드합니다.
    결과 파일: kopis_2024_region_stats.xml
    """
    base_url = "http://www.kopis.or.kr/openApi/restful/prfstsArea"
    aggregated_root = ET.Element("prfsts_aggregated")
    
    for month in range(1, 13):
        month_elem = ET.Element("month", attrib={"period": f"2024{month:02d}"})
        start_date = datetime(2024, month, 1)
        last_day = calendar.monthrange(2024, month)[1]
        end_date = datetime(2024, month, last_day)
        
        stdate_str = start_date.strftime("%Y%m%d")
        eddate_str = end_date.strftime("%Y%m%d")
        params = {
            "service": SERVICE_KEY,
            "stdate": stdate_str,
            "eddate": eddate_str
        }
        print(f"[지역통계] 요청: {stdate_str} ~ {eddate_str}")
        response = requests.get(base_url, params=params, timeout=30)
        if response.status_code != 200:
            print(f"[지역통계] HTTP {response.status_code} 에러: {stdate_str} ~ {eddate_str}")
            continue
        
        try:
            root_resp = ET.fromstring(response.content)
        except Exception as e:
            print(f"[지역통계] XML 파싱 오류: {e} / {stdate_str} ~ {eddate_str}")
            continue
        
        prfst_elements = root_resp.findall("prfst")
        if prfst_elements:
            for prfst in prfst_elements:
                month_elem.append(prfst)
            print(f"[지역통계] 데이터 집계됨: {stdate_str} ~ {eddate_str}")
        else:
            print(f"[지역통계] 조회된 데이터 없음: {stdate_str} ~ {eddate_str}")
        
        aggregated_root.append(month_elem)
    
    output_path = "/opt/airflow/data/kopis_2024_region_stats.xml"
    tree = ET.ElementTree(aggregated_root)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    print(f"[지역통계] 집계 파일 저장: {output_path}")
    
    s3_key = f"{S3_FOLDER}/kopis_2024_region_stats.xml"
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_file(filename=output_path, key=s3_key, bucket_name=S3_BUCKET, replace=True)
    print(f"[지역통계] S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")


# Task 4: 장르별 통계 조회 및 월별 구분하여 S3 업로드


def fetch_genre_stats(**kwargs):
    """
    2024년 1월 1일부터 12월 31일까지의 장르별 통계 데이터를
    Kopis API (prfstsCate)를 각 달 단위로 조회하여, 월별 구분한 XML 요소로 집계한 후,
    로컬 파일 및 S3에 업로드합니다.
    결과 파일: kopis_2024_genre_stats.xml
    """
    base_url = "http://www.kopis.or.kr/openApi/restful/prfstsCate"
    aggregated_root = ET.Element("prfsts_aggregated")
    
    for month in range(1, 13):
        month_elem = ET.Element("month", attrib={"period": f"2024{month:02d}"})
        start_date = datetime(2024, month, 1)
        last_day = calendar.monthrange(2024, month)[1]
        end_date = datetime(2024, month, last_day)
        
        stdate_str = start_date.strftime("%Y%m%d")
        eddate_str = end_date.strftime("%Y%m%d")
        params = {
            "service": SERVICE_KEY,
            "stdate": stdate_str,
            "eddate": eddate_str
        }
        print(f"[장르통계] 요청: {stdate_str} ~ {eddate_str}")
        response = requests.get(base_url, params=params, timeout=30)
        if response.status_code != 200:
            print(f"[장르통계] HTTP {response.status_code} 에러: {stdate_str} ~ {eddate_str}")
            continue
        
        try:
            root_resp = ET.fromstring(response.content)
        except Exception as e:
            print(f"[장르통계] XML 파싱 오류: {e} / {stdate_str} ~ {eddate_str}")
            continue
        
        prfst_elements = root_resp.findall("prfst")
        if prfst_elements:
            for prfst in prfst_elements:
                month_elem.append(prfst)
            print(f"[장르통계] 데이터 집계됨: {stdate_str} ~ {eddate_str}")
        else:
            print(f"[장르통계] 조회된 데이터 없음: {stdate_str} ~ {eddate_str}")
        
        aggregated_root.append(month_elem)
    
    output_path = "/opt/airflow/data/kopis_2024_genre_stats.xml"
    tree = ET.ElementTree(aggregated_root)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    print(f"[장르통계] 집계 파일 저장: {output_path}")
    
    s3_key = f"{S3_FOLDER}/kopis_2024_genre_stats.xml"
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_file(filename=output_path, key=s3_key, bucket_name=S3_BUCKET, replace=True)
    print(f"[장르통계] S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")
    

# Task 5: 기간간별 통계 조회 및 월별 구분하여 S3 업로드


def fetch_period_stats(**kwargs):
    """
    2018년 1월 1일부터 2024년 12월 31일까지의 기간별 통계 데이터를
    Kopis API (prfstsTotal)를 각 년/월 단위로 조회하여, 월별 구분한 XML 요소로 집계한 후,
    로컬 파일 및 S3에 업로드합니다.
    파일명 예시: kopis_2018_period_stats.xml, kopis_2019_period_stats.xml, …, kopis_2024_period_stats.xml
    """
    base_url = "http://www.kopis.or.kr/openApi/restful/prfstsTotal"
    aggregated_root = ET.Element("prfsts_aggregated")
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    # 2018년부터 2024년까지 각 연도별로 처리
    for year in range(2018, 2025):
        aggregated_root = ET.Element("prfsts_aggregated")
        
        # 각 연도의 1월부터 12월까지 데이터 조회
        for month in range(1, 13):
            month_elem = ET.Element("month", attrib={"period": f"{year}{month:02d}"})
            start_date = datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime(year, month, last_day)
            
            stdate_str = start_date.strftime("%Y%m%d")
            eddate_str = end_date.strftime("%Y%m%d")
            params = {
                "service": SERVICE_KEY,
                "ststype": "day",
                "stdate": stdate_str,
                "eddate": eddate_str
            }
            print(f"[기간통계] {year}년 {month:02d}월 요청: {stdate_str} ~ {eddate_str}")
            try:
                response = requests.get(base_url, params=params, timeout=30)
            except Exception as e:
                print(f"[기간통계] {year}-{month:02d} 요청 중 예외 발생: {e} / {stdate_str} ~ {eddate_str}")
                continue
            
            if response.status_code != 200:
                print(f"[기간통계] {year}-{month:02d} HTTP {response.status_code} 에러: {stdate_str} ~ {eddate_str}")
                continue
            
            try:
                root_resp = ET.fromstring(response.content)
            except Exception as e:
                print(f"[기간통계] {year}-{month:02d} XML 파싱 오류: {e} / {stdate_str} ~ {eddate_str}")
                continue
            
            prfst_elements = root_resp.findall("prfst")
            if prfst_elements:
                for prfst in prfst_elements:
                    month_elem.append(prfst)
                print(f"[기간통계] {year}-{month:02d} 데이터 집계됨")
            else:
                print(f"[기간통계] {year}-{month:02d} 조회된 데이터 없음")
            
            aggregated_root.append(month_elem)
        
        # 연도별 집계 파일 생성 및 저장
        output_path = f"/opt/airflow/data/kopis_{year}_period_stats.xml"
        tree = ET.ElementTree(aggregated_root)
        tree.write(output_path, encoding="utf-8", xml_declaration=True)
        print(f"[기간통계] {year} 집계 파일 저장: {output_path}")
        
        # S3에 업로드 (파일명에 연도 포함)
        s3_key = f"{S3_FOLDER}/kopis_{year}_period_stats.xml"
        s3_hook.load_file(filename=output_path, key=s3_key, bucket_name=S3_BUCKET, replace=True)
        print(f"[기간통계] {year} S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")


# DAG 정의 및 Task 순서 지정


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),  # 필요에 따라 조정
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kopis_full_pipeline_2024',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    description="Kopis 데이터 파이프라인: 공연 목록(월별 구분), 상세정보, 지역별 및 장르, 기간별 통계(월별 구분)를 순차적으로 조회하여 S3에 업로드"
) as dag:
    
    task1 = PythonOperator(
        task_id='fetch_performance_list',
        python_callable=fetch_and_upload_kopis_data
    )
    
    task2 = PythonOperator(
        task_id='fetch_performance_details',
        python_callable=fetch_performance_details
    )
    
    task3 = PythonOperator(
        task_id='fetch_region_stats',
        python_callable=fetch_region_stats
    )
    
    task4 = PythonOperator(
        task_id='fetch_genre_stats',
        python_callable=fetch_genre_stats
    )
    
    task5 = PythonOperator(
        task_id = 'fetch_period_stats',
        python_callable=fetch_period_stats
    )
    
    # 순차적 실행: 공연 목록 → 공연 상세정보 → 지역별 통계 → 장르별 통계
    task1 >> task2 >> task3 >> task4 >> task5
