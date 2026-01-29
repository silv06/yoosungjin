import ee
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client
import json
import os


# 1. 환경 변수(금고)에서 정보 가져오기
# 깃허브 금고에 넣은 내용들을 가져옵니다.
gee_key_json = os.getenv('GEE_SERVICE_ACCOUNT_KEY')
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

# 2. GEE 초기화
try:
    # 금고에서 꺼낸 JSON 텍스트를 파이썬 사전(dict) 형태로 변환
    service_account_info = json.loads(gee_key_json)
    credentials = ee.ServiceAccountCredentials(service_account_info['client_email'], info=service_account_info)
    ee.Initialize(credentials, project='absolute-cache-478407-p5')
except Exception as e:
    print(f"인증 실패: {e}")
    # 로컬 테스트용 백업 (환경변수가 없을 때만 작동)
    ee.Initialize(project='absolute-cache-478407-p5')

# 3. Supabase 초기화
supabase = create_client(supabase_url, supabase_key)

# --- 이후 분석 로직(기존과 동일) ---
metadata = supabase.table("oreum_metadata").select("id, x_coord, y_coord").execute().data

def add_all_indices(img):
    v = {'NIR': img.select('B8'), 'RED': img.select('B4'), 'BLUE': img.select('B2'), 
         'SWIR1': img.select('B11'), 'SWIR2': img.select('B12')}
    return img.addBands([
        img.normalizedDifference(['B3', 'B8']).rename('muddy_index'),
        img.expression('2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))', v).rename('green_visual_index'),
        img.expression('(NIR - (SWIR1 - SWIR2)) / (NIR + (SWIR1 - SWIR2))', v).rename('fire_risk_index'),
        img.expression('((SWIR1 + RED) - (NIR + BLUE)) / ((SWIR1 + RED) + (NIR + BLUE))', v).rename('erosion_index')
    ])

features = ee.FeatureCollection([
    ee.Feature(ee.Geometry.Point([m['x_coord'], m['y_coord']]), {'oreum_id': m['id']})
    for m in metadata
])

today = datetime.now()
today_str = today.strftime('%Y-%m-%d')
start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')

latest_image = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                .filterDate(start_date, today_str)
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
                .map(add_all_indices)
                .median())

results = latest_image.reduceRegions(collection=features, reducer=ee.Reducer.mean(), scale=10).getInfo()

data_dict = {}
for f in results['features']:
    props = f['properties']
    o_id = props.get('oreum_id')
    if o_id and props.get('muddy_index') is not None:
        data_dict[o_id] = {
            "oreum_id": o_id, "date": today_str,
            "muddy_index": props.get('muddy_index'),
            "green_visual_index": props.get('green_visual_index'),
            "fire_risk_index": props.get('fire_risk_index'),
            "erosion_index": props.get('erosion_index')
        }

data_to_insert = list(data_dict.values())

if data_to_insert:
    try:
        supabase.table("oreum_daily_stats").upsert(data_to_insert, on_conflict="oreum_id, date").execute()
        print(f"[{datetime.now()}] 자동 업데이트 성공: {len(data_to_insert)}건 저장.")
    except Exception as e:
        print(f"[{datetime.now()}] 저장 오류: {e}")