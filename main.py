import ee
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client
import json
import os
import sys

# --- 1. 환경 변수(금고) 점검 ---
print("🔍 환경 변수 점검을 시작합니다...")

gee_key_json = os.getenv('GEE_SERVICE_ACCOUNT_KEY')
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

# 금고 확인
if not gee_key_json:
    print("❌ [치명적 오류] 'GEE_SERVICE_ACCOUNT_KEY'가 텅 비어있습니다!")
    print("👉 힌트: 깃허브 Settings > Secrets 에 오타가 있거나 값이 저장되지 않았습니다.")
    sys.exit(1) # 여기서 강제로 멈춤 (더 이상 진행 안 함)
else:
    print(f"✅ GEE 키 발견됨! (글자 수: {len(gee_key_json)} 자)")

if not supabase_url or not supabase_key:
    print("❌ [오류] Supabase 설정이 비어있습니다.")
    sys.exit(1)

# --- 2. GEE 초기화 ---
try:
    service_account_info = json.loads(gee_key_json)
    credentials = ee.ServiceAccountCredentials(service_account_info['client_email'], info=service_account_info)
    ee.Initialize(credentials, project='absolute-cache-478407-p5')
    print("✅ Google Earth Engine 인증 성공!")
except Exception as e:
    print(f"❌ 인증 초기화 중 오류: {e}")
    sys.exit(1)

# --- 3. Supabase 연결 ---
supabase = create_client(supabase_url, supabase_key)
metadata = supabase.table("oreum_metadata").select("id, x_coord, y_coord").execute().data

# --- 4. 분석 시작 ---
print("🛰️ 위성 분석 시작...")

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

try:
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
        supabase.table("oreum_daily_stats").upsert(data_to_insert, on_conflict="oreum_id, date").execute()
        print(f"🎉 성공! {len(data_to_insert)}건 저장 완료.")
    else:
        print("☁️ 구름이 많거나 데이터가 없습니다.")

except Exception as e:
    print(f"❌ 분석 중 에러: {e}")