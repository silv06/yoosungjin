import ee
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client
import json
import os
import sys

# ---------------------------------------------------------
# [필수] requirements.txt에 'google-auth'가 꼭 있어야 합니다.
# ---------------------------------------------------------
from google.oauth2.service_account import Credentials 

# --- 1. 환경 변수 점검 ---
print("🔍 환경 변수 점검을 시작합니다...")

gee_key_json = os.getenv('GEE_SERVICE_ACCOUNT_KEY')
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

if not gee_key_json:
    print("❌ [오류] GEE_SERVICE_ACCOUNT_KEY가 비어있습니다.")
    sys.exit(1)
if not supabase_url or not supabase_key:
    print("❌ [오류] Supabase 설정이 비어있습니다.")
    sys.exit(1)

# --- 2. GEE 초기화 (만능 키 처리) ---
try:
    # 1. JSON 텍스트를 딕셔너리로 변환
    service_account_info = json.loads(gee_key_json)

    # 🚨 [핵심] 키 포맷 강제 교정 (사용자 요청 반영)
    if 'private_key' in service_account_info:
        raw_key = service_account_info['private_key']
        
        # (1) '/n'이 있으면 진짜 줄바꿈으로 변경
        if '/n' in raw_key:
            print("🔧 키에서 '/n'을 발견하여 줄바꿈으로 교체합니다.")
            raw_key = raw_key.replace('/n', '\n')
            
        # (2) '\\n'이 있으면 진짜 줄바꿈으로 변경 (안전장치)
        if '\\n' in raw_key:
            raw_key = raw_key.replace('\\n', '\n')
            
        service_account_info['private_key'] = raw_key

    # 2. 구글 인증 객체 생성 (이 방식은 에러가 안 납니다)
    scopes = ['https://www.googleapis.com/auth/earthengine']
    credentials = Credentials.from_service_account_info(
        service_account_info, 
        scopes=scopes
    )

    # 3. 초기화
    ee.Initialize(credentials=credentials, project='absolute-cache-478407-p5')
    print("✅ Google Earth Engine 인증 성공!")

except json.JSONDecodeError:
    print("❌ [인증 실패] GEE 키가 올바른 JSON 형식이 아닙니다.")
    sys.exit(1)
except Exception as e:
    print(f"❌ 인증 초기화 중 오류: {e}")
    sys.exit(1)

# --- 3. Supabase 연결 ---
try:
    supabase = create_client(supabase_url, supabase_key)
    metadata = supabase.table("oreum_metadata").select("id, x_coord, y_coord").execute().data
    if not metadata:
        print("⚠️ [주의] 분석할 오름 데이터(metadata)가 없습니다.")
        sys.exit(0)
except Exception as e:
    print(f"❌ Supabase 연결 오류: {e}")
    sys.exit(1)

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
    print(f"❌ 분석 및 저장 중 에러: {e}")
    sys.exit(1)