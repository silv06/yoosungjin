import ee
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client
import json
import os
import sys

# --- 1. 환경 변수 확인 (디버깅용) ---
print("환경 변수 점검을 시작합니다...")

gee_key_json = os.getenv('GEE_SERVICE_ACCOUNT_KEY')
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

# 금고가 잘 연결되었는지 확인 (내용은 보안상 출력하지 않음)
if not gee_key_json:
    print("❌ 오류: 'GEE_SERVICE_ACCOUNT_KEY'가 비어있습니다! 깃허브 Secrets 설정을 확인하세요.")
    sys.exit(1) # 강제 종료
else:
    print(f"✅ GEE 키 확인됨 (길이: {len(gee_key_json)} 자)")

if not supabase_url or not supabase_key:
    print("❌ 오류: Supabase 설정이 비어있습니다! 깃허브 Secrets 설정을 확인하세요.")
    sys.exit(1)
else:
    print("✅ Supabase 설정 확인됨")


# --- 2. GEE 초기화 ---
try:
    service_account_info = json.loads(gee_key_json)
    credentials = ee.ServiceAccountCredentials(service_account_info['client_email'], info=service_account_info)
    ee.Initialize(credentials, project='absolute-cache-478407-p5')
    print("✅ Google Earth Engine 인증 성공!")
except json.JSONDecodeError:
    print("❌ 오류: GEE 키가 올바른 JSON 형식이 아닙니다. 복사/붙여넣기가 잘못되었을 수 있습니다.")
    sys.exit(1)
except Exception as e:
    print(f"❌ 인증 초기화 중 알 수 없는 오류: {e}")
    sys.exit(1)


# --- 3. Supabase 초기화 ---
try:
    supabase = create_client(supabase_url, supabase_key)
    # 테스트로 데이터 한번 읽어보기
    metadata = supabase.table("oreum_metadata").select("id, x_coord, y_coord").execute().data
    print(f"✅ Supabase 연결 성공! (오름 {len(metadata)}개 로드됨)")
except Exception as e:
    print(f"❌ Supabase 연결 실패: {e}")
    sys.exit(1)


# --- 4. 분석 로직 (기존과 동일) ---
print("🛰️ 위성 이미지 분석 시작...")

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
        # 값이 있는 경우에만 저장
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
        print(f"[{datetime.now()}] 🎉 자동 업데이트 성공: {len(data_to_insert)}건 저장 완료.")
    else:
        print(f"[{datetime.now()}] ☁️ 저장할 데이터가 없습니다 (구름이 많거나 데이터 부족).")

except Exception as e:
    print(f"❌ 분석 중 오류 발생: {e}")