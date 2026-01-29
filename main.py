import ee
import os
import sys
import json
from datetime import datetime, timedelta
from supabase import create_client
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account

# --- 1. 환경 변수 점검 ---
print("🔍 환경 변수 및 인증 점검...")

gee_earth_engine_key=os.getenv('GEE_SERVICE_ACCOUNT_KEY')
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

if not supabase_url or not supabase_key:
    print("❌ [오류] Supabase 설정이 비어있습니다.")
    sys.exit(1)

# --- 2. GEE 초기화 (수정됨) ---
EE_PROJECT_ID = os.environ["absolute-cache-478407-p5D"]

service_account_info = json.loads(
    os.environ["GEE_SERVICE_ACCOUNT_KEY"]
)

credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/earthengine"]
)

ee.Initialize(credentials, project=EE_PROJECT_ID)

# --- 3. Supabase 연결 ---
try:
    supabase = create_client(supabase_url, supabase_key)
    # 데이터가 있는지 살짝 찔러보기
    metadata = supabase.table("oreum_metadata").select("id").limit(1).execute().data
    if not metadata:
        print("⚠️ 오름 메타데이터 테이블이 비어있거나 읽을 수 없습니다.")
        # 데이터가 없어도 에러는 아니므로 종료하지 않음 (상황에 따라 다름)
except Exception as e:
    print(f"❌ Supabase 연결 오류: {e}")
    sys.exit(1)

# --- 4. 분석 시작 ---
print("🛰️ 위성 분석 시작...")

# 메타데이터 전체 가져오기
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

try:
    latest_image = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                    .filterDate(start_date, today_str)
                    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
                    .map(add_all_indices)
                    .median())

    # reduceRegions는 이미지 범위 내의 Feature에 대해서만 계산합니다.
    # FeatureCollection이 비어있으면 에러가 날 수 있으니 체크
    if not metadata:
        print("☁️ 분석할 오름 데이터가 없습니다.")
        sys.exit(0)

    results = latest_image.reduceRegions(collection=features, reducer=ee.Reducer.mean(), scale=10).getInfo()

    data_dict = {}
    for f in results['features']:
        props = f['properties']
        o_id = props.get('oreum_id')
        
        # 값이 계산된 경우만 (None이 아닌 경우)
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
        print("☁️ 구름이 많거나 유효한 위성 데이터가 없습니다.")

except Exception as e:
    print(f"❌ 분석 중 에러: {e}")
    sys.exit(1)