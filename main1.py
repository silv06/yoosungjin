import ee
import os
import sys
import json
from datetime import datetime, timedelta
from supabase import create_client

# --- 1. 환경 변수 점검 ---
print("🔍 환경 변수 및 인증 점검...")

gee_key_json = os.getenv('GEE_SERVICE_ACCOUNT_KEY')
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')

if not gee_key_json:
    print("❌ [오류] GEE_SERVICE_ACCOUNT_KEY가 비어있습니다.")
    sys.exit(1)

if not supabase_url or not supabase_key:
    print("❌ [오류] Supabase 설정이 비어있습니다.")
    sys.exit(1)

print(f"✅ 환경 변수 확인 완료")

# --- 2. GEE 초기화 (Service Account 방식) ---
print("🛰️ Google Earth Engine 초기화 중...")

try:
    # Windows에서 복사 시 /n으로 저장된 경우 자동 수정
    gee_key_json_fixed = gee_key_json.replace('/n', '\n')
    
    # JSON 파싱
    service_account_info = json.loads(gee_key_json_fixed)
    print(f"✅ JSON 파싱 성공")
    
    # Private Key 추출 및 개행 문자 정규화
    private_key = service_account_info['private_key']
    if '/n' in private_key:
        private_key = private_key.replace('/n', '\n')
    
    # ⭐ 핵심: Service Account Credentials 생성
    credentials = ee.ServiceAccountCredentials(
        email=service_account_info['client_email'],
        key_data=private_key
    )
    
    # ⭐ 핵심: ee.Authenticate() 호출 없이 바로 Initialize
    # credentials 파라미터로 Service Account 전달
    ee.Initialize(
        credentials=credentials,
        project=service_account_info.get('project_id', 'absolute-cache-478407-p5')
    )
    
    print("✅ GEE 인증 성공!")
    print(f"   Project: {service_account_info.get('project_id')}")
    print(f"   Service Account: {service_account_info['client_email']}")

except json.JSONDecodeError as e:
    print(f"❌ JSON 파싱 실패: {e}")
    sys.exit(1)
except KeyError as e:
    print(f"❌ JSON에 필수 필드 없음: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ GEE 인증 실패: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# --- 3. Supabase 연결 ---
print("🔗 Supabase 연결 중...")

try:
    supabase = create_client(supabase_url, supabase_key)
    test_query = supabase.table("oreum_metadata").select("id").limit(1).execute().data
    if not test_query:
        print("⚠️  오름 메타데이터 테이블이 비어있습니다.")
    else:
        print("✅ Supabase 연결 성공!")
except Exception as e:
    print(f"❌ Supabase 연결 오류: {e}")
    sys.exit(1)

# --- 4. 분석 시작 ---
print("🛰️ 위성 분석 시작...")

metadata = supabase.table("oreum_metadata").select("id, x_coord, y_coord").execute().data

if not metadata:
    print("☁️ 분석할 오름 데이터가 없습니다.")
    sys.exit(0)

print(f"📍 분석 대상: {len(metadata)}개 오름")

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

print(f"📅 분석 기간: {start_date} ~ {today_str}")

try:
    print("🔍 위성 이미지 수집 중...")
    latest_image = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                    .filterDate(start_date, today_str)
                    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
                    .map(add_all_indices)
                    .median())

    print("📊 지수 계산 중...")
    results = latest_image.reduceRegions(
        collection=features, 
        reducer=ee.Reducer.mean(), 
        scale=10
    ).getInfo()

    data_dict = {}
    for f in results['features']:
        props = f['properties']
        o_id = props.get('oreum_id')
        
        if o_id and props.get('muddy_index') is not None:
            data_dict[o_id] = {
                "oreum_id": o_id, 
                "date": today_str,
                "muddy_index": props.get('muddy_index'),
                "green_visual_index": props.get('green_visual_index'),
                "fire_risk_index": props.get('fire_risk_index'),
                "erosion_index": props.get('erosion_index')
            }
            
    data_to_insert = list(data_dict.values())
    
    if data_to_insert:
        print(f"💾 데이터베이스 저장 중... ({len(data_to_insert)}건)")
        supabase.table("oreum_daily_stats").upsert(
            data_to_insert, 
            on_conflict="oreum_id, date"
        ).execute()
        print(f"🎉 성공! {len(data_to_insert)}건 저장 완료.")
    else:
        print("☁️ 유효한 위성 데이터가 없습니다.")

except Exception as e:
    print(f"❌ 분석 중 에러: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n✅ 모든 작업 완료!")