import requests
import cv2
import numpy as np
import os
import time
import folium
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor
from folium.features import CustomIcon
from datetime import datetime
import json
import glob
from PIL import Image, ImageOps


# 시스템 환경 설정
CPU_COUNT = os.cpu_count()
MAX_WORKERS = min(8, CPU_COUNT * 2)

# 폴더 경로 설정
DOWNLOAD_PATH = "./test/videos"
ICON_PATH = "./test/icons"
MAP_FILE = "./test/cctv_weather_map.html"

# 날씨별 아이콘 설정
WEATHER_ICONS = {
    "맑음": os.path.join(ICON_PATH, "clear.png"),
    "흐림": os.path.join(ICON_PATH, "clear.png"),
    "비": os.path.join(ICON_PATH, "rain.png"),
    "눈": os.path.join(ICON_PATH, "snow.png"),
    "안개": os.path.join(ICON_PATH, "fog.png"),
    "분석 실패": os.path.join(ICON_PATH, "error.png"),
}


# API 인증키 불러오기
with open("./test/AuthKey_NewAPI.txt", "r", encoding="utf-8") as file:
    API_KEY = file.read().strip()

CCTV_API_URL = f"https://openapi.its.go.kr:9443/cctvInfo?apiKey={API_KEY}&type=ex&cctvType=2&minX=126.953356&maxX=127.147719&minY=37.3897&maxY=37.447492&getType=json"

cctv_weather_data = []  # 지도 데이터 저장 리스트

# 기상청 단기 예보 API URL
WEATHER_API_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"

with open("./test/WeatherForecastAPI_KEY.txt", "r", encoding="utf-8") as file:
    WeatherForecastAPI_KEY = file.read().strip()

# 기상청 API 요청 함수
def fetch_weather_data(lat, lon):
    now = datetime.now()
    base_date = now.strftime("%Y%m%d")
    timeTemp = now.time
    # 현재 시간 가져오기
    now = datetime.now()

    # "HHMM" 형식으로 변환
    timeTemp = now.strftime("%H") + "00"
    
    base_time = timeTemp  # 기상청 예보 기준 시간

    params = {
        "serviceKey": WeatherForecastAPI_KEY,
        "numOfRows": 50,        
        "pageNo": 1,
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": "0800",
        "nx": int(lon),  # 좌표 변환 필요
        "ny": int(lat),
    }

    # JSON 문자열을 Python 딕셔너리로 변환
    iterateJsonData = params

    # 재귀적으로 JSON을 탐색하는 함수
    def iterate_json(obj, indent=0):
        retString = ""
        iterJsonCount = 0

        for key, value in obj.items():
            if iterJsonCount == 0:
                retString += "?"
            else :
                retString += "&"

            print(" " * indent + f"{key}: {value}")  # 들여쓰기 추가

            retString += f"{key}={value}"
            iterJsonCount = iterJsonCount + 1

        return retString

    try:
        additionalString = iterate_json(iterateJsonData)
            
        requestFullURL = WEATHER_API_URL + additionalString

        response = requests.get(requestFullURL, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if "response" in data and "body" in data["response"]:
                items = data["response"]["body"]["items"]["item"]
                forecast = {item["category"]: item["fcstValue"] for item in items}
                
                # 하늘 상태 (SKY: 1=맑음, 3=구름 많음, 4=흐림)
                sky_code = int(forecast.get("SKY", 1))
                sky_state = {1: "맑음", 3: "흐림", 4: "흐림"}.get(sky_code, "맑음")
                
                # 강수 형태 (PTY: 0=없음, 1=비, 2=비/눈, 3=눈, 4=소나기)
                pty_code = int(forecast.get("PTY", 0))
                if pty_code == 1:
                    sky_state = "비"
                elif pty_code == 2 or pty_code == 3:
                    sky_state = "눈"
                
                return sky_state
    except Exception as e:
        print(f"❌ 기상청 API 요청 오류: {e}")
    
    return "알 수 없음" 


# CCTV 분석 함수
def analyze_weather(video_path):
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return "분석 실패"

    frame_count = 0
    fog_detected = False
    rain_detected = False
    snow_detected = False
    total_brightness = []
    contrast_values = []
    rain_ratios = []

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret or frame is None or frame_count > 10:
            break
        frame_count += 1

        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        blur = cv2.Laplacian(gray, cv2.CV_64F).var()
        brightness = np.mean(gray)

        total_brightness.append(brightness)
        contrast_values.append(blur)

        # 안개 감지 (기존 로직 유지)
        if blur < 80:
            fog_detected = True

        # 비 감지 개선: 노이즈 제거 후 패턴 분석
        blurred_gray = cv2.medianBlur(gray, 3)
        edges = cv2.Canny(blurred_gray, 75, 200)  # 임계값 조정
        rain_ratio = np.count_nonzero(edges) / edges.size
        rain_ratios.append(rain_ratio)

        # 눈 감지: 밝고 작은 점들이 많을 경우
        _, threshold = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY)
        snow_ratio = np.count_nonzero(threshold) / threshold.size
        if snow_ratio > 0.2:  # 특정 비율 이상이면 눈으로 판단
            snow_detected = True

    cap.release()

    avg_contrast = np.mean(contrast_values) if contrast_values else 0
    avg_rain_ratio = np.mean(rain_ratios) if rain_ratios else 0

    if fog_detected:
        return "안개"
    if avg_rain_ratio > 0.1:  # 기준 상향 조정
        return "비"
    if snow_detected:
        return "눈"
    if avg_contrast < 100:
        return "흐림"
    return "맑음"


prevWeather_info = ""
prevY = 0
prevX = 0

# CCTV 영상 처리 함수
def process_video(cctv):
    global cctv_weather_data

    global prevWeather_info
    global prevY
    global prevX


    url = unquote(cctv["cctvurl"])
    filename = f"{cctv['cctvname'].replace('[', '').replace(']', '')}.mp4"

    video_path = os.path.join(DOWNLOAD_PATH, filename)
    response = requests.get(url, stream=True, timeout=10)
    if response.status_code == 200:
        with open(video_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
    else:
        return False

    analyzed_weather = analyze_weather(video_path)
    integerCCTVCordY = int(cctv["coordy"]) 
    integerCCTVCordX = int(cctv["coordx"])

    weather_info = ""

    if prevY != integerCCTVCordY or prevX != integerCCTVCordX :
        weather_info = fetch_weather_data(cctv["coordy"], cctv["coordx"])
        prevY = integerCCTVCordY
        prevX = integerCCTVCordX
        prevWeather_info = weather_info
    else :
        weather_info = prevWeather_info
    

    cctv_weather_data.append({
        "name": cctv["cctvname"],
        "lat": cctv["coordy"],
        "lon": cctv["coordx"],
        "cctv_weather": analyzed_weather,
        "weather_api": weather_info,
    })

    os.remove(video_path)
    return True

# 지도 업데이트 함수
def update_map():
    global cctv_weather_data

    m = folium.Map(location=[37.5, 127.0], zoom_start=10)

    for data in cctv_weather_data:
        weather = data["cctv_weather"]
        icon_path = WEATHER_ICONS.get(weather, WEATHER_ICONS["분석 실패"])

        # 예보와 CCTV 분석 결과가 다르면 아이콘 반전
        if data["cctv_weather"] != data["weather_api"]:
            inverted_icon_path = f"{icon_path}_inverted.png"

            if not os.path.exists(inverted_icon_path):
                try:
                    img = Image.open(icon_path).convert("RGB")
                    inverted_img = ImageOps.invert(img)
                    inverted_img.save(inverted_icon_path)
                except Exception as e:
                    print(f"이미지 반전 실패: {e}")
                    inverted_icon_path = icon_path  # 실패 시 원본 사용

            icon_path = inverted_icon_path

        icon = CustomIcon(icon_path, icon_size=(30, 30))

        popup_text = f"""
        <b>{data['name']}</b><br>
        CCTV 분석 결과: {data['cctv_weather']}<br>
        기상청 예보: {data['weather_api']}
        """

        folium.Marker(
            location=[data["lat"], data["lon"]],
            icon=icon,
            popup=folium.Popup(popup_text, max_width=250),
        ).add_to(m)

    m.save(MAP_FILE)
    print(f"✅ 지도 업데이트 완료: {MAP_FILE}")


# 메인 실행
def main():
    response = requests.get(CCTV_API_URL)
    if response.status_code == 200:
        data = response.json()
        cctv_list = data["response"]["data"]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(process_video, cctv_list)

        update_map()
        print("🌍 지도 파일 생성 완료!")

    # /videos 폴더 내의 .mp4 파일 삭제
    video_folder = "./test/videos"
    mp4_files = glob.glob(os.path.join(video_folder, "*.mp4"))
    
    for file in mp4_files:
        try:
            os.remove(file)
            print(f"Deleted: {file}")
        except Exception as e:
            print(f"Error deleting {file}: {e}")

if __name__ == "__main__":
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    os.makedirs(ICON_PATH, exist_ok=True)
    main()

