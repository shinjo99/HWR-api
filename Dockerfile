FROM python:3.11-slim

# WeasyPrint가 필요로 하는 시스템 라이브러리 + 한글 폰트 (Noto Sans CJK)
# Note: libreoffice-calc은 OOM 문제로 제거됨 (Railway 메모리 부족)
# xlsb 수식 텍스트 분석이 필요하면 로컬에서 xlsx 변환 후 업로드
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpango-1.0-0 \
    libpangoft2-1.0-0 \
    libharfbuzz0b \
    libcairo2 \
    libgdk-pixbuf-2.0-0 \
    libffi-dev \
    shared-mime-info \
    fonts-noto-cjk \
    fonts-noto-cjk-extra \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 파이썬 패키지
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 복사
COPY . .

# Railway가 $PORT를 주입 (기본 8080)
ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port $PORT"]
