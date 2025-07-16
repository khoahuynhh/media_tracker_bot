# Sử dụng một base image Python gọn nhẹ
FROM python:3.9-slim

# Thiết lập biến môi trường để Python không buffer output
ENV PYTHONUNBUFFERED 1

# Tạo và thiết lập thư mục làm việc bên trong container
WORKDIR /app

# Cài đặt các gói hệ thống cần thiết cho Playwright
RUN apt-get update && apt-get install -y \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Sao chép file requirements.txt và cài đặt các thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Cài đặt trình duyệt cho Playwright bên trong container
RUN playwright install

# Sao chép toàn bộ mã nguồn và các file cấu hình vào container
COPY ./src ./src
COPY ./config ./config
COPY ./static ./static

# Expose cổng mà Uvicorn sẽ chạy
EXPOSE 8000

# Lệnh để khởi động ứng dụng khi container chạy
# Sử dụng host 0.0.0.0 để có thể truy cập từ bên ngoài container
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
