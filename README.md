# Media Tracker Bot - Hệ Thống Theo Dõi Truyền Thông

Đây là dự án backend cho một hệ thống theo dõi truyền thông, thu thập tin tức về các đối thủ cạnh tranh trong ngành FMCG tại Việt Nam. Hệ thống sử dụng kiến trúc Multi-Agent, được xây dựng bằng Python với FastAPI và thư viện `agno`.

## ✨ Tính Năng Chính

-   **Thu thập Tự động:** Tự động crawl tin tức từ hàng trăm nguồn báo chí, website tại Việt Nam.
-   **Phân tích Thông minh:** Sử dụng LLM (GPT, Groq) để phân tích, phân loại nội dung, xác định ngành hàng, nhãn hiệu và các cụm nội dung chính (CSR, Marketing, Ra mắt sản phẩm...).
-   **Tạo Báo cáo:** Tự động tổng hợp dữ liệu và tạo báo cáo phân tích đối thủ cạnh tranh chi tiết.
-   **API Toàn diện:** Cung cấp các API endpoint để chạy pipeline, lấy trạng thái, xem báo cáo và cấu hình hệ thống.
-   **Kiến trúc Hiện đại:** Được xây dựng theo kiến trúc module hóa (Service Layer, Agents, Parsing), dễ dàng bảo trì và mở rộng.
-   **Sẵn sàng cho Cloud:** Đóng gói bằng Docker để dễ dàng triển khai trên các nền tảng đám mây như AWS.

## 📂 Cấu Trúc Dự Án

Dự án được cấu trúc theo mô hình tách biệt các mối quan tâm để tăng tính module và dễ kiểm thử:

-   `src/`: Chứa toàn bộ mã nguồn của ứng dụng.
    -   `main.py`: Tầng API (FastAPI), điểm khởi đầu của ứng dụng.
    -   `services.py`: Tầng dịch vụ, chứa logic điều phối chính của pipeline.
    -   `agents.py`: Định nghĩa các agent (Crawler, Processor, Reporter).
    -   `parsing.py`: Module chuyên xử lý và phân tích dữ liệu thô từ LLM.
    -   `config.py`: Module quản lý việc tải cấu hình từ các file.
    -   `models.py`: Định nghĩa các cấu trúc dữ liệu bằng Pydantic.
-   `config/`: Chứa các file cấu hình có thể chỉnh sửa bởi người dùng.
-   `static/`: Chứa các file tĩnh cho frontend (HTML, CSS, JS).
-   `tests/`: Chứa bộ kiểm thử tự động bằng Pytest.
-   `Dockerfile`: File định nghĩa để đóng gói ứng dụng thành một Docker container.
-   `requirements.txt`: Danh sách các thư viện Python cần thiết.

---

## 🚀 Cài Đặt & Chạy Cục Bộ (Local)

### Yêu cầu tiên quyết

-   Python 3.9+
-   `pip` và `virtualenv`
-   Docker

### Các bước cài đặt

1.  **Clone repository:**
    ```bash
    git clone [https://github.com/khoahuynhh/media_tracker_bot.git](https://github.com/khoahuynhh/media_tracker_bot.git)
    cd media_tracker_bot
    ```

2.  **Tạo và kích hoạt môi trường ảo:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    # Trên Windows: venv\Scripts\activate
    ```

3.  **Cài đặt các thư viện cần thiết:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Cài đặt trình duyệt cho Playwright:**
    Lần đầu tiên chạy, Playwright (sử dụng bởi Crawl4AI) cần tải về các trình duyệt. Lệnh này sẽ làm điều đó.
    ```bash
    playwright install
    ```

5.  **Thiết lập cấu hình:**
    -   Tạo một file tên là `.env` ở thư mục gốc và điền các API key của bạn:
        ```env
        OPENAI_API_KEY=sk-your-openai-key
        GROQ_API_KEY=gsk_your_groq-key
        ```
    -   Thêm các file cấu hình vào thư mục `config/`:
        -   `config/keywords.json`: Định nghĩa các từ khóa theo ngành hàng.
        -   `config/media_sources.csv`: Danh sách các trang báo/website cần crawl.
        *(Bạn có thể tham khảo các file mẫu trong các câu trả lời trước)*

6.  **Chạy server:**
    ```bash
    uvicorn src.main:app --reload
    ```
    Ứng dụng sẽ chạy tại địa chỉ `http://127.0.0.1:8000`. Bạn có thể truy cập `http://127.0.0.1:8000/docs` để xem tài liệu API.

---

## 🧪 Hướng Dẫn Kiểm Thử

Dự án được trang bị một bộ kiểm thử toàn diện bằng Pytest để đảm bảo chất lượng code.

1.  **Cài đặt thư viện test:**
    Nếu bạn chưa cài từ `requirements.txt`, hãy chạy:
    ```bash
    pip install pytest pytest-mock pytest-asyncio
    ```

2.  **Chạy toàn bộ bộ test:**
    Từ thư mục gốc của dự án, chạy lệnh sau:
    ```bash
    pytest -v
    ```-v` (verbose) sẽ hiển thị chi tiết kết quả của từng bài test.

3.  **Kiểm tra độ bao phủ của test (Test Coverage):**
    Để xem các dòng code nào đã được kiểm thử, hãy cài đặt `pytest-cov` và chạy:
    ```bash
    pip install pytest-cov
    pytest --cov=src --cov-report=html
    ```
    Lệnh này sẽ tạo một thư mục `htmlcov/`, mở file `index.html` trong đó để xem báo cáo chi tiết.

---

## ☁️ Triển Khai Lên AWS (Sử dụng Docker & AWS App Runner)

Triển khai ứng dụng bằng Docker container là phương pháp hiện đại, đảm bảo môi trường chạy nhất quán từ local đến production. **AWS App Runner** là một dịch vụ lý tưởng cho việc này vì nó hoàn toàn được quản lý, tự động co giãn và rất dễ sử dụng.

### Bước 1: Tạo `Dockerfile`

Tạo một file tên là `Dockerfile` (không có đuôi file) ở thư mục gốc của dự án với nội dung sau:

```dockerfile
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
```

### Bước 2: Build và Test Docker Image ở Local

1.  **Build image:**
    ```bash
    docker build -t media-tracker-bot .
    ```

2.  **Chạy container từ image vừa build:**
    ```bash
    # Chạy container và truyền file .env vào bên trong
    docker run -p 8000:8000 --env-file .env media-tracker-bot
    ```
    Bây giờ bạn có thể truy cập `http://localhost:8000` để kiểm tra xem ứng dụng có chạy đúng trong container không.

### Bước 3: Đẩy Image Lên Amazon ECR (Elastic Container Registry)

ECR là dịch vụ lưu trữ Docker image của AWS.

1.  **Cài đặt và cấu hình AWS CLI:** Đảm bảo bạn đã cài đặt AWS CLI và cấu hình credentials.

2.  **Tạo một repository trên ECR:**
    ```bash
    aws ecr create-repository --repository-name media-tracker-bot --region ap-southeast-1
    # Thay ap-southeast-1 bằng region bạn muốn
    ```
    Lệnh này sẽ trả về một URI của repository, ví dụ: `123456789012.dkr.ecr.ap-southeast-1.amazonaws.com/media-tracker-bot`.

3.  **Login Docker vào ECR:**
    ```bash
    aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 123456789012.dkr.ecr.ap-southeast-1.amazonaws.com
    ```

4.  **Tag và Push image:**
    ```bash
    # Lấy ECR URI từ bước 2
    ECR_URI=[123456789012.dkr.ecr.ap-southeast-1.amazonaws.com/media-tracker-bot](https://123456789012.dkr.ecr.ap-southeast-1.amazonaws.com/media-tracker-bot)

    # Tag image của bạn
    docker tag media-tracker-bot:latest $ECR_URI:latest

    # Đẩy image lên ECR
    docker push $ECR_URI:latest
    ```

### Bước 4: Triển Khai Bằng AWS App Runner

1.  Truy cập **AWS Management Console** và tìm đến dịch vụ **App Runner**.
2.  Nhấn **Create an App Runner service**.
3.  **Source:**
    -   Chọn **Container registry** và **Amazon ECR**.
    -   Nhấn **Browse** và chọn repository `media-tracker-bot` bạn vừa tạo. Image tag chọn `latest`.
4.  **Service settings:**
    -   Đặt tên cho service, ví dụ `media-tracker-service`.
    -   **Virtual CPU & memory:** Chọn cấu hình phù hợp (bắt đầu với 1 vCPU, 2 GB là ổn).
    -   **Port:** Nhập `8000`.
    -   **Environment variables:** Đây là nơi bạn sẽ cung cấp các API key. **Cách tốt nhất là lưu các key trong AWS Secrets Manager** và tham chiếu chúng ở đây. Cách đơn giản hơn là thêm trực tiếp các biến môi trường:
        -   `KEY`: `OPENAI_API_KEY`, `VALUE`: `sk-...`
        -   `KEY`: `GROQ_API_KEY`, `VALUE`: `gsk_...`
5.  **Security, Networking, Observability:** Bạn có thể để các thiết lập mặc định cho lần đầu.
6.  Nhấn **Next**, sau đó **Create & deploy**. App Runner sẽ tự động lấy image từ ECR, khởi chạy container và cung cấp cho bạn một URL công khai (ví dụ: `https://<id>.awsapprunner.com`).

Vậy là xong! Ứng dụng của bạn đã được triển khai lên cloud một cách chuyên nghiệp.
