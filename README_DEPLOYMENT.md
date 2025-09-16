# Media Tracker Bot - AWS EC2 Deployment Guide

## 📋 Mục Lục
- [Yêu Cầu Hệ Thống](#yêu-cầu-hệ-thống)
- [Phần 1: Chuẩn Bị Project Local](#phần-1-chuẩn-bị-project-local)
- [Phần 2: Tạo AWS EC2 Instance](#phần-2-tạo-aws-ec2-instance)
- [Phần 3: Build và Push Docker Image](#phần-3-build-và-push-docker-image)
- [Phần 4: Deploy trên AWS EC2](#phần-4-deploy-trên-aws-ec2)
- [Phần 5: Kiểm Tra và Quản Lý](#phần-5-kiểm-tra-và-quản-lý)
- [Troubleshooting](#troubleshooting)

---

## 🔧 Yêu Cầu Hệ Thống

### Local Development
- **Docker Desktop** (Windows/Mac) hoặc **Docker Engine** (Linux)
- **Git** để clone/manage code
- **VS Code** hoặc IDE tương tự
- **Docker Hub Account** (tạo tại [hub.docker.com](https://hub.docker.com))

### AWS Account
- **AWS Account** với quyền tạo EC2 instances
- **AWS CLI** (tùy chọn, để quản lý từ command line)

---

## 📁 Phần 1: Chuẩn Bị Project Local

### 1.1 Clone và Setup Project
```bash
# Clone project (nếu chưa có)
git clone <your-repo-url>
cd media_tracker_bot

# Kiểm tra cấu trúc project
ls -la
```

### 1.2 Cấu Hình Environment
```bash
# Tạo file .env với API keys
cat > .env << 'EOF'
OPENAI_API_KEY=sk-proj-your-openai-key-here
GROQ_API_KEY=gsk_your-groq-key-here
DEFAULT_MODEL_PROVIDER=openai
EOF

# Kiểm tra config files
ls config/
# Cần có: media_list.csv, keywords.json
```

### 1.3 Test Local (Tùy chọn)
```bash
# Build và test local
docker compose up --build

# Test endpoints
curl http://localhost:8000/health
curl http://localhost:8000/

# Stop khi test xong
docker compose down
```

---

## 🚀 Phần 2: Tạo AWS EC2 Instance

### 2.1 Login AWS Console
1. Truy cập: https://console.aws.amazon.com/
2. Login với credentials:
   - **Account:** dev_ChiCom
   - **Password:** TagDev2025
   - **Console URL:** https://865626945868.signin.aws.amazon.com/console

### 2.2 Tạo EC2 Instance

#### Bước 1: Launch Instance
1. Vào **EC2 Dashboard** → **Launch Instance**
2. **Name:** `media-tracker-bot-prod`

#### Bước 2: Chọn AMI
- **OS:** Ubuntu Server 22.04 LTS (Free tier eligible)
- **Architecture:** 64-bit (x86)

#### Bước 3: Instance Type
- **Type:** `t3.small` (Recommended) hoặc `t2.micro` (Free tier)
- **vCPUs:** 1-2, **Memory:** 1-2 GB

#### Bước 4: Key Pair
1. **Create new key pair**
2. **Name:** `media_tracker_key`
3. **Type:** RSA
4. **Format:** .pem
5. **Download và lưu an toàn** file .pem

#### Bước 5: Network Settings
- **VPC:** Default
- **Subnet:** Default
- **Auto-assign public IP:** Enable
- **Security Group:** Create new
  - **Name:** `media-tracker-sg`
  - **Description:** Security group for Media Tracker Bot

#### Bước 6: Security Group Rules
**Inbound Rules:**
```
Type            Protocol    Port Range    Source        Description
SSH             TCP         22           0.0.0.0/0      SSH access
Custom TCP      TCP         8000         0.0.0.0/0      Media Tracker Bot
HTTP            TCP         80           0.0.0.0/0      HTTP (optional)
HTTPS           TCP         443          0.0.0.0/0      HTTPS (optional)
```

#### Bước 7: Storage
- **Size:** 8-20 GB (tùy nhu cầu)
- **Type:** gp3 (General Purpose SSD)

#### Bước 8: Launch
1. Review tất cả settings
2. **Launch Instance**
3. **Lưu Instance ID và Public IP**

### 2.3 Cấu Hình SSH Key
```bash
# Windows (Git Bash/PowerShell)
# Di chuyển file .pem vào thư mục .ssh
mkdir -p ~/.ssh
mv Downloads/media_tracker_key.pem ~/.ssh/
chmod 400 ~/.ssh/media_tracker_key.pem

# Linux/Mac
chmod 400 /path/to/media_tracker_key.pem
```

---

## 🐳 Phần 3: Build và Push Docker Image

### 3.1 Login Docker Hub
```bash
# Login Docker Hub (nhập username/password)
docker login

# Verify login
docker info | grep Username
```

### 3.2 Build Production Image
```bash
# Build image với tag production
docker build -t media_tracker_bot:production .

# Verify build
docker images | grep media_tracker_bot
```

### 3.3 Tag và Push Image
```bash
# Thay 'your-dockerhub-username' bằng username thật
DOCKER_USERNAME="your-dockerhub-username"

# Tag image
docker tag media_tracker_bot:production $DOCKER_USERNAME/media_tracker_bot:latest

# Push to Docker Hub
docker push $DOCKER_USERNAME/media_tracker_bot:latest

# Verify push
echo "Image pushed: https://hub.docker.com/r/$DOCKER_USERNAME/media_tracker_bot"
```

**⚠️ LƯU Ý QUAN TRỌNG:**
- Thay `your-dockerhub-username` bằng username Docker Hub thật của bạn
- Đảm bảo repository là public hoặc EC2 có quyền pull
- Lưu lại tên image đầy đủ: `username/media_tracker_bot:latest`

---

## 🖥️ Phần 4: Deploy trên AWS EC2

### 4.1 Kết Nối SSH
```bash
# Thay YOUR_PUBLIC_IP bằng IP thật của instance
ssh -i ~/.ssh/media_tracker_key.pem ubuntu@YOUR_PUBLIC_IP

# Ví dụ:
# ssh -i ~/.ssh/media_tracker_key.pem ubuntu@16.176.142.147
```

### 4.2 Cài Đặt Dependencies
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker ubuntu
rm get-docker.sh

# Install Docker Compose
sudo apt install docker-compose-plugin -y

# Start Docker
sudo systemctl start docker
sudo systemctl enable docker

# Logout và login lại để áp dụng group changes
exit
```

### 4.3 Login lại và Setup Project
```bash
# Login lại
ssh -i ~/.ssh/media_tracker_key.pem ubuntu@YOUR_PUBLIC_IP

# Verify Docker
docker --version
docker compose version

# Create project directory
mkdir -p ~/media_tracker_bot
cd ~/media_tracker_bot
```

### 4.4 Tạo Docker Compose File
```bash
# Thay YOUR_DOCKERHUB_USERNAME bằng username thật
cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  media_tracker_bot:
    image: YOUR_DOCKERHUB_USERNAME/media_tracker_bot:latest
    container_name: media_tracker_bot
    ports:
      - "8000:8000"
    environment:
      - PROJECT_ROOT=/app
      - CONFIG_DIR=/app/config
      - DATA_DIR=/app/data
      - CACHE_DIR=/app/cache
      - LOG_DIR=/app/logs
      - STATIC_DIR=/app/static
      - DATABASE_PATH=/app/data/tasks.db
      - PYTHONPATH=/app
    volumes:
      - ./data:/app/data
      - ./cache:/app/cache
      - ./logs:/app/logs
      - ./config:/app/config
      - ./.env:/app/.env
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
    restart: unless-stopped
EOF

# Sửa username trong file
sed -i 's/YOUR_DOCKERHUB_USERNAME/your-actual-username/g' docker-compose.yml
```

### 4.5 Tạo Directories và Config Files
```bash
# Tạo directories
mkdir -p data/{reports} cache/{crawl_results} logs config static

# Tạo .env file
cat > .env << 'EOF'
OPENAI_API_KEY=sk-proj-your-openai-key-here
GROQ_API_KEY=gsk_your-groq-key-here
DEFAULT_MODEL_PROVIDER=openai
EOF

# Tạo config files cơ bản
cat > config/media_list.csv << 'EOF'
stt,id,name,domain,type,url
1,vnexpress,VnExpress,vnexpress.net,website,https://vnexpress.net
2,thanhnien,Thanh Niên,thanhnien.vn,website,https://thanhnien.vn
3,tuoitre,Tuổi Trẻ,tuoitre.vn,website,https://tuoitre.vn
4,dantri,Dân Trí,dantri.com.vn,website,https://dantri.com.vn
5,vietnamnet,VietnamNet,vietnamnet.vn,website,https://vietnamnet.vn
EOF

cat > config/keywords.json << 'EOF'
{
  "Sữa (UHT)": [
    "Vinamilk",
    "Kun"
  ],
  "Dầu ăn": ["Tường An", "Simply", "Meizan", "dầu ăn"],
  "Thực phẩm": ["thực phẩm", "food", "ăn uống"],
  "Kinh doanh": ["kinh doanh", "doanh nghiệp", "công ty"]
}
EOF

# Set permissions
chmod -R 755 data cache logs config
```

### 4.6 Deploy Application
```bash
# Pull image và start
docker compose pull
docker compose up -d

# Wait for startup
sleep 45

# Check status
docker compose ps
docker compose logs --tail=20
```

### 4.7 Cấu Hình Firewall (Nếu cần)
```bash
# Mở port 8000
sudo ufw allow 8000/tcp
sudo ufw --force enable
sudo ufw status
```

---

## ✅ Phần 5: Kiểm Tra và Quản Lý

### 5.1 Health Checks
```bash
# Test local
curl http://localhost:8000/health
curl http://localhost:8000/

# Test từ browser
# http://YOUR_PUBLIC_IP:8000
# http://YOUR_PUBLIC_IP:8000/health
# http://YOUR_PUBLIC_IP:8000/docs
```

### 5.2 Management Commands
```bash
# Xem logs real-time
docker compose logs -f

# Restart application
docker compose restart

# Stop application
docker compose down

# Update to latest image
docker compose pull && docker compose up -d

# Check container stats
docker stats

# Check disk usage
df -h
docker system df
```

### 5.3 Backup & Maintenance
```bash
# Backup data
tar -czf backup-$(date +%Y%m%d).tar.gz data/ config/ logs/

# Clean old Docker images
docker system prune -f

# Update system packages
sudo apt update && sudo apt upgrade -y
```

---

## 🐛 Troubleshooting

### Container Issues
```bash
# Container không start
docker compose ps
docker compose logs

# Health check failed
curl -v http://localhost:8000/health
docker exec media_tracker_bot curl http://localhost:8000/health

# Port not accessible
sudo ss -tlnp | grep :8000
docker port media_tracker_bot
```

### Network Issues
```bash
# Check firewall
sudo ufw status
sudo iptables -L

# Check security group trên AWS Console
# Đảm bảo port 8000 đã được mở cho 0.0.0.0/0

# Test connectivity
ping YOUR_PUBLIC_IP
telnet YOUR_PUBLIC_IP 8000
```

### Performance Issues
```bash
# Check system resources
free -h
df -h
top
htop

# Check Docker resources
docker stats
docker system df
```

### Log Analysis
```bash
# Application logs
docker compose logs --tail=100 -f

# System logs
sudo journalctl -u docker
sudo dmesg | tail -20

# Check disk space
df -h
du -sh data/ cache/ logs/
```

---

## 📚 Các URLs Quan Trọng

Sau khi deploy thành công:

- **Homepage:** http://YOUR_PUBLIC_IP:8000
- **Health Check:** http://YOUR_PUBLIC_IP:8000/health
- **API Documentation:** http://YOUR_PUBLIC_IP:8000/docs
- **Dashboard:** http://YOUR_PUBLIC_IP:8000/dashboard (if available)

**Demo Login:**
- Username: `admin`
- Password: `123456`

---

## ⚠️ Lưu Ý Quan Trọng

### Security
1. **Không commit API keys** vào Git
2. **Thay đổi default passwords** ngay sau khi deploy
3. **Cập nhật Security Group** chỉ mở port cần thiết
4. **Backup .pem file** ở nơi an toàn

### Performance
1. **Monitor disk space** thường xuyên
2. **Clean logs cũ** định kỳ
3. **Update images** khi có version mới
4. **Scale instance** nếu cần thiều performance

### Maintenance
1. **Backup data** trước khi update
2. **Test trên staging** trước khi deploy production
3. **Monitor application logs** để phát hiện lỗi sớm
4. **Setup alerting** nếu có thể

---

## 🎯 Quick Commands Summary

```bash
# Build & Push (Local)
docker build -t media_tracker_bot:production .
docker tag media_tracker_bot:production username/media_tracker_bot:latest
docker push username/media_tracker_bot:latest

# Deploy (EC2)
docker compose pull && docker compose up -d

# Monitor (EC2)
docker compose ps
docker compose logs -f
curl http://localhost:8000/health

# Maintenance (EC2)
docker compose restart
docker system prune -f
tar -czf backup-$(date +%Y%m%d).tar.gz data/
```

---

**🎉 Chúc mừng! Bạn đã deploy thành công Media Tracker Bot trên AWS EC2!**

Nếu gặp vấn đề, hãy check lại từng bước hoặc tham khao phần Troubleshooting ở trên.