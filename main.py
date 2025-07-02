"""
main.py - Media Tracker Bot Main Orchestrator (FIXED VERSION)
Entry point chính cho hệ thống tracking truyền thông với full config sync
"""

import asyncio
import json
import logging
import os
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

import subprocess

subprocess.run(["playwright", "install"], check=False)


import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import schedule
import time
from threading import Thread
import pandas as pd
from dotenv import load_dotenv
import uuid
import threading

# Load environment variables
load_dotenv()

# Import our modules
from src.models import (
    MediaSource,
    CrawlConfig,
    CompetitorReport,
    BotStatus,
    MediaType,
    create_sample_report,
)
from agents import MediaTrackerTeam

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/media_tracker.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Global variables
tracker_team: Optional[MediaTrackerTeam] = None
current_config: Optional[CrawlConfig] = None
latest_report: Optional[CompetitorReport] = None
app_status = BotStatus()
active_sessions: Dict[str, Dict[str, Any]] = {}  # session_id -> session_data
pipeline_stop_event = threading.Event()


class MediaTrackerOrchestrator:
    """Main orchestrator class cho toàn bộ hệ thống"""

    def __init__(self, config_dir: str = "config", data_dir: str = "data"):
        """
        Initialize orchestrator

        Args:
            config_dir: Thư mục chứa config files
            data_dir: Thư mục chứa data và cache
        """
        self.config_dir = Path(config_dir)
        self.data_dir = Path(data_dir)
        self.reports_dir = self.data_dir / "reports"
        self.cache_dir = self.data_dir / "cache"

        # Tạo directories nếu chưa có
        self._setup_directories()

        # Setup environment variables
        self._setup_environment()

        # Load configuration
        self.config = self._load_configuration()

        # Initialize team
        self.session_teams: Dict[str, MediaTrackerTeam] = {}
        self.is_running = False
        self.current_session_id = None
        self.stop_event = threading.Event()

    def _setup_directories(self):
        """Tạo các thư mục cần thiết"""
        for directory in [
            self.config_dir,
            self.data_dir,
            self.reports_dir,
            self.cache_dir,
            Path("logs"),
            Path("static"),
        ]:
            directory.mkdir(parents=True, exist_ok=True)

        logger.info("Directories setup completed")

    def _setup_environment(self):
        """Setup environment variables và tạo .env file nếu cần"""
        env_file = Path(".env")

        if not env_file.exists():
            # Tạo .env template
            env_template = """# API Keys for Media Tracker Bot
# Chọn ít nhất 1 provider để sử dụng

# OpenAI API Key (Recommended)
OPENAI_API_KEY=your_openai_api_key_here

# Groq API Key (Alternative - Free tier available)
GROQ_API_KEY=your_groq_api_key_here

# Default model provider (openai hoặc groq)
DEFAULT_MODEL_PROVIDER=openai

# Model configurations
OPENAI_MODEL=gpt-4o-mini
GROQ_MODEL=llama-3.1-70b-versatile

# Optional: Crawl4AI configurations
CRAWL4AI_MAX_CONCURRENT=5
CRAWL4AI_TIMEOUT=30

# Logging level
LOG_LEVEL=INFO
"""
            env_file.write_text(env_template, encoding="utf-8")
            logger.warning(
                f"Created .env template file. Please update with your API keys!"
            )
            logger.warning("Get OpenAI API key: https://platform.openai.com/api-keys")
            logger.warning("Get Groq API key: https://console.groq.com/keys")

        # Load environment variables
        load_dotenv()

        # Validate API keys
        self._validate_api_keys()

    def _validate_api_keys(self) -> bool:
        """Validate API keys are properly configured"""
        openai_key = os.getenv("OPENAI_API_KEY")
        groq_key = os.getenv("GROQ_API_KEY")

        if not openai_key or openai_key == "your_openai_api_key_here":
            openai_key = None

        if not groq_key or groq_key == "your_groq_api_key_here":
            groq_key = None

        if not openai_key and not groq_key:
            logger.error("❌ No valid API keys found!")
            logger.error(
                "Please update .env file with either OPENAI_API_KEY or GROQ_API_KEY"
            )
            return False

        if openai_key:
            logger.info("✅ OpenAI API key configured")
        if groq_key:
            logger.info("✅ Groq API key configured")

        return True

    def get_api_key_status(self) -> Dict[str, Any]:
        """Get status of API keys"""
        openai_key = os.getenv("OPENAI_API_KEY")
        groq_key = os.getenv("GROQ_API_KEY")

        return {
            "openai_configured": bool(
                openai_key and openai_key != "your_openai_api_key_here"
            ),
            "groq_configured": bool(groq_key and groq_key != "your_groq_api_key_here"),
            "default_provider": os.getenv("DEFAULT_MODEL_PROVIDER", "openai"),
            "openai_model": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            "groq_model": os.getenv("GROQ_MODEL", "llama-3.1-70b-versatile"),
        }

    def _load_configuration(self) -> CrawlConfig:
        """Load configuration từ files"""
        try:
            config_file = self.config_dir / "crawl_config.json"

            if config_file.exists():
                with open(config_file, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
                    return CrawlConfig(**config_data)

            # Load media list
            media_sources = self._parse_media_list()

            # Load keywords config
            keywords = self._load_keywords_config()

            config = CrawlConfig(
                keywords=keywords,
                media_sources=media_sources,
                date_range_days=30,
                max_articles_per_source=50,
                crawl_timeout=30,
                exclude_domains=["facebook.com", "twitter.com", "instagram.com"],
            )

            # Save config to file
            self.save_config(config)

            logger.info(f"Configuration loaded: {len(media_sources)} media sources")
            return config

        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            return self._create_default_config()

    def save_config(self, config: CrawlConfig):
        """Save configuration to file"""
        try:
            config_file = self.config_dir / "crawl_config.json"
            with open(config_file, "w", encoding="utf-8") as f:
                json.dump(
                    config.model_dump(), f, ensure_ascii=False, indent=2, default=str
                )

            # Also save keywords separately
            keywords_file = self.config_dir / "keywords.json"
            with open(keywords_file, "w", encoding="utf-8") as f:
                json.dump(config.keywords, f, ensure_ascii=False, indent=2)

            logger.info("Configuration saved successfully")
        except Exception as e:
            logger.error(f"Failed to save configuration: {str(e)}")

    def _parse_media_list(self) -> List[MediaSource]:
        """Parse media list từ document gốc thành MediaSource objects"""
        media_sources = []

        try:
            # Parse từ data có sẵn trong documents

            # 1. Newspapers & Magazines
            newspapers_magazines = [
                # Newspapers
                ("AN NINH THE GIOI", MediaType.NEWSPAPER),
                ("AN NINH THU DO", MediaType.NEWSPAPER),
                ("BAO VE PHAP LUAT", MediaType.NEWSPAPER),
                ("BINH DINH", MediaType.NEWSPAPER),
                ("BINH DUONG", MediaType.NEWSPAPER),
                ("BONG DA", MediaType.NEWSPAPER),
                ("BUU DIEN VIET NAM", MediaType.NEWSPAPER),
                ("CAN THO", MediaType.NEWSPAPER),
                ("CONG AN NHAN DAN", MediaType.NEWSPAPER),
                ("CONG AN THANH PHO DA NANG", MediaType.NEWSPAPER),
                ("CONG AN TP.HCM", MediaType.NEWSPAPER),
                ("CONG THUONG", MediaType.NEWSPAPER),
                ("DA NANG", MediaType.NEWSPAPER),
                ("DAI DOAN KET", MediaType.NEWSPAPER),
                ("DAU THAU", MediaType.NEWSPAPER),
                ("DAU TU", MediaType.NEWSPAPER),
                ("DIEN DAN DOANH NGHIEP", MediaType.NEWSPAPER),
                ("DOI SONG & PHAP LUAT", MediaType.NEWSPAPER),
                ("DONG NAI", MediaType.NEWSPAPER),
                ("GIAO DUC & THOI DAI (NGAY)", MediaType.NEWSPAPER),
                ("GIAO DUC TPHCM", MediaType.NEWSPAPER),
                ("GIAO THONG", MediaType.NEWSPAPER),
                ("HA NOI MOI", MediaType.NEWSPAPER),
                ("HA NOI MOI CUOI TUAN", MediaType.NEWSPAPER),
                ("HA TINH", MediaType.NEWSPAPER),
                ("KHOA HOC & DOI SONG", MediaType.NEWSPAPER),
                ("KHOA HOC PHO THONG", MediaType.NEWSPAPER),
                ("KIEN GIANG", MediaType.NEWSPAPER),
                ("KINH TE & DO THI", MediaType.NEWSPAPER),
                ("KINH TE & DO THI CUOI TUAN", MediaType.NEWSPAPER),
                ("KINH TE VIETNAM - THE GIOI", MediaType.NEWSPAPER),
                ("LAM DONG", MediaType.NEWSPAPER),
                ("LAO DONG", MediaType.NEWSPAPER),
                ("LAO DONG CUOI TUAN", MediaType.NEWSPAPER),
                ("LAO DONG THU DO", MediaType.NEWSPAPER),
                ("LAO DONG XA HOI", MediaType.NEWSPAPER),
                ("LONG AN", MediaType.NEWSPAPER),
                ("NGUOI HA NOI", MediaType.NEWSPAPER),
                ("NGUOI LAO DONG", MediaType.NEWSPAPER),
                ("NHA BAO & CONG LUAN", MediaType.NEWSPAPER),
                ("NHAN DAN", MediaType.NEWSPAPER),
                ("NINH THUAN", MediaType.NEWSPAPER),
                ("NONG NGHIEP VIET NAM", MediaType.NEWSPAPER),
                ("NONG THON NGAY NAY", MediaType.NEWSPAPER),
                ("PHAP LUAT & XA HOI", MediaType.NEWSPAPER),
                ("PHAP LUAT TPHCM", MediaType.NEWSPAPER),
                ("PHAP LUAT VIET NAM", MediaType.NEWSPAPER),
                ("PHU NU THU DO", MediaType.NEWSPAPER),
                ("PHU NU TPHCM", MediaType.NEWSPAPER),
                ("PHU NU VIET NAM", MediaType.NEWSPAPER),
                ("QUAN DOI NHAN DAN", MediaType.NEWSPAPER),
                ("SAI GON DAU TU TAI CHINH", MediaType.NEWSPAPER),
                ("SAI GON GIAI PHONG", MediaType.NEWSPAPER),
                ("SUC KHOE & DOI SONG", MediaType.NEWSPAPER),
                ("TAP CHI KINH TE VIET NAM", MediaType.NEWSPAPER),
                ("THANH HOA", MediaType.NEWSPAPER),
                ("THANH NIEN", MediaType.NEWSPAPER),
                ("THE GIOI & VIET NAM", MediaType.NEWSPAPER),
                ("THE THAO & VAN HOA", MediaType.NEWSPAPER),
                ("THE THAO TPHCM", MediaType.NEWSPAPER),
                ("THI TRUONG TAI CHINH & TIEN TE", MediaType.NEWSPAPER),
                ("THOI BAO KINH DOANH", MediaType.NEWSPAPER),
                ("THOI BAO NGAN HANG", MediaType.NEWSPAPER),
                ("THOI BAO TAI CHINH VIET NAM", MediaType.NEWSPAPER),
                ("TIEN PHONG", MediaType.NEWSPAPER),
                ("TUAN TIN TUC", MediaType.NEWSPAPER),
                ("TUOI TRE", MediaType.NEWSPAPER),
                ("TUOI TRE THU DO", MediaType.NEWSPAPER),
                ("TUYEN QUANG", MediaType.NEWSPAPER),
                ("VAN HOA", MediaType.NEWSPAPER),
                ("VIETNAM INVESTMENT REVIEW", MediaType.NEWSPAPER),
                ("VIETNAM NEWS", MediaType.NEWSPAPER),
                ("VINH PHUC", MediaType.NEWSPAPER),
                ("VOV (TIENG NOI VIET NAM)", MediaType.NEWSPAPER),
                ("VTV TRUYEN HINH", MediaType.NEWSPAPER),
                ("XAY DUNG", MediaType.NEWSPAPER),
                ("YEN BAI", MediaType.NEWSPAPER),
                # Magazines
                ("BEAUTY LIFE", MediaType.MAGAZINE),
                ("DAU TU CHUNG KHOAN", MediaType.MAGAZINE),
                ("DEP", MediaType.MAGAZINE),
                ("DOANH NGHIEP & TIEP THI", MediaType.MAGAZINE),
                ("DOANH NHAN - DDDN", MediaType.MAGAZINE),
                ("DOANH NHAN & PHAP LUAT", MediaType.MAGAZINE),
                ("DOANH NHAN SAI GON", MediaType.MAGAZINE),
                ("ELLE (PHAI DEP)", MediaType.MAGAZINE),
                ("ELLE DECORATION", MediaType.MAGAZINE),
                ("ELLE MAN (PHAI MANH)", MediaType.MAGAZINE),
                ("FORBES VIETNAM", MediaType.MAGAZINE),
                ("GIA DINH VIET NAM", MediaType.MAGAZINE),
                ("GIAO DUC & THOI DAI (THANG)", MediaType.MAGAZINE),
                ("GIAO DUC & THOI DAI (TUAN)", MediaType.MAGAZINE),
                ("GOLF VIET NAM", MediaType.MAGAZINE),
                ("HA NOI NGAY NAY", MediaType.MAGAZINE),
                ("HARPER'S BAZAAR", MediaType.MAGAZINE),
                ("HERITAGE", MediaType.MAGAZINE),
                ("HERITAGE FASHION", MediaType.MAGAZINE),
                ("HOA HOC TRO (HHT1)", MediaType.MAGAZINE),
                ("KHAN QUANG DO", MediaType.MAGAZINE),
                ("KHOA HOC & PHAT TRIEN", MediaType.MAGAZINE),
                ("KHOA HOC PHO THONG - SONG XANH", MediaType.MAGAZINE),
                ("KIEN TRUC & DOI SONG", MediaType.MAGAZINE),
                ("KINH TE NONG THON", MediaType.MAGAZINE),
                ("L'OFFICIEL", MediaType.MAGAZINE),
                ("MOI TRUONG & SUC KHOE", MediaType.MAGAZINE),
                ("MUC TIM", MediaType.MAGAZINE),
                ("NANG LUONG MOI", MediaType.MAGAZINE),
                ("NGOI SAO NHO", MediaType.MAGAZINE),
                ("NGUOI DO THI", MediaType.MAGAZINE),
                ("NHI DONG TPHCM", MediaType.MAGAZINE),
                ("NHIP CAU DAU TU", MediaType.MAGAZINE),
                ("NU DOANH NHAN", MediaType.MAGAZINE),
                ("PHAP LUAT VIET NAM CHUYEN DE", MediaType.MAGAZINE),
                ("PHU NU CHU NHAT", MediaType.MAGAZINE),
                ("PORTFOLIO", MediaType.MAGAZINE),
                ("ROBB REPORT", MediaType.MAGAZINE),
                ("RUA VANG", MediaType.MAGAZINE),
                ("SAIGON TIMES WEEKLY", MediaType.MAGAZINE),
                ("SUC KHOE & DOI SONG CUOI THANG", MediaType.MAGAZINE),
                ("SUC KHOE & DOI SONG CUOI TUAN", MediaType.MAGAZINE),
                ("TAP CHI NGAN HANG", MediaType.MAGAZINE),
                ("THE GIOI DIEN ANH", MediaType.MAGAZINE),
                ("THE GIOI PHU NU", MediaType.MAGAZINE),
                ("THI TRUONG - BO CONG THUONG", MediaType.MAGAZINE),
                ("THI TRUONG - BO TAI CHINH", MediaType.MAGAZINE),
                ("THIEN THAN NHO", MediaType.MAGAZINE),
                ("THIEU NIEN TIEN PHONG", MediaType.MAGAZINE),
                ("THOI BAO KINH TE SAI GON", MediaType.MAGAZINE),
                ("THOI BAO VTV ONAIR", MediaType.MAGAZINE),
                ("THOI GIAN", MediaType.MAGAZINE),
                ("THUOC & SUC KHOE", MediaType.MAGAZINE),
                ("TIA SANG", MediaType.MAGAZINE),
                ("TIEP THI & GIA DINH", MediaType.MAGAZINE),
                ("TUOI TRE CUOI TUAN", MediaType.MAGAZINE),
                ("VAN HOA NGHE THUAT", MediaType.MAGAZINE),
                ("VI TRE EM", MediaType.MAGAZINE),
                ("VIETNAM BUSINESS FORUM", MediaType.MAGAZINE),
                ("VIETNAM ECONOMIC NEWS", MediaType.MAGAZINE),
                ("VIETNAM ECONOMIC TIMES", MediaType.MAGAZINE),
                ("WORLD OF WATCHES", MediaType.MAGAZINE),
            ]

            # Add newspapers & magazines
            for i, (name, media_type) in enumerate(newspapers_magazines, 1):
                media_sources.append(MediaSource(stt=i, name=name, type=media_type))

            # 2. Websites
            websites = [
                "1THEGIOI.VN",
                "24H.COM.VN",
                "AFAMILY.VN",
                "ANNINHTHUDO.VN",
                "BAODAUTHAU.VN",
                "BAODAUTU.VN",
                "BAOMOI.COM",
                "BAOPHAPLUAT.VN",
                "BAOTINTUC.VN",
                "BAZAARVIETNAM.VN",
                "BIZHUB.VN",
                "BNEWS.VN",
                "BONGDA.COM.VN",
                "BONGDAPLUS.VN",
                "BSAONLINE.VN",
                "CADN.COM.VN",
                "CAFEBIZ.VN",
                "CAFEF.VN",
                "CAFELAND.VN",
                "CAND.COM.VN",
                "CHATLUONGVACUOCSONG.VN",
                "CHUYENDONGTHITRUONG.VN",
                "COHOIGIAOTHUONG.VN",
                "CONGAN.COM.VN",
                "CONGLUAN.VN",
                "CONGLY.VN",
                "CONGNGHIEPTIEUDUNG.VN",
                "CONGTHUONG.VN",
                "COSMOLIFE.VN",
                "DAIBIEUNHANDAN.VN",
                "DAIDOANKET.VN",
                "DANTRI.COM.VN",
                "DANVIET.VN",
                "DAUTUTHUONGMAI.VN",
                "DAUTUVIETNAM.COM.VN",
                "DELUXEVIETNAM.COM",
                "DEP.COM.VN",
                "DEPVAPHONGCACH.COM",
                "DIENDANDOANHNGHIEP.VN",
                "DIENTUUNGDUNG.VN",
                "DOANHNGHIEPCUOCSONG.VN",
                "DOANHNGHIEPHOINHAP.VN",
                "DOANHNGHIEPTIEPTHI.VN",
                "DOANHNGHIEPVN.VN",
                "DOANHNHANDOISONG.VN",
                "DOANHNHANSAIGON.VN",
                "DOANHNHANTHITRUONG.VN",
                "DOISONGPHAPLUAT.COM.VN",
                "DOISONGTIEUDUNG.VN",
                "ELLE.VN",
                "ELLEMAN.VN",
                "EVA.VN",
                "EVNEXPRESS.NET",
                "FORBES.VN",
                "GENK.VN",
                "GIADINHONLINE.VN",
                "GIADINHVAPHAPLUAT.VN",
                "GIAODUC.NET.VN",
                "GIAODUCTHOIDAI.VN",
                "GOLFANDLIFE.COM.VN",
                "HANOIMOI.VN",
                "HANOINGAYNAY.VN",
                "HANOITIMES.VN",
                "IAV.VN",
                "INVESTGLOBAL.VN",
                "KENH14.VN",
                "KHOAHOCDOISONG.VN",
                "KHOAHOCPHATTRIEN.VN",
                "KHOAHOCPHOTHONG.VN",
                "KIENTHUC.NET.VN",
                "KINHDOANHDOISONG.VN",
                "KINHDOANHNET.VN",
                "KINHDOANHPLUS.VN",
                "KINHTECHUNGKHOAN.VN",
                "KINHTEDOISONG.VN",
                "KINHTEDOTHI.VN",
                "KINHTEMOITRUONG.VN",
                "KINHTENEWS.COM",
                "KINHTENONGTHON.VN",
                "KINHTEVADUBAO.VN",
                "LAMCHAME.COM",
                "LAODONG.VN",
                "LAODONGTHUDO.VN",
                "LECOURRIER.VN",
                "LUXUO.VN",
                "NGAYMOIONLINE.COM.VN",
                "NGAYNAY.VN",
                "NGHENHINVIETNAM.VN",
                "NGUOIDOTHI.NET.VN",
                "NGUOIDUATIN.VN",
                "NGUOIHANOI.VN",
                "NGUOIQUANSAT.VN",
                "NGUOITIEUDUNGONLINE.COM",
                "NHADAUTU.VN",
                "NHANDAN.VN",
                "NHAQUANLY.VN",
                "NHIPCAUDAUTU.VN",
                "NHIPSONGKINHDOANH.VN",
                "NLD.COM.VN",
                "NONGNGHIEP.VN",
                "NONGTHONVIET.COM.VN",
                "NSS.VN",
                "PETROTIMES.VN",
                "PHUNUHIENDAI.VN",
                "PHUNUMOI.NET.VN",
                "PHUNUNET.NET",
                "PHUNUONLINE.VN",
                "PHUNUVIETNAM.VN",
                "PLO.VN",
                "QDND.VN",
                "QLTT.VN",
                "REATIMES.VN",
                "ROBBREPORT.COM.VN",
                "SGGP.ORG.VN",
                "SGTIEPTHI.VN",
                "SOHA.VN",
                "STOCKBIZ.VN",
                "SUCKHOEDOISONG.VN",
                "SUCKHOEMOITRUONG.COM.VN",
                "TAICHINHDOANHNGHIEP.NET.VN",
                "TAICHINHXANH.NET",
                "TAPCHICONGTHUONG.VN",
                "TAPCHIGIAOTHONG.VN",
                "TAPCHILAODONGXAHOI.VN",
                "TAPCHIMATTRAN.VN",
                "TAPCHIMOITRUONG.VN",
                "TAPCHITAICHINH.VN",
                "TAPCHIXAYDUNG.VN",
                "THANHNIEN.VN",
                "THANHNIENVIET.VN",
                "THANHTRA.COM.VN",
                "THEGIOIDIENANH.VN",
                "THEGIOIHOINHAP.VN",
                "THEGIOIVANHOA.NET",
                "THEINVESTOR.VN",
                "THELEADER.VN",
                "THESAIGONTIMES.VN",
                "THETHAOVANHOA.VN",
                "THIEUNIEN.VN",
                "THITRUONG365.COM",
                "THITRUONGTAICHINHTIENTE.VN",
                "THOIBAOTAICHINHVIETNAM.VN",
                "THOIDAI.COM.VN",
                "THUONGGIAONLINE.VN",
                "THUONGHIEUSANPHAM.VN",
                "THUONGHIEUVAPHAPLUAT.VN",
                "THUONGTRUONG.COM.VN",
                "TIENPHONG.VN",
                "TIEPTHIDAUTU24H.VN",
                "TIEPTHIVAGIADINH.VN",
                "TIIN.VN",
                "TINHTE.VN",
                "TINNHANHCHUNGKHOAN.VN",
                "TINTUCONLINE.COM.VN",
                "TINTUCVIETNAM.VN",
                "TOQUOC.VN",
                "TUOITRE.VN",
                "TUOITRENEWS.VN",
                "TUOITRETHUDO.VN",
                "TUYENGIAO.VN",
                "VCCINEWS.COM",
                "VCCINEWS.VN",
                "VEF.VN",
                "VIETBAO.VN",
                "VIETDAILY.VN",
                "VIETNAMBIZ.VN",
                "VIETNAMFINANCE.VN",
                "VIETNAMHOINHAP.VN",
                "VIETNAMNET.VN",
                "VIETNAMNEWS.VN",
                "VIETNAMPLUS.VN",
                "VIETPRESS.VN",
                "VIETQ.VN",
                "VIETSTOCK.VN",
                "VIETTIMES.VN",
                "VIR.COM.VN",
                "VJST.VN",
                "VLR.VN",
                "VNBUSINESS.VN",
                "VNECONOMY.VN",
                "VNEXPRESS.NET",
                "VNFINANCE.VN",
                "VNMEDIA.VN",
                "VNREVIEW.VN",
                "VOH.COM.VN",
                "VOV.VN",
                "VOVGIAOTHONG.VN",
                "VTCNEWS.VN",
                "VTV.VN",
                "YAN.VN",
                "YEAH1.COM",
                "ZNEWS.VN",
            ]

            # Add websites
            start_idx = len(media_sources) + 1
            for i, domain in enumerate(websites, start_idx):
                media_sources.append(
                    MediaSource(
                        stt=i,
                        name=domain.replace(".VN", "")
                        .replace(".COM", "")
                        .replace(".ORG", "")
                        .replace(".NET", ""),
                        type=MediaType.WEBSITE,
                        domain=domain.lower(),
                    )
                )

            # 3. TV Channels
            tv_channels = [
                "BTV1",
                "DANANG TV1",
                "HA NOI TV",
                "HTV7",
                "HTV9",
                "SCTV8",
                "VTV1",
                "VTV2",
                "VTV3",
                "VTV9",
            ]

            # Add TV channels
            start_idx = len(media_sources) + 1
            for i, channel in enumerate(tv_channels, start_idx):
                media_sources.append(
                    MediaSource(
                        stt=i,
                        name=channel,
                        type=MediaType.TV_CHANNEL,
                        reference_name=channel,
                    )
                )

            logger.info(f"Parsed {len(media_sources)} media sources successfully")
            return media_sources

        except Exception as e:
            logger.error(f"Failed to parse media list: {str(e)}")
            return []

    def _load_keywords_config(self) -> Dict[str, List[str]]:
        """Load keywords configuration"""
        try:
            keywords_file = self.config_dir / "keywords.json"

            if keywords_file.exists():
                with open(keywords_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            else:
                # Default keywords based on template
                default_keywords = {
                    "Dầu ăn": [
                        "Tường An",
                        "Coba",
                        "Nortalic",
                        "Ranee",
                        "Nakydaco",
                        "Zachia",
                        "Basso",
                        "Latino Bella",
                        "Metro Chef",
                        "dầu ăn",
                        "cooking oil",
                        "dầu thực vật",
                        "dầu nành",
                        "dầu hướng dương",
                        "dầu cọ",
                        "dầu oliu",
                        "chiên",
                        "rán",
                        "xào",
                        "nấu ăn",
                        "thực phẩm chiên",
                        "chế biến món ăn",
                        "omega-3",
                        "axit béo",
                        "vitamin E",
                    ],
                    "Gia vị": [
                        "gia vị",
                        "seasoning",
                        "spices",
                        "condiment",
                        "nước mắm",
                        "tương ớt",
                        "nước tương",
                        "hạt nêm",
                        "bột ngọt",
                        "muối",
                        "đường",
                        "tiêu",
                        "ớt bột",
                    ],
                    "Gạo & Ngũ cốc": [
                        "gạo",
                        "rice",
                        "ngũ cốc",
                        "cereals",
                        "yến mạch",
                        "lúa mì",
                        "bột mì",
                        "bánh mì",
                        "noodles",
                        "miến",
                    ],
                    "Sữa (UHT)": [
                        "sữa",
                        "milk",
                        "UHT",
                        "sữa tươi",
                        "sữa tiệt trùng",
                        "sữa bột",
                        "sữa đặc",
                        "sữa chua",
                        "yogurt",
                    ],
                    "Baby Food": [
                        "thức ăn trẻ em",
                        "baby food",
                        "sữa bột trẻ em",
                        "dinh dưỡng trẻ em",
                        "bột ăn dặm",
                        "infant formula",
                    ],
                    "Home Care": [
                        "nước rửa chén",
                        "dishwashing liquid",
                        "nước giặt",
                        "detergent",
                        "fabric softener",
                        "nước xả vải",
                        "chăm sóc nhà cửa",
                        "home care",
                        "làm sạch",
                    ],
                }

                # Save default keywords
                with open(keywords_file, "w", encoding="utf-8") as f:
                    json.dump(default_keywords, f, ensure_ascii=False, indent=2)

                return default_keywords

        except Exception as e:
            logger.error(f"Failed to load keywords config: {str(e)}")
            return {}

    def _create_default_config(self) -> CrawlConfig:
        """Tạo default config khi load thất bại"""
        return CrawlConfig(
            keywords={"Dầu ăn": ["Tường An", "dầu ăn"]},
            media_sources=[],
            date_range_days=30,
        )

    async def initialize_team(self, model_provider: str = None):
        """Initialize MediaTrackerTeam"""
        try:
            # Validate API keys first
            if not self._validate_api_keys():
                logger.error("Cannot initialize team: No valid API keys")
                return False

            # Determine model provider
            if not model_provider:
                model_provider = os.getenv("DEFAULT_MODEL_PROVIDER", "openai")

            # Check if requested provider has valid API key
            if model_provider == "openai":
                if (
                    not os.getenv("OPENAI_API_KEY")
                    or os.getenv("OPENAI_API_KEY") == "your_openai_api_key_here"
                ):
                    if (
                        os.getenv("GROQ_API_KEY")
                        and os.getenv("GROQ_API_KEY") != "your_groq_api_key_here"
                    ):
                        logger.warning("OpenAI key not configured, switching to Groq")
                        model_provider = "groq"
                    else:
                        logger.error("No valid API keys for any provider")
                        return False
            elif model_provider == "groq":
                if (
                    not os.getenv("GROQ_API_KEY")
                    or os.getenv("GROQ_API_KEY") == "your_groq_api_key_here"
                ):
                    if (
                        os.getenv("OPENAI_API_KEY")
                        and os.getenv("OPENAI_API_KEY") != "your_openai_api_key_here"
                    ):
                        logger.warning("Groq key not configured, switching to OpenAI")
                        model_provider = "openai"
                    else:
                        logger.error("No valid API keys for any provider")
                        return False

            self.team = MediaTrackerTeam(
                config=self.config, model_provider=model_provider
            )
            logger.info(
                f"Media tracker team initialized successfully with {model_provider}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to initialize team: {str(e)}")
            return False

    async def run_tracking_pipeline(
        self,
        start_date: str = None,
        end_date: str = None,
        custom_keywords: Dict[str, List[str]] = None,
        session_id: str = None,
    ) -> Optional[CompetitorReport]:
        """Chạy toàn bộ tracking pipeline cho từng session riêng biệt"""
        if not session_id:
            logger.error("Session ID is required")
            return None

        if (
            session_id in active_sessions
            and active_sessions[session_id]["status"] == "running"
        ):
            logger.warning(f"Session {session_id} is already running")
            return None

        try:
            self.stop_event = asyncio.Event()
            active_sessions[session_id] = {
                "start_time": datetime.now(),
                "status": "running",
                "stop_event": self.stop_event,
            }

            logger.info(f"[{session_id}] Starting media tracking pipeline...")

            # Clone config riêng cho session
            session_config = self.config.copy()

            if custom_keywords:
                session_config.keywords = custom_keywords

            if start_date and end_date:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                session_config.date_range_days = (end_dt - start_dt).days

            # Tạo team riêng cho session
            team = MediaTrackerTeam(config=session_config, model_provider="openai")

            # Chạy pipeline
            report = await team.run_full_pipeline()

            if report:
                await self._save_report(report)
                logger.info(f"[{session_id}] Pipeline completed successfully")

            return report

        except Exception as e:
            logger.error(f"[{session_id}] Pipeline failed: {str(e)}")
            return None

        finally:
            if session_id in active_sessions:
                active_sessions[session_id]["status"] = "completed"
                del active_sessions[session_id]

    async def run_pipeline_with_stop_check(self) -> Optional[CompetitorReport]:
        """Run pipeline với stop event monitoring"""
        try:
            # Check stop event periodically during execution
            if self.stop_event.is_set():
                logger.info("Pipeline stopped by user")
                return None

            # Run the actual pipeline
            report = await self.team.run_full_pipeline()

            # Check again after completion
            if self.stop_event.is_set():
                logger.info("Pipeline stopped after completion")
                return None

            return report

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            return None

    def stop_pipeline(self, session_id: str = None) -> bool:
        """Stop the running pipeline"""
        try:
            if not self.is_running:
                logger.warning("No pipeline is currently running")
                return False

            # Set stop event
            self.stop_event.set()

            # Update session status
            if session_id and session_id in active_sessions:
                active_sessions[session_id]["status"] = "stopped"
                active_sessions[session_id]["stop_event"].set()

            logger.info("Pipeline stop requested")
            return True

        except Exception as e:
            logger.error(f"Failed to stop pipeline: {str(e)}")
            return False

    async def _save_report(self, report: CompetitorReport):
        """Save report to files"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Save JSON
            json_file = self.reports_dir / f"report_{timestamp}.json"
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(
                    report.model_dump(), f, ensure_ascii=False, indent=2, default=str
                )

            # Save Excel
            excel_file = self.reports_dir / f"report_{timestamp}.xlsx"
            await self._export_to_excel(report, excel_file)

            # Update latest report symlink
            latest_json = self.reports_dir / "latest_report.json"
            latest_excel = self.reports_dir / "latest_report.xlsx"

            if latest_json.exists():
                latest_json.unlink()
            if latest_excel.exists():
                latest_excel.unlink()

            latest_json.symlink_to(json_file.name)
            latest_excel.symlink_to(excel_file.name)

            logger.info(f"Report saved: {json_file}")

        except Exception as e:
            logger.error(f"Failed to save report: {str(e)}")

    async def _export_to_excel(self, report: CompetitorReport, file_path: Path):
        """Export report to Excel format"""
        try:
            with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
                # Summary sheet
                summary_data = []
                for industry_summary in report.industry_summaries:
                    summary_data.append(
                        {
                            "Ngành hàng": industry_summary.nganh_hang,
                            "Nhãn hàng": ", ".join(industry_summary.nhan_hang),
                            "Cụm nội dung": ", ".join(industry_summary.cum_noi_dung),
                            "Số lượng bài": industry_summary.so_luong_bai,
                            "Các đầu báo": ", ".join(industry_summary.cac_dau_bao),
                        }
                    )

                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name="Summary", index=False)

                # Articles sheet
                articles_data = []
                for article in report.articles:
                    articles_data.append(
                        {
                            "STT": article.stt,
                            "Ngày phát hành": article.ngay_phat_hanh.strftime(
                                "%d/%m/%Y"
                            ),
                            "Đầu báo": article.dau_bao,
                            "Cụm nội dung": article.cum_noi_dung,
                            "Tóm tắt nội dung": article.tom_tat_noi_dung,
                            "Link bài báo": str(article.link_bai_bao),
                            "Ngành hàng": article.nganh_hang,
                            "Nhãn hàng": ", ".join(article.nhan_hang),
                            "Keywords": ", ".join(article.keywords_found),
                        }
                    )

                df_articles = pd.DataFrame(articles_data)
                df_articles.to_excel(writer, sheet_name="Articles", index=False)

        except Exception as e:
            logger.error(f"Failed to export to Excel: {str(e)}")

    def get_status(self) -> Dict[str, Any]:
        """Get current status"""
        team_status = self.team.get_status() if self.team else BotStatus()

        return {
            "is_running": self.is_running,
            "team_status": team_status.model_dump(),
            "total_media_sources": len(self.config.media_sources),
            "last_run": team_status.last_run,
            "next_scheduled_run": team_status.next_scheduled_run,
            "session_id": self.current_session_id,
            "active_sessions": len(active_sessions),
        }


# Global orchestrator instance
orchestrator = MediaTrackerOrchestrator()


# FastAPI app
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan events"""
    # Startup
    logger.info("Starting Media Tracker Bot...")

    # Initialize team
    await orchestrator.initialize_team()

    # Setup scheduler
    setup_scheduler()

    yield

    # Shutdown
    logger.info("Shutting down Media Tracker Bot...")


app = FastAPI(
    title="Media Tracker Bot API",
    description="API for Vietnamese Media Tracking and Competitor Analysis",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def get_demo_page():
    """Serve demo.html page"""
    try:
        demo_file = Path("demo.html")
        if demo_file.exists():
            return HTMLResponse(content=demo_file.read_text(encoding="utf-8"))
        else:
            return HTMLResponse(
                content="""
            <html>
                <head><title>Media Tracker Bot</title></head>
                <body>
                    <h1>Media Tracker Bot</h1>
                    <p>Demo page not found. Please create demo.html file.</p>
                    <p><a href="/api/docs">API Documentation</a></p>
                </body>
            </html>
            """
            )
    except Exception as e:
        logger.error(f"Error serving demo page: {str(e)}")
        return HTMLResponse(
            content=f"<html><body><h1>Error: {str(e)}</h1></body></html>"
        )


@app.get("/api/status")
async def get_status():
    """Get bot status"""
    status = orchestrator.get_status()
    # Add API key status
    status["api_keys"] = orchestrator.get_api_key_status()
    return status


@app.get("/api/api-keys/status")
async def get_api_keys_status():
    """Get API keys configuration status"""
    return orchestrator.get_api_key_status()


@app.post("/api/api-keys/update")
async def update_api_keys(api_keys: dict):
    """Update API keys in .env file"""
    try:
        env_file = Path(".env")

        # Read current .env file
        env_content = {}
        if env_file.exists():
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        env_content[key.strip()] = value.strip()

        # Update with new values
        if "openai_api_key" in api_keys and api_keys["openai_api_key"]:
            env_content["OPENAI_API_KEY"] = api_keys["openai_api_key"]

        if "groq_api_key" in api_keys and api_keys["groq_api_key"]:
            env_content["GROQ_API_KEY"] = api_keys["groq_api_key"]

        if "default_provider" in api_keys:
            env_content["DEFAULT_MODEL_PROVIDER"] = api_keys["default_provider"]

        # Write back to .env
        with open(env_file, "w", encoding="utf-8") as f:
            f.write("# API Keys for Media Tracker Bot\n")
            f.write(
                "# Updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n\n"
            )

            for key, value in env_content.items():
                f.write(f"{key}={value}\n")

        # Reload environment
        load_dotenv(override=True)

        # Validate new keys
        is_valid = orchestrator._validate_api_keys()

        # Reinitialize team if needed
        if is_valid:
            await orchestrator.initialize_team()

        return {
            "message": "API keys updated successfully",
            "valid": is_valid,
            "status": orchestrator.get_api_key_status(),
        }

    except Exception as e:
        logger.error(f"Error updating API keys: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/run")
async def run_pipeline(
    background_tasks: BackgroundTasks, request_data: dict = {}, resume: bool = False
):

    # Extract parameters
    start_date = request_data.get("start_date")
    end_date = request_data.get("end_date")
    custom_keywords = request_data.get("custom_keywords")
    session_id = request_data.get("session_id", str(uuid.uuid4()))

    # Validate date format
    if start_date:
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD"
            )

    if end_date:
        try:
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD"
            )

    # Check session conflict
    if session_id in active_sessions:
        raise HTTPException(
            status_code=409, detail="Another user is currently running the pipeline"
        )

    # Run in background
    background_tasks.add_task(
        orchestrator.run_tracking_pipeline,
        start_date=start_date,
        end_date=end_date,
        custom_keywords=custom_keywords,
        session_id=session_id,
    )

    return {
        "message": "Pipeline started",
        "status": "running",
        "session_id": session_id,
        "start_date": start_date,
        "end_date": end_date,
    }


@app.post("/api/stop")
async def stop_pipeline(request_data: dict = {}):
    """Stop the running pipeline"""
    session_id = request_data.get("session_id")

    success = orchestrator.stop_pipeline(session_id)

    if success:
        return {"message": "Pipeline stop requested", "status": "stopping"}
    else:
        raise HTTPException(status_code=400, detail="No pipeline is currently running")


@app.get("/api/reports/latest")
async def get_latest_report():
    """Get latest report"""
    try:
        latest_file = orchestrator.reports_dir / "latest_report.json"
        if latest_file.exists():
            with open(latest_file, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            # Return sample report
            sample = create_sample_report()
            return sample.model_dump()
    except Exception as e:
        logger.error(f"Error getting latest report: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/reports/download/latest")
async def download_latest_report(format: str = "json"):
    """Download latest report"""
    try:
        if format.lower() == "excel":
            file_path = orchestrator.reports_dir / "latest_report.xlsx"
            media_type = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            filename = "latest_report.xlsx"
        else:
            file_path = orchestrator.reports_dir / "latest_report.json"
            media_type = "application/json"
            filename = "latest_report.json"

        if file_path.exists():
            return FileResponse(
                path=file_path, media_type=media_type, filename=filename
            )
        else:
            raise HTTPException(status_code=404, detail="Report not found")

    except Exception as e:
        logger.error(f"Error downloading report: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/config")
async def get_config():
    """Get current configuration"""
    return orchestrator.config.model_dump()


@app.post("/api/config")
async def update_config(config_data: dict):
    """Update configuration"""
    try:
        # Validate and update config
        new_config = CrawlConfig(**config_data)
        orchestrator.config = new_config

        # Save config persistently
        orchestrator.save_config(new_config)

        # Reinitialize team with new config
        await orchestrator.initialize_team()

        return {"message": "Configuration updated successfully"}

    except Exception as e:
        logger.error(f"Error updating config: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/keywords")
async def get_keywords():
    """Get current keywords configuration"""
    return orchestrator.config.keywords


@app.post("/api/keywords")
async def update_keywords(keywords_data: dict):
    """Update keywords configuration"""
    try:
        # Validate keywords format
        if not isinstance(keywords_data, dict):
            raise ValueError("Keywords must be a dictionary")

        for industry, keywords in keywords_data.items():
            if not isinstance(keywords, list):
                raise ValueError(f"Keywords for {industry} must be a list")

        # Update config
        orchestrator.config.keywords = keywords_data

        # Save to file
        keywords_file = orchestrator.config_dir / "keywords.json"
        with open(keywords_file, "w", encoding="utf-8") as f:
            json.dump(keywords_data, f, ensure_ascii=False, indent=2)

        # Save full config
        orchestrator.save_config(orchestrator.config)

        return {"message": "Keywords updated successfully", "keywords": keywords_data}

    except Exception as e:
        logger.error(f"Error updating keywords: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


def setup_scheduler():
    """Setup scheduled tasks"""
    try:
        # Schedule daily run at 6 AM
        schedule.every().day.at("06:00").do(
            lambda: asyncio.create_task(orchestrator.run_tracking_pipeline())
        )

        # Schedule weekly report at Sunday 8 AM
        schedule.every().sunday.at("08:00").do(
            lambda: asyncio.create_task(orchestrator.run_tracking_pipeline())
        )

        # Start scheduler thread
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute

        scheduler_thread = Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()

        logger.info("Scheduler setup completed")

    except Exception as e:
        logger.error(f"Failed to setup scheduler: {str(e)}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Media Tracker Bot")
    parser.add_argument(
        "--mode",
        choices=["server", "run", "test"],
        default="server",
        help="Mode to run: server (API), run (single run), test (test mode)",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind")
    parser.add_argument(
        "--model", choices=["openai", "groq"], default="openai", help="Model provider"
    )

    args = parser.parse_args()

    if args.mode == "server":
        # Run FastAPI server
        logger.info(f"Starting server at {args.host}:{args.port}")
        uvicorn.run(
            "main:app", host=args.host, port=args.port, reload=False, access_log=True
        )

    elif args.mode == "run":
        # Single run
        async def single_run():
            await orchestrator.initialize_team(model_provider=args.model)
            report = await orchestrator.run_tracking_pipeline()
            if report:
                print(f"✅ Pipeline completed successfully!")
                print(f"Total articles: {report.total_articles}")
                print(f"Report saved to: {orchestrator.reports_dir}")
            else:
                print("❌ Pipeline failed")

        asyncio.run(single_run())

    elif args.mode == "test":
        # Test mode
        async def test_mode():
            print("🧪 Testing Media Tracker Bot...")

            # Test configuration
            print(
                f"✅ Configuration loaded: {len(orchestrator.config.media_sources)} media sources"
            )
            print(f"✅ Keywords: {len(orchestrator.config.keywords)} industries")

            # Test team initialization
            success = await orchestrator.initialize_team(model_provider=args.model)
            if success:
                print("✅ Team initialized successfully")

                # Test status
                status = orchestrator.get_status()
                print(
                    f"✅ Status: {status['team_status'].get('current_task', 'Ready')}"
                )

                # Test basic report generation (without actual crawling)
                print("✅ Testing sample report generation...")
                from models import create_sample_report

                sample_report = create_sample_report()
                print(
                    f"✅ Sample report created with {sample_report.total_articles} articles"
                )

                print("🎉 All tests passed!")
                print()
                print("📋 System Overview:")
                print(
                    f"   • Total Media Sources: {len(orchestrator.config.media_sources)}"
                )
                print(
                    f"   • Industries: {', '.join(orchestrator.config.keywords.keys())}"
                )
                print(f"   • Model Provider: {args.model}")
                print()
                print("🚀 Ready to run! Use:")
                print("   python main.py --mode run    # Single pipeline run")
                print("   python main.py --mode server # Start web dashboard")

            else:
                print("❌ Team initialization failed")

        asyncio.run(test_mode())


if __name__ == "__main__":
    main()
