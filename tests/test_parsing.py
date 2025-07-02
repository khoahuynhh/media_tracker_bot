# tests/test_parsing.py
import pytest
from datetime import date
from parsing import ArticleParser

@pytest.fixture
def parser():
    """Tạo một instance của ArticleParser cho các test."""
    return ArticleParser()

def test_parse_valid_json_list(parser, sample_media_source):
    """Kiểm tra trường hợp parser nhận được một chuỗi JSON list hợp lệ."""
    json_content = """[{"title": "Bài báo 1", "summary": "Tóm tắt 1", "link": "https://example.com/1", "date": "2025-07-01"}]"""
    articles = parser.parse(json_content, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Tóm tắt 1"
    assert articles[0].ngay_phat_hanh == date(2025, 7, 1)

def test_parse_structured_text(parser, sample_media_source):
    """Kiểm tra trường hợp parser nhận được văn bản có cấu trúc."""
    text_content = "Tiêu đề: Bài báo Text\nLink: https://example.com/text1\nTóm tắt: Tóm tắt text."
    articles = parser.parse(text_content, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Tóm tắt text."

def test_parse_empty_and_whitespace_content(parser, sample_media_source):
    """Kiểm tra parser xử lý đúng với input rỗng hoặc chỉ có khoảng trắng."""
    assert parser.parse("", sample_media_source) == []
    assert parser.parse("   \n\t ", sample_media_source) == []

def test_parse_malformed_json_falls_back_to_text(parser, sample_media_source):
    """Kiểm tra parser không bị crash khi nhận JSON không hợp lệ và chuyển sang phân tích dạng text."""
    # JSON này sai cú pháp nhưng chứa các từ khóa mà text parser có thể nhận diện
    malformed_json_content = '{"title": "Bài báo JSON lỗi", "link": "https://example.com/malformed" '
    articles = parser.parse(malformed_json_content, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Bài báo JSON lỗi"

def test_parse_text_with_missing_link(parser, sample_media_source):
    """Kiểm tra parser bỏ qua các block text thiếu thông tin quan trọng (link)."""
    text_content = "Tiêu đề: Bài báo thiếu link\nTóm tắt: Nội dung không có link."
    articles = parser.parse(text_content, sample_media_source)
    assert len(articles) == 0

def test_parse_content_with_special_chars(parser, sample_media_source):
    """Kiểm tra parser xử lý đúng các ký tự đặc biệt và emoji."""
    json_content = """[{"title": "Test ❤️ & спецсимволы", "summary": "Nội dung có ký tự đặc biệt", "link": "https://example.com/special", "date": "2025-01-01"}]"""
    articles = parser.parse(json_content, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Nội dung có ký tự đặc biệt"