# tests/test_parsing.py
import pytest
from datetime import date
from parsing import ArticleParser

@pytest.fixture
def parser():
    return ArticleParser()

def test_parse_valid_json_list(parser, sample_media_source):
    json_content = """[{"title": "Bài báo 1", "summary": "Tóm tắt 1", "link": "https://example.com/1", "date": "2025-07-01"}]"""
    articles = parser.parse(json_content, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Tóm tắt 1"

def test_parse_structured_text(parser, sample_media_source):
    text_content = "Tiêu đề: Bài báo Text\nLink: https://example.com/text1\nTóm tắt: Tóm tắt text."
    articles = parser.parse(text_content, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Tóm tắt text."

def test_parse_malformed_json_falls_back_to_text(parser, sample_media_source):
    """
    SỬA LỖI: Cung cấp một đoạn text hợp lệ cho trình phân tích fallback.
    """
    malformed_json_with_parsable_text = '{"title": "Bài báo lỗi" ---\nTiêu đề: Bài báo hợp lệ\nLink: https://example.com/valid'
    articles = parser.parse(malformed_json_with_parsable_text, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Bài báo hợp lệ"