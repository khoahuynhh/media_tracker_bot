# # tests/test_parsing.py
# import pytest
# from datetime import date
# from parsing import ArticleParser

# @pytest.fixture
# def parser():
#     return ArticleParser()

# def test_parse_valid_json_list(parser, sample_media_source):
#     json_content = """[{"title": "Bài báo 1", "summary": "Tóm tắt 1", "link": "https://example.com/1", "date": "2025-07-01"}]"""
#     articles = parser.parse(json_content, sample_media_source)
#     assert len(articles) == 1
#     assert articles[0].tom_tat_noi_dung == "Tóm tắt 1"

# def test_parse_structured_text(parser, sample_media_source):
#     text_content = "Tiêu đề: Bài báo Text\nLink: https://example.com/text1\nTóm tắt: Tóm tắt text."
#     articles = parser.parse(text_content, sample_media_source)
#     assert len(articles) == 1
#     assert articles[0].tom_tat_noi_dung == "Tóm tắt text."

# def test_parse_malformed_json_falls_back_to_text(parser, sample_media_source):
#     """
#     SỬA LỖI: Cung cấp một đoạn text hợp lệ cho trình phân tích fallback.
#     """
#     malformed_json_with_parsable_text = '{"title": "Bài báo lỗi" ---\nTiêu đề: Bài báo hợp lệ\nLink: https://example.com/valid'
#     articles = parser.parse(malformed_json_with_parsable_text, sample_media_source)
#     assert len(articles) == 1
#     assert articles[0].tom_tat_noi_dung == "Bài báo hợp lệ"


# Khoa-test
from src.parsing import ArticleParser
from src.models import MediaSource, MediaType
import pprint


def test_parse_valid_json():
    content = """
    ```json
    [
        {
            "tiêu đề": "Triệt phá đường dây buôn lậu, sản xuất dầu thực vật giả hơn 1.000 tấn",
            "ngày phát hành": "26-06-2025",
            "tóm tắt nội dung": "(Dân trí) - Ba người cầm đầu bị bắt, hơn 1.000 tấn dầu thực vật giả bị thu giữ.",
            "link bài báo": "https://dantri.com.vn/phap-luat/triet-pha-duong-day-buon-lau-san-xuat-dau-thuc-vat-gia-hon-1000-tan-20250626210430474.htm"
        },
        {
            "tiêu đề": "Vụ dầu chân nuôi 'biến' thành thực phẩm cho người",
            "ngày phát hành": "26-06-2025",
            "tóm tắt nội dung": "Gắn nhãn là “dầu thực vật” hoặc “dầu ăn cho người” đã sai lệch bản chất.",
            "link bài báo": "https://dantri.com.vn/kinh-doanh/vu-dau-chan-nuoi-bien-thanh-thuc-pham-cho-nguoi-20250626090431301.htm"
        }
    ]
    ```
    """

    media_source = MediaSource(
        stt=1, name="Dân trí", type=MediaType.WEBSITE, domain="dantri.com.vn"
    )

    parser = ArticleParser()
    articles = parser.parse(content, media_source)

    print(f"✅ Parsed {len(articles)} articles:")
    for article in articles:
        pprint.pprint(article.model_dump(mode="json"))


if __name__ == "__main__":
    test_parse_valid_json()
