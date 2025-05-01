from bs4 import BeautifulSoup


class HTMLFormatter:
    @classmethod
    def format_description(cls, raw_html: str) -> str:
        """
        HTML 태그를 유지하면서 태그 간에 두 줄 간격을 추가.
        중첩 태그를 처리하며, <script>와 <style> 태그는 무시.
        """
        soup = BeautifulSoup(raw_html, "html.parser")
        formatted_html = ""
        for element in soup.descendants:
            if element.name in ["script", "style"]:
                continue
            if element.name:
                formatted_html += str(element).strip() + "\n\n"
        return formatted_html.strip()


def transform_image_list_to_html(product_image_list: list[str]) -> str:

    html_list = [
        f'<img src="{url}" style="margin:5px;" loading="lazy" />'
        for url in product_image_list
    ]
    return "\n".join(html_list)