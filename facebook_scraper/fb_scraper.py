import re
import json
import itertools
from json import JSONDecodeError
from datetime import datetime
from functools import partial


import logging
from requests_html import HTMLSession, Element
from requests import RequestException
from urllib.parse import urljoin

from typing import List

from utils import make_html_element, decode_css_url

logger = logging.getLogger(__name__)

FB_MOBILE_BASE_URL = "https://m.facebook.com"
DEFAULT_PAGE_LIMIT = 10

class FacebookConnector:
    base_url = FB_MOBILE_BASE_URL


    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/76.0.3809.87 Safari/537.36"
    )
    cookie = 'locale=en_US;'
    default_headers = {
        'User-Agent': user_agent,
        'Accept-Language': 'en-US,en;q=0.5',
        'cookie': cookie,
    }

    def __init__(self, session=None, requests_kwargs=None):
        if session is None:
            session = HTMLSession()
            session.headers.update(self.default_headers)

        if requests_kwargs is None:
            requests_kwargs = {}

        self.session = session
        self.requests_kwargs = requests_kwargs

    def get(self, url, **kwargs):
        try:
            response = self.session.get(url=url, **self.requests_kwargs, **kwargs)
            response.raise_for_status()
            return response
        except RequestException as ex:
            logger.exception("Exception while requesting URL: %s\nException: %r", url, ex)
            raise


class PageParser:
    """ Only for pagination. """
    json_prefix = 'for (;;);'
    cursor_regex = re.compile(r'href:"(/page_content[^"]+)"')  # First request
    cursor_regex_2 = re.compile(r'href":"(\\/page_content[^"]+)"')  # Other requests
    page_path = "posts/"

    def __init__(self, page_url, connector):
        self.connector = connector
        self.response = self.connector.get(page_url)
        self.html = None
        self.cursor_blob = None
        self._parse()

    @classmethod
    def iterator(cls, page_name, connector):
        page_url = urljoin(FB_MOBILE_BASE_URL, f"/{page_name}/{cls.page_path}")
        while page_url:
            parser = cls(page_url, connector)
            page_html = parser.get_html()
            yield page_html
            next_page_url = parser.get_next_url()
            if next_page_url:
                page_url = urljoin(FB_MOBILE_BASE_URL, next_page_url)
            else:
                url = None

    def get_html(self) -> Element:
        return self.html

    def get_next_url(self) -> str:
        logger.debug("Looking for next page.")
        match = self.cursor_regex.search(self.cursor_blob)
        if match:
            return match.groups()[0]
        match = self.cursor_regex_2.search(self.cursor_blob)
        if match:
            value = match.groups()[0]
            return value.encode('utf-8').decode('unicode_escape').replace('\\/', '/')
        logger.debug("No next page found.")
        return None

    def _parse(self):
        if self.response.text.startswith(self.json_prefix):
            self._parse_json()
        else:
            self._parse_html()

    def _parse_html(self):
        # TODO: Why are we uncommenting HTML?
        self.html = make_html_element(
            self.response.text.replace('<!--', '').replace('-->', ''), url=self.response.url,
        )
        self.cursor_blob = self.response.text

    def _parse_json(self):
        prefix_length = len(self.json_prefix)
        data = json.loads(self.response.text[prefix_length:])  # Strip 'for (;;);'
        for action in data['payload']['actions']:
            if action['cmd'] == 'replace':
                self.html = make_html_element(action['html'], url=FB_MOBILE_BASE_URL)
            elif action['cmd'] == 'script':
                self.cursor_blob = action['code']

        assert self.html is not None
        assert self.cursor_blob is not None


class VideoGridPageParser(PageParser):
    # First request
    cursor_regex = re.compile(r'href:"(/[^"]+/videos/more/\?cursor=[^"]+)"')
    # Other requests
    cursor_regex_2 = re.compile(
            r'href":"(\\/[^"]+\\/videos\\/more\\/\?cursor=[^"]+)"'
    )

    page_path = "video_grid/"


class Extractor:
    html_tag = None

    def __init__(self, connector):
        self.connector = connector

    def _get_tags(self, page_html) -> List[Element]:
        tags = page_html.find(self.html_tag)
        if not tags:
            logger.warning(f"No raw posts (<{self.html_tag}> elements) were found in this page.")
            if logger.isEnabledFor(logging.DEBUG):
                import html2text
                content = html2text.html2text(html.html)
                logger.debug("The page content is:\n %s\n", content)
        return tags

    def _data_from_tag(self, tag):
        raise Exception("not implemented!!!")

    def extract(self, page_html):
        for tag in self._get_tags(page_html):
            yield self._data_from_tag(tag)


class PostExtractor(Extractor):
    likes_regex = re.compile(r'like_def[^>]*>([0-9,.]+)')
    comments_regex = re.compile(r'cmt_def[^>]*>([0-9,.]+)')
    shares_regex = re.compile(r'([0-9,.]+)\s+Shares', re.IGNORECASE)
    link_regex = re.compile(r"href=\"https:\/\/lm\.facebook\.com\/l\.php\?u=(.+?)\&amp;h=")
    photo_link = re.compile(r'href=\"(/[^\"]+/photos/[^\"]+?)\"')
    image_regex = re.compile(
        r'<a href=\"([^\"]+?)\" target=\"_blank\" class=\"sec\">View Full Size<\/a>',
        re.IGNORECASE,
    )
    image_regex_lq = re.compile(r"background-image: url\('(.+)'\)")
    post_url_regex = re.compile(r'/story.php\?story_fbid=')
    shares_and_reactions_regex = re.compile(
        r'<script>.*bigPipe.onPageletArrive\((?P<data>\{.*RelayPrefetchedStreamCache.*\})\);'
        '.*</script>'
    )
    bad_json_key_regex = re.compile(r'(?P<prefix>[{,])(?P<key>\w+):')
    more_url_regex = re.compile(r'(?<=…\s)<a href="([^"]+)')
    post_story_regex = re.compile(r'href="(\/story[^"]+)" aria')


    html_tag = "article"

    def _get_tags(self, page_html) -> List[Element]:
        all_article_tags = super()._get_tags(page_html)
        posts = []
        # posts sharing the content of an other post
        # have inside <article> an other <article>
        # for the shared post but without data-ft
        for article in all_article_tags:
            # a "real" post has always a data-ft attribute
            if 'data-ft' in article.attrs.keys():
                posts.append(article)
        return posts

    def _data_from_tag(self, tag) -> dict:
        data_ft = self._data_ft(tag)
        post = {
            'post_id': data_ft.get('mf_story_key'),
            **self._text(tag)
        }
        return post

    def _data_ft(self, tag) -> dict:
        _data_ft = {}
        try:
            data_ft_json = tag.attrs['data-ft']
            _data_ft = json.loads(data_ft_json)
        except JSONDecodeError as ex:
            logger.error("Error parsing data-ft JSON: %r", ex)
        except KeyError:
            logger.error(f"data-ft attribute not found. tag: {tag}")

        return _data_ft

    def _text(self, tag) -> dict:

        # Open this article individually because not all content is fully loaded when skimming
        # through pages.
        # This ensures the full content can be read.
        element = tag

        has_more = self.more_url_regex.search(element.html)
        if has_more:
            match = self.post_story_regex.search(element.html)
            if match:
                url = urljoin(FB_MOBILE_BASE_URL, match.groups()[0].replace("&amp;", "&"))
                response = self.connector.get(url)
                element = response.html.find('.story_body_container', first=True)

        nodes = element.find('p, header')
        if nodes:
            post_text = []
            shared_text = []
            ended = False
            for node in nodes[1:]:
                if node.tag == 'header':
                    ended = True

                # Remove '... More'
                # This button is meant to display the hidden text that is already loaded
                # Not to be confused with the 'More' that opens the article in a new page
                if node.tag == 'p':
                    node = make_html_element(
                        html=node.html.replace('>… <', '><', 1).replace('>More<', '', 1)
                    )

                if not ended:
                    post_text.append(node.text)
                else:
                    shared_text.append(node.text)

            text = '\n'.join(itertools.chain(post_text, shared_text))
            post_text = '\n'.join(post_text)
            shared_text = '\n'.join(shared_text)

            return {
                'text': text,
                'post_text': post_text,
                'shared_text': shared_text,
            }

        return None


class VideoExtractor(Extractor):

    _page_id = None
    html_tag = "i"  # videos always in <i> Tag

    def _get_page_id(self, page_html):
        page_id_regex = re.compile(r'CurrentPage.+pageID:"([^"]+)",pageName')
        match = page_id_regex.search(page_html.full_text)
        if match:
            page_id = match.groups()[0]
            logger.debug(f"found page_id: {page_id}")
            return page_id
        else:
            logger.debug(f"can't find page_id")
            return None

    def _get_tags(self, page_html) -> List[Element]:
        if not self._page_id:
            self._page_id = self._get_page_id(page_html)
        all_i_tags = super()._get_tags(page_html)
        videos = []
        for i in all_i_tags:
            try:
                if i.attrs['data-sigil'] == "playInlineVideo":
                    videos.append(i)
            except KeyError:
                pass
        return videos

    def _data_from_tag(self, tag) -> dict:
        data_store = self._data_store(tag)
        video_url = urljoin(FB_MOBILE_BASE_URL, f"story.php?story_fbid="+\
                      f"{data_store['videoID']}&id={self._page_id}")
        video = {
            'page_id': self._page_id,
            'id': data_store['videoID'],
            'src': data_store['src'],
            'thumbnail': self._thumbnail(tag),
            'url': video_url,
            **self._details(video_url)

        }
        return video

    def _details(self, video_url):
        logger.debug(f"get details from {video_url}")
        response = self.connector.get(video_url)
        html = response.html
        # sometime links are broken
        # maybe catch error, or skip when story_body not found
        post_body = html.find("div.story_body_container", first=True)

        # publish_time
        data_ft = {}
        try:
            # parent element has data-store with id and src
            parent = next(post_body.element.iterancestors())
            data_ft_str = parent.attrib['data-ft']
            data_ft = json.loads(data_ft_str)
        except JSONDecodeError as ex:
            logger.error("Error parsing data-store JSON: %r", ex)
        except KeyError:
            logger.error("data-store attribute not found")
        timestamp = data_ft['page_insights'][self._page_id]['post_context']['publish_time']
        publish_time = datetime.fromtimestamp(timestamp)
        # description
        nodes = post_body.find('p, header')
        if nodes:
            post_text = []
            shared_text = []
            ended = False
            for node in nodes[1:]:
                if node.tag == 'header':
                    ended = True

                # Remove '... More'
                # This button is meant to display the hidden text that is already loaded
                # Not to be confused with the 'More' that opens the article in a new page
                if node.tag == 'p':
                    node = make_html_element(
                        html=node.html.replace('>… <', '><', 1).replace('>More<', '', 1)
                    )

                if not ended:
                    post_text.append(node.text)
                else:
                    shared_text.append(node.text)

            text = '\n'.join(itertools.chain(post_text, shared_text))
            post_text = '\n'.join(post_text)
            shared_text = '\n'.join(shared_text)

        return {
            "publish_time": publish_time,
            "text": text,
            'post_text': post_text,
            'shared_text': shared_text,
        }


    def _thumbnail(self, tag) -> str:
        style_attr = tag.attrs["style"]
        url_regex = re.compile(r"background-image: url\('(https[^']+)'\)")
        match = url_regex.search(style_attr)
        thumbnail_url = match.groups()[0] if match else ""
        return decode_css_url(thumbnail_url)

    def _data_store(self, tag) -> dict:
        _data_store = {}
        try:
            # parent element has data-store with id and src
            parent = next(tag.element.iterancestors())
            data_store_json = parent.attrib['data-store']
            _data_store = json.loads(data_store_json)
        except JSONDecodeError as ex:
            logger.error("Error parsing data-ft JSON: %r", ex)
        except KeyError:
            logger.error(f"data-ft attribute not found. tag: {tag}")
        return _data_store


class FacebookScraper:
    def __init__(self, connector):
        self.connector = connector

    def _extract_content(self, page_iterator, extractor_cls) -> dict:
        extractor = extractor_cls(self.connector)
        for page in page_iterator:
            for data_obj in extractor.extract(page):
                yield data_obj

    def extract_videos(self, page_name) -> dict:
        logging.info(f"extract videos ...")
        page_iterator = VideoGridPageParser.iterator(page_name, self.connector)
        return self._extract(page_iterator, VideoExtractor)

    def extract_posts(self, page_name) -> dict:
        logging.info(f"extract posts ...")
        page_iterator = PageParser.iterator(page_name, self.connector)
        return self._extract_content(page_iterator, PostExtractor)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    con = FacebookConnector()
    scraper = FacebookScraper(con)

    i = 0
    for video in scraper.extract_posts("3HOFoundation"):
        print(video)
        i += 1
    print(f"{i} videos fetched!")
