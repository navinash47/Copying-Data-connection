from dataclasses import dataclass
from typing import Dict, List


@dataclass
class HkmArticleTranslation:
    knowledge_state: str
    culture: str
    title: str
    issue: str
    environment: str
    resolution: str
    cause: str
    tags: List[str]


@dataclass
class HkmArticle:
    content_id: int
    translations: List[HkmArticleTranslation]

    def is_published(self):
        if self.translations:
            # assumption is that all translations have the same status (the status is set for the whole article in
            # ComAround)
            return self.translations[0].knowledge_state.casefold() == 'published'.casefold()
        else:
            return False


def decode_hkm_article_from_json_dict(json: Dict):
    if 'translations' in json:
        translations = [
            decode_hkm_article_translations_from_json_dict(element_json) for element_json in json['translations']]
    else:
        translations = []
    return HkmArticle(content_id=json['contentId'], translations=translations)


def decode_hkm_article_translations_from_json_dict(json: Dict):
    if json is None:
        return None
    else:
        return HkmArticleTranslation(
            knowledge_state=json['knowledgeState'],
            culture=json['culture'],
            title=json['title'],
            issue=json.get('issue'),
            environment=json.get('environment'),
            resolution=json.get('resolution'),
            cause=json.get('cause'),
            tags=json.get('tags'))


@dataclass
class HkmResults:
    pages: int
    content_ids: List[int]
