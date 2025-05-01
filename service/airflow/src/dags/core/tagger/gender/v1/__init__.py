from dataclasses import dataclass, field
import hashlib
import json
from typing import Literal
from openai import OpenAI, AsyncOpenAI
import textwrap
from .prompt import PROMPT_FOR_CATEGORIES_TAGGING
from .types import GenderTag, GenderCategory
from core.tagger.utils.cost_tractor import TokenUsageTracker


@dataclass(init=True, slots=True)
class GenderTagger:
    model: Literal[
        "gpt-4.5-preview-2025-02-27",
        "o1",
        "o3-mini",
        "gpt-4o",
        "gpt-4o-mini",
    ] = field(init=True, default="gpt-4o-mini")
    temperature: float = field(init=True, default=0.0)
    api_key: str | None = field(init=True, default=None)
    client: OpenAI = field(init=False)
    aclient: AsyncOpenAI = field(init=False)
    prompt: str = field(init=False, default=textwrap.dedent(PROMPT_FOR_CATEGORIES_TAGGING))
    tranker: TokenUsageTracker = field(init=False, default_factory=TokenUsageTracker)

    def __post_init__(self):
        self.client = OpenAI(api_key=self.api_key)
        self.aclient = AsyncOpenAI(api_key=self.api_key)

    def tagging(self, title: str, images: list[str]):
        response = self.client.beta.chat.completions.parse(**self.parse_inputs(title, images))
        return self.post_process(response)

    async def async_tagging(self, title: str, images: list[str]):
        response = await self.aclient.beta.chat.completions.parse(**self.parse_inputs(title, images))
        return self.post_process(response)

    def parse_inputs(self, title, image_urls) -> dict:
        return dict(
            model=self.model,
            temperature=0,
            response_format=GenderTag,
            messages=[
                {"role": "system", "content": self.prompt},
                {"role": "user", "content": title},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": title},
                        *[
                            {
                                "type": "image_url",
                                "image_url": {"url": image_url},
                            }
                            for image_url in image_urls
                        ],
                    ],
                },
            ],
        )

    def post_process(self, response) -> list[GenderCategory]:
        self.tranker.add_usage(
            prompt_tokens=response.usage.prompt_tokens, completion_tokens=response.usage.completion_tokens
        )
        response_text = response.choices[0].message.content
        response_json = json.loads(response_text)
        return GenderTag.model_validate(response_json).gender

    def generate_key(self, title: str, images: list[str], version: str) -> str:
        inputs = self.parse_inputs(title, images)
        inputs["version"] = version
        inputs["model"] = self.model
        return hashlib.sha256(json.dumps(inputs, sort_keys=True, default=str).encode()).hexdigest()
