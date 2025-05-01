import hashlib
import json
from langchain_mongodb import MongoDBCache
from langchain_core.caches import RETURN_VAL_TYPE
from langchain_core.load.dump import dumps


def _dumps_generations(generations: RETURN_VAL_TYPE) -> str:
    return json.dumps([dumps(_item) for _item in generations])


class HashMongoDBCache(MongoDBCache):
    def _generate_keys(self, prompt: str, llm_string: str) -> dict[str, str]:
        prompt_key = hashlib.sha512(prompt.encode()).hexdigest()
        llm_key = hashlib.sha512(llm_string.encode()).hexdigest()
        return {self.PROMPT: prompt_key, self.LLM: llm_key}

    def update(self, prompt: str, llm_string: str, return_val: RETURN_VAL_TYPE) -> None:
        """Update cache based on prompt and llm_string."""
        self.collection.update_one(
            {**self._generate_keys(prompt, llm_string)},
            {
                "$set": {
                    self.RETURN_VAL: _dumps_generations(return_val),
                    "inputs": json.loads(prompt),
                    "llm_config": llm_string,
                },
            },
            upsert=True,
        )
