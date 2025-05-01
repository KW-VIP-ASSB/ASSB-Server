from dataclasses import dataclass, field

# 모델별 비용 (1K 토큰당 달러 기준)
MODEL_PRICING = {
    "o1": {"input": 15.0, "output": 60.0},
    "o3-mini": {"input": 1.10, "output": 4.40},
    "gpt-4o": {"input": 2.5, "output": 10.0},
    "gpt-4o-mini": {"input": 0.150, "output": 0.600},
}


@dataclass
class TokenUsageTracker:
    prompt_tokens_list: list[int] = field(default_factory=list)
    completion_tokens_list: list[int] = field(default_factory=list)
    cost_list: list[float] = field(default_factory=list)
    model: str = field(default="gpt-4o-mini", init=True)

    def add_usage(self, prompt_tokens: int, completion_tokens: int):
        self.prompt_tokens_list.append(prompt_tokens)
        self.completion_tokens_list.append(completion_tokens)
        M = 1000000
        input_cost = (prompt_tokens / M) * MODEL_PRICING[self.model]["input"]
        output_cost = (completion_tokens / M) * MODEL_PRICING[self.model]["output"]
        total_cost = input_cost + output_cost
        self.cost_list.append(total_cost)

    @property
    def average_usage(self):
        num_tasks = len(self.prompt_tokens_list)
        if num_tasks == 0:
            return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

        avg_prompt = sum(self.prompt_tokens_list) / num_tasks
        avg_completion = sum(self.completion_tokens_list) / num_tasks
        avg_total = avg_prompt + avg_completion

        return {"prompt_tokens": avg_prompt, "completion_tokens": avg_completion, "total_tokens": avg_total}

    @property
    def total_usage(self):
        """전체 토큰 사용량 반환"""
        total_prompt = sum(self.prompt_tokens_list)
        total_completion = sum(self.completion_tokens_list)
        total_tokens = total_prompt + total_completion
        return {"prompt_tokens": total_prompt, "completion_tokens": total_completion, "total_tokens": total_tokens}

    @property
    def average_cost(self):
        if len(self.cost_list) == 0:
            return 0.0
        return sum(self.cost_list) / len(self.cost_list)

    @property
    def total_cost(self):
        return sum(self.cost_list)
