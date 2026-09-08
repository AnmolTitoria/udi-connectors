from typing import Literal

from udi_packages.config import BaseConfig


class PromptDataConfig(BaseConfig):
    source_type: Literal["prompt_data"] = "prompt_data"

    # Free-text description of the dataset, e.g. "200 rows with name, email,
    # signup_date and is_active" — parsed by fields.parse_prompt() into a
    # field list and row count. No external API calls; everything here is
    # generated locally with Faker/random.
    prompt: str
    row_count: int = 100
    batch_size: int = 500
    seed: int | None = None
