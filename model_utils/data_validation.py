from typing import Annotated
from pydantic import BaseModel, AfterValidator, model_validator

# https://docs.pydantic.dev/latest/concepts/validators/
# Read above for better understanding of validators in Pydantic
def validate_non_empty(data: list[str]) -> list[str]:
    """To make sure the input is a list of strings and not empty or other data types."""
    if data == []:
        raise ValueError("Data list cannot be empty.")
    return data

class PredictRequest(BaseModel):
    Subject: Annotated[list[str], AfterValidator(validate_non_empty)]
    Body: Annotated[list[str], AfterValidator(validate_non_empty)]
    
    @model_validator(mode='after')
    def check_lengths(self):
        if len(self.Subject) != len(self.Body):
            raise ValueError(f"Subject and Body must have equal length. Subject: {len(self.Subject)}, Body: {len(self.Body)}")
        return self