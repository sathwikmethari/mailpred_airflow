import bentoml
from transformers import AutoTokenizer, AutoModel

"""
    Dont forget to change the embed input to list[str], list[str] in the future!!!
"""

def save_model_in_store(model_name: str):
    """
        For saving HuggingFace transformer model and tokenizer into BentoML model store.
    """
    hf_model = AutoModel.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    saved_model = bentoml.transformers.save_model(
            "embedmodel",
            hf_model,
            custom_objects={"tokenizer": tokenizer}
    )
    saved_tokenizer = bentoml.transformers.save_model(
            "embedmodel_tokenizer",
            tokenizer,
            custom_objects={"tokenizer": tokenizer}
    )
    print(f"Saved to BentoML-store: {saved_model.tag}")
    print(f"Saved to BentoML-store: {saved_tokenizer.tag}")


if __name__=="__main__":    
    model_name = "sentence-transformers/all-MiniLM-L6-v2"
    save_model_in_store(model_name)
