import bentoml
import xgboost as xgb
from transformers import AutoTokenizer, AutoModel


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

def save_xgb_classifier(model_name: str, model_path) -> None:
    """
        For saving classifier model into BentoML model store.
    """

    cls = xgb.Booster()
    cls.load_model(model_path)
    bentoml.xgboost.save_model(model_name, cls)
    print(f"Classifier saved to BentoML-store")

if __name__=="__main__":
    model_name = "sentence-transformers/all-MiniLM-L6-v2"
    cls_path = "../data/model_MiniLM-L6.json"
    cls_version = "xgb-model-v3"
    
    save_model_in_store(model_name)
    save_xgb_classifier(cls_version, cls_path)
