import bentoml
with bentoml.importing():
    import gc
    import torch
    import xgboost as xgb
    import torch.nn.functional as F
    from data_validation import PredictRequest
    from transformers import AutoModel, AutoTokenizer

model_name = "embedmodel"
tokenizer_name = "embedmodel_tokenizer"
xgb_model = "xgb-model-v2"
THRESHOLD = 0.35

#https://docs.bentoml.com/en/latest/build-with-bentoml/services.html
@bentoml.service(resources={"gpu": 1})
class PredService:
    def __init__(self):
        # Load the actual model and tokenizer within the instance context
        model_ref = bentoml.models.get(f"{model_name}:latest")
        tokenizer_ref = bentoml.models.get(f"{tokenizer_name}:latest")

        self.device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_ref.path)
        self.model = AutoModel.from_pretrained(model_ref.path)
        self.model.to(self.device)
        self.classifier = bentoml.xgboost.load_model(f"{xgb_model}:latest")
        
    def mean_pooling(self, model_output, attention_mask):
        token_embeddings = model_output[0] #First element of model_output contains all token embeddings
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float().to(device=self.device) #.float() equivalent to .to(torch.float32)
        return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

    @bentoml.api(route="/predict")
    def predict(self, input: PredictRequest) -> list[int]:

        sub_tokenized = self.tokenizer(input.Subject, truncation=True, max_length=20, padding=True, return_tensors="pt")
        body_tokenized = self.tokenizer(input.Body, truncation=True, max_length=512, padding=True, return_tensors="pt")
        
        sub_tokenized = {k: v.to(self.device) for k, v in sub_tokenized.items()}
        body_tokenized = {k: v.to(self.device) for k, v in body_tokenized.items()}

        with torch.no_grad():
            sub_outputs = self.model(**sub_tokenized)
            body_outputs = self.model(**body_tokenized)
        
            sub_cls_embeddings_t = self.mean_pooling(sub_outputs, sub_tokenized['attention_mask'])
            body_cls_embeddings_t = self.mean_pooling(body_outputs, body_tokenized['attention_mask'])
            
            sub_cls_embeddings_t = F.normalize(sub_cls_embeddings_t, p=2, dim=1)
            body_cls_embeddings_t = F.normalize(body_cls_embeddings_t, p=2, dim=1)

            # removes gradient updation
            embd = torch.cat((sub_cls_embeddings_t, body_cls_embeddings_t), 1).detach()
            
        # predict using xgboost
        dpred = xgb.DMatrix(embd)
        pred_proba = self.classifier.predict(dpred)
        pred_binary = (pred_proba > THRESHOLD)  # If above threshold it is considered as Imp email!
                                             # Decrease thresh value to minimize False Negatives.
        # print(pred_proba)
        pred_binary = pred_binary.astype(int).tolist()
        del sub_tokenized
        del body_tokenized

        gc.collect()
        torch.cuda.empty_cache()
        return pred_binary
