""" Helper functions for Training Model """
import torch, gc
from transformers import AutoModel, AutoTokenizer

def get_embeddings(df, model_name: str):
    """
        Generates embeddings of cleaned corpus for ml classification.
    """
   
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
        
    sub_list = df.loc[:, "Subject"].astype(str).tolist() # turns object to string and returns list of strs
    body_list = df.loc[:, "Body"].astype(str).tolist()
    
    """ Tokenizer takes input list[str], list[list[str]], not just str!!! """
    sub_tokenized = tokenizer(sub_list, truncation=True, max_length=30, padding=True, return_tensors="pt")
    body_tokenized = tokenizer(body_list, truncation=True, max_length=512, padding=True, return_tensors="pt")
    
    if torch.cuda.is_available():
        model.cuda()
        sub_tokenized = {k: v.cuda() for k, v in sub_tokenized.items()}
        body_tokenized = {k: v.cuda() for k, v in body_tokenized.items()}

    with torch.no_grad():
        sub_outputs = model(**sub_tokenized)
        body_outputs = model(**body_tokenized)

        sub_cls_embeddings_t = sub_outputs.last_hidden_state[:, 0, :]
        body_cls_embeddings_t = body_outputs.last_hidden_state[:, 0, :]
        # removes gradient updation
        embd = torch.cat((sub_cls_embeddings_t, body_cls_embeddings_t), 1).detach()
        
    del sub_tokenized
    del body_tokenized
    del model
    del tokenizer

    gc.collect()
    torch.cuda.empty_cache()

    return embd
