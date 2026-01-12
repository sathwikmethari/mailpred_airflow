""" Helper functions for Training Model """
import torch, gc
import torch.nn.functional as F
from transformers import AutoModel, AutoTokenizer

def mean_pooling(model_output, attention_mask, device):
    token_embeddings = model_output[0] #First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float().to(device=device) #.float equivalent to .to(torch.float32)
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

def get_embeddings(text_dic: dict[str, list[str]], model_name: str):
    """
        Generates embeddings of cleaned corpus for ml classification.
    """
   
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
    device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")       
    
    """ Tokenizer takes input list[str], list[list[str]], not just str!!! """
    sub_tokenized = tokenizer(text_dic["Subject"], truncation=True, max_length=20, padding=True, return_tensors="pt")
    body_tokenized = tokenizer(text_dic["Body"], truncation=True, max_length=512, padding=True, return_tensors="pt")
    
    if torch.cuda.is_available():
        model.cuda()
        sub_tokenized = {k: v.cuda() for k, v in sub_tokenized.items()}
        body_tokenized = {k: v.cuda() for k, v in body_tokenized.items()}

    with torch.no_grad():
        sub_outputs = model(**sub_tokenized)
        body_outputs = model(**body_tokenized)
    
        sub_cls_embeddings_t = mean_pooling(sub_outputs, sub_tokenized['attention_mask'], device = device)
        body_cls_embeddings_t = mean_pooling(body_outputs, body_tokenized['attention_mask'], device = device)
        
        sub_cls_embeddings_t = F.normalize(sub_cls_embeddings_t, p=2, dim=1)
        body_cls_embeddings_t = F.normalize(body_cls_embeddings_t, p=2, dim=1)

        # removes gradient updation
        embd = torch.cat((sub_cls_embeddings_t, body_cls_embeddings_t), 1).detach()
        
    del sub_tokenized
    del body_tokenized
    del model
    del tokenizer

    gc.collect()
    torch.cuda.empty_cache()

    return embd
