import torch, optuna
import pandas as pd
import xgboost as xgb
from dags.utils.gm_main_utils import get_embeddings
from sklearn.model_selection import train_test_split 
from sklearn.metrics import recall_score, confusion_matrix
from dags.utils.gm_data_utils import decode_zip, extract_headers, decode_body

model_name = "distilbert-base-uncased"

print("PyTorch version:", torch.__version__)
print("CUDA available:", torch.cuda.is_available())
print(f"Using AutoTokenizer >> {model_name}\n")

path_1 = "data/imp_22-08-2025-03-23.json.gz"
path_2 = "data/unimp_22-08-2025-03-23.json.gz"
path_3 = "data/15-08-2025-10-41.json.gz"

decompressed_data_1 = decode_zip(path_1)
decompressed_data_2 = decode_zip(path_2)
decompressed_data_3 = decode_zip(path_3)

df_imp =pd.DataFrame(decompressed_data_1)[["Payload"]]
df_unimp =pd.DataFrame(decompressed_data_2)[["Payload"]]
df_unlb =pd.DataFrame(decompressed_data_3)[["Payload"]]

df_imp["Subject"] = df_imp["Payload"].apply(extract_headers)
df_imp["Body"] = df_imp["Payload"].apply(decode_body)
df_imp["Important"] = 1
df_imp = df_imp.drop(["Payload"], axis=1)

df_unimp["Subject"] = df_unimp["Payload"].apply(extract_headers)
df_unimp["Body"] = df_unimp["Payload"].apply(decode_body)
df_unimp["Important"] = 0
df_unimp = df_unimp.drop(["Payload"], axis=1)

df_unlb["Subject"] = df_unlb["Payload"].apply(extract_headers)
df_unlb["Body"] = df_unlb["Payload"].apply(decode_body)
df_unlb = df_unlb.drop(["Payload"], axis=1)

train = pd.concat([df_imp, df_unimp])

# Imputing with Subject x10
train["Body"] = train["Body"].fillna(train["Subject"]*10)

print("Generating embeddings of labelled data.")
X = get_embeddings(train, model_name)
print("Finished generating embeddings.\n")
y = torch.from_numpy(train.loc[:,"Important"].values).cuda()

# Train Test Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.25, shuffle=True, random_state=42)

# XGB Dmatrix
dtrain = xgb.DMatrix(X_train, label= y_train)
dtest = xgb.DMatrix(X_test)
y_test_np = y_test.cpu().numpy()

default_params = {
        "objective": "binary:logistic", 
        "eval_metric": "logloss",
        "tree_method": "hist",
        "device": "cuda",
        "random_state": 42,
}

# Optuna Hyperparam tuning
def objective2(trial):
    params = {
        "max_depth": trial.suggest_int("max_depth", 3, 15),
        "eta": trial.suggest_float("eta", 0.01, 0.3),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "gamma": trial.suggest_float("gamma", 0, 5),
        "reg_alpha": trial.suggest_float("reg_alpha", 0, 5.0),
        "reg_lambda": trial.suggest_float("reg_lambda", 0, 5.0),
        **default_params
    }
    
    cls = xgb.train(params, dtrain, num_boost_round = 1500)    
    y_pred_proba = cls.predict(dtest)

    # Convert probabilities to binary predictions
    y_pred_binary = (y_pred_proba > 0.50).astype(int)
    
    score = recall_score(y_test_np, y_pred_binary)
    return score

# Run optimization
print("Starting Optuna Tuning.")
study = optuna.create_study(direction="maximize")
study.optimize(objective2, n_trials=25, n_jobs=5)
print("\nFinished Optuna Tuning.")

# Train wiht best params
print("Training with optuna params.")
optuna_params = study.best_trial.params
cls = xgb.train({**optuna_params,
                 **default_params,}, dtrain, num_boost_round = 1500)
print("Finished training.\n")

# Embeddings of Unlabelled data
print("Generating embeddings of unlabelled data.")
unlb_embeddings = get_embeddings(df_unlb, model_name)
print("Finished generating embeddings.\n")

# Semi-Supervised Learning with best param model
epochs = 10
thresh = 0.95

X_c, y_c = X_train.clone(), y_train.clone()
X_unlb = unlb_embeddings.clone()

print("Starting Semi Supervised Learning.")
for epoch in range(1,epochs):
    print(f"\n>>>> Epoch {epoch} <<<<")

    dtrain = xgb.DMatrix(X_c, label=y_c)
    model = xgb.train({**optuna_params,
                       **default_params}, dtrain, num_boost_round=1500)

    if len(X_unlb) == 0:
        print("No unlabeled data left.")
        break

    d_unlb = xgb.DMatrix(X_unlb)
    y_proba = model.predict(d_unlb)
    y_proba = torch.from_numpy(y_proba).cuda()

    mask = (y_proba > thresh) | (y_proba < 1 - thresh) # a tensor of True, False... of shape y_proba
    
    if mask.sum() == 0:                                # True==1, False==0
        print("No high-confidence samples found.")
        break

    X_pseudo = X_unlb[mask]
    y_pseudo = (y_proba[mask] > 0.5).int() # torch.int

    # move high confidence rows from unlabeled to training set
    X_c = torch.vstack([X_c, X_pseudo]) # stacks vertically. similar to 1d append but for high d's. 
    y_c = torch.cat([y_c, y_pseudo], axis = 0)

    # Remove high confidence rows
    X_unlb = X_unlb[~mask]

    print(f"Added {len(X_pseudo)} pseudo samples. Labeled samples size: {len(y_c)}")

print("Training final model with predicted labels and optuna params.")
# Train final model with predicted labels
dfinal = xgb.DMatrix(X_c, label=y_c)
final_model = xgb.train({**optuna_params,
                 **default_params,}, dfinal, num_boost_round = 1500)

print("Testing.")
dunlabeled = xgb.DMatrix(X_test)
y_pred_proba = final_model.predict(dunlabeled)

# Convert probabilities to binary predictions
# Decrease threshold for negating False Negatives
y_pred_binary = (y_pred_proba >= 0.5).astype(int)

print(f"Recall Score >> {recall_score(y_test_np, y_pred_binary)}")
print(f"Confusion Matrix >>\n{confusion_matrix(y_test_np, y_pred_binary)}")

print("Saving Model")
final_model.save_model("data/model.json")