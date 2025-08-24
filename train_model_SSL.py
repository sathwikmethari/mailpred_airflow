import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from dags.utils.utils import decode_zip, extract_headers, decode_body, get_embeddings

path_1 = "data/imp_22-08-2025-03-23.json.gz"
path_2 = "data/unimp_22-08-2025-03-23.json.gz"
decompressed_data_1 = decode_zip(path_1)
decompressed_data_2 = decode_zip(path_2)

df_imp =pd.DataFrame(decompressed_data_1)[["Payload"]]
df_unimp =pd.DataFrame(decompressed_data_2)[["Payload"]]

df_imp["Subject"] = df_imp["Payload"].apply(extract_headers)
df_imp["Body"] = df_imp["Payload"].apply(decode_body)
df_imp["Important"] = 1
df_imp = df_imp.drop(["Payload"], axis=1)

df_unimp["Subject"] = df_unimp["Payload"].apply(extract_headers)
df_unimp["Body"] = df_unimp["Payload"].apply(decode_body)
df_unimp["Important"] = 0
df_unimp = df_unimp.drop(["Id", "Payload"], axis=1)

train = pd.concat([df_imp, df_unimp])
train.isnull().sum()



#X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, shuffle=True, random_state=42)




# dtrain = xgb.DMatrix(X_train, label=y_train)
# params = {
#     "objective": "binary:logistic", 
#     "eval_metric": "logloss",
#     "eta": 0.1,
#     "max_depth": 6
# }
# model = xgb.train(params, dtrain, num_boost_round=200)

# dunlabeled = xgb.DMatrix(X_test)
# y_pred_proba = model.predict(dunlabeled)