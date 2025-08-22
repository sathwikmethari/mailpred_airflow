import xgboost as xgb
from sklearn.model_selection import train_test_split
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