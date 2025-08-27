<h1 align="center">🌀 Airflow Pipeline</h1>

<p align="center">
  <b>An Apache Airflow pipeline that performs ELT (Extract, Load, Transform) and applies a Machine Learning.</b><br/>
  Extract → Load → Transform → Predict → Delete
</p>

---

## 📖 Overview
This project provides a production-ready **Airflow DAG** that:

- 📥 **Extracts** raw data from Gmail API. 
- 🗄 **Saves** the data.
- 🧹 **Transforms** & cleans the data for prediction.
- 🤖 **Predicts** outcomes with a **Pre-Semi-Supervised** trained ML model.  
- 📊 **Sends** Results for downstream analytics.  

---

## 🛠 Tech Stack
| Component      | Technology |
|----------------|------------|
| Workflow Orchestration | [Apache Airflow](https://airflow.apache.org/) |
| Language | [Python](https://www.python.org/) |
| Data Processing/Transformation | [NumPy](https://numpy.org/), [Pandas](https://pandas.pydata.org/), [PyTorch](https://pytorch.org/), [HuggingFace](https://huggingface.co/) |
| ML Models | [XGBoost](https://xgboost.readthedocs.io/) |
| Hyperparameter Tuning | [Optuna](https://optuna.org/) |
| Deployment | [Docker](https://www.docker.com/) |

---

## ⚡ Quick Start

### 🔹 1. Extraction
- Used **Asynchronous Python** programming to make the extraction process faster and more efficient.
### 🔹 2. Loading
- Extracted data is compressed with **msgspec** and saved in local storage as backup.
### 🔹 3. Preprocessing
- Extracted gmail payload is cleaned using custom functions.
- Useful text data is extracted and then turned into tokens using **HuggingFace** pretrained transformers.
- Embeddings are then generated using these tokens.
### 🔹 4. Prediction
- XGboost model is trained on sample of manually labelled data.
- Using Semi-Supervised Learning, model is further trained on larger sample of data.
- Preprocessed data is fed into the trained model for predicting unimportant emails.
### 🔹 5. Deletion
- Used asynchronous python functions for faster and more efficient deletion. 
