"""
Trains an XGBoost classifier on ml/data/fraud_dataset.csv and serializes it.

Two artifacts get saved, and both are needed at inference time:
  - fraud_model.json: the trained model itself (XGBoost's native format,
    not pickle, so it isn't tied to the exact library version used to train it)
  - feature_schema.json: the exact one-hot column order and category lists
    used during training. New transactions must be one-hot encoded against
    these same categories, in this same order, or the model will misread
    every feature it's given.
"""
import json

import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split

DATA_PATH = "ml/data/fraud_dataset.csv"
MODEL_PATH = "ml/models/fraud_model.json"
SCHEMA_PATH = "ml/models/feature_schema.json"

TRANSACTION_TYPES = ["transfer", "deposit", "withdrawal", "fee", "reversal"]


def featurize(df: pd.DataFrame, occupations: list[str]) -> pd.DataFrame:
    df = df.copy()
    df["transaction_type"] = pd.Categorical(df["transaction_type"], categories=TRANSACTION_TYPES)
    df["occupation"] = pd.Categorical(df["occupation"], categories=occupations)
    return pd.get_dummies(df, columns=["transaction_type", "occupation"])


def main():
    df = pd.read_csv(DATA_PATH)
    occupations = sorted(df["occupation"].unique())

    X = featurize(df.drop(columns=["is_fraud"]), occupations)
    y = df["is_fraud"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        eval_metric="logloss",
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    print(classification_report(y_test, preds, target_names=["not_fraud", "fraud"]))
    print("confusion matrix:\n", confusion_matrix(y_test, preds))

    model.save_model(MODEL_PATH)
    with open(SCHEMA_PATH, "w") as f:
        json.dump(
            {
                "columns": list(X.columns),
                "transaction_types": TRANSACTION_TYPES,
                "occupations": occupations,
            },
            f,
        )

    print(f"Saved model to {MODEL_PATH}")
    print(f"Saved feature schema to {SCHEMA_PATH}")


if __name__ == "__main__":
    main()
