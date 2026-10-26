"""
Synthetic dataset for bootstrapping a fraud classifier.

Real data isn't available yet: `users` has no age/occupation columns, and
there's no login-attempts source wired up in the pipeline. Shaped like the
real transaction types, otherwise fully synthetic.

Label rule: fraud = (login attempts in the last 30 min is unusually high)
AND (transaction amount is unusually high). Either alone is not fraud.
"""
import numpy as np
import pandas as pd
from faker import Faker

N_ROWS = 20_000
LOGIN_ATTEMPTS_THRESHOLD = 5
AMOUNT_THRESHOLD = 5_000
OUTPUT_PATH = "ml/data/fraud_dataset.csv"

TRANSACTION_TYPES = ["transfer", "deposit", "withdrawal", "fee", "reversal"]
TRANSACTION_TYPE_WEIGHTS = [0.5, 0.2, 0.2, 0.05, 0.05]

fake = Faker()
Faker.seed(42)
rng = np.random.default_rng(42)


def generate_row():
    transaction_type = rng.choice(TRANSACTION_TYPES, p=TRANSACTION_TYPE_WEIGHTS)
    customer_age = int(np.clip(rng.normal(42, 15), 18, 90))
    occupation = fake.job()

    # ~6% of rows are deliberately drawn from an "attack-shaped" regime:
    # a burst of login attempts shortly before an unusually large transaction.
    # Everything else is ordinary account activity, which will almost never
    # cross both thresholds at once. This keeps the fraud rule meaningful
    # (amount alone, or login attempts alone, is still not fraud) while
    # guaranteeing enough positive examples to actually train and evaluate on.
    if rng.random() < 0.06:
        amount = round(float(rng.lognormal(mean=9.0, sigma=0.6)), 2)
        login_attempts_last_30min = int(rng.poisson(10))
    else:
        amount = round(float(rng.lognormal(mean=5.0, sigma=1.1)), 2)
        login_attempts_last_30min = int(rng.poisson(1))

    return transaction_type, amount, customer_age, occupation, login_attempts_last_30min


def main():
    rows = [generate_row() for _ in range(N_ROWS)]
    df = pd.DataFrame(
        rows,
        columns=[
            "transaction_type",
            "amount",
            "customer_age",
            "occupation",
            "login_attempts_last_30min",
        ],
    )

    df["is_fraud"] = (
        (df["login_attempts_last_30min"] > LOGIN_ATTEMPTS_THRESHOLD)
        & (df["amount"] > AMOUNT_THRESHOLD)
    ).astype(int)

    print(df["is_fraud"].value_counts(normalize=True).rename("fraction"))

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {len(df)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
