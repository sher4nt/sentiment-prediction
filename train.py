import sys
import pandas as pd
from joblib import dump
from sklearn.linear_model import LogisticRegression

path_in, path_out = sys.argv[1], sys.argv[2]

df = pd.read_csv(path_in)
X_train, y_train = df.iloc[:,1:], df.iloc[:,0]

model = LogisticRegression()
model.fit(X_train, y_train)

dump(model, path_out)