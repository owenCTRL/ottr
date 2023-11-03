import pandas as pd
import numpy as np

holdings = pd.read_csv('data/IWD_holdings.csv', sep=";").dropna()
holdings = holdings.sort_values('Weight (%)', ascending=False)
holdings[holdings['Weight (%)'] > np.percentile(holdings['Weight (%)'], 95)].head(10)

print(holdings)