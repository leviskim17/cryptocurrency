import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

data = pd.read_csv('predictionLSTM.csv',header=None)
print (data)

short_rolling = data.rolling(window=2).mean()

long_rolling = data.rolling(window=10).mean()

returns = data.pct_change(1)

log_returns = np.log(data).diff()

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16,12))

for c in log_returns:
    ax1.plot(log_returns.index, log_returns[c].cumsum(), label=str(c))

ax1.set_ylabel('Cumulative returns')
ax1.legend(loc='best')

for c in log_returns:
    ax2.plot(log_returns.index, 100*(np.exp(log_returns[c].cumsum()) - 1), label=str(c))

ax2.set_ylabel('Total returns (%)')
ax2.legend(loc='best')

plt.show()