import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import pyspark as spark
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import mean_squared_error
sc = spark.SparkContext()
sql = spark.SQLContext(sc)
from pyspark.sql.types import *
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from elephas.utils.rdd_utils import to_simple_rdd

def series_to_supervised(data, n_in=1, n_out=1, dropnan=True):
    n_vars = 1 if type(data) is list else data.shape[1]
    df = pd.DataFrame(data)
    cols, names = list(), list()
    # input sequence (t-n, ... t-1)
    for i in range(n_in, 0, -1):
        cols.append(df.shift(i))
        names += [('var%d(t-%d)' % (j+1, i)) for j in range(n_vars)]
    # forecast sequence (t, t+1, ... t+n)
    for i in range(0, n_out):
        cols.append(df.shift(-i))
        if i == 0:
            names += [('var%d(t)' % (j+1)) for j in range(n_vars)]
        else:
            names += [('var%d(t+%d)' % (j+1, i)) for j in range(n_vars)]
    # put it all together
    agg = pd.concat(cols, axis=1)
    agg.columns = names
    # drop rows with NaN values
    if dropnan:
        agg.dropna(inplace=True)
    return agg


# ## Linear Regression
df1 = pd.read_csv('./meta/modeldata.csv',header=None)
df1.columns = ['DateTime','Negative','Neutral','Positive','Compound','Price']
print (df1.head())


# ## Exploratory analysis
values = df1.values
# specify columns to plot
groups = [ 1,2,3,4,5]
i = 1
# plot each column
plt.figure()
for group in groups:
    plt.subplot(len(groups), 1, i)
    plt.plot(values[:, group])
    plt.title(df1.columns[group], y=0.5, loc='right')
    i += 1
plt.show()

df1.set_index('DateTime',inplace=True)  #making DateTime as index. Because we are doing Time series Analysis
cols = df1.columns.tolist()
cols = cols[-1:] + cols[:-1]   #just making the price as the first column
df1 = df1[cols]
df1.head()

scaler = MinMaxScaler(feature_range=(0, 1)) #scaling values from 0 to 1
scaled = scaler.fit_transform(df1.values)

n_hours = 5
n_features = 5              
n_obs = n_hours*n_features

reframed = series_to_supervised(scaled, n_hours, 1)  #t-3,t-2,t-1 values of all the columns. you can change n_hours for this
reframed.head()

reframed = reframed.drop(reframed.columns[-4:], axis=1)  #dropping all the present values except present price
reframed.head()  #Note: here last column is present price which is our target variable

cols = reframed.columns.tolist()
cols = cols[-1:] + cols[:-1]   #just making the price as the first column
reframed = reframed[cols]
reframed.head()

ratio = 0.5
train = reframed[int(ratio*len(reframed)):]
test = reframed[int(ratio*len(reframed)):]

from pyspark.ml.linalg import Vectors
reg_DF =sql.createDataFrame(train)
ad_df_train = reg_DF.rdd.map(lambda x: [Vectors.dense(x[1:]), x[0]]).toDF(['features', 'label'])
ad_df_train.show(5)

reg_DF_test = sql.createDataFrame(test)
ad_df_test = reg_DF_test.rdd.map(lambda x: [Vectors.dense(x[1:]), x[0]]).toDF(['features', 'label'])

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = 'features', labelCol = 'label')

lr_model = lr.fit(ad_df_train)

predictions = lr_model.transform(ad_df_test)
someDF = predictions.select("prediction", "label").show(5)

predictions = predictions.toPandas()
aaa = ad_df_test.toPandas()

plt.plot(predictions['label'], label='actual')
plt.plot(predictions['prediction'], label='prediction')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.show()

print("Coefficients: %s" % str(lr_model.coefficients))
print("Intercept: %s" % str(lr_model.intercept))

