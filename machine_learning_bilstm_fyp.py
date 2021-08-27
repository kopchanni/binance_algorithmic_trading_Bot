# -*- coding: utf-8 -*-
"""machine_learning_bilstm_fyp.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1-ed32Yu17z8UymPuccRUGz9OpjDhWWx-

**REQUIREMENTS / DEPENDANCIES**
"""

!pip install tensorflow-gpu==2.0.0-alpha0

# !pip install tensorflow==2.0.0

"""MAIN CODE"""

# Commented out IPython magic to ensure Python compatibility.
import os
import numpy as np
import tensorflow as tf
import pandas as pd
import seaborn as sns
from pylab import rcParams as rcc
import matplotlib.pyplot as plt
from matplotlib import rc
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.layers import Bidirectional, Dropout, Activation, Dense, LSTM


# %matplotlib inline

sns.set(style='whitegrid', palette='muted', font_scale=1.5)

rcc['figure.figsize'] = 14, 8

RANDOM_SEED = 42

np.random.seed(RANDOM_SEED)

"""GETTING DATA FROM CSV FILE FROM DIRECTORY"""

from google.colab import drive
drive.mount('/content/drive')

import numpy as np
import pandas as pd
data = pd.read_csv(filepath_or_buffer='/content/drive/MyDrive/Machine learning data (fyp)/ETHUSD - 14-08-21 1h.csv')

data

data = data.sort_values('Date')
data.head()

data.shape

import matplotlib.pyplot as plt

ax = data.plot(x='Date', y='Close');
ax.set_xlabel("Date")
ax.set_ylabel("Close Price (USD)")

"""**USING MINMAXSCALER FOR DATA SCALING FOR RANGE**"""

from sklearn.preprocessing import *
from sklearn.cluster import *

SCALER = MinMaxScaler()
prices = data.Close.values.reshape(-1,1)
scaled_prices = SCALER.fit_transform(prices)

scaled_prices.shape

prices.shape

np.isnan(scaled_prices).any()

scaled_prices = scaled_prices[~np.isnan(scaled_prices)]
scaled_prices = scaled_prices.reshape(-1, 1)
np.isnan(scaled_prices).any()

scaled_prices.shape

"""**DATA PREPROCESSING**"""

lenght_of_sequence = 100

def to_sequences(data, lenght_of_sequence):
    list_ = []

    for index in range(len(data) - lenght_of_sequence):
        list_.append(data[index: index + lenght_of_sequence])

    return np.array(list_)

def preprocess_data(dataraw, lenght_of_sequence, training_split):

    data = to_sequences(dataraw, lenght_of_sequence)

    _train = int(training_split * data.shape[0])

    train_X = data[:_train, :-1, :]
    train_Y = data[:_train, -1, :]

    test_X = data[_train:, :-1, :]
    test_Y = data[_train:, -1, :]

    return train_X, train_Y, test_X, test_Y

train_X, train_Y, test_X, test_Y = preprocess_data(dataraw=scaled_prices, lenght_of_sequence=lenght_of_sequence, training_split = 0.5)

test_X.shape

train_X.shape[-1]

"""**MODEL DEVELOPMENT**"""

from keras.layers import Input, LSTM, Dense, TimeDistributed, Activation, BatchNormalization, Dropout, Bidirectional
from keras.models import Sequential

from keras.layers import CuDNNLSTM
#drop 20 percent 
DO = 0.2
#SIZE OF WINDOW
SIZE = lenght_of_sequence - 1
#Linear model initalising for layers  
forecast = Sequential()
#adding layers 
#Bi-LSTM with faster implementation on LSTM layer
#input = 1
forecast.add(Bidirectional(
    CuDNNLSTM(units=SIZE, return_sequences=True),
  input_shape=(SIZE, train_X.shape[-1])
))
#dropout layer
forecast.add(Dropout(rate=DO))

forecast.add(Bidirectional(
    CuDNNLSTM((SIZE * 2), return_sequences=True)
))
#Dropout for Previous layer
forecast.add(Dropout(rate=DO))

forecast.add(Bidirectional(CuDNNLSTM(SIZE, return_sequences=False)
))
#taking all neurons from previous layer
#with output size 1
forecast.add(Dense(units=1))

#output to one layer by activation function
forecast.add(Activation('linear'))

"""**MODEL TRAINING**"""

BATCHSIZE = 64
#training choice
#Loss function mse
#optimizer function ADAM
forecast.compile(
    loss='mean_squared_error',
    optimizer='Adam'
)

history = forecast.fit(
    train_X,
    train_Y,
    epochs=100 ,
    batch_size=BATCHSIZE,
    shuffle=False,
    validation_split= 0.5
)

per = forecast.evaluate(test_X, test_Y)

print(per*100 ,'percent')

plt.plot(history.history['loss'])
plt.plot(history.history['val_loss'])
plt.title('model loss')
plt.ylabel('loss')
plt.xlabel('epoch')

plt.legend(['train', 'test'], loc='upper left')
plt.show()

"""**MODEL PREDICTION**"""

y_pre = forecast.predict(test_X)

y_inverse_test = SCALER.inverse_transform(test_Y)
y_pre_inverse = SCALER.inverse_transform(y_pre)

"""**PLOTTING DATA**"""

import matplotlib.pyplot as plt

 
plt.plot(y_inverse_test, label="Actual Price", color='darkgreen')
plt.plot(y_pre_inverse, label="Predicted Price", color='blue')
 
plt.title('ETH price prediction BiLSTM')
plt.xlabel('Time [HOURS]')
plt.ylabel('Price')
plt.legend(loc='best')
 
plt.show();

last_prediction_price = [y_pre_inverse[-1]]
# last_prediction_arr = np.array(object=last_prediction_price)
# np.append(arr=last_prediction_arr,values=last_prediction_price)
# print(last_prediction_arr)

model_path_saving = '/content/drive/MyDrive/Machine learning data (fyp)/Prediction models'
path = '/content/drive/MyDrive/Machine learning data (fyp)/'

#saving model
# model.save(filepath=path, overwrite=True, save_traces=True,)
checkpoint = keras.callbacks.ModelCheckpoint(filepath=path,save_weights_only=True)

# model.save(filepath=model_path_saving)
model.save_weights(filepath=model_path_saving,overwrite=True)

# model.load_weights(filepath=path)

!ls '/content/drive/MyDrive/Machine learning data (fyp)/'

# model.save(filepath=path)