# importing modules
import pandas as pd
import numpy as np
import csv
from keras.layers.core import Dense, Activation, Dropout
from keras.models import Sequential
import sys
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay
import keras
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split
from plot_keras_history import plot_history
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from keras.utils import np_utils
import pandas as pd
from sklearn.utils import shuffle
sys.path.append('../resource')
from MSSqlDb import MSSqlDbWrapper

mssql_instance = MSSqlDbWrapper("../config/config1.txt")
con = mssql_instance.get_connect()
df = pd.read_sql("SELECT * from packet_table order by IP, PORT;",con)
df['PlanPurchasedPlatform'] = pd.Categorical(df.PlanPurchasedPlatform).codes
df['PlanType'] = pd.Categorical(df.PlanType).codes
df['PlanAction'] =  pd.Categorical(df.PlanAction).codes
df = shuffle(df)
print(df.head())
print(df.columns)
y = df['ChurnLabel']
df.drop(['ChurnLabel','UserId','UserPlanId'],inplace=True, axis=1)
X = df
X.fillna(0,inplace=True)
np.nan_to_num(X)
# scaled_X = X
Xscaler = StandardScaler()
Xscaler.fit(X)
scaled_X = Xscaler.transform(X)
np.nan_to_num(scaled_X)
pca = PCA(0.95)
pca.fit(scaled_X)
scaled_X = pca.transform(scaled_X)
print(scaled_X.shape)

#train, test set splitting
#train_x, test_x, train_y, test_y = train_test_split( scaled_X, np.array(y), test_size=1/7.0, random_state=0)
train_x = scaled_X
train_y = y

train_y=np_utils.to_categorical(train_y,num_classes=2)
#test_y=np_utils.to_categorical(test_y,num_classes=2)
print("Shape of y_train",train_y.shape)
#print("Shape of y_test",test_y.shape)

model=Sequential()
model.add(Dense(1000,input_dim=scaled_X.shape[1],activation='relu'))
model.add(Dense(400,activation='relu'))
model.add(Dense(100,activation='relu'))
model.add(Dropout(0.1))
model.add(Dense(2,activation='sigmoid'))
model.compile(loss='binary_crossentropy',optimizer='adam',metrics=['accuracy',keras.metrics.Recall()])
print(model.summary())


df = pd.read_sql("SELECT [UserId],[UserPlanId],[PlanPurchasedPlatform],[PlanDurationDays],[PlanType],[IsOverLapPlan],[MediaTimeInSecsW0],[MediaTimeInSecsW1],[MediaTimeInSecsW2],[MediaTimeInSecsW3],[MediaTimeInSecsW4],[MediaTimeInSecsW5],[MediaTimeInSecsW6],[MediaTimeInSecsW7],[MediaCountW0],[MediaCountW1],[MediaCountW2],[MediaCountW3],[MediaCountW4],[MediaCountW5],[MediaCountW6],[MediaCountW7],[CollectionCountW0],[CollectionCountW1],[CollectionCountW2],[CollectionCountW3],[CollectionCountW4],[CollectionCountW5],[CollectionCountW6],[CollectionCountW7],[CollectionCount25PW0],[CollectionCount25PW1],[CollectionCount25PW2],[CollectionCount25PW3],[CollectionCount25PW4],[CollectionCount25PW5],[CollectionCount25PW6],[CollectionCount25PW7],[CollectionCount50PW0],[CollectionCount50PW1],[CollectionCount50PW2],[CollectionCount50PW3],[CollectionCount50PW4],[CollectionCount50PW5],[CollectionCount50PW6],[CollectionCount50PW7],[CollectionCount75PW0],[CollectionCount75PW1],[CollectionCount75PW2],[CollectionCount75PW3],[CollectionCount75PW4],[CollectionCount75PW5],[CollectionCount75PW6],[CollectionCount75PW7],[IsPaidNew1],[IsPlanActive],[CollectionCount100PW0],[CollectionCount100PW1],[CollectionCount100PW2],[CollectionCount100PW3],[CollectionCount100PW4],[CollectionCount100PW5],[CollectionCount100PW6],[CollectionCount100PW7],[MediaTimeRMean1],[MediaTimeRMean2],[MediaTimeRMean3],[MediaTimeRMean4],[MediaTimeRMean5],[MediaTimeRMedian1],[MediaTimeRMedian2],[MediaTimeRMedian3],[MediaTimeRMedian4],[MediaTimeRMedian5],[MediaTimeRSTD1],[MediaTimeRSTD2],[MediaTimeRSTD3],[MediaTimeRSTD4],[MediaTimeRSTD5],[MediaTimeRMin1],[MediaTimeRMin2],[MediaTimeRMin3],[MediaTimeRMin4],[MediaTimeRMin5],[MediaTimeRMax1],[MediaTimeRMax2],[MediaTimeRMax3],[MediaTimeRMax4],[MediaTimeRMax5],[MediaCountRMean1],[MediaCountRMean2],[MediaCountRMean3],[MediaCountRMean4],[MediaCountRMean5],[MediaCountRMedian1],[MediaCountRMedian2],[MediaCountRMedian3],[MediaCountRMedian4],[MediaCountRMedian5],[MediaCountRSTD1],[MediaCountRSTD2],[MediaCountRSTD3],[MediaCountRSTD4],[MediaCountRSTD5],[MediaCountRMin1],[MediaCountRMin2],[MediaCountRMin3],[MediaCountRMin4],[MediaCountRMin5],[MediaCountRMax1],[MediaCountRMax2],[MediaCountRMax3],[MediaCountRMax4],[MediaCountRMax5],[CollectionCountRMean1],[CollectionCountRMean2],[CollectionCountRMean3],[CollectionCountRMean4],[CollectionCountRMean5],[CollectionCountRMedian1],[CollectionCountRMedian2],[CollectionCountRMedian3],[CollectionCountRMedian4],[CollectionCountRMedian5],[CollectionCountRSTD1],[CollectionCountRSTD2],[CollectionCountRSTD3],[CollectionCountRSTD4],[CollectionCountRSTD5],[CollectionCountRMin1],[CollectionCountRMin2],[CollectionCountRMin3],[CollectionCountRMin4],[CollectionCountRMin5],[CollectionCountRMax1],[CollectionCountRMax2],[CollectionCountRMax3],[CollectionCountRMax4],[CollectionCountRMax5],[CollectionCount25PMean1],[CollectionCount25PMean2],[CollectionCount25PMean3],[CollectionCount25PMean4],[CollectionCount25PMean5],[CollectionCount25PMedian1],[CollectionCount25PMedian2],[CollectionCount25PMedian3],[CollectionCount25PMedian4],[CollectionCount25PMedian5],[CollectionCount25PSTD1],[CollectionCount25PSTD2],[CollectionCount25PSTD3],[CollectionCount25PSTD4],[CollectionCount25PSTD5],[CollectionCount25PMin1],[CollectionCount25PMin2],[CollectionCount25PMin3],[CollectionCount25PMin4],[CollectionCount25PMin5],[CollectionCount25PMax1],[CollectionCount25PMax2],[CollectionCount25PMax3],[CollectionCount25PMax4],[CollectionCount25PMax5],[CollectionCount50PRMean1],[CollectionCount50PRMean2],[CollectionCount50PRMean3],[CollectionCount50PRMean4],[CollectionCount50PRMean5],[CollectionCount50PRMedian1],[CollectionCount50PRMedian2],[CollectionCount50PRMedian3],[CollectionCount50PRMedian4],[CollectionCount50PRMedian5],[CollectionCount50PRSTD1],[CollectionCount50PRSTD2],[CollectionCount50PRSTD3],[CollectionCount50PRSTD4],[CollectionCount50PRSTD5],[CollectionCount50PRMin1],[CollectionCount50PRMin2],[CollectionCount50PRMin3],[CollectionCount50PRMin4],[CollectionCount50PRMin5],[CollectionCount50PRMax1],[CollectionCount50PRMax2],[CollectionCount50PRMax3],[CollectionCount50PRMax4],[CollectionCount50PRMax5],[CollectionCount75PRMean1],[CollectionCount75PRMean2],[CollectionCount75PRMean3],[CollectionCount75PRMean4],[CollectionCount75PRMean5],[CollectionCount75PRMedian1],[CollectionCount75PRMedian2],[CollectionCount75PRMedian3],[CollectionCount75PRMedian4],[CollectionCount75PRMedian5],[CollectionCount75PRSTD1],[CollectionCount75PRSTD2],[CollectionCount75PRSTD3],[CollectionCount75PRSTD4],[CollectionCount75PRSTD5],[CollectionCount75PRMin1],[CollectionCount75PRMin2],[CollectionCount75PRMin3],[CollectionCount75PRMin4],[CollectionCount75PRMin5],[CollectionCount75PRMax1],[CollectionCount75PRMax2],[CollectionCount75PRMax3],[CollectionCount75PRMax4],[CollectionCount75PRMax5],[CollectionCount100PRMean1],[CollectionCount100PRMean2],[CollectionCount100PRMean3],[CollectionCount100PRMean4],[CollectionCount100PRMean5],[CollectionCount100PRMedian1],[CollectionCount100PRMedian2],[CollectionCount100PRMedian3],[CollectionCount100PRMedian4],[CollectionCount100PRMedian5],[CollectionCount100PRSTD1],[CollectionCount100PRSTD2],[CollectionCount100PRSTD3],[CollectionCount100PRSTD4],[CollectionCount100PRSTD5],[CollectionCount100PRMin1],[CollectionCount100PRMin2],[CollectionCount100PRMin3],[CollectionCount100PRMin4],[CollectionCount100PRMin5],[CollectionCount100PRMax1],[CollectionCount100PRMax2],[CollectionCount100PRMax3],[CollectionCount100PRMax4],[CollectionCount100PRMax5],[NumberOfdaysUserProgressedPlanStartDate],[NumberOfdaysUserProgressedCancelledDate],[PlanAction],[ChurnLabel] FROM [Weekly].[Intermediate] where Monthly_Run_Id = 4 and IgnoreFlag = 0;",con)
df['PlanPurchasedPlatform'] = pd.Categorical(df.PlanPurchasedPlatform).codes
df['PlanType'] = pd.Categorical(df.PlanType).codes
df['PlanAction'] =  pd.Categorical(df.PlanAction).codes
df = shuffle(df)
print(df.head())
print(df.columns)
y = df['ChurnLabel']
df.drop(['ChurnLabel','UserId','UserPlanId'],inplace=True, axis=1)
X = df
X.fillna(0,inplace=True)
np.nan_to_num(X)
# scaled_X = X
scaled_X = Xscaler.transform(X)
np.nan_to_num(scaled_X)
scaled_X = pca.transform(scaled_X)
print(scaled_X.shape)

test_x = scaled_X
test_y = y
test_y=np_utils.to_categorical(test_y,num_classes=2)
history = model.fit(train_x,train_y,validation_data=(test_x,test_y),batch_size=500,epochs=4,verbose=1)

plot_history(history.history, path="standard.png")
plt.show()

prediction=model.predict(test_x)
y_label=np.argmax(test_y,axis=1)
predict_label=np.argmax(prediction,axis=1)

# accuracy=np.sum(y_label==predict_label)/length * 100 
# print("Accuracy of the dataset",accuracy )
print(classification_report(y_label, predict_label))