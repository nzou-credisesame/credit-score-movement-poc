
# coding: utf-8

# In[ ]:


import util as util
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import matplotlib.cm as cm


from sklearn.cross_validation import train_test_split
from sklearn.metrics.ranking import roc_auc_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import accuracy_score
from sklearn import tree

import statsmodels.api as sm
from scipy.stats.stats import pearsonr
from sklearn.ensemble import RandomForestClassifier


def trunc_balance(x):
    if x == np.inf:
        return 1
    elif x>1:
        return 1
    else:
        return x
    
def classify_score(x):
    if abs(x)>=50:
        return 3*np.sign(x)
    elif abs(x)>=30:
        return 2*np.sign(x)
    elif abs(x)>= 10:
        return 1*np.sign(x)
    elif abs(x)>=0:
        return 0

    
def letter_converting(f_name, s_name, new_name):
    df[new_name] = df[f_name].apply(lambda x: str(x)) + df[s_name].apply(lambda x: str(x))
    df[new_name] = df[new_name].apply(lambda x: int(x))
    
def trunc(x):
    if abs(x)>10:
        return 10*np.sign(x)
    else:
        return x

def cu_grade(x):
    if x<2:
        return int(x*10)/10.0
    else:
        return 2


def custom_groupby(col, groupby, sample, trunc_10= False, savetocsv = False):
    if trunc:
        sample[col] = sample[col].apply(lambda x: trunc(x))
        
    val = list(set(sample[groupby]))
    tmp = sample[sample[groupby] ==val[0]].groupby(col).size().reset_index().rename(columns= {0:val[0]})
    tmp[str(val[0])+'_perc'] = tmp[val[0]]/sum(tmp[val[0]])
    col_list = [col, str(val[0])+'_perc']
    for i in val[1:]:

        tmp1 = sample[sample[groupby] ==i].groupby(col).size().reset_index().rename(columns= {0:i})
        tmp1[str(i)+'_perc'] = tmp1[i]/sum(tmp1[i])

        tmp  = pd.merge(tmp, tmp1, on=col, how='outer')
        col_list.append(str(i)+'_perc')
        
    if savetocsv:
        tmp.to_csv('groupbyresults.csv')
    return tmp[col_list]


# In[ ]:


query = open('0-profile-data.sql','r') .read()
df = util.redshift_query_to_df(query)
# raw data
print 'raw data size: ' + str(df.shape)

df = df.dropna()
print 'Drop NA values: ' + str(df.shape)


# # Variable PreProcessing

# In[ ]:


df['delta_score'] = df.second_cs - df.first_cs
df['delta_score_grade'] =  df.delta_score.apply(lambda x: classify_score(x))
df['first_cs_grade'] =  df.first_cs.apply(lambda x: (int(x)/25)*25)

df['cu_change'] = df.s_credit_utilization_ratio - df.f_credit_utilization_ratio
df['bal_change'] = (df.s_total_open_balance - df.f_total_open_balance)/ df.f_total_open_balance 
df['auto_change'] = df.s_auto_open_count - df.f_auto_open_count
df['cc_change'] = df.s_cc_open_count - df.f_cc_open_count
df['pl_change'] = df.s_pl_open_count - df.f_pl_open_count

df['acc_other_change'] = df.s_other_open_count - df.f_other_open_count
df['cl_change'] = df.s_total_open_limit - df.f_total_open_limit
df['late_change'] = df.s_open_late_count - df.f_open_late_count
df['col_change'] =  df.s_collection_open_count - df.f_collection_open_count

# Balance Change:
## (new_balance - first_balance)/first_balance
df['bal_change'] = (df.s_total_open_balance - df.f_total_open_balance)/ df.f_total_open_balance 
df.bal_change = df.bal_change.apply(lambda x: trunc_balance(x))

# Pay down by personal loan
## balance decreased
## new personal loan showed up
df['pl_paydown'] = (df.bal_change < 0) & (df.pl_change >0)
# Increase credit limit by new credit card
## credit limit increase
## new credit card showed up
df['cc_limitinc'] = (df.cl_change > 0) & (df.cc_change >0)

for i in df.columns:
    if df[i].dtype == 'timedelta64[ns]':
        df[i] = df[i].apply(lambda x: (x.days)/365)
df.f_oldest_open_account_age = df.f_oldest_open_account_age

# Converting Grade to Numbers
## A->A : 11
## A->B : 12

letter_list = [u'first_payment_history_grade', u'first_credit_utilization_grade',
        u'first_credit_age_grade', u'first_account_mix_grade', u'first_credit_inquiries_grade'
        , u'second_payment_history_grade', u'second_first_credit_utilization_grade'
        , u'second_credit_age_grade', u'second_account_mix_grade', u'second_credit_inquiries_grade']
for i in letter_list:
    
    df[i] = df[i].apply(lambda x: ord(x)-64)
    
letter_converting('first_account_mix_grade', 'second_account_mix_grade', 'mix_grade_change')
letter_converting('first_credit_inquiries_grade', 'second_credit_inquiries_grade', 'inquiries_grade_change')
letter_converting('first_credit_age_grade', 'second_credit_age_grade', 'age_grade_change')
letter_converting('first_payment_history_grade', 'second_payment_history_grade', 'payment_grade_change')


df['f_credit_utilization_ratio_grade'] = df.f_credit_utilization_ratio.apply(lambda x: cu_grade(x))
df['s_credit_utilization_ratio_grade'] = df.s_credit_utilization_ratio.apply(lambda x: cu_grade(x))


# # Feature Selection

# In[ ]:


col_list = []
for i in df.columns:
    if (df[i].dtype == 'int64') or (df[i].dtype == 'float64') or ((df[i].dtype == 'bool')):
        col_list.append(i)
col_list.remove('creditinfoid')
col_list.remove( 'first_cs')
col_list.remove( 'second_cs')
col_list.remove('delta_score')
col_list.remove('delta_score_grade')
col_list.remove('s_credit_utilization_ratio')
col_list.remove('s_total_open_balance')
col_list.remove('s_auto_open_count')
col_list.remove('s_cc_open_count')
col_list.remove('s_pl_open_count')
col_list.remove('s_other_open_count')
col_list.remove('s_total_open_limit')
col_list.remove('s_open_late_count')
col_list.remove('s_collection_open_count')
col_list.remove('cc_change')
for i in letter_list:
    col_list.remove(i)


# In[ ]:


sample = df.dropna()
print sample.groupby('delta_score_grade').size()
X = sample[col_list]
y = sample['delta_score_grade']


# In[ ]:


classifier = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=0)
classifier.fit(X, y)
prediction = classifier.predict(X)
print accuracy_score(y, prediction)
x = {}
for name, importance in zip(X.columns, classifier.feature_importances_):
    x[name]=importance
sorted(x, key=x.get, reverse=True)


# In[ ]:


sorted(x, key=x.get, reverse=True)


# # Analytics

# Sign up profile

# people with CC

# In[ ]:


sample = df[df.f_cc_open_count >0]

f, ax = plt.subplots(figsize=(12, 4))
fig = sns.boxplot(x= 'delta_score_grade' , y="cu_change", data=sample, showfliers=False)


# In[ ]:


sample = df[df.f_cc_open_count >0]
sample = sample[['f_credit_utilization_ratio_grade', 's_credit_utilization_ratio_grade', 'delta_score']]
sample.groupby(['f_credit_utilization_ratio_grade', 's_credit_utilization_ratio_grade']).size().reset_index().to_csv('temp.csv')


# Negative marks change

# In[ ]:


sample = df
sample = sample[['f_open_late_count','s_open_late_count', 'delta_score']]
sample.groupby(['f_open_late_count','s_open_late_count']).size().reset_index().to_csv('temp.csv')


# In[ ]:


sample = df
sample = sample[['f_collection_open_count','s_collection_open_count', 'delta_score']]
sample.groupby(['f_collection_open_count','s_collection_open_count']).size().reset_index().to_csv('temp.csv')

