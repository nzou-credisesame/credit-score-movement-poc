
# coding: utf-8

# In[ ]:


import util as util
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime


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
    if x>=50:
        return 1
    else:
        return 0
    
def letter_converting(f_name, s_name, new_name):
    df[new_name] = df[f_name].apply(lambda x: str(x)) + df[s_name].apply(lambda x: str(x))
    df[new_name] = df[new_name].apply(lambda x: int(x))
    
def trunc(x):
    if abs(x)>10:
        return 10*np.sign(x)
    else:
        return x

def custom_hist(group1, group2, labels, title):
    N = len(labels)
    
    fig, ax = plt.subplots(figsize=(12, 4))
    ind = np.arange(N)    # the x locations for the groups
    width = 0.35         # the width of the bars
    p1 = ax.bar(ind, group1, width, color='skyblue', bottom=0, label = 'Medium Increase Group')
    p2 = ax.bar(ind + width, group2, width,
                color='salmon', bottom=0, label = 'High Increase Group')

    ax.set_title(title)
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(labels)
    ax.legend(loc = 'best')
    
def custom_groupby(col, groupby, sample, trunc_10= False, savetocsv = False):
    if trunc:
        sample[col] = sample[col].apply(lambda x: trunc(x))

    tmp1 = sample[sample[groupby] ==0].groupby(col).size().reset_index()
    tmp2 = sample[sample[groupby] ==1].groupby(col).size().reset_index().rename(columns= {0:1})

    sample  = pd.merge(tmp1, tmp2, on=col, how='outer')
    sample['perc0'] = sample[0]/sum(tmp1[0])
    sample['perc1'] = sample[1]/sum(tmp2[1])
    if savetocsv:
        sample.to_csv('groupbyresults.csv')
    return sample

def cu_grade(x):
    if x<2:
        return int(x*10)/10.0
    else:
        return 2


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


df = df.dropna()
df = df[sample.f_cc_open_count >0]
sample = df
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


# # Analytics

# Sign up profile

# In[ ]:


# In[ ]:


col = 'f_cc_open_count'
groupby = 'delta_score_grade'
sample = df[[col, groupby]]

temp = custom_groupby(col, groupby, sample, trunc_10= True)
custom_hist(temp['perc0'], temp['perc1'],             temp.f_cc_open_count.values, 'Initial Credit Card Account Number vs Score Improvement')


# In[ ]:


col = 'f_auto_open_count'
groupby = 'delta_score_grade'
sample = df[[col, groupby]]

temp = custom_groupby(col, groupby, sample, trunc_10= True)
custom_hist(temp['perc0'], temp['perc1'],             temp[col].values, 'Initial Credit Card Account Number vs Score Improvement')


# In[ ]:


col = 'f_open_late_count'
groupby = 'delta_score_grade'
sample = df[[col, groupby]]

temp = custom_groupby(col, groupby, sample, trunc_10= True)
custom_hist(temp['perc0'], temp['perc1'],temp[col].values, title = '')


# Credit Utilization

# In[ ]:


df['f_credit_utilization_ratio_grade'] = df.f_credit_utilization_ratio.apply(lambda x: cu_grade(x))
df['s_credit_utilization_ratio_grade'] = df.s_credit_utilization_ratio.apply(lambda x: cu_grade(x))


# people with CC

# In[ ]:

f, ax = plt.subplots(figsize=(12, 4))
fig = sns.boxplot(x= 'delta_score_grade' , y="s_credit_utilization_ratio", data=sample, showfliers=False)


# In[ ]:


sample[['delta_score_grade' ,"cu_change"]].groupby('delta_score_grade').describe().reset_index()

f, ax = plt.subplots(figsize=(12, 4))
fig = sns.boxplot(x= 'delta_score_grade' , y="cu_change", data=sample, showfliers=False)


# In[ ]:


col = 'f_credit_utilization_ratio_grade'
groupby = 'delta_score_grade'
sample = df[[col, groupby]]

temp = custom_groupby(col, groupby, sample, trunc_10= True)
custom_hist(temp['perc0'], temp['perc1'],temp[col].values, title = '')


# In[ ]:


col = 's_credit_utilization_ratio_grade'
groupby = 'delta_score_grade'
sample = df[[col, groupby]]

temp = custom_groupby(col, groupby, sample, trunc_10= True)
custom_hist(temp['perc0'], temp['perc1'],temp[col].values, title = '')


# Negative marks change

# In[ ]:


col = 'col_change'
groupby = 'delta_score_grade'
sample = df[[col, groupby]]

temp = custom_groupby(col, groupby, sample, trunc_10= True)
custom_hist(temp['perc0'], temp['perc1'],temp[col].values, title = '')


# In[ ]:


col = 'late_change'
groupby = 'delta_score_grade'
sample = df[[col, groupby]]

temp = custom_groupby(col, groupby, sample, trunc_10= True)
custom_hist(temp['perc0'], temp['perc1'],temp[col].values, title = '')

