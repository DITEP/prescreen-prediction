"""
Data visualization of patient features and targets

>>> %reload_ext autoreload
>>> %autoreload 2
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from prescreen.vcare import targets, vital_signs
from sklearn.manifold import TSNE
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, roc_curve

sns.set()


df: pd.DataFrame = targets.fetch()
signs: pd.DataFrame = vital_signs.fetch()

cols_target, cols_signs = list(df.columns), list(signs.columns)

n_SF = (df['screenfail'] == 1).sum()

df_join = df.merge(signs, 'outer', on=None, left_on='id',
                        right_on='patient_id')
df_join.drop(['nip_x', 'nip_y', 'patient_id'], axis=1, inplace=True)


# pandas plot
df.plot(y='time', kind='hist', title='distribution of survival time',
        bins=128)
plt.show()

df.plot(y='status', kind='hist')
df.plot(y='screenfail', kind='hist')
plt.title('Distribution des Screen Failures')
plt.show()

print(signs.count(numeric_only=True))


# seaborn plots
sns.stripplot(x='status', y='time', data=df, jitter=True)
plt.show()

sns.countplot(x='screenfail', data=df)
plt.title('distribution of failures (n={})'.format(n_SF))
plt.show()

sns.distplot(a=signs['mean_dbp'].fillna(-1))
plt.title('mean diastelic blood pressure distribution')
plt.show()

vars= {'mean_dbp': df_join['mean_dbp'].dropna().mean(),
       'mean_sdp': df_join['mean_sdp'].dropna().mean(),
       'mean_ecog': df_join['mean_ecog'].dropna().mean(),
       'mean_height': df_join['mean_height'].dropna().mean(),
       'mean_weight': df_join['mean_weight'].dropna().mean(),
       'mean_temperature': df_join['mean_temperature'].dropna().mean(),
       'mean_eva': df_join['mean_eva'].dropna().mean(),
       'mean_sato2': df_join['mean_sato2'].dropna().mean(),
       'mean_pulse': df_join['mean_pulse'].dropna().mean(),
       'mean_frequency': df_join['mean_frequency'].dropna().mean()}


plot = sns.pairplot(data=df_join.dropna(), hue='screenfail',
             vars=list(vars.keys()), size=4)
# plot.savefig('data/pairplot_signs.png')
plt.show(plot)


X_embed = TSNE().fit_transform(df_join.fillna(value=vars)[list(vars.keys())])
df_join['embed1'] = X_embed[:, 0]
df_join['embed2'] = X_embed[:, 1]


plt.scatter(x= df_join['embed1'], y=df_join['embed2'],
            c= df_join['screenfail'],
            cmap='viridis', s=20)
plt.colorbar()
plt.show()



# df_filled = df_join.fillna(value=vars)
#
# x_train, x_test, y_train, y_test = \
#     train_test_split(df_filled[list(vars.keys())],
#                      df_join['screenfail'].fillna(0), test_size=0.33,
#                      stratify=df_join['screenfail'].fillna(0))
#
# clf = RandomForestClassifier()
# clf.fit(x_train, y_train)
#
# score = clf.score(x_test, y_test)
# auc = roc_auc_score(y_test, clf.predict_proba(x_test)[:, 1])
#
# print('accuracy: {} \n auc: {}'.format(score, auc))
#
# fpr, tpr, _ = roc_curve(y_test, clf.predict_proba(x_test)[:, 1])
#
# plt.title('baseline ROC for vital signs')
# plt.plot(fpr, tpr, 'b',
# label='AUC = %0.2f'% auc)
# plt.legend(loc='lower right')
# plt.plot([0,1],[0,1],'r--')
# plt.xlim([-0.1,1.2])
# plt.ylim([-0.1,1.2])
# plt.ylabel('True Positive Rate')
# plt.xlabel('False Positive Rate')
# plt.show()
#
#
# y_pred = clf.predict(x_test)
# y_prob = clf.predict_proba(x_test)
#
# y_pred.sum()
