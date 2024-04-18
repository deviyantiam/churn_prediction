from matplotlib import pyplot as plt
from sklearn.metrics import classification_report, confusion_matrix
import shap
import numpy as np
import pandas as pd
import seaborn as sns


def shap_viz(clf, df, col_feature):
    """
    plot shap values
    input
        model,
        df,
        feature column name list
    """
    explainer = shap.TreeExplainer(clf)

    #get Shap values from preprocessed data
    shap_values = explainer.shap_values(df[col_feature])[1]

    # #plot the feature importance
    shap.summary_plot(shap_values,df[col_feature])
 
def df_metrics(y_prob,y_act,model_desc,metric_df):
    """
    document the metric values of all model experimentation
    input:
        y_prob,
        y_actual,
        model description
    output: df
    """
    temp=pd.DataFrame(classification_report(y_prob,y_act, output_dict=True)).transpose()
    temp['model']=model_desc
    metric_df=metric_df._append(temp.iloc[1],ignore_index=True)
    return metric_df

def conf_matrix_viz(confusion_matrix_df):
    # visualization for confusion matrix
    plt.figure(figsize=(10, 3))
    sns.heatmap(data=confusion_matrix_df,annot = True,fmt = '',cmap='RdBu',vmin=0,vmax=500)
    plt.ylabel('Actual')
    plt.xlabel('Prediction')
    plt.show()

def metric_for_test_df(thres,title,y_test,y_test_pred, model_desc, df_metric):
    """
    display metrics and document them in a table
    input:
        threshold of the proba_score
        model_name for title of plot
        y_test
        y_test_pred
        model_description as identifier in a table
        df_metric
    output:
        confusion_matrix_df,
        df_metric
    """
    print('thre={}'.format(thres),'TEST {title}'.format(title=title))
    print('Classification report: \n',classification_report(y_test,np.where(y_test_pred[:,1]>0.5,1,0)))
    df_metric=df_metrics(y_test,np.where(y_test_pred[:,1]>0.5,1,0),model_desc,df_metric)
    confusion_matrix_df=confusion_matrix(y_test,np.where(y_test_pred[:,1]>0.5,1,0))
    return confusion_matrix_df, df_metric

def metric_for_train_df(thres,title,y_train,y_train_pred):
    """
    display metrics 
    input:
    threshold of the proba_score
    model_name for title of plot
    y_train
    y_train_pred
    model_description as identifier in a table
    output:
    confusion_matrix_df
    """
    print('thre={}'.format(thres),'TRAIN {title}'.format(title=title))
    print('Classification report: \n',classification_report(y_train,np.where(y_train_pred[:,1]>0.5,1,0)))
    confusion_matrix_df=confusion_matrix(y_train,np.where(y_train_pred[:,1]>0.5,1,0))
    return confusion_matrix_df

def correlation_plot(df, col_numerical, col_target):
    """
    display correlation_plot 
    output:
    confusion_matrix_df
    """
    corr_=pd.DataFrame(df[col_numerical+[col_target]].corr()[col_target]).sort_values(by=col_target,ascending=True)
    color_=corr_[col_target].apply(lambda x: 'g' if x>0.5 else ('r' if x<-0.5 else 'b'))
    corr_[col_target].plot(figsize=(10,20),kind='barh',color=color_)
    return corr_

def feature_importance_plot(col_feature, clf):
    """
    display feature_importance
    input:
        column list
        model
    output:
    feature importance df
    """
    feature_import=pd.DataFrame({'col':col_feature,'value':clf.feature_importances_})
    feature_import.sort_values('value',ascending=True,inplace=True)
    plt.figure(figsize=(20,10))
    plt.barh(feature_import['col'].head(30), feature_import['value'].head(30))
    plt.show()
    return feature_import