import bin.get_data as get_data
import bin.clustering as clustering
from matplotlib import pyplot as plt
from datetime import datetime, date, time, timedelta
import bin.preprocess as prc
import pandas as pd


# Based model class ======================
class ClusteringModel:
    def __init__(self):
        self.clustered_df = pd.DataFrame()
        self.clustered_column_name = ''
        self.fit_finished = False
        self.describe_features = []

    def describe(self):
        if self.fit_finished:
            return clustering.describe_km(self.clustered_df, cluster_id_column_name=self.clustered_column_name, feature_columns=self.describe_features)
        else:
            print('Fit model first!')
            return ''

# Sub class ==============================
class CustomerValue(ClusteringModel):
    def __init__(self):
        self.rfm_features = ['monetary_stdrd', 'frequency_stdrd', 'leader_score']
        self.rfm_clustered_features = ['uid','monetary','frequency','leader_score']
        self.fit_finished = False
        self.clustered_column_name = 'customer_value_cid'
        self.describe_features = ['monetary', 'frequency', 'leader_score']

    def fit(self, prc_df:pd.DataFrame, k=0):
        """訓練客戶價值分析模型。
        Main method : Clustering.

        Args:
            prc_df (pd.DataFrame): order dataframe after preprocessing.
            k (int, optional): k==0 will choose bestk by silhouette. Defaults to 0.
        """        
        # Modeling
        ## Feature matrix
        self.rfm_df = prc_df[self.rfm_features]
        ## Clustering
        if k == 0:
            self.km_model = clustering.kmeans_by_bestK(
                self.rfm_df, max_k=20, feature_columns=self.rfm_features)
        else:
            self.km_model = clustering.kmeans(
                self.rfm_df, k=k, feature_columns=self.rfm_features)
        self.clustered_df = prc_df[self.rfm_clustered_features]
        self.clustered_df[self.clustered_column_name] = self.km_model.labels_
        self.fit_finished = True

class CustomerBehavior(ClusteringModel):
    def __init__(self):
        self.cb_features = {'season': [
                'datediff_stdrd', 'spring', 'summer', 'autumn', 'winter'
            ],
            'month':[
                'datediff_stdrd', 1,2,3,4,5,6,7,8,9,10,11,12
            ]}
        self.clustered_column_name = 'customer_behavior_cid'
        self.fit_finished = False
        self.describe_features = ['monetary', 'frequency', 'leader_score','datediff']

    def fit(self, prc_df:pd.DataFrame, type:str='month', k:int=0):
        """訓練客戶行為分析模型。
        Main method : Clustering.

        Args:
            prc_df (pd.DataFrame): Ordering dataframe after preprocessing.
            type (str, optional): Time seperate to season or month. Defaults to 'month'.
            k (int, optional): K value for clustering. Defaults to 0, using best K for clustering.
        """        
        self.describe_features = self.describe_features+self.cb_features[type]
        # Modeling
        ## Feature matrix
        self.cb_df = prc_df[self.cb_features[type]]
        ## Clustering
        if k == 0:
            # get Best K by Silhouette.
            self.km_model = clustering.kmeans_by_bestK(self.cb_df, feature_columns=self.cb_features[type], max_k=20)
        else:
            self.km_model = clustering.kmeans(
                self.cb_df, feature_columns=self.cb_features[type], k=k)
        self.clustered_df = prc_df.copy()
        self.clustered_df[self.clustered_column_name] = self.km_model.labels_
        self.fit_finished = True

