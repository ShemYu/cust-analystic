from sklearn.cluster import KMeans as km
from sklearn.metrics import silhouette_score
from multiprocessing import Queue, Process, Manager
import bin.feature as feature
import numpy as np
import pandas as pd

# 分群 ===================================


def kmeans(feature_matrix: pd.DataFrame(), k: int = 2, feature_columns: list = []):
    """包裝Kmeans

    Args:
        feature_matrix (pd.DataFrame): 經過前處理的特徵矩陣
        k (int, optional): 分K群. Defaults to 2.
        feature_columns (list, optional): 選擇分群用的特徵的columns. Defaults to [].

    Returns:
        sklearn.cluster.KMeans: sklearn Kmeans分群結果物件
    """    
    if not feature_columns:
        feature_columns = feature_matrix.columns
    # uid以外作為特徵
    feature_matrix = feature_matrix[feature_columns]
    # 轉np array做輸入用
    x = np.array(feature_matrix)
    # 跑Kmeans分群
    km_model = km(n_clusters=k, random_state=0).fit(x)
    return km_model


def best_k(feature_matrix:pd.DataFrame(), max_k:int=20):
    """利用輪廓係數取得區間內的BestK

    Args:
        feature_matrix (pd.DataFrame): 特徵矩陣
        max_k (int, optional): 測試的最大值. Defaults to 20.
    """    
    sils = get_sils(feature_matrix, max_k)
    return sils.index(max(sils))+2

def kmeans_by_bestK(feature_matrix:pd.DataFrame(), feature_columns:list=[], max_k:int=20):
    """用BestK進行Kmeans分群

    Args:
        feature_matrix (pd.DataFrame): 特徵矩陣
        feature_columns (list, optional): . Defaults to [].
    """    
    km_model = kmeans(feature_matrix, k=best_k(feature_matrix, max_k=max_k), feature_columns=feature_columns)
    return km_model

def get_sils(fm, max_k, columns:list=[], multi_p:bool=False):
    sils = []
    if not columns:
        columns = fm.columns
    if multi_p:
        que = Queue()
        return_dict = Manager().dict()
        features = fm[columns]
        x = np.array(features)
        # put in queue
        for K in range(2, max_k):
            que.put((fm,K,return_dict))
        # put in process
        plist = []
        for _ in range(5):
            p = Process(target=mp_get_sils_worker, args=(que, return_dict))
            plist.append(p)
            p.start()
        # join
        for p in plist:
            p.join()
        # result sils
        sils = [return_dict[K] for K in range(2, max_k)]
    else:
        features = fm[columns]
        x = np.array(features)
        for K in range(2, max_k):
            temp_kmeans = kmeans(x, k=K)
            sils.append(silhouette_score(x, temp_kmeans.labels_))
    return sils


def mp_get_sils_worker(que,return_dict):
    while not que.empty():
        x, K, return_dict = que.get()
        temp_kmeans = kmeans(x, k=K)
        return_dict[K] = silhouette_score(x, temp_kmeans.labels_)

def describe_km(fm_clustered, feature_columns=[], cluster_id_column_name='cid'):
    """群中心以dataframe方式呈現

    Args:
        fm_clustered (pd.DataFrame): 分群結果，已 column_name='cid' 記錄分群編號。
        feature_columns (list, optional): 選填特徵columns. Defaults to [].

    Returns:
        pd.DataFrame: Result, sort by column 'monetary'.
    """    
    season_columns = ['spring', 'summer', 'autumn', 'winter']
    if not feature_columns:
        feature_columns = fm_clustered.columns
    cid_list = fm_clustered[cluster_id_column_name].unique()
    describe_df = []
    for cid in cid_list:
        temp_row = {}
        subdf = fm_clustered.loc[fm_clustered[cluster_id_column_name] == cid]
        temp_row['Cluster ID'] = cid
        for cname in feature_columns:
            temp_row[cname] = subdf[cname].mean()
        temp_row['Num of Cluster'] = len(subdf.index)
        describe_df.append(temp_row)
    seasons = list(set(season_columns) & set(feature_columns))
    if seasons:
        # 先抓母體分佈
        seasons_distribution = feature.season(fm_clustered)
        # 計算與母體分佈差距
        for index, row in enumerate(describe_df):
            for s in seasons:
                describe_df[index][s] = row[s]/seasons_distribution[s]

    return pd.DataFrame(describe_df).sort_values(['monetary'],ascending=False)
