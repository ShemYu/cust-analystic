from multiprocessing import Process, Queue, Manager
import bin.feature as feature
import pandas as pd
import time

# number of multiprocessing
_NUM_OF_MP = 5

# Feature's preprocessing functions
## bin/feature.Numerical and bin/feature.Categorical

# dictionary for features mapping to preprocessing functions
## monetary : 單位時間消費總金額
## frequency : 單位時間消費頻率
## leader_score : 同團人數
## datediff : 使用者購買行為，出團前多少天下單
## season : 偏好季節 
## month : 出團月份

## feature.Numerical : 數值型特徵前處理方法
## feature.Categorical : 類別型特徵前處理方法

_FEATURE_PREPROC = {
    'monetary': feature.Numerical('benefit', result_column='monetary').sum_value,
    'frequency': feature.Numerical('',result_column='frequency').count_row,
    'leader_score': feature.Numerical('leader_score').sum_value,
    'datediff': feature.Numerical('datediff').sum_value,
    'season': feature.Categorical('season', 
        unique_label=['spring', 'summer', 'autumn', 'winter']
        ).one_hot_encoding,
    'month': feature.Categorical('month_of_group_date',
        unique_label=[i for i in range(1,13)]
        ).one_hot_encoding
}

def main(raw_data: pd.DataFrame(), have_paid=True, filter:list=['monetary', 'frequency', 'leader_score', 'datediff', 'month'], multi_p=True):
    """Customer value, Customer behavior's Preprocessing.

    Args:
        raw_data (pd.DataFrame): Raw data of order.
        have_paid (bool, optional): Raw data filter by have_paid or not. Defaults to True.
        filter (list, optional): Selected features in Defaults. Defaults to ['monetary', 'fequency', 'leader_score', 'datediff', 'season', 'month'].

    Returns:
        pd.DataFrame: DF finished preprocessing. 
    """    
    # initialize
    ## Multi
    return_list = Manager().list()
    ff = filter
    
    # denoise
    ## have_paid 最終付款
    if have_paid:
        raw_data = raw_data.loc[raw_data['have_paid'] == 1]
    ## 特殊訂單去除
    raw_data.loc[raw_data['uid'] == 23192, 'uid'] = 119519
    raw_data = raw_data.loc[raw_data['uid'] != 34117]
    ## leader_score優先整理
    raw_data['leader_score'] = feature.leader_score(raw_data['type_count'])
    
    # group by user
    rd_gb_uid = raw_data.groupby('uid')

    if multi_p:
        # multiprocessing, 暫時出狀況待修
        que = Queue()
        for uid, gb_df in rd_gb_uid:
            que.put((uid, gb_df, ff))

        plist = []
        for _ in range(_NUM_OF_MP):
            p = Process(target=worker, args=(que,return_list))
            plist.append(p)
            p.start()
        for p in plist:
            p.join()
    else:
        for uid, gb_df in rd_gb_uid:
            return_list.append(trans_by_uid(uid, gb_df, ff))
    
    # return_list to pd.DataFrame and drop null data
    preproc_data = pd.DataFrame(list(return_list))
    preproc_data = preproc_data.dropna()

    # datediff, leader_score
    for column_name in list(set(['datediff', 'leader_score']) & set(filter)):
        preproc_data[column_name] = preproc_data[column_name]/preproc_data['frequency']

    # Standardization
    preproc_data['monetary_stdrd'] = feature.monetary(preproc_data['monetary'])
    preproc_data['frequency_stdrd'] = feature.frequency(
        preproc_data['frequency'])
    preproc_data['datediff_stdrd'] = feature.frequency(
        preproc_data['datediff'])

    return preproc_data


def worker(que, return_list):
    while not que.empty():
        uid, gb_df, ff = que.get()
        return_list.append(trans_by_uid(uid, gb_df, ff))


def trans_by_uid(uid: str, gb_df: pd.DataFrame, ff:list):
    temp_row = {}
    temp_row['uid'] = uid
    for feature in ff:
        if feature in _FEATURE_PREPROC:
            temp_row = _FEATURE_PREPROC[feature](temp_row, gb_df)
        else:
            print('Feature "{}" doesn\'t have a preprocessing function!'.format(feature))
    return temp_row

