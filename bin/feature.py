import math
import pandas as pd

# Based feature type ================================================
class Categorical(object):
    def __init__(self, column:str, unique_label:list=[]):
        self.col = column
        self.ul = unique_label

    def one_hot_encoding(self, result: dict, raw_data: pd.DataFrame):
        column_data = raw_data[self.col]
        vc = column_data.value_counts()
        for label in self.ul:
            if label in vc:
                result[label] = vc[label]
            else:
                result[label] = 0
        return result

class Numerical(object):
    def __init__(self, raw_column:str, result_column:str=''):
        self.col = raw_column
        self.res_col = result_column

    def sum_value(self, result:dict, raw_data:pd.DataFrame):
        if self.res_col:
            result[self.res_col] = raw_data[self.col].sum()
        else:
            result[self.col] = raw_data[self.col].sum()
        return result

    def count_row(self, result:dict, raw_data:pd.DataFrame):
        result[self.res_col] = len(raw_data.index)
        return result

# Customized features ===========================================
def month_feature(month: int, round_n=''):
    """月份轉圓形二維度座標，表示的月份間正確距離。
    一維度特徵轉二維

    Args:
        month (int): 月份
    """    
    month_deg = 30*(month%12)
    month_rad = math.radians(month_deg)
    # 12月座標
    x = 1
    y = 0
    # 旋轉
    ## x' = coxø*x - sinø*y
    ## y' = sinø*x + cosø*y
    xr = math.cos(month_rad)*x - math.sin(month_rad)*y
    yr = math.sin(month_rad)*x + math.cos(month_rad)*y
    if round_n:
        return round(xr, round_n), round(yr, round_n)
    return xr, yr

def leader_score(type_count_column: pd.Series()) -> list:
    """Leader scores: 希望能以該用戶平均出團人數，代表該用戶的其中一種意見領袖能力(攜伴能力)

    Args:
        type_count_column (pd.Series): type_count column in dataframe.

    Returns:
        list: 處理後的leader_score list
    """        
    tc_list = [map(int,tc.split(',')) for tc in type_count_column]
    leader_scores = []
    # 分數計算
    for type_count in tc_list:
        leader_score = 0
        for idx, sum_of_type in enumerate(type_count, 1):
            leader_score+=sum_of_type/idx
        leader_scores.append(leader_score)
    # leader_scores = pd.Series(leader_scores)
    # zscore 標準化
    # leader_scores = zscore_standardization(leader_scores)
    return leader_scores

def frequency(frequency_column:pd.Series())->pd.Series():
    """Frequency特徵正規化：
        極值正規化
        均值正規化
        Z-score 標準化
        離散化
        一般化

    Args:
        frequency_column (pd.Series): 從db query出dataframe後，依據group by us_profile_id後統計出的frequency list

    Returns:
        pd.Series: 經特徵處理後的frequency list
    """
    return zscore_standardization(frequency_column)

def monetary(monetary_column:pd.Series())->pd.Series():
    """Frequency特徵正規化：
        極值正規化
        均值正規化
        Z-score 標準化
        離散化
        一般化

    Args:
        monetary_column (pd.Series): data 當中的 monetary series

    Returns:
        pd.Series: 處理後的monetary series
    """    
    return zscore_standardization(monetary_column)

def season(preprc_data:pd.Series())->dict:
    """季節類別轉偏好比例

    Args:
        season_sub_column (pd.Series): sub_column group by us_profile_id

    Returns:
        dict: 春夏秋冬偏好比例dictionary
    """    
    seasons = ['spring', 'summer', 'autumn', 'winter']
    season_distribution = {}
    for season in seasons:
        season_distribution[season] = preprc_data[season].sum()
    sum_of_order = sum([season_distribution[s] for s in seasons])
    season_distribution = {s:season_distribution[s]/sum_of_order for s in season_distribution}
    return season_distribution

def month_to_seasons(month_column:pd.Series()) -> list:
    """月份轉季節

    Args:
        month_column (pd.Series): column of month feature.

    Returns:
        list: column of season.
    """    
    month_season = {3:'spring', 4:'spring', 5:'spring', 
                6:'summer', 7:'summer', 8:'summer',
                9:'autumn', 10:'autumn', 11:'autumn',
                12:'winter', 1:'winter', 2:'winter'}
    season_column = [month_season[int(m)] for m in month_column]
    return season_column

def get_constellation(month: int, date: int) -> str:
    """月/日轉星座特徵，做客戶行為特徵

    Args:
        month (int): 月份
        date (int): 日期

    Returns:
        str: 星座名稱
    """
    dates = (21, 20, 21, 21, 22, 22, 23, 24, 24, 24, 23, 22)
    constellations = ("摩羯座", "水瓶座", "雙魚座", "牡羊座", "金牛座", "雙子座",
                      "巨蟹座", "獅子座", "處女座", "天秤座", "天蝎座", "射手座", "魔羯座")
    if date < dates[month-1]:
        return constellations[month-1]
    else:
        return constellations[month]


# Standardization/Geralization ====================================
def zscore_standardization(series:pd.Series()):
    """針對pd.Series進行Zscore標準化

    Args:
        series (pd.Series): zscore標準化輸入Column series

    Returns:
        [list]: Result
    """    
    mean = series.mean()
    std = series.std()
    return [(f-mean)/std for f in series]

# Is tw tour ====================================
