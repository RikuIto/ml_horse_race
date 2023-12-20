import time
import re
import pandas as pd 
import urllib.request
from bs4 import BeautifulSoup
import warnings
from tqdm.notebook import tqdm
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score
import numpy as np
warnings.filterwarnings("ignore")

def parse_horse_file(horse_results):
    df = horse_results.copy()
    # 着順に数字以外の文字列が含まれているものを取り除く
    df['着順'] = pd.to_numeric(df['着順'], errors='coerce')
    df.dropna(subset = ['着順'], inplace=True)
    df['着順']= df['着順'].astype(int)

    df['date'] = pd.to_datetime(df['日付'])
    df.drop(['日付'], axis=1, inplace=True) 

    df['賞金'].fillna(0, inplace=True)
    return df

def get_average_horse_results(horse_results,horse_id_list,date,n_samples = 'all'):
    target_df = horse_results.query('index in @horse_id_list')
    if n_samples == 'all':
        filterd_df = target_df[target_df['date']<date]
    elif n_samples > 0:
        filterd_df = target_df[target_df['date']<date].sort_values('date',ascending=False).head(n_samples)
    else:
        raise ValueError('n_samples must be positive integer or "all"')
    
    avg_df = filterd_df.groupby(level = 0)['着順', '賞金'].mean()
    avg_df.rename(columns={'着順':f'着順_avg_{n_samples}_R', '賞金':f'賞金_avg_{n_samples}_R'}, inplace=True)

    return avg_df

def merge(race_results,horse_results,n_samples = 'all'):
    horse_results = parse_horse_file(horse_results[['日付', '着順', '賞金']])
    date_list = race_results['date'].unique()
    merged_list = []
    for date in tqdm(date_list):
        df = race_results[race_results['date'] == date]
        horse_id_list = df['horse_id']
        horse_results_avg = get_average_horse_results(horse_results,horse_id_list,date,n_samples)
        merged_df = df.merge(horse_results_avg,left_on = 'horse_id', right_index=True, how='left')
        merged_list.append(merged_df)
    merged_df = pd.concat(merged_list)
    return merged_df

def split_data(df,test_size= 0.3):
    sorted_id_list = df.sort_values(by=['date']).index.unique()
    train_id_list = sorted_id_list[:round(len(sorted_id_list)*(1-test_size))]
    test_id_list = sorted_id_list[round(len(sorted_id_list)*(1-test_size)):]
    train = df.loc[train_id_list]#.drop(['date'],axis=1)
    test = df.loc[test_id_list]#.drop(['date'],axis=1)
    return train, test

def gain(return_func,X,n_samples = 100,lower = 50,min_threshold = 0.5):
    gain = {}
    for i in tqdm(range(n_samples)):
        threshold = i/n_samples
        n_bets,return_rate = return_func(X,threshold)
        if n_bets > lower:
            gain[n_bets] = return_rate
    return pd.Series(gain)

class Return:
    def __init__(self,return_tables):
        self.return_tables = return_tables

    @property
    def fukusho(self):
        fukusho = self.return_tables[self.return_tables[0] == '複勝'][[1,2]]
        wins = fukusho[1].str.split('br', expand = True).drop([3,4], axis = 1)
        wins.columns = ['win_0', 'win_1', 'win_2']
        returns = fukusho[2].str.split('br', expand = True).drop([3,4], axis = 1)
        returns.columns = ['return_0', 'return_1', 'return_2']

        df = pd.concat([wins, returns], axis = 1)
        for column in df.columns:
            df[column] = df[column].str.replace(',', '')
        return df.fillna(0).astype(int)

    @property
    def tansho(self):
        tansho = self.return_tables[self.return_tables[0] == '単勝'][[1,2]]
        tansho.columns = ['win', 'return']

        for column in tansho.columns:
            tansho[column] = pd.to_numeric(tansho[column], errors='coerce')
        return tansho

class ModelEvaluator:
    def __init__(self,model,return_tables,std = True):
        self.model = model
        self.fukusho = Return(return_tables).fukusho
        self.tansho = Return(return_tables).tansho
        self.std = std

    def predict_proba(self,X):
        #0と1に分類される確率を求めて、そのうち、1になる確率を返す
        proba = pd.Series(self.model.predict_proba(X)[:,1], index=X.index)
        if self.std:
            standard_scaler = lambda x: (x - x.mean())/x.std()
            proba = proba.groupby(level = 0).transform(standard_scaler)
            proba = (proba - proba.min())/(proba.max() - proba.min())
        return proba

    def predict(self,X,threshold = 0.5):
        y_pred = self.predict_proba(X)
        return [0 if y < threshold else 1 for y in y_pred]

    def score(self,X,y):
        return roc_auc_score(y,self.predict_proba(X))

    def feature_importance(self,X,n_features = 20):
        importances = pd.DataFrame({'feature':X.columns,'importance':self.model.feature_importances_})
        return importances.sort_values(by='importance',ascending=False).head(n_features)

#1になる確率がthreshold以上のものを抽出する
    def predict_table(self,X,threshold = 0.5, bet_only = True):
        pred_table = X.copy()
        pred_table['pred'] = self.predict(X,threshold)
        if bet_only:
            return pred_table[pred_table['pred'] == 1]['馬番']
        else:
            return pred_table

    def fukusho_return(self,X,threshold = 0.5):
        pred_table = self.predict_table(X,threshold)
        n_bets = len(pred_table)
        money = -100 * n_bets
        df = self.fukusho.copy()
        df = df.merge(pred_table,left_index = True, right_index=True, how='right')
        for i in range(3):
            money += df[df[f'win_{i}'] == df['馬番']][f'return_{i}'].sum()
        return_rate = (n_bets * 100 + money)/(n_bets * 100)
        return n_bets,return_rate

    def tansho_return(self,X,threshold = 0.5):
        pred_table = self.predict_table(X,threshold)
        n_bets = len(pred_table)
        money = -100 * n_bets
        df = self.tansho.copy()
        df = df.merge(pred_table,left_index = True, right_index=True, how='right')
        money += df[df['win'] == df['馬番']]['return'].sum()
        return_rate = (n_bets * 100 + money)/(n_bets * 100)
        return n_bets,return_rate

    def tansho_return_proper(self,X,threshold = 0.5):
        pred_table = self.predict_table(X,threshold)
        n_bets = len(pred_table)

        df = self.tansho.copy()
        df = df.merge(pred_table,left_index = True, right_index=True, how='right')
        return_rate = len(df.query('win == 馬番'))/(100/df['return'].sum())

        return n_bets,return_rate

class DataProcessor:
    def __init__(self):
        self.data = pd.DataFrame() # raw data
        self.data_p = pd.DataFrame() # after preprocessing
        self.data_h = pd.DataFrame() # after merging horse results
        self.data_pe = pd.DataFrame() # after merging peds
        self.data_c = pd.DataFrame() # process categorycal

    def merge_horse_results(self,horse_results,n_samples_list = [5,9,'all']):
        self.data_h = self.data_p.copy()
        for n_samples in n_samples_list:
            self.data_h = merge(self.data_h,horse_results,n_samples)

    def merge_peds(self,peds):
        self.data_pe = self.data_h.merge(peds,left_on = 'horse_id', right_index=True, how='left')
        self.no_peds = self.data_pe[self.data_pe['peds_0'].isnull()]['horse_id'].unique()
        if len(self.no_peds) > 0:
            print('no peds, please scrape peds')
            print(self.no_peds)
        return self.no_peds
    
    def process_categorycal(self,le_horse,le_jockey,results_m):
        df = self.data_pe.copy()
        #Label encoding for horse_id, jockey_id
        mask_horse = df['horse_id'].isin(le_horse.classes_)
        new_horse_id = df['horse_id'].mask(mask_horse).dropna().unique()
        le_horse.classes_ = np.concatenate([le_horse.classes_,new_horse_id])
        df['horse_id'] = le_horse.transform(df['horse_id'])

        mask_jockey = df['jockey_id'].isin(le_jockey.classes_)
        new_jockey_id = df['jockey_id'].mask(mask_jockey).dropna().unique()
        le_jockey.classes_ = np.concatenate([le_jockey.classes_,new_jockey_id])
        df['jockey_id'] = le_jockey.transform(df['jockey_id'])

        #horse_id, jockey_idをpandasのcategory型に変換
        df['horse_id'] = df['horse_id'].astype('category')
        df['jockey_id'] = df['jockey_id'].astype('category')

        #列を一定にするため
        #pandasのcategory型にしてからダミー変数化
        weathers = results_m['weather'].unique()
        race_types = results_m['race_type'].unique()
        ground_states = results_m['ground_state'].unique()
        sexes = results_m['性'].unique()

        df['weather'] = pd.Categorical(df['weather'],weathers)
        df['race_type'] = pd.Categorical(df['race_type'],race_types)
        df['ground_state'] = pd.Categorical(df['ground_state'],ground_states)
        df['性'] = pd.Categorical(df['性'],sexes)

        df = pd.get_dummies(df,columns= ['weather', 'race_type', 'ground_state', '性'])
        self.data_c = df

class ShutubaTable(DataProcessor):
    def __init__(self):
        super(ShutubaTable,self).__init__()

    def scrape_shutuba_table(self,race_id_list,date):
        for race_id in tqdm(race_id_list):
            url = 'https://race.netkeiba.com/race/shutuba.html?race_id=' + race_id
            df = pd.read_html(url)[0]
            df = df.T.reset_index(level=0,drop=True).T

            html = urllib.request.urlopen(url).read()
            soup = BeautifulSoup(html, 'html.parser')

            texts = soup.find('div', attrs={'class': 'RaceData01'}).text
            texts = re.findall(r'\w+', texts)
            for text in texts:
                if 'm' in text:
                    df['course_len'] = [int(re.findall(r'\d+', text)[0])] * len(df)
                if text in ['曇','晴','雨', '小雨', '小雪', '雪']:
                    df['weather'] = [text] * len(df)
                if text in ['良', '稍重', '重', '不良']:
                    df['ground_state'] = [text] * len(df)
                if '芝' in text:
                    df['race_type'] = ['芝'] * len(df)
                if '障' in text:
                    df['race_type'] = ['障害'] * len(df)
                if 'ダ' in text:
                    df['race_type'] = ['ダート'] * len(df)

            df['date'] = [date] * len(df)

            horse_id_list = []
            horse_td_list = soup.find_all('td', attrs={'class': 'HorseInfo'})
            for td in horse_td_list:
                horse_id = re.findall(r'\d+', td.find('a')['href'])[0]
                horse_id_list.append(horse_id)
            
            jockey_id_list = []
            jockey_td_list = soup.find_all('td', attrs={'class': 'Jockey'})
            for td in jockey_td_list:
                jockey_id = re.findall(r'\d+', td.find('a')['href'])[0]
                jockey_id_list.append(jockey_id)
            df['horse_id'] = horse_id_list
            df['jockey_id'] = jockey_id_list

            df.index = [race_id] * len(df)

            df = df[df['印']!= '除外']
            self.data = self.data.append(df)
            time.sleep(1)
    
    def preprocessing(self):
        df = self.data.copy()

        #convert to int
        df[['枠', '馬番', '斤量']] = df[['枠', '馬番', '斤量']].astype(int)

        #性齢を性と年齢に分割
        df['性']= df['性齢'].map(lambda x: str(x)[0])
        df['年齢']= df['性齢'].map(lambda x: str(x)[1:]).astype(int)
        
        # #馬体重を体重と体重変化に分割
        try:
            if df['馬体重(増減)'].isnull().all():
                df['体重'] = np.nan
                df['体重変化'] = np.nan
            else:   
                df['体重']= df['馬体重(増減)'].str.split('(').str[0].astype(int)
                df['体重変化']= df['馬体重(増減)'].str.split('(').str[1].str.split(')').str[0].astype(int)
        except:
            print('something error')
        
        df['date'] = pd.to_datetime(df['date'])
        
        df = df[['枠', '馬番', '斤量','course_len','weather','race_type',
        'ground_state', 'date', 'horse_id', 'jockey_id','性', '年齢', '体重', '体重変化']]
        
        self.data_p = df.rename(columns={'枠': '枠番'})

class Results(DataProcessor):
    def __init__(self,results):
        super(Results,self).__init__()
        self.data = results

    def preprocessing(self):
        df = self.data.copy()
        # 着順に数字以外の文字列が含まれているものを取り除く
        df = df[~(df['着順'].astype(str).str.contains('\D'))]
        df['着順']= df['着順'].astype(int)
        df['rank'] = df['着順'].map(lambda x: 1 if x < 4 else 0)

        #性齢を性と年齢に分割
        df['性']= df['性齢'].map(lambda x: str(x)[0])
        df['年齢']= df['性齢'].map(lambda x: str(x)[1:]).astype(int)
        
        # #馬体重を体重と体重変化に分割
        df['体重']= df['馬体重'].str.split('(').str[0].astype(int)
        df['体重変化']= df['馬体重'].str.split('(').str[1].str.split(')').str[0].astype(int)
        
        #データをfloat型に変換
        df['単勝']= df['単勝'].astype(float)
        
        #いらない列を削除
        df = df.drop(['タイム', '着差', '調教師', '性齢', '馬体重','馬名', '騎手', '単勝', '着順', '人気'], axis=1)
        
        df['date'] = pd.to_datetime(df['date'], format='%Y年%m月%d日')
        
        self.data_p = df

    def process_categorycal(self):
        self.le_horse = LabelEncoder().fit(self.data_pe['horse_id'])
        self.le_jockey = LabelEncoder().fit(self.data_pe['jockey_id'])
        super().process_categorycal(self.le_horse,self.le_jockey,self.data_pe)

class Peds:
    def __init__(self,peds):
        self.peds = peds
        self.peds_e = pd.DataFrame() # after label encoding and transforming into category

    def encode(self):
        df = self.peds.copy()
        for column in df.columns:
            df[column] = LabelEncoder().fit_transform(df[column].fillna('Na'))
        self.peds_e = df.astype('category')

