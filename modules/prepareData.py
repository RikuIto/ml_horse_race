import time
from unittest import skip
from urllib.request import urlopen
from tqdm.notebook import tqdm
import os
import pandas as pd
from bs4 import BeautifulSoup
import re
import glob

def get_race_id_list(start_year: int = 2024, end_year: int = 2025):
    race_id_list = []
    for year in range(start_year,end_year,1):
        for place in range(1,11,1):
        # 01 札幌, 02 函館, 03 福島, 04 新潟, 05 東京, 06 中山, 07 中京, 08 京都, 09 阪神, 10 小倉
            for kai in range(1,7,1):
                for day in range(1,9,1):
                    for r in range(1,13,1):
                        race_id = str(year) + str(place).zfill(2) + str(kai).zfill(2) + str(day).zfill(2) + str(r).zfill(2)
                        race_id_list.append(race_id)
    return race_id_list

def getHTMLRace(race_id_list: list,skip: bool = True):
    """
    netkeiba.comのraceページのhtmlをスクレイピングしてdata/html/raceに保存する関数
    """
    # file_name_list = []
    for race_id in tqdm(race_id_list):
        url = 'https://db.netkeiba.com/race/' + race_id
        file_name = 'data/html/race/'+ race_id + '.bin'
        try:
            #check existance of page
            df = pd.read_html(url)
        except:
            if os.path.isfile(file_name):
                os.remove(file_name)
                print(f'{file_name} remove done')
            continue
        if skip and os.path.isfile(file_name):
            print(f'race_id {race_id} skipped.')
            continue
        with open(file_name, 'wb')as f:
            html = urlopen(url).read()
            f.write(html)
            print(f'{file_name} write done')
        time.sleep(0.1)

def getRawDataRaceResults(html_path_list: list):
    """
    raceページのhtmlを受け取って、レース結果テーブルに変換する関数
    """
    race_results = {}
    for html_path in tqdm(html_path_list):
        try:
            with open(html_path, 'rb') as f:
                html = f.read()#保存してあるbinファイルを読みこむ
                df = pd.read_html(html)[0]#レース結果のテーブルを取得
                soup = BeautifulSoup(html, 'html.parser')#htmlをBeautifulSoupで解析

                #馬IDを取得
                horse_id_list = []
                horse_a_list = soup.find('table', attrs = {'summary':'レース結果'}).find_all('a', attrs = {'href': re.compile('^/horse/')})
                for horse_a in horse_a_list:
                    horse_id = re.findall(r'\d+', horse_a['href'])
                    horse_id_list.append(horse_id[0])
                #騎手IDを取得
                jockey_id_list = []
                jockey_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all( "a", attrs={"href": re.compile("^/jockey")} )
                for jockey_a in jockey_a_list:
                    jockey_id = re.findall(r'\d+', jockey_a['href'])
                    jockey_id_list.append(jockey_id[0])
                
                df["horse_id"] = horse_id_list
                df["jockey_id"] = jockey_id_list
                
                #インデックスをrace_idにする
                race_id = re.findall('(?<=race/)\d+', html_path)[0]
                df.index = [race_id] * len(df)

                race_results[race_id] = df
        except:
            os.remove(html_path)
    #pd.DataFrame型にして一つのデータにまとめる
    race_results_df = pd.concat([race_results[key] for key in race_results])
    return race_results_df

def getRawDataRaceInfos(html_path_list: list):
    """
    raceページのhtmlを受け取って、レース情報(天気等)テーブルに変換する関数
    """
    race_infos = {}
    for html_path in tqdm(html_path_list):
        try:
            with open(html_path, 'rb') as f:
                html = f.read()#保存してあるbinファイルを読みこむ
                soup = BeautifulSoup(html, 'html.parser')#htmlをBeautifulSoupで解析
            #天候、レースの種類、コースの長さ、馬場の状態、日付をスクレイピング
            texts = (
                soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
                + soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
            )
            info = re.findall(r'\w+', texts)
            df = pd.DataFrame()
            for text in info:
                if text in ["芝", "ダート"]:
                    df["race_type"] = [text] 
                if "障" in text:
                    df["race_type"] = ["障害"] 
                if "m" in text:
                    df["course_len"] = [int(re.findall(r"\d+", text)[-1])] 
                if text in ["良", "稍重", "重", "不良"]:
                    df["ground_state"] = [text] 
                if text in ["曇", "晴", "雨", "小雨", "小雪", "雪"]:
                    df["weather"] = [text]
                if "年" in text:
                    df["date"] = [text] 
                
            #インデックスをrace_idにする
            race_id = re.findall('(?<=race/)\d+', html_path)[0]
            df.index = [race_id] 
            race_infos[race_id] = df
        except:
            print(f'{html_path} is not exsist')
            # os.remove(html_path)
    #pd.DataFrame型にして一つのデータにまとめる
    race_infos_df = pd.concat([race_infos[key] for key in race_infos])
    return race_infos_df

def getRawDataReturnTables(html_path_list:list):
    """
    raceページのhtmlを受け取って、払い戻しテーブルに変換する関数
    """
    return_tables = {}
    for html_path in tqdm(html_path_list):
        try:
            with open(html_path, 'rb') as f:
                html = f.read()#保存してあるbinファイルを読みこむ

                html = html.replace(b'<br />', b'br')
                dfs = pd.read_html(html)

                #dfsの1番目に単勝〜馬連、2番目にワイド〜三連単がある
                df = pd.concat([dfs[1], dfs[2]])

                race_id = re.findall('\d+', html_path)[0]
                df.index = [race_id] * len(df)
                return_tables[race_id] = df
        except:
            print(f'{html_path} is not exsist')
            # os.remove(html_path)
    #pd.DataFrame型にして一つのデータにまとめる
    return_tables_df = pd.concat([return_tables[key] for key in return_tables])
    return return_tables_df

def get_horse_id_list():
    race_results_df = pd.read_pickle('data/raw/race_results/race_results.pickle')
    horse_id_list = race_results_df['horse_id'].unique()
    return horse_id_list

def getHTMLHorse(horse_id_list: list, update: bool = True):
    """
    netkeiba.comのhorseページのhtmlをスクレイピングしてdata/html/horseに保存する関数
    """
    for horse_id in tqdm(horse_id_list):
        url = 'https://db.netkeiba.com/horse/' + horse_id
        html = urlopen(url).read()
        file_name = 'data/html/horse/'+ horse_id + '.bin'
        with open(file_name, 'wb')as f:
            f.write(html)
        if update and os.path.isfile(file_name):
            print(f'horse_id {horse_id} updated.')
        else:
            print(f'horse_id {horse_id} saved.')
        time.sleep(0.1)

def getRawDataHorse(html_path_list:list):
    """
    horseページのhtmlを受け取って、馬の過去成績のdataframeテーブルに変換する関数
    """
    horse_results = {}
    for html_path in tqdm(html_path_list):
        try:
            with open(html_path, 'rb') as f:
                html = f.read()#保存してあるbinファイルを読みこむ
                
                df = pd.read_html(html)[3]
                #受賞歴がある馬の場合、3番目に受賞歴テーブルが来るため、4番目のデータを取得する
                if df.columns[0]=='受賞歴':
                    df = pd.read_html(html)[4]

                horse_id = re.findall('(?<=horse/)\d+', html_path)[0]            
                df.index = [horse_id] * len(df)
                horse_results[horse_id] = df
        except:
            print(f'{html_path} is not exsist')

    #pd.DataFrame型にして一つのデータにまとめる
    horse_results_df = pd.concat([horse_results[key] for key in horse_results])
    return horse_results_df

def getHTMLPed(horse_id_list: list,skip: bool = True):
    """
    netkeiba.comのpedページのhtmlをスクレイピングしてdata/html/pedに保存する関数
    """
    for horse_id in tqdm(horse_id_list):
        url = 'https://db.netkeiba.com/horse/ped/' + horse_id
        html = urlopen(url).read()
        file_name = 'data/html/ped/'+ horse_id + '.bin'
        if skip and os.path.isfile(file_name):
            print(f'horse_id {horse_id} skipped.')
            continue
        with open(file_name, 'wb')as f:
            f.write(html)
        print(f'horse_id {horse_id} saved.')
        time.sleep(0.1)

def getRawDataPeds(html_path_list:list):
    """
    pedページのhtmlを受け取って、馬の血統データのdataframeテーブルに変換する関数
    """
    peds = {}
    for html_path in tqdm(html_path_list):
        try:
            with open(html_path, 'rb') as f:
                html = f.read()
                #保存してあるbinファイルを読みこむ
                df = pd.read_html(html)[0]
                #重複を削除して1列のSeries型データに直す
                generations = {}
                horse_id = re.findall('(?<=ped/)\d+', html_path)[0]            
                
                for i in reversed(range(5)):
                    if i == 4:
                        generations[i] = df[i]
                    elif i == 3:
                        # chose one jump rows
                        generations[i] = df[i].iloc[::2]
                    elif i == 2:
                        # chose two jump rows
                        generations[i] = df[i].iloc[::4]
                    elif i == 1:
                        # chose three jump rows
                        generations[i] = df[i].iloc[::8]
                    elif i == 0:
                        # chose four jump rows
                        generations[i] = df[i].iloc[::16]
                    else:
                        print('error')
                ped = pd.concat([generations[i] for i in range(5)]).rename(horse_id)
                peds[horse_id] = ped.reset_index(drop=True)
        except:
            print(f'{html_path} is not exsist')
    
    #pd.DataFrame型にして一つのデータにまとめる
    peds_df = pd.concat([peds[key] for key in peds], axis=1).T.add_prefix('peds_')
    return peds_df

def get_html_path_list(dir:str):
    return glob.glob(f'data/html/{dir}/*.bin')

def update_files(last_update_date: str, update_date: str, target_file: str,update_data: pd.DataFrame):
    # old_data = pd.read_pickle(f'data/raw/{target_file}/{target_file}_{last_update_date}.pickle')
    # wip_重複を削除するコードを追加する必要がある
    old_data = pd.read_pickle(f'data/raw/{target_file}/{target_file}.pickle')
    latest_data = pd.concat([old_data, update_data])
    latest_data.to_pickle(f'data/raw/{target_file}/{target_file}_{update_date}.pickle')

def get_update_files_path_list(target_file: str,update_target_file_id_list: list):
    all_target_file_html_path_list = get_html_path_list(target_file)
    update_file_html_path_list = []
    for path in all_target_file_html_path_list:
        race_id = path.split('/')[-1].split('.')[0]
        if race_id in update_target_file_id_list:
            update_file_html_path_list.append(path)
        else:
            pass
    return update_file_html_path_list

def update_all_data(last_update_date: str, update_date: str):
    '''
    レース、馬、血統の全データを更新する関数
    '''
    print('start getting all race HTML')
    # all_race_id_list = get_race_id_list()
    # getHTMLRace(all_race_id_list)
    print('get all race HTML done!')
    
    print('start getting all race info')
    # all_race_html_path_list = get_html_path_list('race')
    # race_infos = getRawDataRaceInfos(all_race_html_path_list)
    # race_infos.to_pickle(f'data/raw/race_infos/race_infos_{update_date}.pickle')
    print('get all race info done!')

    print('start update race results')
    race_infos = pd.read_pickle('data/raw/race_infos/race_infos.pickle')
    race_infos['date'] = pd.to_datetime(race_infos['date'].str.replace('年', '-').str.replace('月', '-').str.replace('日', ''))
    race_infos = race_infos[race_infos['date'] >= last_update_date]
    update_race_id_list = race_infos.index.unique().tolist()
    update_race_html_path_list = get_update_files_path_list('race',update_race_id_list)
    update_race_results = getRawDataRaceResults(update_race_html_path_list)
    update_files(last_update_date, update_date,'race_results',update_race_results)
    print('update race results done!')

    print('start update return tables')
    update_return_tables = getRawDataReturnTables(update_race_html_path_list)
    update_files(last_update_date, update_date,'return_tables',update_return_tables)
    print('update return tables done!')

    print('start update horse')
    update_horse_id_list = update_race_results['horse_id'].unique().tolist()
    getHTMLHorse(update_horse_id_list)
    update_horse_html_path_list = get_update_files_path_list('horses',update_horse_id_list)
    update_horse = getRawDataHorse(update_horse_html_path_list)
    update_files(last_update_date, update_date,'horses',update_horse)
    print('update horse done!')
    
    print('start update ped')
    getHTMLPed(update_horse_id_list)
    update_ped_html_path_list = get_update_files_path_list('peds',update_horse_id_list)
    update_ped = getRawDataPeds(update_ped_html_path_list)
    update_files(last_update_date, update_date,'peds',update_ped)
    print('update ped done!')

def main():
    '''
    メイン関数
    '''
    race_id_list = get_race_id_list()
    getHTMLRace(race_id_list)
    print('get race HTML done!')
    
    race_html_path_list = get_html_path_list('race')
    print('get race_html_path_list')
    race_results = getRawDataRaceResults(race_html_path_list)
    race_results.to_pickle('data/raw/race_results/race_results.pickle')
    print('race results done!')
    race_infos = getRawDataRaceInfos(race_html_path_list)
    race_infos.to_pickle('data/raw/race_infos/race_infos.pickle')
    print('race info done!')
    return_tables = getRawDataReturnTables(race_html_path_list)
    return_tables.to_pickle('data/raw/return_tables/return_tables.pickle')
    print('return tabeles done!')

    horse_id_list = get_horse_id_list()
    print('get horse_id_list')
    getHTMLHorse(horse_id_list)
    print('get horse HTLM done!')
    horse_html_path_list = get_html_path_list('horse')
    print('get horse_html_path_list')
    horse = getRawDataHorse(horse_html_path_list)
    horse.to_pickle('data/raw/horse/horse.pickle')
    print('horse done!')

    getHTMLPed(horse_id_list)
    print('get ped HTML done!')
    peds_html_path_list = get_html_path_list('ped')
    print('get peds_html_path_list')
    peds = getRawDataPeds(peds_html_path_list)
    peds.to_pickle('data/raw/peds/peds.pickle')
    print('peds done!')

if __name__ == '__main__':
    main()