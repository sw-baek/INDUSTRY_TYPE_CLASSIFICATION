import time
import OpenDartReader as dt
from tqdm import tqdm
import pandas as pd 

api_key = '#' # DART API 들어가서 직접 발급
dart = dt(api_key)

# 업종분류할 종목 불러오기
fics_tg =pd.read_excel('./FICS_TG.xlsx')
fics_tg = fics_tg.apply(lambda x: x.str.strip('A'), axis = 1) # FNGUIDE GICODE 에는 A가 붙어서 A 삭제
report_cd = []
for i in tqdm(fics_tg['GICODE']):
    try:
        report_cd.append(dart.list(str(i), start='2021-01-09', end='2021-04-09', kind='A')['rcept_no'][0])
        time.sleep(1) # 안하면 금감원에서 IP 차단함
    except KeyError :
        report_cd.append(0) # 사업보고서 없으면 리스트에 0 추가
        
fics_tg['report_cd'] = report_cd

url_li = []
for i in tqdm(fics_tg['report_cd']):
    try:
        d_url = dart.sub_docs(str(i), match = '사업의 내용')
        d_url_2 = d_url.loc[d_url['title'].str.contains('사업의 내용')] 
        url = d_url_2['url'][d_url_2.index[0]] # 이거 안 하면 url...으로 보여줌
        url_li.append(url)
        time.sleep(1)
    except IndexError :
        url_li.append(0)
    except UnboundLocalError:
        url_li.append(0)   # 오류 나면 그냥 0으로 list 에 append
        
fics_tg['url'] = url_li

df_li = []

for i in tqdm(fics_tg['url']):
    try:
        url = str(i)
        
        # HTML에 포함 되어 있는 모든 테이블 불러오기
        tables = pd.read_html(url, header = 0, encoding = 'utf-8')
        
        # table에 포함되어야 할 키워드들 
        keywords = ['사업부문', '매출액', '매출', '비중', '비율', '영업수익', '구분', '부문', '제[1-9][0-9]기', '제', '기', '합계']
        
        # keywords를 가장 많이 포함하고 있는 테이블 하나를 갖고 오기 
        tg_df_li = []
        cnt_li = []
        
        for table in tables :
            cnt = 0
            for keyword in keywords :
                if keyword in str(table):
                    cnt += 1
            cnt_li.append(cnt)
        
        # 테이블 당 키워드를 몇 개 포함하고 있는지 빈 리스트에 저장 후 가장 큰 값을 갖고 있는 table append
        
        where_max_list = [i for i, value in enumerate(cnt_li) if value == max(cnt_li)] # 최대값 리스트 확인 
        if len(where_max_list) >= 2 :
            df_li.append(tables[where_max_list[0]])
            df_li.append(tables[where_max_list[1]])
        else :
            df_li.append(tables[cnt_li.index(max(cnt_li))])
            df_li.append(tables[cnt_li.index(sorted(cnt_li, reverse = True)[1])])
        time.sleep(1)
    except :
        df_li.append(0)
        df_li.append(0)