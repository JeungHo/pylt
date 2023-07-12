expr_str = 'as1 =="충청남도" '   
sgg = df_total.query(expr_str).groupby(['sggcd','as1','as2']).size().to_frame('건').reset_index()
print(sgg)

from datetime import datetime
import time
import pyautogui as p
import os

dt = datetime.today().strftime('%Y%m%d')

for i in range(0,len(sgg)):
    # if len(sgg) < 9:
    #     break
    print(datetime.today(),sgg.iloc[i]['sggcd'],sgg.iloc[i]['as2'],sgg.iloc[i]['건'],len(sgg),'-',i+1)
    sigunguCode = sgg.iloc[i]['sggcd']
    sigunguName = sgg.iloc[i]['as2']

    time.sleep(5)
    p.alert('sleep 5 \n '+ sigunguCode + ' ' + sigunguName +' '+ str(sgg.iloc[i]['건']) )

    if len(sigunguCode) == 5:
        url = 'http://apis.data.go.kr/1613000/AptListService2/getSigunguAptList'
        params = {'serviceKey': akey,
                  'sigunguCode': sigunguCode,
                  'pageNo': '1',
                  'numOfRows': '1'
                  }
    elif len(sigunguCode) == 2:
        url = 'http://apis.data.go.kr/1613000/AptListService2/getSidoAptList'  # 시도
        params = {'serviceKey': akey,
                  'sidoCode': sigunguCode,
                  'pageNo': '1',
                  'numOfRows': '1'
                  }

    r = requests.get(url, params=params)
    xml_data = json.loads(json.dumps(xmltodict.parse(r.text)))
    row = xml_data['response']['body']['totalCount']
    pages = int(int(row) / 500) + 1

    print(datetime.today(), sigunguCode, sigunguName, '단지수', row , pages + 1)

    for x in range(1, pages + 1):
        if len(sigunguCode) == 5:
            url = 'http://apis.data.go.kr/1613000/AptListService2/getSigunguAptList'  # 시군구
            params = {'serviceKey': akey,
                      'sigunguCode': sigunguCode,
                      'pageNo': str(x),
                      'numOfRows': '500'
                      }
        elif len(sigunguCode) == 2:
            url = 'http://apis.data.go.kr/1613000/AptListService2/getSidoAptList'  # 시도
            params = {'serviceKey': akey,
                      'sidoCode': sigunguCode,
                      'pageNo': str(x),
                      'numOfRows': '500'
                      }
        r = requests.get(url, params=params, verify=False)
        xml_data = xmltodict.parse(r.text)
        json_data = json.loads(json.dumps(xml_data))
        ord_data = json_data['response']['body']['items']['item']
        if x == 1:
            df_apt = pd.DataFrame(ord_data)  # ,index=[0]
        else:
            tmp = pd.DataFrame(ord_data)  # ,index=[0]
            df_apt = pd.concat([df_apt, tmp])

    print(datetime.today(), sigunguCode, sigunguName, '단지수', len(df_apt))
    p.alert('작업준비 ' + ' ' + sigunguCode + ' ' + sigunguName + '\n' + '단지수 ' + str(len(df_apt)))

    # time.sleep(3)

    for x in range(0, len(df_apt)):
        # print(x,df_apt['kaptCode'][x])
        url1 = 'http://apis.data.go.kr/1613000/AptBasisInfoService1/getAphusBassInfo'  # base 기본
        params = {'serviceKey': akey,
                  'kaptCode': df_apt['kaptCode'][x]
                  }
        r1 = requests.get(url1, params=params, verify=False)
        xml_data1 = xmltodict.parse(r1.text)
        json_data1 = json.loads(json.dumps(xml_data1))
        ord_data1 = json_data1['response']['body']['item']
        if x == 0:
            # df1 = pd.DataFrame.from_records(ord_data1) #
            df1 = pd.DataFrame(ord_data1, index=[0])  #
        elif x % 100 != 0:
            # tmp1 = pd.DataFrame.from_records(ord_data1) # ,index=[0]
            tmp1 = pd.DataFrame(ord_data1, index=[0])  #
            df1 = pd.concat([df1, tmp1])
        else:
            p.alert(sigunguCode + ' ' + sigunguName + '\n\n 3초 대기 ' + str(x) +' - ' + str(len(df_apt)) )
            time.sleep(3)
            # tmp1 = pd.DataFrame.from_records(ord_data1) #,index=[0]
            tmp1 = pd.DataFrame(ord_data1, index=[0])  #
            df1 = pd.concat([df1, tmp1])

    print(datetime.today(), sigunguCode, sigunguName, '기본정보', len(df1))
    p.alert(sigunguCode + ' ' + sigunguName + '\n' + '기본정보 ' + str(len(df1)))

    for x in range(0, len(df_apt)):
        url2 = 'http://apis.data.go.kr/1613000/AptBasisInfoService1/getAphusDtlInfo'  # dt 상세
        params = {'serviceKey': akey,
                  'kaptCode': df_apt['kaptCode'][x]
                  }
        r2 = requests.get(url2, params=params, verify=False)
        xml_data2 = xmltodict.parse(r2.text)
        json_data2 = json.loads(json.dumps(xml_data2))
        ord_data2 = json_data2['response']['body']['item']
        # print(x,'상세',df_apt['kaptCode'][x])
        if x == 0:
            df2 = pd.DataFrame(ord_data2, index=[0])
        elif x % 100 == 0:
            time.sleep(30)
            tmp2 = pd.DataFrame(ord_data2, index=[0])
            df2 = pd.concat([df2, tmp2])
        else:
            tmp2 = pd.DataFrame(ord_data2, index=[0])
            df2 = pd.concat([df2, tmp2])

    print(datetime.today(), sigunguCode, sigunguName, '상세정보', len(df2))

    df1['시군구'] = df1['bjdCode'].str[:5]
    
    df1.rename(columns={'codeMgrNm': '관리방식', 'kaptTel': '연락처', 'kaptFax': 'Fax', 
                        'kaptDongCnt': '동', 'hoCnt': '호', 'kaptdaCnt': '세대',
                        'kaptUsedate': '사용승인', 'codeSaleNm': '분양형태', 'doroJuso': '도로명', 'kaptAddr': '지번','bjdCode':'법정동',
                        }, inplace=True)

    df2.rename(columns={'codeMgr': '일반관리', 'kaptMgrCnt': '관리인원', 'kaptCcompany': '일반관리업체',
                        'codeSec': '경비방식', 'kaptdScnt': '경비인원', 'kaptdSecCom': '경비업체',
                        'kaptdEcnt': '승강기', 'kaptCode': '단지코드', 'kaptName': '단지명', }, inplace=True)

    # df1['연락처'] = df1['연락처'].apply(str)
    df1[['동', '호', '세대']] = df1[['동', '호', '세대']].apply(pd.to_numeric)
    df2[['관리인원', '경비인원', '승강기']] = df2[['관리인원', '경비인원', '승강기']].apply(pd.to_numeric)

    df_merge = pd.merge(df1, df2, how='left', left_on='kaptCode', right_on='단지코드')  # on=None,

    columns = ['시군구', '분양형태', '단지코드', '사용승인', '단지명', '연락처', 'Fax',
               '세대', '동', '호', '승강기',
               '관리방식', '일반관리', '관리인원', '일반관리업체', '경비방식', '경비인원', '경비업체',
               '도로명', '지번', '법정동',
               ]

    df_merge[columns].fillna('')
    df_merge.sort_values(['시군구', '사용승인'], ascending=False)[columns].fillna('').to_excel('./내보내기/' + sigunguCode + '_의무관리공동주택_' + dt + '.xlsx', sheet_name=sigunguName)

    print(datetime.today(), sigunguCode, sigunguName, '조회완료', len(df_merge))
    p.alert('작업완료 ' + '\n' + sigunguCode + ' ' + sigunguName + ' '+str(len(df_merge))+ ' -'+str(i) )

p.alert( '완료' + expr_str)
print( datetime.today(), expr_str)

os.startfile('d:/py/nb/내보내기/')
