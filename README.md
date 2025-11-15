# pylt
- 지방세 담당자를 위한 파이썬
-  (관리비공개)의무관리 공동주택
- aptlist.py 전국 공동주택목록(단위 all,sido,sigungu)
- getListSigungu.py 시도별목록을 활용하여 시군구별 자료 취합
- ltaptlist 전국 공동주택목록중 특정 시도의 시군구 집계
- rpa_통신판매업.ipynb 통신판매사업자 변동자료 자료정비 대상 확인
- ltis_지방인허가.ipynb 지방행정인허가 데이터 개방 변동 데이터 처리
- tn_pubr_public_banner_api.ipynb 현수막 게시대
- 행정구역_인구현황.ipynb 연령별, 세대원수 별 인구현황
- 행정동코드(text 10), 행정기관코드(orgcd, text 10)
- 승강기 설치정보
- 면허분 자료 정리
- 정부24 민원사무 처리기준
- 명절(설,추석) 연휴 의료기관 정보
- 무인민원발급기
- geolocation 

# 행정동 코드 표준화 및 api처리 필요
- 행정기관코드 자료확인(행정안전부 홈페이지 https://www.mois.go.kr/frt/bbs/type001/commonSelectBoardList.do?bbsId=BBSMSTR_000000000052 업무안내 > 지방자치분권실 > 주민등록,인감,행정사 > 주민등록,인감,행정사)
행정안전부_주민등록업무 행정기관 행정동 코드
humd_cd 행정동코드 10
sido_nm  시·도명  40
sgg_nm 시·군·구명 40
umd_nm 읍·면·동명 20
humd_nm 행정동명 102
adpt_de 생성일 8
del_de 말소일  8

## 2025.11.04. 기준
 "https://www.mois.go.kr/cmm/fms/FileDown.do?atchFileId=FILE_001399377CsGSGj&fileSn=3" # layout
 'https://www.mois.go.kr/cmm/fms/FileDown.do?atchFileId=FILE_001399377CsGSGj&fileSn=1' # jscode
 "https://www.mois.go.kr/cmm/fms/FileDown.do?atchFileId=FILE_001399377CsGSGj&fileSn=2" # 말소코드 포함

# 민원처리기준표 고시
- 행정안전부(민원제도과), 044-205-2451
- 옥외광고물 
- https://www.law.go.kr/행정규칙/민원처리기준표일부개정고시
- https://www.law.go.kr/행정규칙/민원처리기준표고시
- https://www.law.go.kr/행정규칙/민원처리기준표일부개정고시/(2023-55,20230713)
- [시행 2023. 10. 4.] [행정안전부고시 제2023-68호, 2023. 10. 4., 일부개정]
- [시행 2023. 8. 4.] [행정안전부고시 제2023-61호, 2023. 8. 4., 일부개정]
- [시행 2023. 7. 13.] [행정안전부고시 제2023-55호, 2023. 7. 13., 일부개정]
- [시행 2023. 6. 13.] [행정안전부고시 제2023-45호, 2023. 6. 7., 일부개정]
- [시행 2023. 2. 9.] [행정안전부고시 제2023-8호, 2023. 2. 3., 일부개정]
- [시행 2022. 12. 21.] [행정안전부고시 제2022-71호, 2022. 12. 15., 일부개정]
- [시행 2022. 9. 29.] [행정안전부고시 제2022-62호, 2022. 9. 23., 일부개정]
- 첨부파일 2022년 제6차 민원처리기준표 고시문.hwp
- 제2022-62호 민원처리기준표 고시문(제개정문).hwp
- df.query('링크민원주소.notnull() ')
- df.query('링크민원주소.isnull() ') 
- df.query('링크민원주소.isna() ') 
- notnull함수와 notna함수는 isnull함수와 반대로 작동합니다
- dropna함수 결측치를 제거
- fillna함수는 결측치를 대체
- str.contains(word) == False 포함하지 않음
- ‣최근 5년간 민원 신청건수 없으므로 폐지
- ‣민원명 현행화

  
