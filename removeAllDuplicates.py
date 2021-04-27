import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

# """
# Remove all duplicates from all the sheets
# Result:
#  - one global sheet named "df_all", containing all the products scraped since the beginning
#  - one new sheet with only the non-duplicates products
#  - each time we launch the program:
#         -> the content of the new sheet will be copied to the global sheet
#         -> the new sheet content will be overwritted by the newest non-duplicates products
# """


# ACCESS GOOGLE SHEET
gc = gspread.service_account(filename='credentials.json')
sh = gc.open_by_key('1q2BIzxdVmaaLJkPu1CzyhRQJrzEr8EuNmCdoDdUn-TU')

worksheets = sh.worksheets()
print("Worksheets = ", worksheets)
print("Number of worksheets =", len(worksheets))

# TO CREATE THE GLOBAL SHEET:
# wk_df_all = sh.add_worksheet(title="df_all", rows=100, cols=20)

# LOAD ALL THE CONTENT
records = []
for worksheet in worksheets:
    records.extend(worksheet.get_all_records())
df_all = pd.DataFrame(records)
df_all.drop_duplicates(subset=['name'], keep="first", inplace=True)
print("df_all = \n", df_all)
wk_df_all = sh.worksheet("df_all")
set_with_dataframe(wk_df_all, df_all)

del worksheets[0]  # exclude the global sheet
# DROP THE DUPLICATES LOOP
for worksheet in worksheets :
    print("Working on sheet:", worksheet)
    worksheet.update('A4', 'G')  # <===== insert new records here
    df = pd.DataFrame(worksheet.get_all_records())
    df_all_2 = pd.concat([df_all, df_all])  # make sure to drop all values from df_all
    df1 = pd.concat([df_all_2, df])
    df1 = df1.reset_index(drop=True)  # reset the index
    df1_gpby = df1.groupby(list(df1.columns))  # group by
    idx = [x[0] for x in df1_gpby.groups.values() if len(x) == 1]
    df1.reindex(idx)  # reindex
    print("Reindexed df1 = \n", df1)
    df1.drop_duplicates(subset=['name'], keep=False, inplace=True)
    print("duplicates-free df1: \n", df1)
    new_df_all = pd.concat([df_all, df1])
    worksheet.clear()
    set_with_dataframe(worksheet, df1)
    set_with_dataframe(wk_df_all, new_df_all)