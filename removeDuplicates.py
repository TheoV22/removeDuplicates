import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

# ACCES GOOGLE SHEET
gc = gspread.service_account(filename='credentials.json')
sh = gc.open_by_key('1q2BIzxdVmaaLJkPu1CzyhRQJrzEr8EuNmCdoDdUn-TU')
worksheet = sh.get_worksheet(0)

# APPEND DATA TO SHEET
df = pd.DataFrame(worksheet.get_all_records())
df.drop_duplicates(subset=['name'], keep="first", inplace=True)

# CLEAR SHEET CONTENT
range_of_cells = worksheet.range('A1:D10') #-> Select the range you want to clear
for cell in range_of_cells:
    cell.value = ''
worksheet.update_cells(range_of_cells)

set_with_dataframe(worksheet, df) #-> THIS EXPORTS YOUR DATAFRAME TO THE GOOGLE SHEET
