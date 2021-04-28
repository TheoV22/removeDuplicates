#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 27 14:59:42 2021

@author: Gustavo
"""

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

credentials = Credentials.from_service_account_file(
    #'GoogleCreds/googlecreds.json',
    '/Users/theovaneccelpoel/GoogleTest/GoogleSheets/credentials.json',
    scopes=scopes
)

def main():

    try:
        gc = gspread.authorize(credentials)
        print('Accessed Google Sheet')
    except:
        print('Access denied')

    #spreadsheet_key = '1oC4U8EKnL0g2EBAVT9UV8SUvqaTHUlrjxPcY1R8RSzg'
    spreadsheet_key ='1q2BIzxdVmaaLJkPu1CzyhRQJrzEr8EuNmCdoDdUn-TU'
    sh = gc.open_by_key(spreadsheet_key)

    # Get all the worksheets from the spreadsheet :
    worksheets = sh.worksheets()

    # Get all the values from all the worksheets :
    records = []
    for worksheet in worksheets:
        records.extend(worksheet.get_all_records())

    # Set the global sheet
    df_all = pd.DataFrame(records)
    df_all.drop_duplicates(subset=['name'], keep="first", inplace=True)
    wk_df_all = sh.worksheet("df_all")
    set_with_dataframe(wk_df_all, df_all)

    # Add a worksheet to write the new values there :
    try:
        sh.add_worksheet(title=None, rows="100", cols="36")
        worksheet = sh.get_worksheet(len(worksheets))
        print("New sheet created :", worksheet)
    except:
        print("Can't create new sheet")

    # Convert the csv file into a dataframe :
    df_new = pd.read_csv('/Users/theovaneccelpoel/GoogleTest/amazonweb/CSV/keyword_table.csv', sep='\t')
    # INSERT NEW VALUES HERE ABOVE

    # Make sure we will drop all values from df_all :
    df_all_2 = pd.concat([df_all, df_all])
    df1 = pd.concat([df_all_2, df_new])

    # Do reset index / reindex in order to keep only non-duplicate values from the new values added :
    df1 = df1.reset_index(drop=True)
    df1_gpby = df1.groupby(list(df1.columns))
    idx = [x[0] for x in df1_gpby.groups.values() if len(x) == 1]
    df1.reindex(idx)
    df1.drop_duplicates(subset=['name'], keep=False, inplace=True)

    # Update the new worksheet with only non-duplicates
    worksheet.clear()
    set_with_dataframe(worksheet, df1)

    # Update the global sheet with the new non-duplicates
    new_df_all = pd.concat([df_all, df1])
    set_with_dataframe(wk_df_all, new_df_all)

    # Get the id of the new created sheet :
    WORKSHEET_ID = worksheet.id
    print("worksheet ID = ", WORKSHEET_ID)

    # Create the request_body to update dimensions properties of new sheet :
    request_body = {
        'requests': [
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': WORKSHEET_ID,
                        'dimension': 'COLUMNS',
                        'startIndex': 25,
                        'endIndex': 30
                    },
                    'properties': {
                        'pixelSize': 122
                    },
                    'fields': 'pixelSize'
                }
            },
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': WORKSHEET_ID,
                        'dimension': 'ROWS',
                        'startIndex': 1,
                    },
                    'properties': {
                        'pixelSize': 100
                    },
                    'fields': 'pixelSize'
                }
            }
        ]

    }

    # Update the dimension properties of the worksheet :
    service = build('sheets', 'v4', credentials=credentials)
    response = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_key,
        body=request_body
    ).execute()

    GLOBALSHEET_ID = 1637199561
    request_body_Global = {
        'requests': [
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': GLOBALSHEET_ID,
                        'dimension': 'COLUMNS',
                        'startIndex': 25,
                        'endIndex': 30
                    },
                    'properties': {
                        'pixelSize': 122
                    },
                    'fields': 'pixelSize'
                }
            },
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': GLOBALSHEET_ID,
                        'dimension': 'ROWS',
                        'startIndex': 1,
                    },
                    'properties': {
                        'pixelSize': 100
                    },
                    'fields': 'pixelSize'
                }
            }
        ]

    }

    # Update the dimension properties of the global sheet :
    service = build('sheets', 'v4', credentials=credentials)
    response = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_key,
        body=request_body_Global
    ).execute()

    print('Google Sheet was updated successfully : {}, {}'.format(df_new.shape[0], df_new.shape[1]))

if __name__ == '__main__':
    main()
