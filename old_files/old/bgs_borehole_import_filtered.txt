import pandas as pd
import requests
import re
import logging
logging.basicConfig(level=logging.DEBUG, filename='BGS_import.log')



# imports the csv
df=pd.read_csv(r'C:\Users\OGONAHT\OneDrive - Jacobs\NEAR\Python importing\GeoIndexData.txt', delimiter='\t')
print(df.head(10))
print(df.size, df.shape)

# removes any rows (referemces) that do not have dates
def remove_missing_date():
    for row, location in zip(df['YEAR_KNOWN'], df.index):
        # this checks if the element can be writen in integer form
        try:
            int(row)
        # this will remove a row from the df if the element in the YEAR_KNOWN row cannot be converted to int (meaning it is string nan)
        except:
            df.drop(location, inplace=True)
print(df.size, df.shape)


# removes any references that do not have dates within the range provided 
def date_filter(lower, upper):
    for row, location in zip(df['YEAR_KNOWN'], df.index):  
        try:
            int(row)
            # removes any references that do not fit within the date range provided
            if row not in range(lower, upper):
                df.drop(location, inplace=True)
        except:
            # notes if there are still references with missing dates, and adds to log file
            logging.error('References missing dates have not been excluded')
    print(df.size, df.shape)


# removes any references that do not have a length (depth) within the given range
def length_filer(lower, upper):
    for row, location in zip(df['LENGTH'], df.index):
        if row not in range(lower, upper):
            df.drop(location, inplace=True)

     

# downloads the pdfs and saves them as the name of the reference column
def download_pdf_file(url: str, pdf_file_name) -> bool:
    # accesses the pdf
    response = requests.get(url)
    if response.status_code==200:
        # finds the folder where the pdf will be saved
        pdf=open(str(r'C:\Users\OGONAHT\OneDrive - Jacobs\NEAR\Python importing\PDF\{dates}\{name}'.format(name=pdf_file_name, dates='Including_Dates')), 'wb')
        # writes and saves the pdf
        pdf.write(response.content)
        pdf.close()
        # notifies you the pdf has been downloaded
        logging.info('PDF Downloaded')

# removes extra charatcers from url column
index_count=1
for row in df['Record']:
    # adds a count for the row you are looking at so you can use iloc to acces data
    index_count+=1
    # saves a copy of the data before it is modified
    b=row
    # removes the excess characters from before the url
    row=row[9:]
    #removes the excess characters from after the url
    char_count=0
    for x in row:
        char_count+=1
        if x=="'":
            row=row[:char_count-1]
            # records the reference, date, and depth of the boreholes
            reference_name=df.iloc[index_count,1]
            reference_date=df.iloc[index_count,6]
            reference_depth=df.iloc[index_count,5]
            # names the pdf as you want it
            reference_name=f'{reference_name}_{reference_date}.pdf'
            download_pdf_file(row,reference_name)
            # stops the script after x boreholes
            if index_count>10:
                exit()
            break
    