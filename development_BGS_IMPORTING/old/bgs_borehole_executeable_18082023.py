# things to do in order of importance 
# issue with references including '/' in their name
# add way to check the 'boreholes downlaoded' file against the dataframe to see if anything wasnt downloaded and then indicate why
# add sucess or faliure statements to the logging file

import pandas as pd
import requests
import os
import logging






class filters:

    def remove_bad_characters():
            # lists the characters that will break the script if they dont go away
            bad_characters=['/']
            # checks through the reference column while providing an index for each reference
            for row in df['REFERENCE']:
                for char in row:
                    # checks if chaacter in reference is bad and if so replaces it with an underscore
                    if char in bad_characters:
                        row.replace(char, '_')
                    print(row)

    # removes any rows (referemces) that do not have dates
    def remove_missing_date():
        for row, location in zip(df['YEAR_KNOWN'], df.index):
            # this checks if the element can be writen in integer form
            try:
                int(row)
            # this will remove a row from the df if the element in the YEAR_KNOWN row cannot be converted to int (meaning it is string nan)
            except:
                df.drop(location, inplace=True)

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
    def length_filter(lower, upper):
        for row, location in zip(df['LENGTH'], df.index):
            if row not in range(lower, upper):
                df.drop(location, inplace=True)

     

class name_alteration:
    def reference_date(index_count):
        reference_date=df.iloc[index_count,6]
        return reference_date

    def reference_depth(index_count):
        reference_depth=df.iloc[index_count,5]
        return reference_depth


class main:
    def __init__(self) -> None:
        self.file_setup()
        self.changing_df
        self.running_downloads()


    # makes a folder in your current working directory called Boreholes
    def file_setup(self):
        # sets up the file path to the boreholes folder for the downloads to go to
        self.pdf_path=f'{os.getcwd()}'+'\Boreholes'
        self.existing_files=os.listdir()
        # removes any boreholes left over from past runs
        if 'Boreholes' in self.existing_files:
            os.rmdir('Boreholes')
        os.mkdir(self.pdf_path)
        #removes any logging info from previous run
        if 'BGS_import_log.log' in self.existing_files:
            os.remove('BGS_import_log.log')
        
        # sets up the logging file
        logging.basicConfig(level=logging.DEBUG, filename='BGS_import_log.log')
        logging.info(f'{self.existing_files}, {self.pdf_path}')
        # imports the csv to the current working directory
        global df
        df=pd.read_csv(r'./GeoIndexData.txt', delimiter='\t')
        logging.info(df.head(10))
        logging.info(f'{df.size}, {df.shape}')
        logging.info(self.pdf_path)

    # this is used to remove the things from the df that we dont like/want
    def changing_df():
        #calling functions
        filters.remove_bad_characters()
        filters.remove_missing_date()
        filters.date_filter(1970,1990)
        # creates temporary csv of the boreholes you are going to download
        df.to_csv(f'{os.getcwd()}/boreholes_downloaded.csv')

# downloads the pdfs and saves them as the name of the reference column
    def download_pdf_file(self, url: str, pdf_file_name) -> bool:
        # accesses the pdf
        response = requests.get(url)
        if response.status_code==200:
            # finds the folder where the pdf will be saved
            logging.debug('pdf_opened')
            pdf=open(str(r'{filepath}\{name}'.format(filepath=self.pdf_path, name=pdf_file_name, )), 'wb')
            # writes and saves the pdf
            pdf.write(response.content)
            pdf.close()
            # notifies you the pdf has been downloaded
            logging.info('PDF Downloaded')

    def running_downloads(self):
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
                    # records the reference of the borehole being downloaded
                    reference_name=df.iloc[index_count,1]
                    # names the pdf as you want it, change this to change name
                    reference_name=f'{reference_name}_{name_alteration.reference_depth(index_count)}.pdf'
                    self.download_pdf_file(row,reference_name)
                    # limits the script to downloading only 1000 boreholes at a time
                    if index_count>10:
                        exit()
                    break

instance=main()
