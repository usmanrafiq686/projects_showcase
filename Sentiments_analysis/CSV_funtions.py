import glob, os
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk.stem import PorterStemmer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import sys

sys.setrecursionlimit(10000)
nltk.download('stopwords')
subreddit_names = ['1','2']

# Base_Directory =
os.chdir(Base_Directory)
years = []
tokenizer = RegexpTokenizer('\w+') # pattern =  words also concatenation
en_stopwords = set(stopwords.words('english')) # removing english stopwords the words like the, he, have 
ps = PorterStemmer()
analyzer = SentimentIntensityAnalyzer()
ticker_list = ['1', '2', '3', '4']

def csv_shaper_submissions():
    for subreddit_dir in subreddit_names:
        first_sub_dir = Base_Directory + '\\' + subreddit_dir
        for dirs in years:
            yearly_dir = first_sub_dir + '\\' + str(dirs)
            os.chdir(yearly_dir)
            files_names = glob.glob("*_submissions.csv")
            new_path= os.path.join(yearly_dir, '1_sort_file')
            if os.path.exists(new_path) and os.path.isdir(new_path): # If the folder already exist it doesn't create new one 
                print('1_sort_file Already exist')
            else:
                os.makedirs('2_with_sentiment')
                print('2_with_sentiment Directory Created')
            for iterations in files_names:
                try:
                    shaper = pd.read_csv(iterations, low_memory=False)
                except Exception:
                    shaper = pd.DataFrame()
                if shaper.empty== False: # Check if the file is not empty
                    shaper["time_str"] = pd.to_datetime(shaper["created_utc"], unit='s').dt.strftime('%Y-%m-%dT%H:%M:%S')
                    shaper.sort_values(["time_str"], inplace=True)
                    os.chdir(first_sub_dir + '\\' + str(dirs)+'\\1_sort_file')
                    shaper.to_csv(iterations[:-4] + '_sorted.csv', encoding='utf-8')
                os.chdir('..')
            os.chdir('..')

def csv_shaper_comments():
    for subreddit_dir in subreddit_names:
        first_sub_dir = Base_Directory + '\\' + subreddit_dir
        for dirs in years:
            yearly_dir = first_sub_dir + '\\' + str(dirs)
            os.chdir(yearly_dir)
            files_names = glob.glob("*_comments.csv")
            new_path= os.path.join(yearly_dir, '1_sort_file')
            if os.path.exists(new_path) and os.path.isdir(new_path): # If the folder already exist it doesn't create new one 
                print('1_sort_file Already exist')
            else:
                os.makedirs('2_with_sentiment')
                print('2_with_sentiment Directory Created')
            for iterations in files_names:
                try:
                    shaper = pd.read_csv(iterations, low_memory=False)
                except Exception:
                    shaper = pd.DataFrame()
                if shaper.empty== False:
                    shaper["time_str"] = pd.to_datetime(shaper["created_utc"], unit='s').dt.strftime('%Y-%m-%dT%H:%M:%S')
                    shaper.sort_values(["time_str"], inplace=True)
                    os.chdir(first_sub_dir + '\\' + str(dirs)+'\\1_sort_file')
                    shaper.to_csv(iterations[:-4] + '_sorted.csv', encoding='utf-8')
                os.chdir('..')
            os.chdir('..')
        
def sentimental_analsis_submissions():
    for subreddit_dir in subreddit_names:
        first_sub_dir = Base_Directory + '\\' + subreddit_dir
        for dirs in years:
            yearly_dir = first_sub_dir + '\\' + str(dirs)
            os.chdir(yearly_dir)
            files_names = glob.glob("*_submissions.csv")
            new_path= os.path.join(yearly_dir, '2_with_sentiment')
            if os.path.exists(new_path) and os.path.isdir(new_path):
                print('2_with_sentiment Already exist')
            else:
                os.makedirs('2_with_sentiment')
                print('2_with_sentiment Directory Created')
            for iterations in files_names:
                try:
                    submission_file = pd.read_csv(iterations, low_memory=False)
                except Exception:
                    submission_file = pd.DataFrame()
                if submission_file.empty== False: # Check if the file is not empty
                    submissions_selftext= submission_file['selftext']
                    submissions_title= submission_file['title']
                    clean_submissions_selftext = []        
                    for submissions_finder in submissions_selftext:
                        submissions_finder = str(submissions_finder) 
                        lowercase_submissions =submissions_finder.lower() # text lower case  
                        tokens = tokenizer.tokenize(lowercase_submissions) # get a list of tokens

                        new_tokens = [token for token in tokens if token not in en_stopwords] # performing tokeninzation and stopword 
                                                                                                # removal at sametime
                        stemmed_tokens = [ps.stem(tokens) for tokens in new_tokens] # Perform stemming
                        string_submissions = " ".join(stemmed_tokens)
                        clean_submissions_selftext.append(string_submissions)

                    polarity_list_selftext = [] 
                    for polarity_calculator in clean_submissions_selftext:
                        vs = analyzer.polarity_scores(polarity_calculator) # vador polarity score calculation
                        polarity_list_selftext.append(vs)
                    sentiment_selftext = pd.DataFrame (polarity_list_selftext)
                    sentiment_selftext = sentiment_selftext.rename(columns={'neg': 'Selftext_Neg', 'neu': 'Selftext_Neu', 
                                              'pos': 'Selftext_Pos', 
                                              'compound': 'Selftext_Score'})
                    sentiment_selftext['Selftext_Vader'] = 'Neutral'
                    sentiment_selftext.loc[sentiment_selftext['Selftext_Score'] > 0.10, 'Selftext_Vader'] = 'Positive'
                    sentiment_selftext.loc[sentiment_selftext['Selftext_Score'] < -0.10, 'Selftext_Vader'] = 'Negative' 
                    file_selftext = pd.concat([submission_file, sentiment_selftext], axis=1)
                    clean_submissions_title = []
                    for submissions_finder in submissions_title:
                        submissions_finder = str(submissions_finder) 
                        lowercase_submissions =submissions_finder.lower() # text lower case  
                        tokens = tokenizer.tokenize(lowercase_submissions) # get a list of tokens

                        new_tokens = [token for token in tokens if token not in en_stopwords] # performing tokeninzation and stopword 
                                                                                                # removal at sametime
                        stemmed_tokens = [ps.stem(tokens) for tokens in new_tokens] # Perform stemming
                        string_submissions = " ".join(stemmed_tokens)
                        clean_submissions_title.append(string_submissions)

                    polarity_list_title = [] 
                    for polarity_calculator in clean_submissions_title:
                        vs = analyzer.polarity_scores(polarity_calculator) # vador polarity score calculation
                        polarity_list_title.append(vs)
                    sentiment_title = pd.DataFrame (polarity_list_title)
                    sentiment_title= sentiment_title.rename(columns={'neg': 'Title_Neg', 
                                              'neu': 'Title_Neu', 'pos': 'Title_Pos', 
                                              'compound': 'Title_Score'})

                    sentiment_title['Title_Vader'] = 'Neutral'
                    sentiment_title.loc[sentiment_title['Title_Score'] > 0.10, 'Title_Vader'] = 'Positive'
                    sentiment_title.loc[sentiment_title['Title_Score'] < -0.10, 'Title_Vader'] = 'Negative' 
                    full_file = pd.concat([file_selftext, sentiment_title], axis=1)
                    full_file["time_str"] = pd.to_datetime(full_file["created_utc"], unit='s').dt.strftime('%Y-%m-%dT%H:%M:%S')
                    full_file.sort_values(["time_str"], inplace=True)
                    os.chdir(first_sub_dir + '\\' + str(dirs)+'\\2_with_sentiment')
                    full_file.to_csv(iterations[:-4] + '_sorted.csv', encoding='utf-8')
                os.chdir('..')
            os.chdir('..')          

def sentimental_analsis_comments():
    for subreddit_dir in subreddit_names:
        first_sub_dir = Base_Directory + '\\' + subreddit_dir
        for dirs in years:
            yearly_dir = first_sub_dir + '\\' + str(dirs)
            os.chdir(yearly_dir)
            files_names = glob.glob("*_comments.csv")
            new_path= os.path.join(yearly_dir, '2_with_sentiment')
            if os.path.exists(new_path) and os.path.isdir(new_path):
                print('2_with_sentiment Already exist')
            else:
                os.makedirs('2_with_sentiment')
                print('2_with_sentiment Directory Created')
            for iterations in files_names:
                try:
                    submission_file = pd.read_csv(iterations, low_memory=False)
                except Exception:
                    submission_file = pd.DataFrame()
                if submission_file.empty== False: # Check if the file is not empty
                    submissions_body = submission_file['body']
                    clean_submissions_body = []        
                    for submissions_finder in submissions_body:
                        submissions_finder = str(submissions_finder) 
                        lowercase_submissions =submissions_finder.lower() # text lower case  
                        tokens = tokenizer.tokenize(lowercase_submissions) # get a list of tokens

                        new_tokens = [token for token in tokens if token not in en_stopwords] # performing tokeninzation and stopword 
                                                                                                # removal at sametime
                        stemmed_tokens = [ps.stem(tokens) for tokens in new_tokens] # Perform stemming
                        string_submissions = " ".join(stemmed_tokens)
                        clean_submissions_body.append(string_submissions)

                    polarity_list_body = [] 
                    for polarity_calculator in clean_submissions_body:
                        vs = analyzer.polarity_scores(polarity_calculator) # vador polarity score calculation
                        polarity_list_body.append(vs)
                    sentiment_body = pd.DataFrame (polarity_list_body)
                    sentiment_body = sentiment_body.rename(columns={'neg': 'Body_Neg', 'neu': 'Body_Neu', 
                                              'pos': 'Body_Pos', 
                                              'compound': 'Body_Score'})
                    sentiment_body['Body_Vader'] = 'Neutral'
                    sentiment_body.loc[sentiment_body['Body_Score'] > 0.10, 'Body_Vader'] = 'Positive'
                    sentiment_body.loc[sentiment_body['Body_Score'] < -0.10, 'Body_Vader'] = 'Negative' 
                    full_file = pd.concat([submission_file, sentiment_body], axis=1)
                    full_file["time_str"] = pd.to_datetime(full_file["created_utc"], unit='s').dt.strftime('%Y-%m-%dT%H:%M:%S')
                    full_file.sort_values(["time_str"], inplace=True)
                    os.chdir(first_sub_dir + '\\' + str(dirs)+'\\2_with_sentiment')
                    full_file.to_csv(iterations[:-4] + '_sorted.csv', encoding='utf-8')
                os.chdir('..')
            os.chdir('..')          

try:
    csv_shaper_submissions()
    csv_shaper_comments()
    sentimental_analsis_submissions()
    sentimental_analsis_comments()
except Exception as error:
    print(error)
    