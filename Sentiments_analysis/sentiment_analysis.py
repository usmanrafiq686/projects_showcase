import pandas as pd
import sys
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk.stem import PorterStemmer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
comment_file_name=
post_file_name =

comments = pd.read_csv(comment_file_name,skiprows=[1],low_memory=False, parse_dates=['Time'],index_col=['Time'])
posts = pd.read_csv(post_file_name,skiprows=[1],low_memory=False, parse_dates=['Time'],index_col=['Time'])
print('File loaded')
comments = comments.drop_duplicates(keep=False)
posts = posts.drop_duplicates(keep=False)
print('Dropped duplicates')

sys.setrecursionlimit(10000)
nltk.download('stopwords')
tokenizer = RegexpTokenizer('\w+') # pattern =  words also concatenation
en_stopwords = set(stopwords.words('english')) # removing english stopwords the words like the, he, have 
ps = PorterStemmer()
analyzer = SentimentIntensityAnalyzer()

def post_text_sentiment():
    print('Post_text_sentiment_analysis Started .......')
    pos_strng_text= posts['Post_Text'].apply(lambda x:str(x).lower())
    pos_txt_tokens = pos_strng_text.apply(lambda x: tokenizer.tokenize(x))
    pos_txt_new_tokens = pos_txt_tokens.apply(lambda x: 
                                              [col_token
                                               for col_token in x 
                                               if col_token not in en_stopwords])
    
    pos_txt_stemed_tokens = pos_txt_new_tokens.apply(lambda x:
                                                     [ps.stem(stem_token) 
                                                      for stem_token in x])
    
    pos_txt_strng_text = pos_txt_stemed_tokens.apply(lambda x:" ".join(x))
    pos_txt_vs = pos_txt_strng_text.apply(lambda x: analyzer.polarity_scores(x))
    print('Post_text_sentiment Vador completed')
    pos_txt_sent = pd.DataFrame(pos_txt_vs)
    pos_txt_sent['Text_compound'] = pos_txt_vs.apply(lambda x:
                                                          [value 
                                                           for key,value in x.items()][3])    
    pos_txt_sent['Text_Vador'] = pos_txt_sent['Text_compound'].apply(lambda x:
                                                                               'Positive' 
                                                                               if x>0.10 else 
                                                                               ('Negative' if x< -0.10 
                                                                                else 'Neutral'))
    pos_txt_sent.index = posts.index
    pos_txt_sent.rename(columns={'Post_Text':'Post_Text_Vador_Values'}, inplace=True)
    return pos_txt_sent

def post_title_sentiment():
    print('Post_title_sentiment_analysis Started .......')
    pos_strng_titl= posts['Post_Title'].apply(lambda x:str(x).lower())
    pos_titl_tokens = pos_strng_titl.apply(lambda x: tokenizer.tokenize(x))
    pos_titl_new_tokens = pos_titl_tokens.apply(lambda x: 
                                              [col_token 
                                               for col_token in x 
                                               if col_token not in en_stopwords])
    pos_titl_stemed_tokens = pos_titl_new_tokens.apply(lambda x:
                                                     [ps.stem(stem_token) 
                                                      for stem_token in x])
    pos_titl_strng_text = pos_titl_stemed_tokens.apply(lambda x:" ".join(x))
    pos_titl_vs = pos_titl_strng_text.apply(lambda x: analyzer.polarity_scores(x))
    print('Post_title_sentiment Vador completed')
    pos_titl_sent = pd.DataFrame(pos_titl_vs)
    pos_titl_sent['Title_compound'] = pos_titl_vs.apply(lambda x:
                                                          [value 
                                                           for key,value in x.items()][3])    
    pos_titl_sent['Title_Vador'] = pos_titl_sent['Title_compound'].apply(lambda x:
                                                                               'Positive' 
                                                                               if x>0.10 else 
                                                                               ('Negative' if x< -0.10 
                                                                                else 'Neutral'))
    pos_titl_sent.index = posts.index
    pos_titl_sent.rename(columns={'Post_Title':'Post_Title_Vador_Values'}, inplace=True)
    return pos_titl_sent


def comment_text_sentiments():
    print('Comment_text_sentiment_analysis Started .......')
    cm_strng_txt= comments['Comment_Text'].apply(lambda x:str(x).lower())
    
    cm_txt_tokens = cm_strng_txt.apply(lambda x: tokenizer.tokenize(x))
    
    cm_txt_new_tokens = cm_txt_tokens.apply(lambda x: 
                                          [col_token for col_token in x 
                                           if col_token not in en_stopwords])
    
    cm_txt_stemmed_tokens = cm_txt_new_tokens.apply(lambda x:
                                                  [ps.stem(stem_token) for stem_token in x])
    
    cm_txt_string_text = cm_txt_stemmed_tokens.apply(lambda x:" ".join(x))
    cm_txt_vs = cm_txt_string_text.apply(lambda x: analyzer.polarity_scores(x))
    
    print('Comment_text_sentiment Vador completed')
    
    cm_txt_sentiment = pd.DataFrame(cm_txt_vs)
    cm_txt_sentiment['Text_compound'] = cm_txt_vs.apply(lambda x:
                                                      [value for key,value in x.items()][3])
    
    cm_txt_sentiment['Text_Vador'] = cm_txt_sentiment['Text_compound'].apply(lambda x:
                                                                           'Positive' 
                                                                           if x>0.10 
                                                                           else ('Negative' 
                                                                                 if x< -0.10 
                                                                                 else 'Neutral'))
    cm_txt_sentiment.index = comments.index
    cm_txt_sentiment.rename(columns={'Comment_Text':'Comment_Text_Vador_Values'}, inplace=True)
    return cm_txt_sentiment

       
def comment_title_sentiment():
    print('Comment_title_sentiment_analysis Started .......')
    cm_strng_titl= comments['Comment_Title'].apply(lambda x:str(x).lower())
    cm_titl_tokens = cm_strng_titl.apply(lambda x: tokenizer.tokenize(x))
    cm_new_tokens = cm_titl_tokens.apply(lambda x: 
                                    [col_token for col_token in x 
                                     if col_token not in en_stopwords])
    cm_stemmed_tokens = cm_new_tokens.apply(lambda x:
                                            [ps.stem(stem_token) for stem_token in x])
    cm_strng_titl = cm_stemmed_tokens.apply(lambda x:" ".join(x))
    cm_titl_vs = cm_strng_titl.apply(lambda x: analyzer.polarity_scores(x))
    print('Comment_tittle_sentiment Vador completed')
    cm_titl_sentiment = pd.DataFrame(cm_titl_vs)
    cm_titl_sentiment['title_compound'] = cm_titl_sentiment['Comment_Title'].apply(lambda x:
                                                                               [value for key,value in x.items()][3])
    cm_titl_sentiment['Title_Vador'] = cm_titl_sentiment['title_compound'].apply(lambda x: 'Positive' if x>0.10 else ('Negative' if x< -0.10 else 'Neutral'))
    cm_titl_sentiment.index = comments.index
    cm_titl_sentiment.rename(columns={'Comment_Title':'Comment_Title_Vador_Values'}, inplace=True)
    return cm_titl_sentiment

pos_txt_sent = post_text_sentiment()
pos_titl_sent = post_title_sentiment()
cm_txt_sent = comment_text_sentiments()
cm_titl_sent = comment_title_sentiment()

full_file_post = pd.concat([posts, pos_txt_sent, pos_titl_sent], axis=1)
full_file_comments = pd.concat([comments, cm_txt_sent, cm_titl_sent], axis=1)

full_file_comments.to_csv('Comments_Sentiment_Values.csv', encoding = 'utf-8')
full_file_post.to_csv('Posts_Sentiment_Values.csv', encoding = 'utf-8')