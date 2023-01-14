import pandas as pd
from datetime import datetime
import datetime as dt
import praw
from pmaw import PushshiftAPI
startTime = datetime.now()
def code():
    try:
        startTime = datetime.now()    
        reddit = praw.Reddit(
            # client_id=
            # client_secret=
            # password=
            # user_agent=
            # username=
        )
              
        start_epoch=int(dt.datetime(2021, 11, 2).timestamp())
        end_epoch=int(dt.datetime(2017, 11, 1).timestamp())
        api_praw = PushshiftAPI(praw=reddit)
        gen = list(api_praw.search_submissions(before = start_epoch, after = end_epoch, 
                                       subreddit="Data",limit=100000,
                                       sort_type="created_utc"))
        df = pd.DataFrame(gen)
        df.to_csv('Testing.csv', encoding='utf-8')
        print("Time Taken by code", datetime.now() - startTime)
        
    except Exception:
        code()
code()