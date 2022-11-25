import subprocess
import sys
subprocess.check_call([sys.executable, "-m", "pip", "install", 'selenium'])
subprocess.check_call([sys.executable, "-m", "pip", "install", 'urllib3'])
subprocess.check_call([sys.executable, "-m", "pip", "install", ' beautifulsoup4'])
subprocess.check_call([sys.executable, "-m", "pip", "install", ' pandas'])


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from concurrent import futures 
from urllib.request import Request, urlopen 
import urllib
from bs4 import BeautifulSoup as soup
import pandas as pd
from time import sleep, perf_counter
from urllib.request import Request, urlopen 
import os
import threading
import logging
start_time= perf_counter()
logging.basicConfig(level=logging.INFO, filename="Image_Scrapper_log", filemode="a+", format="%(asctime)-15s %(levelname)-8s %(message)s")


def post_scrapper(start_page, end_page):
    try:
        for pages in range(start_page,end_page): # Looping through the pages of the website
            err_loop = pages
            headers = {'User-Agent': 'Mozilla/5.0','Connection': 'keep-alive'}
            """
            URL is hidden due to confidentiality
            """
            req = Request(url , headers=headers) 
            opener=urllib.request.build_opener()
            opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
            urllib.request.install_opener(opener)    
            webpage = urlopen(req).read() #Reading the data
            page_soup = soup(webpage, "html.parser") #Parsing the data into html soup (method defined in Beutifulsoup lib)
            results = page_soup.find(id="main") #Defining the elecment to be extracted
            posts = results.find_all('article', attrs={'class':'clearfix'}) #Defining the elecment to be extracted
            post_text_list = [] # list for storing text data
            post_title_list= [] 
            post_id_list = [] 
            post_author_list = [] 
            post_time_list = [] 
            post_userid_list = [] 
            post_image_name = []
            p_im_loc_list = []
            current_dir = os.getcwd()
            
            for post in posts: 
                post_texts = post.find("div",class_="text")  # Defining element to be extrated for text post
                post_title = post.find("h2",class_="post_title")
                post_id= post.find('a', attrs={'data-post' : True})
                post_author = post.find("span", class_="post_author")
                post_time = post.find("span",class_="time_wrap")
                post_user_id= post.find("span",class_="poster_hash")
                post_imdata = post.find('div', class_="thread_image_box")
                
                images_link = []
                
                if post_imdata is not None:
                    post_image_data = post_imdata.find_all('a', attrs={'class':'btnr parent'})

                    for ps_im_link in post_image_data:
                    
                        post_image_link = ps_im_link.get('href')
                        
                        images_link.append(post_image_link)
                        
                    if images_link:
                        images_link = images_link[1]
                        remove_words = 'https://www.google.com/searchbyimage?image_url='
                        image_url = images_link.replace(remove_words, '')  
                        
                        filename =str(image_url[-14:])
                    else:
                        print('I got an empty list')
                        p_hyperlink_name = 'No Data'
                        filename ='No Data'
                        post_image_name.append(filename)
                        p_im_loc_list.append(p_hyperlink_name)

                    try:
                        urllib.request.urlretrieve((image_url), filename)

                    except:
                        print('URL Did not worked so adding URL not found')
                        post_image_name.append('URL Not Found')
                        p_im_loc_list.append('URL Not Found')

                    p_hyperlink_name = current_dir + '\\' + filename
                    post_image_name.append(filename)
                    p_im_loc_list.append(p_hyperlink_name)
                
                if post_user_id is not None: #Excluding the none 
                    ps_us_id = post_user_id.get_text() #Get text out of labels
                    post_userid_list.append(ps_us_id) #Appending the data into list


                if post_author is not None:
                    pst_author = post_author.get_text()
                    post_author_list.append(pst_author)

                if post_time is not None:
                    ti = post_time.get_text()
                    post_time_list.append(ti)

                if post_id is not None:
                    pst_id = post_id.get('data-post-id') #Getting further tag data
                    post_id_list.append(pst_id)

                if post_title is not None:
                    pstlt = post_title.get_text() 
                    post_title_list.append(pstlt)

                if post_texts is not None:
                    pst = post_texts.get_text()
                    post_text_list.append(pst)

            ##------- Defining Post Dataframes---------####

            post_time_df = pd.DataFrame(post_time_list)

            post_userid_df = pd.DataFrame(post_userid_list)

            post_id_df = pd.DataFrame(post_id_list)

            post_a_df = pd.DataFrame(post_author_list)

            post_title_df = pd.DataFrame(post_title_list)

            post_text_df = pd.DataFrame(post_text_list)

            post_im_loc_df = pd.DataFrame(p_im_loc_list)

            post_im_nm_df = pd.DataFrame(post_image_name)

            post_merged_df = pd.concat([post_time_df, post_userid_df, post_id_df, 
                                        post_a_df, post_title_df, post_text_df, post_im_loc_df, post_im_nm_df],  
                                  axis=1, keys=["Time", "User_ID", "Post_ID" , 
                                                "Post_Author", "Post_Title", "Post_Text", "Post_Image_Loc",
                                                "Image_Hyperlink"]) # Merging all dataframes

            if post_merged_df['Image_Hyperlink'] is not None:
                post_merged_df['Image_Hyperlink'] = '=HYPERLINK("'+ post_merged_df["Post_Image_Loc"] +'","' + post_merged_df["Image_Hyperlink"]+'")'

            file_name = 'posts.csv'
            path = current_dir + '/' + file_name 
            if os.path.exists(path):
                print('Post File already exist')
                post_merged_df.to_csv(file_name, mode='a', index=False, header=False,  encoding='utf-8')
            else:
                post_merged_df.to_csv(file_name, index=False, encoding='utf-8') #Creatind CSV file from dataframes
                print('Post CSV file created')
            print('Posts for page number ' + str(pages) + ' Done')
        
    except Exception as err:
        logging.info(f'Error named = {err} - Occured at Pages number {err_loop}')
        post_scrapper(err_loop, end_page)
        print(f'Something went wrong starting scrapping posts from the page {err_loop }')
            

def comment_scrapper(start_page, end_page):
    try:
        for pages in range(start_page , end_page):
            """
            URL is hidden due to confidentiality
            """
            mutex = threading.Lock()
            mutex.acquire()
            driver = Service("/home/calab/Desktop/testing/chromedriver") # Path need to updated
            driver = webdriver.Chrome(service=driver)
            driver.get(url) #opening URL 
            get_source = driver.page_source #Getting HTML content
            opener=urllib.request.build_opener()
            opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
            urllib.request.install_opener(opener)    
            page_soup = soup(get_source, "html.parser") #Parsing the data into html soup (method defined in Beutifulsoup lib)
            results = page_soup.find(id="main") #Defining the elecment to be extracted
            posts = results.find_all('article', attrs={'class':'clearfix'}) # Getting id of each post for XPATH
            pst_id_list = []
            for post in posts: 
                post_id= post.find('a', attrs={'data-post' : True})

                if post_id is not None:
                    pst_id = post_id.get('data-post-id') #Getting further tag data
                    pst_id_list.append(pst_id)
            i = 1        
            for click in pst_id_list: 
                try:
                    if i ==1:
                        xpath = '//*[@id="'+str(click)+ '"]/div[6]/span/a/i'  # First XPATH
                        more_buttons = driver.find_element(by=By.XPATH, value=xpath).click()
                    else:
                        xpath = '//*[@id="'+str(click)+ '"]/div[5]/span/a/i'
                        more_buttons = driver.find_element(by=By.XPATH, value=xpath).click() # Remaining XPATHs 

                except:
                    pass
                i = i + 1
            
            sleep(4) # Wait to load all values of the page
            page_source = driver.page_source    
            page_soup = soup(page_source, "html.parser")
            results = page_soup.find(id="main")
            comments = results.find_all('div', attrs={'class':'post_wrapper'})

            comment_text_list = [] # list for comment text data
            comment_title_list = []
            comment_id_list = []
            comment_author_list = []
            comment_time_list = []
            comment_userid_list = []
            cs_image_name = []
            c_im_loc_list = []
            current_dir = os.getcwd()
            
            for comment in comments:
                comment_text = comment.find("div",class_="text") # Defining element to be extrated 
                comment_title = comment.find("h2",class_="post_title")
                comment_id= comment.find('a', attrs={'data-post' : True})
                comment_author = comment.find("span", class_="post_author")
                comment_time = comment.find("span",class_="time_wrap")
                comment_userid= comment.find("span",class_="poster_hash")
                comment_imdata = comment.find('div', class_="thread_image_box")
                if comment_imdata is not None:
                    comment_image_data = comment_imdata.find('img', attrs={'src' : True})
                    comment_image_data = comment_image_data.get('src')
                    filename =str(comment_image_data[-18:])
                    c_hyperlink_name = current_dir + '\\' + filename
                    cs_image_name.append(filename)
                    c_im_loc_list.append(c_hyperlink_name)
                    
                    try:
                        urllib.request.urlretrieve((comment_image_data), filename) #Reterieve Image from URL
                    
                    except:
                        print('URL Did not worked so adding URL not found')
                        cs_image_name.append('URL Not Found')
                        c_im_loc_list.append('URL Not Found')
                else:
                    cs_image_name.append('No Image in the comment')
                    c_im_loc_list.append('No Image in the comment')

                if comment_userid is not None: #Excluding the none value
                    cs_usid = comment_userid.get_text()
                    comment_userid_list.append(cs_usid)

                if comment_time is not None:
                    cti = comment_time.get_text()
                    cti = " ".join([cti.strip()]) # Removing /n from the time
                    comment_time_list.append(cti)

                if comment_author is not None:
                    cst_author = comment_author.get_text()
                    comment_author_list.append(cst_author)

                if comment_id is not None:
                    cst_id = comment_id.get('data-post-id') #Getting further tag data
                    comment_id_list.append(cst_id)

                if comment_title is not None:
                    cstlt = comment_title.get_text() 
                    cstlt = " ".join([cstlt.strip()]) # Removing /n from the title
                    comment_title_list.append(cstlt)

                if comment_text is not None:
                    cst = comment_text.get_text()
                    cst = " ".join([cst.strip()]) # Removing /n from the text
                    comment_text_list.append(cst)

            ##------- Comments Dataframes---------####

            comment_time_df = pd.DataFrame(comment_time_list)

            comment_userid_df = pd.DataFrame(comment_userid_list)

            comment_id_df = pd.DataFrame(comment_id_list)

            comment_a_df = pd.DataFrame(comment_author_list)

            comment_title_df = pd.DataFrame(comment_title_list)

            comment_text_df = pd.DataFrame(comment_text_list)

            comment_im_loc_df = pd.DataFrame(c_im_loc_list)
            
            comment_im_nm_df = pd.DataFrame(cs_image_name)
            
            comment_merged_df = pd.concat([comment_time_df,
                                   comment_userid_df, comment_id_df, comment_a_df, 
                                   comment_title_df, comment_text_df, comment_im_loc_df, comment_im_nm_df],
                                          axis=1, keys=["Time", "Comment_UserID", "Comment_ID" ,
                                                        "Comment_Author", "Comment_Title", "Comment_Text",
                                                        "Image_Loc", "Image_Hyperlink"])
            if comment_merged_df['Image_Hyperlink'] is not None:
                comment_merged_df['Image_Hyperlink'] = '=HYPERLINK("'+ comment_merged_df["Image_Loc"] +'","' + comment_merged_df["Image_Hyperlink"]+'")'

            file_name = 'comments.csv'
            path = current_dir + '/' + file_name 
            if os.path.exists(path):
                comment_merged_df.to_csv(file_name, mode='a', index=False, header=False,  encoding='utf-8')
                print('Comment file already available')
                print('Comments for page number ' + str(pages) + ' Done')
            else:
                comment_merged_df.to_csv(file_name, index=False, encoding='utf-8') #Creatind CSV file from dataframes
                print('CSV file created')
                print('Comments for page number ' + str(pages) + ' Done')
                
            mutex.release()
    except Exception as err:
        logging.info(f'Error named = {err} - Occured at Pages number {err_loop}')
        comment_scrapper(err_loop, end_page)
        print(f'Something went wrong starting scrapping posts from the page {err_loop }')
        
#### ------  Calling Threads for Posts and Comments ------ #####

post_threads = futures.ThreadPoolExecutor (max_workers= 4 ) # Define number of threads to be used
comment_threads = futures.ThreadPoolExecutor (max_workers= 4 )

post_threads.submit (post_scrapper, 16300, 83350 ) # 1st thread for post
comment_threads.submit (comment_scrapper, 16300, 83350 ) # 1st thread for comments

post_threads.submit (post_scrapper, 83350, 150400)  #2nd for posts
comment_threads.submit (comment_scrapper, 83350, 150400)  #2nd for comments

post_threads.submit (post_scrapper, 150400, 217450)  #3rd for posts
comment_threads.submit (comment_scrapper, 150400, 217450)  #3rd for comments

post_threads.submit (post_scrapper, 217450, 284500)  #4th for posts
comment_threads.submit (comment_scrapper, 217450, 284500)  #4th for comments


print ( "Posts ----- All tasks started." ) 
logging.info("Posts ----- All tasks started.")
print ( "Comments ----- All tasks started." )
logging.info("Comments ----- All tasks started.")

post_threads.shutdown () # Closing all Post threads
comment_threads.shutdown () # Closing all comments threads
end_time= perf_counter()

hours, rem = divmod(end_time - start_time, 3600) # converting time into hours
minutes, seconds = divmod(rem, 60) # converting time into minutes and seconds
print("It took {:0>2} hours :{:0>2} minutes :{:05.2f} seconds to complete".format(int(hours),int(minutes),seconds)) # Time
logging.info('It took {:0>2} hours :{:0>2} minutes :{:05.2f} seconds to complete'.format(int(hours),int(minutes),seconds))
print ( "Posts ----- All tasks done." )
