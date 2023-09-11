import function
import WebCrawling
import time
import ApiMethods
while True:
    # این بخش حلقه برای خزش وب است
    if function.is_time_between(17,19):
        webCrawlingCheak = False
        while webCrawlingCheak == False and function.is_time_between(20,22):
            try:
                WebCrawling.WC()
                webCrawling = True
                print('Web Crawling successful')
            except:
                print('Web Crawling Break')
                time.sleep(60)

    if function.is_time_between(8,17) and function.is_time_divisible(15):
    # این بخش حلقه برای دریافت اطلاعات از کار گزاری است
        function.getTseToday()
    # این بخش حلقه برای دریافت اطلاعات از کار گزاری است
        ApiMethods.GetTodayAllTrade()
        time.sleep(60)



    