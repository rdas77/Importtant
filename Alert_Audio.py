import schedule
import time
import cx_Oracle
#import csv
import pyttsx3
from datetime import datetime 
        
# Functions setup
def sudo_placement():
    print("Get ready for Sudo Placement at Geeksforgeeks")
  
def good_luck():
    print("Good Luck for Test")
  

    
  
def bedtime():
    print("It is bed time go rest")
      
def EagleEye_Alert():
    #import pyautogui as pag
    #pag.alert(text="Hello World", title="The Hello World Box")
    con = cx_Oracle.connect('EQUITY/EQUITY@localhost/XE')
    cur = con.cursor()
    cur.execute("select a.symbol symbol, a.rowid row_id,remark_v_1 remark_v ,a.sms_trg_flg_v sms_trg_flg_v from daily_shortsell_call a where   a.SMS_TRG_FLG_V ='N'  AND rownum < 2")
    res = cur.fetchall()
    for symbol,row_id,remark_v,sms_trg_flg_v in res:
        print ('The symbol is~~~~',symbol)
        print ('The rowid is ~~~~',row_id)
        
        if (sms_trg_flg_v=='N') :
            print ( 'inside audio block');
            try:
                #remark_v = symbol +'price'+str(last_price)+'open price ~'+str(open_price_n)+'max price ~'+str(max_price_n)+'min price ~'+str(min_price_n)+remark_v
                #pywhatkit.sendwhatmsg_instantly(phone_no="+918105708878", message=remark_v,tab_close=True)
                #pywhatkit.sendwhatmsg_to_group_instantly( group_id="B7jDGP6RbwMAWAnoKEmvq4",message=remark_v,tab_close=True,wait_time = 40)
                
                #time.sleep(1)
                #pyautogui.click()
                #time.sleep(1)
                #keyboard.press(Key.enter)
                #keyboard.release(Key.enter)
                
                
                
                
                

                #if audio_message_val_n==9 and audio_alert_flg_v !='Y' :
                engine = pyttsx3.init()  # Initialize the pyttsx3 engine
                engine.say(remark_v)  # Add the text that you want the engine to say
                engine.runAndWait()  # Process the speech
                sql = ("update daily_shortsell_call set SMS_TRG_FLG_V = 'Y'  where rowid = :row_id")
                cur.execute(sql, [ row_id])
                print("Speech sucessfully processed!")
                
                
            except Exception as e:
                print(str(e))        
           
            
    con.commit()
    con.close()
    print("Successfully one batch is completed ~~~",datetime.now())
  
# Task scheduling
# After every 10mins geeks() is called. 
schedule.every(1).minutes.do(EagleEye_Alert)
  
# After every hour geeks() is called.
#schedule.every().hour.do(geeks)
  
# Every day at 12am or 00:00 time bedtime() is called.
#schedule.every().day.at("00:00").do(bedtime)
  
# After every 5 to 10mins in between run work()
#schedule.every(5).to(10).minutes.do(work)
  
# Every monday good_luck() is called
#schedule.every().monday.do(good_luck)
  
# Every tuesday at 18:00 sudo_placement() is called
#schedule.every().tuesday.at("18:00").do(sudo_placement)
  
# Loop so that the scheduling task
# keeps on running all time.
while True:
  
    # Checks whether a scheduled task 
    # is pending to run or not
    schedule.run_pending()
    time.sleep(1)
