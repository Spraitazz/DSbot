#
#
#
#This module is the entry point to the data saver bot (DSbot). In the current implementation, it is not a proper Unix daemon process,
#however, to allow for testing of functionality related to communication with the DSbot, waiting on console input is
#delegated to a separate thread, in which console input is appended to a "buffer".
#
#This module starts, in a separate thread, run() from the dsbot.py module, and waits for console commands. All output is
#to a file, using the logging module.
#
#
#
#TO DO: make into a proper Unix daemon using existing libraries
#
#
#
from logging.handlers import TimedRotatingFileHandler

from definitions import *
import dsbot

logger.setLevel(logging.DEBUG)
log_path = '{}/log'.format(LOGS_DIR)
handler = TimedRotatingFileHandler(log_path, when='midnight', backupCount=100)
handler.suffix = '%Y%m%d'
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - [%(filename)30s:%(lineno)4s - %(funcName)30s()] - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

#check that there is a contracts database (the correct definition is in create_contracts_table.py)
if not os.path.isfile(CONTRACTS_DB_PATH):
    msg = 'cannot find contracts database (sqlite3) file at CONTRACTS_DB_PATH = {}. '.format(CONTRACTS_DB_PATH) + \
    'Please run "python utils_sqlite.py" to create it, or specify correct path in the CONTRACTS_DB_PATH variable in definitions.py'
    logger.critical(msg)
    raise Exception(msg)
    
if not os.path.isdir(BARS_DIR_RELPATH):
    msg = 'please create directory at BARS_DIR_RELPATH = {}'.format(BARS_DIR_RELPATH)
    logger.critical(msg)
    raise Exception(msg)


console_buf = []
msg_from_q = Queue()
msg_to_q = Queue()

#first, make possible non-blocking input from console
def console_input(buf: list):
    while True:
        buf.append(input('\nsend me a message:\n'))
        time.sleep(0.21)
threading.Thread(target=console_input, args=(console_buf,), daemon=True).start() 


#start DSbot
main_thread = threading.Thread(target=dsbot.run, args=(msg_from_q, msg_to_q), daemon=True, name='DSbot_main_thread')
main_thread.start()
status = 'started'

#finally, do forever:
while True:   
    #check for input from console
    while console_buf:
        inp = console_buf.pop(0)
        
        if inp == 'status':
            print(status,'\n')  
            
        elif inp[:len('add task')] == 'add task':
            try:
                msg_to_q.put(inp[len('add task'):], block=True, timeout=0.2) 
            except:
                raise 
                
        elif inp == 'stop':            
            print('stopping (currently does nothing).')     
             
        elif inp == 'kill':
            print('killing')
            exit(0)           
                   
        else:
            print('"{}" is not a valid command'.format(inp))


    #check for messages from DSbot
    try:
        msg_from = msg_from_q.get(block=False)
        print(msg_from)
    except:
        pass

    time.sleep(0.2)


