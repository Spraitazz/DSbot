#
#
#
#This module contains high-level functionality of the DSbot. It is not called explicitly, but rather by starting DSbot
#using run.py. A database connection is made and used persistently for DSbot's work, 
#as writing to the database, in the current implementation, carries no risk of multiple concurrent access.
#
#As the DSbot's current implementation is a transition from a sequential (over the different symbols desired)
#to a concurrent program, not all the functionality has yet been made async, which is why the event loop
#needs to be accessible from the threads introduced below.
#
#This module starts, from run_async.py, in 4 separate threads:
#one for client.run (internal function in ibapi's Client class),
#one for the error processor, which deals with the different errors that IB might return, as well as connection problems,
#one for the request submitter, which awaits for requests to submit, submitting them in line with IB limitations on throughput,
#one for the data writer, which waits for returned requests and writes data to the database
#
#Two types of task are currently defined: 
#
#1. get_opt: for a given symbol and barsize, download options contracts for the next maturity date in the range [from_maturity, to_maturity]
#2. get_opt_loop: continuously get_opt for a given symbol and barsize, looking for maturities up to a future date as defined in the configuration. 
#
#
#
from definitions import *
from utils import *
from daily_break import break_loop
import error_processor
import data_writer
import request_submitter
import dsbot_funcs

task_q = asyncio.Queue()
tasks = set()

#
#this is the entry point to the dsbot "daemon"
#
def run(msg_to_q: Queue, msg_from_q: Queue):    
    msg_to_q.put({'t': time.time(), 'from': DSBOT, 'type': TYPE_MESSAGE, 'text': 'started'})    
    tokens['running'] = True  
    asyncio.run(run_async(msg_to_q, msg_from_q)) #, debug=True)

#
#this is started by run() above, in the current implementation threading is used
#with asyncio, so when starting threads that will have to notify (when request is done)
#by setting an asyncio.Event, for example, we need to pass them the event loop
#    
async def run_async(msg_to_q: Queue, msg_from_q: Queue):   

    #make sure to go to break on time
    threading.Thread(target=break_loop, args=(msg_to_q,), daemon=True, name='break_checker_thread').start()
      
    wrapper = DataSaverWrapper()
    client = Client(wrapper)      
      
    #open database 
    con = None
    try:
        con = await aiosqlite.connect(CONTRACTS_DB_PATH, check_same_thread=False)
        con.row_factory = aiosqlite.Row
    except:
        logger.exception('')     
    
    #need to pass this to spawned threads (request submitter, data_writer) so they can notify when request data is ready
    main_event_loop = asyncio.get_running_loop() 
    
    #start error processor thread, it takes from client._wrapper.errors, acts on them, then puts in errors_processed list    
    error_processor_thread = threading.Thread(target=error_processor.run, args=(client,), name='error_processor_thread', daemon=True)                                              
    error_processor_thread.start()            
    
    #the main requirement is a connection to IBprogram (IB TWS/IB Gateway), so first try connect to IBprogram     
    #connect, start client.run() thread
    client.connect(CLIENT_IP, CLIENT_PORT, DATA_SAVER_CLIENT_ID)   
    #client.run() is a loop that checks for messages at client, then sleeps for 200 ms
    def client_run(client):
        client.run()                
    api_thread = threading.Thread(target=client_run, args=(client,), daemon=True, name='api_thread')
    api_thread.start() 
    time.sleep(0.1)
    logger.info('attempted client connection, connected: {}'.format(client.isConnected()))        
                       
    #start the data writer, it waits for completed requests and processes them (puts into DB), until the token is not set to False again                
    data_writer_thread = threading.Thread(target=data_writer.run, args=(client, con), kwargs={'main_event_loop': main_event_loop}, name='data_writer_thread', daemon=True)
    data_writer_thread.start()        

    #start req submitter
    req_submitter_thread = threading.Thread(target=request_submitter.run, args=(client,), kwargs={'main_event_loop': main_event_loop}, name='request_submitter_thread', daemon=True)
    req_submitter_thread.start()    
    
    #set market data type for collecting data
    client.reqMarketDataType(MARKET_DATA_TYPE)      
    
    #by default, we seek to constantly get available OPT maturity contracts from today up to today + MAX_FUTURE_MATURITY_DAYS
    for symbol in SYMBOLS[:20]:        
        task_info = {
        'f': lambda con=con, symbol=symbol, barsize=CONFIG_BARSIZE: get_opt_loop(con, symbol, barsize),
        'desc': 'task: get_opt_loop(symbol={}, barsize={})'.format(symbol, CONFIG_BARSIZE)
        }
        task_q.put_nowait(task_info)
    
    async with asyncio.TaskGroup() as tg:
        task_do_tasks = tg.create_task(do_tasks())
        task_maturity = tg.create_task(wait_for_messages(msg_from_q))

#
#Using a task queue allows for new tasks to be added at runtime. E.g. a one-off request, get_opt(), for a symbol not in SP500
#for example through the console
#
#Currently we have 2 types of "tasks":
#
#1. get_opt()
#2. get_opt_loop()
#
async def do_tasks():
    while True:
        #BSbot can be "stopped" through console (from run.py) by modifying running_flag
        if tokens['running']: 
            while not task_q.empty():
                task_info = await task_q.get()                    
                task = asyncio.create_task(task_info['f']())
                tasks.add(task)
                task.add_done_callback(tasks.discard)
                logger.info('started task: {}'.format(task_info['desc']))                    
        await asyncio.sleep(0.2)
      

#
#NOT IMPLEMENTED: this function will allow for adding tasks to task_q through the console
#      
async def wait_for_messages(msg_from_q: Queue):
    #check for messages from DSbot
    while True:
        try:
            msg_from = msg_from_q.get(block=False)
            print('add task message to dsbot: {}'.format(msg_from))
        except Empty:
            pass
        except Exception:
            raise
        await asyncio.sleep(0.2)
          

#
#
#??? #if no maturity, then the TASK IS DONE for this symbol (new task will be added on day change automatically)
#
#
#This function:
#
#1. Makes requests to find the next maturity date possible (from maturity_from to maturity_to) for symbol OPTs. 
#using dsbot.find_next_maturity():
#2. dsbot.get_daily_bars(): check for up-to-date SP500 symbols' daily bars (1st in memory, next in DB, finally actually request). Used for strike price selection
#
##2. get_stk_spot(): ????check for spot (MIDPOINT? close?) values for that date/for all DTE???. If don't have this, no point getting the OPT data???
#
#and finally, using this information, submits OPT contracts to request_submitter, saving them to DB upon return (in data_writer)
#
#Returns None if couldnt get OPTS (due to some reason??), otherwise return the maturity date for which OPTS requests have been done for this symbol
#
#TO DO: in current implementation this would return None even when requesting after expiration (REQUEST_STATUS_EXPIRED),
#in which case should improve on return status (REQUEST_STATUS_EXPIRED) to indicate that we shouldn't wait for next day to re-try this symbol
#
#TO DO: can still get OPT for symbols without the daily bars, but there's too few like that to care atm
#
#TO DO: check that barsize and DTEs are permitted given current program limitations/hardcoding
#
async def get_opt(con: aiosqlite.Connection, symbol: str, barsize: str, maturity_from: datetime.date, maturity_to: datetime.date):      
    
    if maturity_from > maturity_to:
        logger.info('Symbol: {}. maturity_from (= {}) > maturity_to (= {})'.format(symbol, maturity_from, maturity_to))
        return None
    next_maturity_res = None
    daily_bars, latest_date = None, None
    if symbol == 'SPY':
        #in current implementation don't use past year's daily bars for SPY for strike price calculation (instead using its whole history for whatever reason)        
        next_maturity_res = await dsbot_funcs.find_next_maturity(con, symbol, maturity_from, maturity_to)   
    else:
        #can do these concurrently, although not really changing anything in this case as request submitter is sequential?
        async with asyncio.TaskGroup() as tg:
            task_daily_bars = tg.create_task(dsbot_funcs.get_daily_bars(con, symbol))
            task_maturity = tg.create_task(dsbot_funcs.find_next_maturity(con, symbol, maturity_from, maturity_to))
    
        daily_bars_res = task_daily_bars.result() 
        next_maturity_res = task_maturity.result()
        #if we don't have daily bars, there is no need to check for maturities,
        #as in the current implementation we won't be able to determine strike prices for OPTS
        if daily_bars_res is None:
            return None
        daily_bars, latest_date = daily_bars_res
    
    if next_maturity_res is None:     
        #cannot proceed   
        return None       
         
    mature_date, possible_strikes, trading_hours = next_maturity_res 
    logger.info('Symbol: {}. Found maturity on {}'.format(symbol, mature_date))         
    
    DTE_all = None
    if symbol == 'SPY':
        DTE_all = np.arange(1,CONFIG_DTE_MAX_SPY+1).tolist()
    else: 
        DTE_all = np.arange(1,CONFIG_DTE_MAX_SP500+1).tolist() 
    
    #check which DTEs are to be requested, and therefore for which trading dates we need STK
    date_today = datetime.datetime.now().date()
    possible_DTE = get_possible_DTE(date_today=date_today, mature_date=mature_date, max_DTE=max(DTE_all))                      
    DTEs_cur_symbol = get_admissible_DTE(DTE_all, possible_DTE, strict_DTE=CONFIG_STRICT_DTE)
        
    if CONFIG_STRICT_DTE:
        logger.info('Symbol: {}. Of desired DTE, {}, the following are left after removing weekends: {}'.format(symbol, DTE_all, DTEs_cur_symbol))
    else:  
        info_str = '(CONFIG_STRICT_DTE = {}) Symbol: {}. Of desired DTE: {}, the following will be requested given the option maturity date'.format(CONFIG_STRICT_DTE, symbol, DTE_all) + \
                    ' and assuming the maximum DTE can be max(DTE_all) (={}): {}'.format(max(DTE_all), DTEs_cur_symbol)                
        logger.info(info_str)        
        
    trading_dates = [mature_date - datetime.timedelta(days=DTE) for DTE in DTEs_cur_symbol] 
    logger.info('Symbol: {}. The trading dates corresponding to the DTE are: {}'.format(symbol, trading_dates))   
       
    #next, need to get STK (historical data type = STK_DATA_TYPE_OPT) for the above determined trading dates    
    spot_close_vals = await dsbot_funcs.get_stk_spot(con, symbol, trading_dates, barsize, STK_DATA_TYPE_OPT, trading_hours)   
    DTE_with_spots = [DTE for DTE in DTEs_cur_symbol if mature_date - datetime.timedelta(days=DTE) in spot_close_vals.keys()]
    logger.info('Symbol: {}. Have STK (close) values for the following DTE: {}'.format(symbol, DTE_with_spots))
    
    #check what strikes are to be requested. Max frac changes for SPY hardcoded, taken from 100 years data.
    #For other symbols, using past year's, 1 day bars, frac changes
    strikes = None                        
    if symbol == 'SPY':
        strikes = {DTE: prep_strikes(spot_close_vals[mature_date - datetime.timedelta(days=DTE)], MAX_FRAC_DEVIATION_STRIKE_SPX[DTE]) for DTE in DTE_with_spots}
    else:
        dates_1year = daily_bars['date'].astype(str).tolist()
        close_vals_1year = daily_bars['close'].values.tolist()    
        frac_changes = get_frac_changes_spot(close_vals_1year, dates_1year, DTE_with_spots, date_fmt=DATE_FMT_DAILY_BARS)
        strikes = {}
        DTE_to_del = []
        for DTE in DTE_with_spots:
        	f_bounds = get_f_asymrange_percentile(CONFIG_PERCENTILE_MAX_FRAC_DEV, frac_changes[DTE])
        	if f_bounds is None:
        		DTE_to_del.append(DTE)
        		logger.warning('Symbol: {}. Had no positive spot price change over DTE of {}'.format(symbol, DTE))
        	strikes[DTE] = prep_strikes_asymfrac(spot_close_vals[mature_date - datetime.timedelta(days=DTE)], lower_f=f_bounds['l'], upper_f=f_bounds['u']) 
        for DTE in DTE_to_del:
        	DTE_with_spots.remove(DTE)
                                                
    #check if any of the strikes are not amongst possible_strikes returned by dsbot_funcs.find_next_maturity() above
    strikes = check_strikes(strikes, possible_strikes)    
    
    #finally, make OPT requests and await them     
    DTE_groups, strikes_grouped_DTE = await dsbot_funcs.prep_DTE_groups(DTE_with_spots, strikes, max_window_days=MAX_PAST_WINDOW_DAYS_OPT[barsize])          
    logger.info('Symbol: {}. The DTE groups, at past_window of {}, are: {}'.format(symbol, MAX_PAST_WINDOW_DAYS_OPT[barsize], DTE_groups))          
    await dsbot_funcs.make_opt_requests(con, symbol, mature_date, barsize, DTE_groups, strikes_grouped_DTE, trading_hours) 
    
    return mature_date

#
#TO DO: check for latest maturity date in DB (need separate table for completed tasks "get OPTS symbol = , barsize = , mature date =, DONE")
#
async def get_opt_loop(con: aiosqlite.Connection, symbol: str, barsize: str, maturity_date_have_opt_latest: datetime.date = datetime.datetime.now().date() - datetime.timedelta(days=1)):
   
    maturity_from = maturity_date_have_opt_latest + datetime.timedelta(days=1)
    date_today = datetime.datetime.now().date()
    maturity_to = date_today + datetime.timedelta(days=MAX_FUTURE_MATURITY_DAYS)
    
    res = await get_opt(con, symbol, barsize, maturity_from, maturity_to)
    
    latest_mature_date_now = maturity_date_have_opt_latest
    if res is None:
        logger.info('Symbol: {}. Could not get OPT. Will re-try next day'.format(symbol))
        now = datetime.datetime.now()  
        #wait until day change
        target_time = now + datetime.timedelta(days=1) #datetime.datetime(now.year, now.month, now.day, BREAK_START_H, BREAK_START_MIN + BREAK_LEN_MIN)  
        target_time = target_time.replace(hour=0, minute=0, second=0)        
        sleep_s = (target_time - now).total_seconds()  
        await asyncio.sleep(sleep_s)
    else:
        #if im here, means I have done OPT requests for mature_date
        mature_date = res
        logger.info('Symbol: {}. OPT requests done for maturity date: {}. Will check for next possible maturity.'.format(symbol, mature_date))  
        latest_mature_date_now = mature_date   
        
    await get_opt_loop(con, symbol, barsize, maturity_date_have_opt_latest=latest_mature_date_now)




                
     
  


