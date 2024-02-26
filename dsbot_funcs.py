#
#
#This module contains DSbot's contract request formation functionality. The functions defined here used in dsbot.py.
#
#All contract requests are submitted either through submit_request() or request_tracked_group(), where the latter is
#used for awaiting on a group of requests, for example in get_stk_spot() and make_opt_requests()
#
#The main ingridients required to prepare an OPT request are defined here: 
#1. get_daily_bars() which returns the past year's STK bars for the symbol, with barsize = '1 D', for strike price calculation
#2. find_next_maturity()
#3. get_stk_spot()
#
#Finally, make_opt_requests() defines and requests the option contracts we are interested in
#
#TO DO: unique_counter should be locked, as it is possible for submit_request and request_tracked_group to be called concurrently
#
from definitions import *
from utils import *
from sql_calls import *

#contract requests are formed in this module only
contract_id_lock = asyncio.Lock()
contract_id = 0

#
#All requests to be submitted to the request submitter are "tracked" by awaiting on an asyncio.Event
#a reference to which is stored in the ContractRequest's .done attribute
#
async def submit_request(contract_request: ContractRequest, priority: int): 
    
    global contract_id
    
    #check everything ok with request
    if contract_request.req_type == CONTRACT_REQUEST:
        if len(contract_request.bars) != 4:
            logger.error('expected bars dict to have exactly 4 keys for CONTRACT_REQUEST, bars: {}'.format(bars))
            return None
    elif contract_request.req_type == CONTRACT_DETAILS_REQUEST:
        if len(contract_request.bars) != 0:
            logger.error('expected bars dict to have exactly 0 keys for CONTRACT_DETAILS_REQUEST, bars: {}'.format(bars))
            return None
    else:
        logger.error('unknown contract request type: {}'.format(contract_request.req_type))
        return None
    
    #assign UNIQUE contract_id
    async with contract_id_lock:
        contract_request.contract_id = contract_id 
        contract_id += 1
        
    #add tracker
    contract_request.done = asyncio.Event()
        
    #finally, try to add to req submitter's q
    #
    #TO DO: block put with timeout, and only return None if either timed out, or queue full
    #
    try:
        #if unique_counter is incremented here only, no need for lock  
        req_submitter_q.put((priority, next(unique_counter), contract_request), block=False)
    except:                        
        logger.exception('')
        return None
        
    return contract_request
    

#
#given contract_requests, a list of ContractRequest instances, and a priority int
#this function submits requests to req_submitter_q and awaits until all requests are
#made by req_submitter and processed by data_writer 
#    
async def request_tracked_group(contract_requests: list[ContractRequest], priority: int):

    global contract_id
    #add tracking no and put task on task queue
    tracking_no = uuid.uuid4()
    #print(tracking_no, type(tracking_no))
    tracking_info[tracking_no] = {}
    tracking_info[tracking_no]['counter'] = len(contract_requests)
    tracking_info[tracking_no]['done'] = asyncio.Event()    
    for contract_request in contract_requests:    
        contract_request.tracking_no = tracking_no           
        #assign UNIQUE contract_id
        async with contract_id_lock:
            contract_request.contract_id = contract_id            
            contract_id += 1   
        #put in req submitter's q
        req_submitter_q.put((priority, next(unique_counter), contract_request), block=False)                
    #wait for all requests of this func to complete
    await tracking_info[tracking_no]['done'].wait()


#Returns, the tuple of (up-to-date bars, latest_date) if these are available, otherwise None
#by first checking in DB, followed by requesting.
#
#Currently the 1D bars for the past year are used to estimate a reasonable range of strike prices for this symbol
#
async def get_daily_bars(con: aiosqlite.Connection, symbol: str):

    async def request_and_wait():
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.currency = 'USD'  

        bars = {
        'end_datetime': datetime.datetime.now().date(),  
        'past_window': '1 Y',
        'what_to_show': 'MIDPOINT',
        'barsize': '1 day'
        }
        
        priority = PRIORITY_REQUEST_DAILY_BARS        
        contract_request = ContractRequest(CONTRACT_REQUEST, contract, bars=bars) 
               
        contract_request = await submit_request(contract_request, priority)        
        if contract_request is None:
            #here because of issue putting request in req submitter q
            logger.error('Symbol: {}. contract request is None.'.format(symbol))
            return None            
    
        await contract_request.done.wait()
        return contract_request
        
    async def update():
        contract_request = await request_and_wait()                
        contract_id = contract_request.contract_id
        bars_daily_pastyear = None
        latest_date = None
        if contract_id in contract_requests_completed:            
            bars_daily_pastyear = copy.deepcopy(contract_requests_completed[contract_id]) 
            del contract_requests_completed[contract_id]
            latest_date_str = str(bars_daily_pastyear['date'].iloc[-1])
            latest_date = datetime.datetime.strptime(latest_date_str, DATE_FMT_DAILY_BARS).date()
            logger.info('Symbol: {}. Downloaded daily bars for past year.'.format(symbol))
        else:
            logger.info('Symbol: {}. Couldnt download daily bars for past year.'.format(symbol))
            return None
        return (bars_daily_pastyear, latest_date)
        
    bars_daily_pastyear, latest_date = await db_get_daily_bars(con, symbol)
    if bars_daily_pastyear is None:
        #dont have, request               
        logger.info('Symbol: {}. No daily bars for past year in DB. Requesting.'.format(symbol))  
        res = await update()
        if res is not None:      
            bars_daily_pastyear, latest_date = res      
            #check that data is not outdated
            date_today = datetime.datetime.now().date()
            bars_outdated_days = (date_today - latest_date).days
            if bars_outdated_days > SP500_DAILY_BARS_EXPIRATION_DAYS:
                logger.error('Symbol: {}. Freshly requested daily bars are outdated.'.format(symbol))
                return None    
            else:
                return (bars_daily_pastyear, latest_date)
    else:
        #check that data is not outdated
        date_today = datetime.datetime.now().date()
        bars_outdated_days = (date_today - latest_date).days
        if bars_outdated_days > SP500_DAILY_BARS_EXPIRATION_DAYS:
            #need to update: request
            logger.info('Symbol: {}. Daily bars for past year outdated in DB. Requesting.'.format(symbol))   
            res = await update()
            if res is not None:
                bars_daily_pastyear, latest_date = res    
                return (bars_daily_pastyear, latest_date)
        else:
            #up-to-date
            logger.info('Symbol: {}. Got daily bars from DB.'.format(symbol))
            return (bars_daily_pastyear, latest_date)
                
    return None
            
        
#Returns, for this symbol:
#        
#1. next maturity date for symbol in [maturity_from, maturity_to], for example, for SPY have maturity days everyday (except weekends and celebrations)
#while for AAPL they are weekly 
#2. existing contracts' strike prices
#3. option trading hours
#        
#This function does not check for maturities on weekends, and the priority order is to check today, tomorrow,
#and then the closest future Friday, followed by the remaining dates in [maturity_from, maturity_to]
#
#TO DO: instead of return None, should continue in some places here? 
#
#TO DO: replace return None at the end?
#
#when searcing for the next maturity date using reqContractDetails, 
#we also get the possible strike prices and trading hours for the symbol
async def find_next_maturity(con: aiosqlite.Connection, symbol: str, maturity_from: datetime.date, maturity_to: datetime.date):
    
    maturity_dates_to_check = [] #[maturity_from + datetime.timedelta(days=i) for i in range((maturity_to-maturity_from).days+1)]    
    if maturity_from == maturity_to:
        maturity_dates_to_check.append(maturity_from)
    else:       
        #we always start checking for maturities today and tomorrow (in case its past opt expiration hour on that day)
        days_future_maturity = [0, 1]
        #next, if maturity_from is not thursday or friday, we look for the closest future friday, as most SP500 symbols' opts seem to expire on fridays
        weekday = maturity_from.weekday()
        if not (weekday == 3 or weekday == 4):
            days_to_next_Friday = (4 - weekday) % 7
            if maturity_from + datetime.timedelta(days_to_next_Friday) <= maturity_to:
                days_future_maturity.append(days_to_next_Friday)
        #then, check all remaining future days until max_future_maturity_days
        max_days_future_maturity = (maturity_to - maturity_from).days
        for fut_days in range(2, max_days_future_maturity + 1):
            if fut_days not in days_future_maturity:
                days_future_maturity.append(fut_days)
        #finally, remove weekends
        maturity_dates_to_check += [date for date in (maturity_from + datetime.timedelta(days=fd) for fd in days_future_maturity) if date.weekday not in [5,6]]
      
    for maturity in maturity_dates_to_check:           
        contract = Contract()
        contract.symbol = symbol
        contract.lastTradeDateOrContractMonth = maturity.strftime(MATURE_DATE_FMT)
        contract.secType = 'OPT'
        contract.exchange = 'SMART'
        contract.currency = 'USD'
        contract.multiplier = DEFAULT_MULTIPLIER_OPTS  

        contract_request = ContractRequest(CONTRACT_DETAILS_REQUEST, contract)                
        contract_request = await submit_request(contract_request, PRIORITY_REQUEST_MATURITY) 
        if contract_request is None:
            #here because of issue putting request in req submitter q
            logger.error('Symbol: {}. contract request is None.'.format(symbol))
            return None            
        
        logger.info('Symbol: {}. Checking if have maturity on {}.'.format(symbol, maturity)) 
        await contract_request.done.wait()     
     
        #
        #TO DO: here should be either completed or no contract error, 
        #should store to remember and not check this date anymore next time
        #
        #TO DO: if REQUEST_STATUS_EXPIRED here, returning None would result in waiting until next day in get_opt_loop in data_saver_bot.py
        #whereas we would like instead to ??????
        #
        contract_id = contract_request.contract_id
        if contract_id not in contract_requests_completed:
            logger.info('Symbol: {}. No maturity on {}.'.format(symbol, maturity)) 
            continue
            
        possible_contracts = contract_requests_completed[contract_id]    

        try:
            possible_strikes = {'CALL': [item.contract.strike for item in possible_contracts if item.contract.right == 'C'],
                                'PUT': [item.contract.strike for item in possible_contracts if item.contract.right == 'P']}
        except:
            logger.exception('len(possible_contracts): {}. possible_contracts[0]: {}.'.format(len(possible_contracts), possible_contracts[0]))
        
        #
        #TO DO: should I return None here, or just continue to check next maturity?
        #
        opt_trading_hours_dict = get_opt_trading_hours(possible_contracts)
        if opt_trading_hours_dict is None:
            logger.warning('Symbol: {}. opt_trading_hours_dict is None, maturity: {}.'.format(symbol, maturity))
            return None

        del contract_requests_completed[contract_id]
        return (maturity, possible_strikes, opt_trading_hours_dict)    
        
    logger.info('Symbol: {}. No more maturity dates found from {} to {}.'.format(symbol, maturity_from, maturity_to ))
    return None      
    
#prep requests for stk contracts
#
#TO DO: first check in memory, then in DB, finally make requests
#
#TO DO: check barsize, stk_info_type are one of allowed ones
#
async def get_stk_spot(con: aiosqlite.Connection, symbol: str, trading_dates: list[datetime.date], barsize: str, what_to_show: str, trading_hours: dict) -> dict[pandas.DataFrame]:
          
    if barsize not in BARSIZES:
        raise ValueError('barsize of {} is not one of the allowed barsizes: {}'.format(barsize, BARSIZES))
        return {}
        
    if what_to_show not in STK_INFO_TYPES:
        raise ValueError('what_to_show of {} is not one of the allowed values: {}'.format(what_to_show, STK_INFO_TYPES))
        return {}
        
    if len(trading_dates) == 0:
        return {}
        
    spot_close_vals = {}           
           
    #check which trading dates' info already have
    spot_close_vals_db = await db_get_stk(con, symbol, trading_dates, trading_hours, what_to_show=what_to_show, barsize=barsize)
    spot_close_vals |= spot_close_vals_db
    
    trading_dates_have_stk = list(spot_close_vals.keys())                
    logger.info('Symbol: {}. At barsize of {}, already have bars for the following dates: {}.'.format(symbol, barsize, trading_dates_have_stk))
    trading_dates_no_stk = copy.deepcopy(trading_dates)
    for date in trading_dates_have_stk:
        if date in trading_dates_no_stk:
            trading_dates_no_stk.remove(date)
        else:
            logger.error('Symbol: {}. trading dates: {}. trading_dates_have_stk: {}'.format(symbol, trading_dates, trading_dates_have_stk))
        
    if len(trading_dates_no_stk) == 0:
        #got everything we needed from DB
        return spot_close_vals
        
    logger.info('Symbol: {}. Remaining dates: {}'.format(symbol, trading_dates_no_stk))
    
    #group remaining trading dates
    trading_date_groups = []
    i = 0
    j = i + 1
    sorted_trading_dates = sorted(trading_dates_no_stk)
    while j < len(sorted_trading_dates):
        if (sorted_trading_dates[j] - sorted_trading_dates[i]).days >= MAX_PAST_WINDOW_DAYS_STK[barsize]:
            trading_date_groups.append(sorted_trading_dates[i:j])
            i = j        
        j += 1
    trading_date_groups.append(sorted_trading_dates[i:j])
    
    contract_requests = []
    for td in trading_date_groups: 
        latest_trading_date = max(td)
        past_window_days = (td[-1] - td[0]).days + 1
        past_window = '{} D'.format(past_window_days)
        end_datetime = latest_trading_date + datetime.timedelta(days=1)  
        
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.currency = 'USD'        

        bars = {
        'end_datetime': end_datetime, 
        'past_window': past_window,
        'what_to_show': what_to_show,
        'barsize': barsize
        }      

        contract_request = ContractRequest(CONTRACT_REQUEST, contract, bars=bars)  
        contract_requests.append(contract_request)
    
    priority = PRIORITY_REQUEST_STK
    logger.info('Symbol: {}. {} STK contract requests sent to request submitter, awaiting.'.format(symbol, len(contract_requests)))
    await request_tracked_group(contract_requests, priority)   
       
    trading_dates_copy = copy.deepcopy(trading_dates_no_stk)
    for i_req, contract_req in enumerate(contract_requests):
        contract_id = contract_req.contract_id
        if contract_id not in contract_requests_completed:
            continue
            
        bars = contract_requests_completed[contract_id]     
        trading_dates_this = trading_date_groups[i_req]
        
        
        for td in trading_dates_this:
            if len(trading_dates_copy) == 0:
                #have all I need     
                break            
            if td in trading_dates_copy:               
                bars_td = bars[bars.apply(lambda bar: datetime.datetime.fromtimestamp(int(bar['date'])).date() == td, axis=1)] 
                bar_inds_opt_trading_hours = [idx for idx, date in enumerate(bars_td['date'].values) if trading_hours_check(datetime.datetime.fromtimestamp(int(date)), trading_hours)]
                close_vals = [bars_td['close'].values[idx] for idx in bar_inds_opt_trading_hours]

                if len(close_vals) > 0:
                    spot_close_vals[td] = close_vals
                    #have bars for this trading date, so can remove it
                    trading_dates_copy.remove(td)
                    
    return spot_close_vals


async def make_opt_requests(con: aiosqlite.Connection, symbol: str, mature_date: datetime.date, barsize: str, DTE_groups: list, strikes_grouped_DTE: list, trading_hours: dict):

    contract_requests = []
        
    if len(DTE_groups) == 0 or len(strikes_grouped_DTE) == 0:
        return []
        
    #this only needed because datetime has as superclass the date class, and isinstance cannot distinguish then 
    #(it will say its datetime.date even if the object is actually datetime.datetime)
    mature_date_str = cal2strdate([mature_date.year, mature_date.month, mature_date.day]).replace('-', '')
    
    for i in range(len(DTE_groups)):
        DTEs = sorted(DTE_groups[i])  
        smallest_DTE = None 
        try: 
            smallest_DTE = DTEs[0]
        except:
            logger.exception('DTE groups: {}'.format(DTE_groups))
            
        latest_opt_trading_date = mature_date - datetime.timedelta(days=smallest_DTE)
        if len(DTEs) == 1:
            past_window_days = 1
        else:            
            past_window_days = DTEs[-1] - DTEs[0] + 1
        past_window = '{} D'.format(past_window_days)
        end_datetime = latest_opt_trading_date + datetime.timedelta(days=1) 

        strikes_this_group = strikes_grouped_DTE[i]
        for info_type in OPT_INFO_TYPES:
            right = info_type[:-4]
            bid_or_ask = info_type[-3:]  
            for strike in strikes_this_group[info_type]: 
                contract = Contract()
                contract.symbol = symbol
                contract.secType = 'OPT'
                contract.exchange = 'SMART'
                contract.currency = 'USD'
                contract.multiplier = DEFAULT_MULTIPLIER_OPTS
                contract.right = right
                contract.lastTradeDateOrContractMonth = mature_date_str 
                contract.strike = strike
                
                bars = {
                'end_datetime': end_datetime, 
                'past_window': past_window,
                'what_to_show': bid_or_ask,
                'barsize': barsize
                }
                
                contract_request = ContractRequest(CONTRACT_REQUEST, contract, bars=bars, opt_trading_hours=trading_hours)  
                contract_requests.append(contract_request)                
                   
    logger.info('Symbol: {}. Number of OPT contracts desired: {}'.format(symbol, len(contract_requests)))    
    #check which already have in DB    
    idx_have = await db_check_saved(con, contract_requests)
    tmp = [cr for i, cr in enumerate(contract_requests) if i not in idx_have]
    contract_requests = tmp
    #submit requests and await
    logger.info('Symbol: {}. Number of OPT contracts to be requested: {} (after checking for those already have in DB)'.format(symbol, len(contract_requests)))
    priority = PRIORITY_REQUEST_OPT  
    await request_tracked_group(contract_requests, priority)
    

#TO DO: add check for celebrations
async def get_possible_DTE(date_today: datetime.date = datetime.datetime.now().date(), mature_date: datetime.date = datetime.datetime.now().date(), max_DTE: int = 45) -> list[int]:
    if mature_date < date_today:
        raise ValueError('mature date of {} is before todays date, {}. It is not possible to get expired options contracts on IB'.format(mature_date, date_today))
    min_DTE = (mature_date - date_today).days + 1
    possible_DTE = [DTE for DTE in range(min_DTE, max_DTE+1) if not is_weekend(mature_date - datetime.timedelta(days=DTE))]
    return possible_DTE
    
async def get_admissible_DTE(DTE_all: list[int], possible_DTE: list[int], strict_DTE: bool = True):
    DTEs_cur_symbol = []
    for DTE in DTE_all:
        if DTE in possible_DTE:
            DTEs_cur_symbol.append(DTE)
        elif (DTE not in possible_DTE) and not strict_DTE:
            #in this case, for each DTE in DTE_all, find nearest in possible_DTE to keep same number (len(DTE_all)) if possible
            closest_possible_DTE = possible_DTE[np.argmin([abs(dte - DTE) for dte in possible_DTE])]
            possible_DTE_copy = possible_DTE.copy()
            while (closest_possible_DTE in DTEs_cur_symbol) and len(possible_DTE_copy) > 0:
                possible_DTE_copy.remove(closest_possible_DTE)
                closest_possible_DTE = possible_DTE_copy[np.argmin([math.abs(dte - DTE) for dte in possible_DTE_copy if math.abs(dte - DTE) <= MAX_NEXT_CLOSEST_DTE])]
            if len(possible_DTE_copy) > 0:
                DTEs_cur_symbol.append(closest_possible_DTE)
    return DTEs_cur_symbol
        
    
#TO DO:
#check max_window_days is int
#check max_window_days > 0
#check max_window_days < sorted(DTEs)[-1] - sorted(DTEs)[0]
async def prep_DTE_groups(DTEs: list[int], strikes: dict[int, dict[str, list[float]]], max_window_days: int = 1): 

    if max_window_days < 1:
        raise ValueError('max_window_days must be > 0') 
        
    if len(strikes) == 0:
        return ([], [])
    
    DTE_groups = []
    strikes_grouped_DTE = [] 
    
    u, u_counts = np.unique(DTEs, return_counts=True)
    if (u_counts > 1).any():
        logger.warning('did not expect duplicate DTEs here, taking unique to proceed.')

    DTEs = u.tolist()
    #group DTE 
    i = 0
    j = i + 1
    while j < len(DTEs):
        if DTEs[j] - DTEs[i] >= max_window_days:
            DTE_groups.append(DTEs[i:j])
            i = j        
        j += 1
    DTE_groups.append(DTEs[i:j])

    #get unique strikes for each group, for each info type
    for DTEs_this in DTE_groups:
        this_strike_dict = {info_type: [] for info_type in OPT_INFO_TYPES}
        for info_type in OPT_INFO_TYPES:
            for DTE in DTEs_this:
                this_strike_dict[info_type] = this_strike_dict[info_type] + strikes[DTE][info_type]
            #TO DO: probably easier to define a numpy serializer at some point
            this_strike_dict[info_type] = np.unique(this_strike_dict[info_type]).tolist() 
        strikes_grouped_DTE.append(this_strike_dict)
        
    return (DTE_groups, strikes_grouped_DTE)    


